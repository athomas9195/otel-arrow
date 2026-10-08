// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Single connection owner with read-only portal pages and observed cleanup.

use super::{
    adapter::{Action, Command, Operation, Reply},
    adapter::{Error, Result, database},
    config::Validated,
    query::{Plan, Signature},
    transport::{Guard, GuardState},
    value as convert,
};
use futures::{StreamExt, future::poll_fn};
use otel_arrow_dfe_engine::capability::auth::BasicAuthCredential;
use otel_arrow_dfe_scraper::database::{ColumnMetadata, CompositeCursor, QueryPage};
use secrecy::ExposeSecret;
use std::{
    cell::RefCell,
    net::SocketAddr,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_postgres::{
    CancelToken, Client, IsolationLevel, NoTls, Statement, config::SslMode, types::ToSql,
};

type CancelSlot = Rc<RefCell<Option<(CancelToken, SocketAddr)>>>;
struct Session {
    client: Option<Client>,
    driver: JoinHandle<Result<()>>,
    statement: Option<Statement>,
}
struct Worker {
    validated: Validated,
    session: Option<Session>,
    signature: Option<Signature>,
    guard: Arc<GuardState>,
    cancel: CancelSlot,
    poisoned: bool,
    principal: Option<secrecy::SecretString>,
}

pub(crate) async fn run(
    validated: Validated,
    mut commands: mpsc::Receiver<Command>,
    busy: Arc<AtomicBool>,
) -> Result<()> {
    let mut worker = Worker {
        validated,
        session: None,
        signature: None,
        guard: Arc::new(GuardState::default()),
        cancel: Rc::new(RefCell::new(None)),
        poisoned: false,
        principal: None,
    };
    while let Some(command) = commands.recv().await {
        if matches!(command.action, Action::Shutdown) {
            return worker.close().await.and(if worker.poisoned {
                Err(Error::Cleanup)
            } else {
                Ok(())
            });
        }
        worker.guard.active.store(true, Ordering::Release);
        worker.guard.notices.store(0, Ordering::Release);
        let cancel = worker.cancel.clone();
        let operation = command.operation.clone();
        let mut result = {
            let credential = command.credential.ok_or(Error::Credential)?;
            let future = worker.perform(command.action, &operation, &credential);
            tokio::pin!(future);
            tokio::select! {
                biased;
                result = &mut future => result,
                _ = operation.notify.notified() => {
                    operation.cancel();
                    cancel_native(&cancel).await;
                    match tokio::time::timeout(Duration::from_secs(2), &mut future).await {
                        Ok(_) => Err(Error::Cancelled),
                        Err(_) => Err(Error::Cleanup),
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(30)) => {
                    operation.cancel();
                    cancel_native(&cancel).await;
                    match tokio::time::timeout(Duration::from_secs(2), &mut future).await {
                        Ok(_) => Err(Error::Cancelled),
                        Err(_) => Err(Error::Cleanup),
                    }
                }
            }
        };
        if result.as_ref().err() == Some(&Error::Cleanup) {
            worker.poisoned = true;
        }
        if worker.guard.limit.load(Ordering::Acquire) && !worker.poisoned {
            result = Err(Error::Limit);
        }
        if result.is_err() {
            operation.cancel();
            if result.as_ref().err() == Some(&Error::Cleanup) {
                worker.poisoned = true;
            }
            if worker.close().await.is_err() {
                worker.poisoned = true;
                result = Err(Error::Cleanup);
            }
        }
        worker.guard.active.store(false, Ordering::Release);
        busy.store(false, Ordering::Release);
        let _ = command.reply.send(result);
    }
    worker.close().await?;
    if worker.poisoned {
        Err(Error::Cleanup)
    } else {
        Ok(())
    }
}

async fn cancel_native(slot: &CancelSlot) {
    let token = slot.borrow().clone();
    if let Some((token, address)) = token {
        // This is only a request. The operation and driver are still awaited.
        let _ = tokio::time::timeout(Duration::from_secs(1), async {
            let stream = tokio::net::TcpStream::connect(address).await?;
            token
                .cancel_query_raw(stream, NoTls)
                .await
                .map_err(std::io::Error::other)
        })
        .await;
    }
}

impl Worker {
    async fn close(&mut self) -> Result<()> {
        let _ = self.cancel.borrow_mut().take();
        if let Some(session) = self.session.as_mut() {
            let _ = session.statement.take();
            let _ = session.client.take();
            match tokio::time::timeout(Duration::from_secs(1), &mut session.driver).await {
                Ok(Ok(_terminal)) => {}
                _ => {
                    // Aborting is not proof that the server operation stopped.
                    session.driver.abort();
                    let _ = (&mut session.driver).await;
                    let _ = self.session.take();
                    self.poisoned = true;
                    return Err(Error::Cleanup);
                }
            }
            let _ = self.session.take();
        }
        Ok(())
    }

    async fn connect(&mut self, op: &Operation, credential: &BasicAuthCredential) -> Result<()> {
        if self.poisoned {
            return Err(Error::Cleanup);
        }
        op.check()?;
        self.guard = Arc::new(GuardState::default());
        self.guard.active.store(true, Ordering::Release);
        let config = &self.validated.config.connection;
        if self
            .principal
            .as_ref()
            .is_some_and(|principal| principal.expose_secret() != credential.expose_username())
        {
            return Err(Error::Credential);
        }
        op.check()?;
        let mut pg = tokio_postgres::Config::new();
        let _ = pg
            .host(&config.host)
            .port(config.port)
            .dbname(&config.database)
            .user(credential.expose_username())
            .password(credential.expose_password())
            .ssl_mode(SslMode::Disable)
            .connect_timeout(Duration::from_secs(10))
            .options(
                "-c search_path=pg_catalog -c TimeZone=UTC -c default_transaction_read_only=on \
                -c statement_timeout=30000 -c idle_in_transaction_session_timeout=30000 \
                -c application_name=otel_arrow_postgresql_receiver",
            );
        let guard = self.guard.clone();
        let connected = tokio::time::timeout(Duration::from_secs(10), async {
            let stream = tokio::net::TcpStream::connect((config.host.as_str(), config.port))
                .await
                .map_err(|_| Error::Unavailable)?;
            let address = stream.peer_addr().map_err(|_| Error::Unavailable)?;
            let (client, connection) = pg
                .connect_raw(Guard::new(stream, guard), NoTls)
                .await
                .map_err(database)?;
            Ok::<_, Error>((client, connection, address))
        })
        .await;
        drop(pg);
        let (client, mut connection, address) = connected.map_err(|_| Error::Unavailable)??;
        let driver = tokio::task::spawn_local(async move {
            loop {
                match poll_fn(|cx| connection.poll_message(cx)).await {
                    None => return Ok(()),
                    Some(Err(e)) => return Err(database(e)),
                    Some(Ok(tokio_postgres::AsyncMessage::Notice(_))) => {}
                    Some(Ok(_)) => return Err(Error::Limit),
                }
            }
        });
        *self.cancel.borrow_mut() = Some((client.cancel_token(), address));
        if self.principal.is_none() {
            self.principal = Some(credential.expose_username().to_owned().into());
        }
        self.session = Some(Session {
            client: Some(client),
            driver,
            statement: None,
        });
        op.check()
    }

    async fn perform(
        &mut self,
        action: Action,
        op: &Operation,
        credential: &BasicAuthCredential,
    ) -> Result<Reply> {
        op.check()?;
        if self.poisoned {
            return Err(Error::Cleanup);
        }
        if matches!(action, Action::Reconnect) {
            self.close().await?;
            self.connect(op, credential).await?;
            return Ok(Reply::Done);
        }
        if self.session.is_none() {
            self.connect(op, credential).await?;
        }
        let session = self.session.as_mut().ok_or(Error::Cleanup)?;
        let plan = &self.validated.plan;
        let tx = session
            .client
            .as_mut()
            .ok_or(Error::Cleanup)?
            .build_transaction()
            .read_only(true)
            .isolation_level(IsolationLevel::ReadCommitted)
            .start()
            .await
            .map_err(database)?;
        let result = async {
            op.check()?;
            let signature = plan.catalog(&tx, op).await?;
            op.check()?;
            if self.signature.as_ref().is_some_and(|old| old != &signature) {
                return Err(Error::Metadata);
            }
            if session.statement.is_none() {
                let timestamp_type =
                    convert::native_type(&plan.expected[plan.timestamp].source_type)?;
                let tie_type = convert::native_type(&plan.expected[plan.tie].source_type)?;
                let statement = tx
                    .prepare_typed(&plan.sql, &[timestamp_type.clone(), tie_type.clone()])
                    .await
                    .map_err(database)?;
                op.check()?;
                if statement.params() != [timestamp_type, tie_type]
                    || statement.columns().len() != plan.expected.len()
                {
                    return Err(Error::Metadata);
                }
                for ((column, reference), expected) in statement
                    .columns()
                    .iter()
                    .zip(&plan.projections)
                    .zip(&plan.expected)
                {
                    let source = signature.columns.get(reference).ok_or(Error::Metadata)?;
                    if column.name() != expected.name
                        || column.table_oid() != Some(source.oid)
                        || column.column_id() != Some(source.attribute)
                        || column.type_().oid() != source.type_oid
                        || column.type_modifier() != source.modifier
                    {
                        return Err(Error::Metadata);
                    }
                }
                session.statement = Some(statement);
            }
            self.signature = Some(signature);
            let columns = metadata(plan);
            let Action::Execute(cursor) = action else {
                return Ok(Reply::Columns(columns));
            };
            let timestamp = convert::cursor_time(
                &cursor.timestamp,
                plan.expected[plan.timestamp].type_modifier,
            )?;
            let tie_type = &plan.expected[plan.tie].source_type;
            convert::check_tie(cursor.tie_breaker, tie_type)?;
            let timestamp: Box<dyn ToSql + Sync> =
                if plan.expected[plan.timestamp].source_type == "timestamp" {
                    Box::new(timestamp.naive_utc())
                } else {
                    Box::new(timestamp)
                };
            let tie: Box<dyn ToSql + Sync> = match tie_type.as_str() {
                "int2" => Box::new(i16::try_from(cursor.tie_breaker).map_err(|_| Error::Value)?),
                "int4" => Box::new(i32::try_from(cursor.tie_breaker).map_err(|_| Error::Value)?),
                "int8" => Box::new(cursor.tie_breaker),
                _ => return Err(Error::Metadata),
            };
            op.check()?;
            let statement = session.statement.as_ref().ok_or(Error::Metadata)?;
            let portal = tx
                .bind(statement, &[timestamp.as_ref(), tie.as_ref()])
                .await
                .map_err(database)?;
            op.check()?;
            let mut page = QueryPage {
                columns,
                rows: Vec::with_capacity(1000),
            };
            let mut used = page.rows.capacity() as u64
                * size_of::<otel_arrow_dfe_scraper::database::CursorRow>() as u64
                + page
                    .columns
                    .iter()
                    .map(|c| {
                        (size_of::<ColumnMetadata>() + c.name.capacity() + c.source_type.capacity())
                            as u64
                    })
                    .sum::<u64>();
            let mut previous = cursor;
            'page: while page.rows.len() < 1000 {
                op.check()?;
                let target = (1000 - page.rows.len()).min(300);
                let stream = tx
                    .query_portal_raw(&portal, target as i32)
                    .await
                    .map_err(database)?;
                op.check()?;
                tokio::pin!(stream);
                let mut fetched = 0;
                while let Some(native) = stream.next().await {
                    op.check()?;
                    let native = native.map_err(database)?;
                    let row = convert::row(
                        &native,
                        plan,
                        self.validated.common.output().timestamp_column.as_deref(),
                    )?;
                    op.check()?;
                    let next_cursor = row.cursor.as_composite().ok_or(Error::Value)?;
                    ensure_advance(&previous, next_cursor, plan)?;
                    let size = row.row.normalized_size()
                        + next_cursor.timestamp.capacity() as u64
                        + size_of::<CompositeCursor>() as u64;
                    if used.saturating_add(size) > self.validated.common.max_normalized_bytes() {
                        if page.rows.is_empty() {
                            return Err(Error::Limit);
                        }
                        break 'page;
                    }
                    used += size;
                    previous = next_cursor.clone();
                    page.rows.push(row);
                    fetched += 1;
                }
                op.check()?;
                if fetched < target {
                    break;
                }
            }
            Ok(Reply::Page(page))
        }
        .await;
        // Rollback also closes discarded portal rows. Never hold a snapshot over ACK.
        let rollback = tx.rollback().await.map_err(database);
        rollback?;
        op.check()?;
        result
    }
}

fn metadata(plan: &Plan) -> Vec<ColumnMetadata> {
    plan.expected
        .iter()
        .map(|c| ColumnMetadata {
            name: c.name.clone(),
            source_type: c.source_type.clone(),
            nullable: c.nullable,
        })
        .collect()
}

fn ensure_advance(previous: &CompositeCursor, next: &CompositeCursor, plan: &Plan) -> Result<()> {
    let modifier = plan.expected[plan.timestamp].type_modifier;
    if (
        convert::cursor_time(&previous.timestamp, modifier)?,
        previous.tie_breaker,
    ) >= (
        convert::cursor_time(&next.timestamp, modifier)?,
        next.tie_breaker,
    ) {
        return Err(Error::Value);
    }
    Ok(())
}
