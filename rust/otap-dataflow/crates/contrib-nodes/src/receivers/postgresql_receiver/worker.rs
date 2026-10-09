// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Single connection owner with read-only portal pages and observed cleanup.

use super::{
    adapter::{Action, Command, Error, Operation, Reply, Result, database},
    config::{OPERATION_TIMEOUT, Validated},
    framing::{BoundedBackend, READ_BUFFER_BYTES, ReceiveFailure},
    query::{CatalogStatements, MAX_PAGE_ROWS, Plan, Signature},
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

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);
const CANCEL_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);
const CANCEL_COMPLETION_TIMEOUT: Duration = Duration::from_secs(2);
const CONNECTION_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

struct Session {
    client: Option<Client>,
    driver: JoinHandle<Result<()>>,
    statement: Option<Statement>,
    catalog: Option<CatalogStatements>,
    receive_failure: ReceiveFailure,
}
struct Worker {
    validated: Validated,
    session: Option<Session>,
    signature: Option<Signature>,
    cancel: CancelSlot,
    poisoned: bool,
    principal: Option<secrecy::SecretString>,
    fetch_size: FetchSize,
    timestamp_cache: convert::CursorTimestampCache,
}

#[derive(Default)]
struct FetchSize {
    largest_row_bytes: Option<u64>,
}

impl FetchSize {
    fn target(&self, row_limit: usize, bytes_left: u64) -> usize {
        self.largest_row_bytes
            .map_or(1, |size| bytes_left / size)
            .min(row_limit as u64) as usize
    }

    fn observe(&mut self, row_bytes: u64) {
        self.largest_row_bytes = Some(self.largest_row_bytes.unwrap_or(1).max(row_bytes));
    }
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
        cancel: Rc::new(RefCell::new(None)),
        poisoned: false,
        principal: None,
        fetch_size: FetchSize::default(),
        timestamp_cache: convert::CursorTimestampCache::default(),
    };
    while let Some(command) = commands.recv().await {
        if matches!(command.action, Action::Shutdown) {
            return worker.close().await.and(if worker.poisoned {
                Err(Error::Cleanup)
            } else {
                Ok(())
            });
        }
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
                    match tokio::time::timeout(CANCEL_COMPLETION_TIMEOUT, &mut future).await {
                        Ok(_) => Err(Error::Cancelled),
                        Err(_) => Err(Error::Cleanup),
                    }
                }
                _ = tokio::time::sleep(OPERATION_TIMEOUT) => {
                    operation.cancel();
                    cancel_native(&cancel).await;
                    match tokio::time::timeout(CANCEL_COMPLETION_TIMEOUT, &mut future).await {
                        Ok(_) => Err(Error::Timeout),
                        Err(_) => Err(Error::Cleanup),
                    }
                }
            }
        };
        // Requests see only a closed driver channel after a framing failure.
        // Preserve the actual receive limit instead of retrying an unavailable DB.
        if result.as_ref().err() != Some(&Error::Cleanup)
            && let Some(error) = worker
                .session
                .as_ref()
                .and_then(|session| session.receive_failure.get())
        {
            result = Err(error);
        }
        if result.as_ref().err() == Some(&Error::Cleanup) {
            worker.poisoned = true;
        }
        if result.is_err() {
            operation.cancel();
            if worker.close().await.is_err() {
                worker.poisoned = true;
                result = Err(Error::Cleanup);
            }
        }
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
        let _ = tokio::time::timeout(CANCEL_REQUEST_TIMEOUT, async {
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
            let _ = session.catalog.take();
            let _ = session.client.take();
            let stopped = match tokio::time::timeout(
                CONNECTION_SHUTDOWN_TIMEOUT,
                &mut session.driver,
            )
            .await
            {
                Ok(joined) => joined.is_ok(),
                Err(_) => {
                    // Aborting is not proof that the server operation stopped.
                    session.driver.abort();
                    let _ = (&mut session.driver).await;
                    false
                }
            };
            let _ = self.session.take();
            if !stopped {
                self.poisoned = true;
                return Err(Error::Cleanup);
            }
        }
        Ok(())
    }

    async fn connect(&mut self, op: &Operation, credential: &BasicAuthCredential) -> Result<()> {
        if self.poisoned {
            return Err(Error::Cleanup);
        }
        op.check()?;
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
            .connect_timeout(CONNECTION_TIMEOUT)
            .options(format!(
                "-c search_path=pg_catalog -c TimeZone=UTC -c default_transaction_read_only=on \
                -c statement_timeout={} -c idle_in_transaction_session_timeout={} \
                -c application_name=otel_arrow_postgresql_receiver",
                OPERATION_TIMEOUT.as_millis(),
                OPERATION_TIMEOUT.as_millis(),
            ));
        let receive_failure = ReceiveFailure::default();
        let connected = tokio::time::timeout(CONNECTION_TIMEOUT, async {
            let stream = tokio::net::TcpStream::connect((config.host.as_str(), config.port))
                .await
                .map_err(|_| Error::Unavailable)?;
            let address = stream.peer_addr().map_err(|_| Error::Unavailable)?;
            // Match the native driver's socket setting when supplying our own stream.
            stream.set_nodelay(true).map_err(|_| Error::Unavailable)?;
            let stream = tokio::io::BufReader::with_capacity(READ_BUFFER_BYTES, stream);
            let stream = BoundedBackend::new(stream, receive_failure.clone());
            let (client, connection) = pg
                .connect_raw(stream, NoTls)
                .await
                .map_err(|error| receive_failure.get().unwrap_or_else(|| database(error)))?;
            Ok::<_, Error>((client, connection, address))
        })
        .await;
        drop(pg);
        let (client, mut connection, address) =
            connected.map_err(|_| receive_failure.get().unwrap_or(Error::Unavailable))??;
        let driver_failure = receive_failure.clone();
        let driver = tokio::task::spawn_local(async move {
            loop {
                match poll_fn(|cx| connection.poll_message(cx)).await {
                    None => return Ok(()),
                    Some(Err(e)) => {
                        let error = driver_failure.get().unwrap_or_else(|| database(e));
                        driver_failure.set(Some(error));
                        return Err(error);
                    }
                    Some(Ok(tokio_postgres::AsyncMessage::Notice(_))) => {}
                    Some(Ok(_)) => {
                        driver_failure.set(Some(Error::Limit));
                        return Err(Error::Limit);
                    }
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
            catalog: None,
            receive_failure,
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
            if session.catalog.is_none() {
                session.catalog = Some(CatalogStatements::prepare(&tx, op).await?);
            }
            let signature = plan
                .catalog(&tx, session.catalog.as_ref().ok_or(Error::Metadata)?, op)
                .await?;
            op.check()?;
            if self.signature.as_ref().is_some_and(|old| old != &signature) {
                return Err(Error::Metadata);
            }
            if session.statement.is_none() {
                let timestamp_type = plan.native_types[plan.timestamp].clone();
                let tie_type = plan.native_types[plan.tie].clone();
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
            let mut previous = (timestamp.naive_utc(), cursor.tie_breaker);
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
                rows: Vec::with_capacity(MAX_PAGE_ROWS),
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
            'page: while page.rows.len() < MAX_PAGE_ROWS {
                op.check()?;
                // Probe one row initially; retain the largest observed size across
                // pages so wide rows do not repeatedly drain oversized fetch groups.
                let target = self.fetch_size.target(
                    (MAX_PAGE_ROWS - page.rows.len()).min(self.validated.common.fetch_size_rows()),
                    self.validated
                        .common
                        .max_normalized_bytes()
                        .saturating_sub(used),
                );
                if target == 0 {
                    if page.rows.is_empty() {
                        return Err(Error::Limit);
                    }
                    break;
                }
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
                    let decoded = convert::row(&native, plan, &mut self.timestamp_cache)?;
                    op.check()?;
                    ensure_advance(&previous, &decoded.position)?;
                    let row = decoded.row;
                    let next_cursor = row.cursor.as_composite().ok_or(Error::Value)?;
                    let size = row.row.normalized_size()
                        + next_cursor.timestamp.capacity() as u64
                        + size_of::<CompositeCursor>() as u64;
                    self.fetch_size.observe(size);
                    if used.saturating_add(size) > self.validated.common.max_normalized_bytes() {
                        if page.rows.is_empty() {
                            return Err(Error::Limit);
                        }
                        break 'page;
                    }
                    used += size;
                    previous = decoded.position;
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

fn ensure_advance(
    previous: &convert::CursorPosition,
    next: &convert::CursorPosition,
) -> Result<()> {
    if previous >= next {
        return Err(Error::Value);
    }

    Ok(())
}

#[cfg(test)]
postgresql_module_tests!(worker);
