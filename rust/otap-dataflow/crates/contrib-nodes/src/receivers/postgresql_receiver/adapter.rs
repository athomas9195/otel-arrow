// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! One asynchronous PostgreSQL session with bounded portal reads and cancellation.

use super::{
    config::{AuthenticationConfig, ConnectionConfig, parse_timestamp},
    value,
};
use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use futures::TryStreamExt;
use otel_arrow_dfe_engine::error::ReceiverErrorKind;
use otel_arrow_dfe_otap::tls_utils::read_file_with_limit_async;
use otel_arrow_dfe_scraper::database::{
    CellValue, ColumnMetadata, CompiledQuery, CompositeCursor, CursorRow, DatabaseSystem,
    DriverAdapter, DriverCancellation, QueryPage, Row,
};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, pem::PemObject},
};
use std::{collections::HashSet, mem::size_of, time::Duration};
use tokio::{io::AsyncReadExt, task::JoinHandle};
use tokio_postgres::{Client, Statement, config::SslMode, types::Type};
use tokio_postgres_rustls::MakeRustlsConnect;
use tokio_util::sync::CancellationToken;

const CLEANUP_TIMEOUT: Duration = Duration::from_secs(3);

/// Sanitized driver failures; PostgreSQL errors/SQL/row values never enter their source chain.
#[derive(Debug, thiserror::Error)]
pub enum PgError {
    /// A credential file could not be read or was invalid.
    #[error("PostgreSQL credential file is unreadable, empty or oversized")]
    Credentials,
    /// CA material or the process crypto provider is invalid.
    #[error("PostgreSQL TLS configuration is invalid")]
    Tls,
    /// Connection, TLS verification, or login failed.
    #[error("PostgreSQL connection, server verification or authentication failed")]
    Connect,
    /// Server rejected query or transaction work.
    #[error("PostgreSQL query operation failed")]
    Query,
    /// Page operation exceeded its configured deadline.
    #[error("PostgreSQL operation exceeded query.timeout")]
    Timeout,
    /// Cancellation was requested.
    #[error("PostgreSQL operation cancelled")]
    Cancelled,
    /// Required cleanup could not be confirmed.
    #[error("PostgreSQL cancellation or cleanup could not be confirmed")]
    Cleanup,
    /// Query has not passed startup validation.
    #[error("PostgreSQL query must be validated before execution")]
    NotValidated,
    /// Schema does not satisfy the supported cursor/output contract.
    #[error(
        "PostgreSQL result requires distinct columns and non-null timestamp/integer cursor columns"
    )]
    Metadata,
    /// An output scalar cannot be normalized losslessly.
    #[error("PostgreSQL value cannot be normalized without loss")]
    Conversion,
    /// Output type is outside this adapter's supported set.
    #[error("PostgreSQL result contains an unsupported type")]
    UnsupportedType,
    /// A single row cannot fit the configured page memory bound.
    #[error("PostgreSQL row exceeds max_batch_bytes")]
    RowTooLarge,
    /// Source cursor was null or not strictly ordered.
    #[error("PostgreSQL cursor is null, invalid or does not strictly advance")]
    Cursor,
}

/// Cooperative cancellation of one entire adapter operation.
#[derive(Clone)]
pub struct PgCancellation(CancellationToken);
#[async_trait(?Send)]
impl DriverCancellation for PgCancellation {
    type Error = PgError;
    async fn cancel(&self) -> Result<(), PgError> {
        self.0.cancel();
        Ok(())
    }
}

struct Session {
    client: Client,
    task: JoinHandle<Result<(), tokio_postgres::Error>>,
    tls: MakeRustlsConnect,
    statement: Option<Statement>,
    columns: Vec<ColumnMetadata>,
    cursor_indices: (usize, usize),
}

/// PostgreSQL implementation of the shared database adapter contract.
pub struct PostgreSqlAdapter {
    expected_sql: String,
    config: ConnectionConfig,
    auth: AuthenticationConfig,
    session: Option<Session>,
    operation: CancellationToken,
    cleanup_failed: bool,
}

impl PostgreSqlAdapter {
    pub(super) fn new(
        config: ConnectionConfig,
        auth: AuthenticationConfig,
        expected_sql: String,
    ) -> Self {
        Self {
            expected_sql,
            config,
            auth,
            session: None,
            operation: CancellationToken::new(),
            cleanup_failed: false,
        }
    }

    async fn connect(&mut self, timeout: Duration) -> Result<(), PgError> {
        if self.session.is_some() {
            return Ok(());
        }
        let connect = async {
            let username = secret(&self.auth.username_file).await?;
            let password = secret(&self.auth.password_file).await?;
            let ca = read_file_with_limit_async(&self.config.tls.ca_file)
                .await
                .map_err(|_| PgError::Tls)?;
            let mut roots = RootCertStore::empty();
            for cert in CertificateDer::pem_slice_iter(&ca) {
                roots
                    .add(cert.map_err(|_| PgError::Tls)?)
                    .map_err(|_| PgError::Tls)?;
            }
            if roots.is_empty() {
                return Err(PgError::Tls);
            }
            let provider = rustls::crypto::CryptoProvider::get_default()
                .cloned()
                .ok_or(PgError::Tls)?;
            let tls = ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|_| PgError::Tls)?
                .with_root_certificates(roots)
                .with_no_client_auth();
            let tls = MakeRustlsConnect::new(tls);
            use secrecy::ExposeSecret;
            let mut config = tokio_postgres::Config::new();
            _ = config
                .host(&self.config.host)
                .port(self.config.port)
                .dbname(&self.config.database)
                .user(username.expose_secret())
                .password(password.expose_secret())
                .application_name("otel-arrow-postgresql")
                .ssl_mode(SslMode::Require)
                .connect_timeout(timeout)
                .options("-c default_transaction_read_only=on -c timezone=UTC");
            let (client, connection) = config
                .connect(tls.clone())
                .await
                .map_err(|_| PgError::Connect)?;
            // Exactly one async protocol pump per session, no blocking pool and no shared data-path state.
            let task = tokio::spawn(connection);
            Ok(Session {
                client,
                task,
                tls,
                statement: None,
                columns: Vec::new(),
                cursor_indices: (0, 0),
            })
        };
        self.session = Some(tokio::select! {
            biased;
            _ = self.operation.cancelled() => return Err(PgError::Cancelled),
            result = tokio::time::timeout(timeout, connect) => result.map_err(|_| PgError::Timeout)??,
        });
        Ok(())
    }

    async fn run(
        &mut self,
        query: &CompiledQuery,
        cursor: &CompositeCursor,
        validate: bool,
    ) -> Result<QueryPage, PgError> {
        if query.sql() != self.expected_sql {
            return Err(PgError::NotValidated);
        }
        let started = tokio::time::Instant::now();
        self.connect(query.timeout()).await?;
        let session = self.session.as_mut().ok_or(PgError::Connect)?;
        let result = tokio::select! {
            biased;
            _ = self.operation.cancelled() => Err(PgError::Cancelled),
            result = tokio::time::timeout_at(started + query.timeout(), page(session, query, cursor, validate)) =>
                result.unwrap_or(Err(PgError::Timeout)),
        };
        if result.is_err() {
            // Draining ROLLBACK behind cancellation confirms the server operation has finished.
            if !tokio::time::timeout(CLEANUP_TIMEOUT, async {
                if matches!(result, Err(PgError::Timeout | PgError::Cancelled)) {
                    session
                        .client
                        .cancel_token()
                        .cancel_query(session.tls.clone())
                        .await
                        .map_err(|_| PgError::Cleanup)?;
                }
                match session.client.batch_execute("ROLLBACK").await {
                    Ok(()) => Ok(()),
                    // PostgreSQL cancellation has no completion response and can race the rollback.
                    // A second rollback confirms ReadyForQuery within the same cleanup deadline.
                    Err(error)
                        if error.code()
                            == Some(&tokio_postgres::error::SqlState::QUERY_CANCELED) =>
                    {
                        session
                            .client
                            .batch_execute("ROLLBACK")
                            .await
                            .map_err(|_| PgError::Cleanup)
                    }
                    Err(_) => Err(PgError::Cleanup),
                }
            })
            .await
            .is_ok_and(|result| result.is_ok())
            {
                self.cleanup_failed = true;
                return Err(PgError::Cleanup);
            }
        }
        result
    }
}

#[async_trait(?Send)]
impl DriverAdapter for PostgreSqlAdapter {
    type Error = PgError;
    type Cancellation = PgCancellation;
    fn system(&self) -> DatabaseSystem {
        DatabaseSystem::PostgreSql
    }
    fn begin_operation(&mut self) -> Result<PgCancellation, PgError> {
        self.operation = CancellationToken::new();
        Ok(PgCancellation(self.operation.clone()))
    }
    async fn validate_query(
        &mut self,
        query: &CompiledQuery,
    ) -> Result<Vec<ColumnMetadata>, PgError> {
        Ok(self
            .run(query, &query.watermark().initial, true)
            .await?
            .columns)
    }
    async fn execute(
        &mut self,
        query: &CompiledQuery,
        cursor: &CompositeCursor,
    ) -> Result<QueryPage, PgError> {
        self.run(query, cursor, false).await
    }
    async fn shutdown(&mut self) -> Result<(), PgError> {
        if let Some(session) = self.session.take() {
            drop(session.client);
            let mut task = session.task;
            match tokio::time::timeout(CLEANUP_TIMEOUT, &mut task).await {
                Ok(Ok(Ok(()))) => {}
                _ => {
                    task.abort();
                    let _ = task.await;
                    return Err(PgError::Cleanup);
                }
            }
        }
        if self.cleanup_failed {
            Err(PgError::Cleanup)
        } else {
            Ok(())
        }
    }
    fn classify_error(error: &PgError) -> ReceiverErrorKind {
        match error {
            PgError::Credentials
            | PgError::Tls
            | PgError::Metadata
            | PgError::UnsupportedType
            | PgError::NotValidated
            | PgError::Cursor
            | PgError::RowTooLarge => ReceiverErrorKind::Configuration,
            PgError::Connect => ReceiverErrorKind::Connect,
            PgError::Cleanup | PgError::Cancelled => ReceiverErrorKind::Shutdown,
            _ => ReceiverErrorKind::Transport,
        }
    }
}

impl Drop for PostgreSqlAdapter {
    fn drop(&mut self) {
        if let Some(session) = &self.session {
            session.task.abort();
        }
    }
}

async fn secret(path: &std::path::Path) -> Result<secrecy::SecretString, PgError> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|_| PgError::Credentials)?;
    let mut bytes = Vec::new();
    _ = file
        .take(4097)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| PgError::Credentials)?;
    if bytes.len() > 4096 {
        return Err(PgError::Credentials);
    }
    let mut value = String::from_utf8(bytes).map_err(|_| PgError::Credentials)?;
    if value.ends_with('\n') {
        _ = value.pop();
        if value.ends_with('\r') {
            _ = value.pop();
        }
    }
    if value.is_empty() || value.contains('\0') {
        return Err(PgError::Credentials);
    }
    Ok(secrecy::SecretString::from(value))
}

async fn metadata(
    client: &Client,
    statement: &Statement,
    query: &CompiledQuery,
) -> Result<(Vec<ColumnMetadata>, (usize, usize)), PgError> {
    let mut names = HashSet::new();
    let mut columns = Vec::new();
    let mut timestamp = None;
    let mut id = None;
    for (index, column) in statement.columns().iter().enumerate() {
        if !names.insert(column.name().to_ascii_lowercase()) {
            return Err(PgError::Metadata);
        }
        if !value::supported(column.type_()) {
            return Err(PgError::UnsupportedType);
        }
        let is_ts = column.name() == query.watermark().timestamp_column;
        let is_id = column.name() == query.watermark().tie_breaker_column;
        let nullable = if is_ts || is_id {
            let oid = column.table_oid().ok_or(PgError::Metadata)?;
            let attribute = column.column_id().ok_or(PgError::Metadata)?;
            let row = client.query_opt(
                "SELECT attnotnull FROM pg_catalog.pg_attribute WHERE attrelid=$1 AND attnum=$2 AND NOT attisdropped",
                &[&oid, &attribute],
            ).await.map_err(|_| PgError::Metadata)?.ok_or(PgError::Metadata)?;
            !row.try_get::<_, bool>(0).map_err(|_| PgError::Metadata)?
        } else {
            true
        };
        if (is_ts || is_id) && nullable {
            return Err(PgError::Metadata);
        }
        if is_ts {
            if !matches!(*column.type_(), Type::TIMESTAMP | Type::TIMESTAMPTZ) {
                return Err(PgError::Metadata);
            }
            timestamp = Some(index);
        }
        if is_id {
            if !matches!(*column.type_(), Type::INT2 | Type::INT4 | Type::INT8) {
                return Err(PgError::Metadata);
            }
            id = Some(index);
        }
        columns.push(ColumnMetadata {
            name: column.name().to_owned(),
            source_type: column.type_().name().to_owned(),
            nullable,
        });
    }
    Ok((
        columns,
        (
            timestamp.ok_or(PgError::Metadata)?,
            id.ok_or(PgError::Metadata)?,
        ),
    ))
}

async fn page(
    session: &mut Session,
    query: &CompiledQuery,
    cursor: &CompositeCursor,
    validate: bool,
) -> Result<QueryPage, PgError> {
    if validate {
        let statement = session
            .client
            .prepare_typed(query.sql(), &[Type::TIMESTAMPTZ, Type::INT8])
            .await
            .map_err(|_| PgError::Query)?;
        let (columns, indices) = metadata(&session.client, &statement, query).await?;
        session.columns = columns;
        session.cursor_indices = indices;
        session.statement = Some(statement);
        return Ok(QueryPage {
            columns: session.columns.clone(),
            rows: Vec::new(),
        });
    }
    let statement = session.statement.as_ref().ok_or(PgError::NotValidated)?;
    let timestamp: DateTime<Utc> =
        parse_timestamp(&cursor.timestamp).map_err(|_| PgError::Cursor)?;
    let transaction = session
        .client
        .build_transaction()
        .read_only(true)
        .start()
        .await
        .map_err(|_| PgError::Query)?;
    // Numeric milliseconds are derived from validated Duration, never interpolated operator SQL.
    transaction
        .batch_execute(&format!(
            "SET LOCAL statement_timeout = {}",
            query.timeout().as_millis()
        ))
        .await
        .map_err(|_| PgError::Query)?;
    let portal = transaction
        .bind(statement, &[&timestamp, &cursor.tie_breaker])
        .await
        .map_err(|_| PgError::Query)?;
    let mut rows = Vec::new();
    let mut used = (rows.capacity() * size_of::<CursorRow>()) as u64;
    let mut previous = (timestamp, cursor.tie_breaker);
    let mut done = false;
    while !done && rows.len() < query.max_rows() {
        let count = (query.max_rows() - rows.len()).min(query.fetch_size_rows());
        let stream = transaction
            .query_portal_raw(&portal, count as i32)
            .await
            .map_err(|_| PgError::Query)?;
        tokio::pin!(stream);
        let mut fetched = 0;
        while let Some(row) = stream.try_next().await.map_err(|_| PgError::Query)? {
            fetched += 1;
            if row.columns().len() != session.columns.len() {
                return Err(PgError::Metadata);
            }
            let mut values = Vec::with_capacity(row.len());
            let mut row_bytes =
                (size_of::<Row>() + values.capacity() * size_of::<CellValue>()) as u64;
            if row_bytes > query.max_batch_bytes() {
                return Err(PgError::RowTooLarge);
            }
            for index in 0..row.len() {
                if row.columns()[index].name() != session.columns[index].name
                    || row.columns()[index].type_().name() != session.columns[index].source_type
                {
                    return Err(PgError::Metadata);
                }
                let value = value::decode(&row, index, query.max_batch_bytes() - row_bytes)?;
                row_bytes = row_bytes.saturating_add(value.normalized_size());
                if row_bytes > query.max_batch_bytes() {
                    return Err(PgError::RowTooLarge);
                }
                values.push(value);
            }
            let (ts_index, id_index) = session.cursor_indices;
            let ts = match &values[ts_index] {
                CellValue::Timestamp(value) | CellValue::TimestampTz(value) => {
                    parse_timestamp(value).map_err(|_| PgError::Cursor)?
                }
                _ => return Err(PgError::Cursor),
            };
            let CellValue::Int64(id) = values[id_index] else {
                return Err(PgError::Cursor);
            };
            if (ts, id) <= previous {
                return Err(PgError::Cursor);
            }
            let cursor = CompositeCursor::new(ts.to_rfc3339_opts(SecondsFormat::Micros, true), id);
            let row = Row { values };
            let cost = row
                .normalized_size()
                .saturating_add(cursor.timestamp.capacity() as u64);
            let extra_capacity = if rows.len() == rows.capacity() {
                rows.capacity().max(1) * size_of::<CursorRow>()
            } else {
                0
            };
            if used
                .saturating_add(cost)
                .saturating_add(extra_capacity as u64)
                > query.max_batch_bytes()
            {
                if rows.is_empty() {
                    return Err(PgError::RowTooLarge);
                }
                done = true;
                break;
            }
            if rows.len() == rows.capacity() {
                let old = rows.capacity();
                rows.reserve_exact(old.max(1));
                used += ((rows.capacity() - old) * size_of::<CursorRow>()) as u64;
            }
            used += cost;
            previous = (ts, id);
            rows.push(CursorRow { row, cursor });
            // Ready protocol buffers must not starve the operation's stop/deadline controls.
            if rows.len().is_multiple_of(64) {
                tokio::task::yield_now().await;
            }
        }
        if fetched < count {
            done = true;
        }
    }
    transaction.rollback().await.map_err(|_| PgError::Query)?;
    Ok(QueryPage {
        columns: session.columns.clone(),
        rows,
    })
}
