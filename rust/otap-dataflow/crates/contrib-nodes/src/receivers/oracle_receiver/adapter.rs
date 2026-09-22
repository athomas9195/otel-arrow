// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Oracle implementation of the database adapter contract.

use async_trait::async_trait;
use oracle::sql_type::{IntervalDS, IntervalYM, OracleType, Timestamp};
use oracle::{Connection, Row as OracleRow};
use otel_arrow_dfe_engine::error::ReceiverErrorKind;
use otel_arrow_dfe_scraper::database::{
    CellValue, ColumnMetadata, CompiledQuery, CompositeCursor, CursorRow, DatabaseSystem,
    DriverAdapter, DriverCancellation, QueryPage, Row,
};
use secrecy::{ExposeSecret, SecretString, zeroize::Zeroizing};
use std::io::Read;
use std::mem::size_of;
use std::path::Path;
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock};

// Oracle client initialization is process-global. The mutex only serializes
// the one-time directory choice when multiple pipeline instances start.
static ORACLE_CLIENT_DIRECTORY: OnceLock<Mutex<Option<String>>> = OnceLock::new();
const MAX_CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const MAX_CREDENTIAL_BYTES: u64 = 64 * 1024;
const MAX_TIMESTAMP_COMPONENT_DIGITS: usize = 9;

#[derive(Clone)]
pub(crate) struct OracleAdapterConfig {
    pub(crate) connect_string: String,
    pub(crate) instant_client_dir: String,
    pub(crate) username_file: String,
    pub(crate) password_file: String,
}

/// Oracle adapter that reuses one connection across non-overlapping polls.
///
/// A long-lived `spawn_blocking` worker owns the session and prepared statement;
/// cancellation temporarily shares a connection handle to interrupt active work.
/// [`DriverAdapter::shutdown`] closes the work channel and awaits worker cleanup.
/// Dropping the adapter without shutdown closes the channel but does not join
/// the worker. Cleanup remains worker-owned; stuck native work can prevent it
/// from completing.
pub struct OracleAdapter {
    config: OracleAdapterConfig,
    worker: Option<std::sync::mpsc::SyncSender<OracleWork>>,
    worker_join: Option<tokio::task::JoinHandle<()>>,
    cancellation: OracleCancellation,
}

type OracleWork = Box<dyn FnOnce(&mut Option<OracleSession>) + Send>;

/// One exclusively owned Oracle connection and the query artifacts prepared on it.
struct OracleSession {
    connection: Arc<Connection>,
    prepared: Option<OraclePreparedQuery>,
}

/// Cached statement and immutable decode plan derived from its first result metadata.
struct OraclePreparedQuery {
    statement: oracle::Statement,
    columns: Vec<ColumnMetadata>,
    types: Vec<OracleType>,
    timestamp_index: usize,
    tie_breaker_index: usize,
}

impl OracleAdapter {
    /// Creates an adapter whose connection is opened lazily on first use.
    pub(crate) fn new(config: OracleAdapterConfig) -> Self {
        Self {
            config,
            worker: None,
            worker_join: None,
            cancellation: OracleCancellation::default(),
        }
    }

    /// Runs one synchronous Oracle operation without blocking the local engine core.
    async fn run_blocking<T>(
        &mut self,
        query: &CompiledQuery,
        cursor: &CompositeCursor,
        operation: fn(
            Option<OracleSession>,
            &OracleAdapterConfig,
            &CompiledQuery,
            &CompositeCursor,
            &OracleCancellation,
        ) -> Result<(OracleSession, T), OracleAdapterError>,
    ) -> Result<T, OracleAdapterError>
    where
        T: Send + 'static,
    {
        // Run synchronous query jobs serially on a blocking worker with a
        // capacity-one queue. Retain the session here between polls and drop
        // it here once queued work finishes and the channel closes.
        if self.worker.is_none() {
            let (sender, receiver) = std::sync::mpsc::sync_channel::<OracleWork>(1);
            self.worker_join = Some(tokio::task::spawn_blocking(move || {
                let mut session = None;
                while let Ok(work) = receiver.recv() {
                    work(&mut session);
                }
                drop(session);
            }));
            self.worker = Some(sender);
        }
        let worker = self.worker.as_ref().expect("worker initialized");
        // Clone inputs so queued work owns its data and can outlive this future.
        let config = self.config.clone();
        let query = query.clone();
        let cursor = cursor.clone();
        let cancellation = self.cancellation.clone();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        worker
            .try_send(Box::new(move |session| {
                let result = operation(session.take(), &config, &query, &cursor, &cancellation)
                    .map(|(next, value)| {
                        *session = Some(next);
                        value
                    });
                _ = sender.send(result);
            }))
            .map_err(|_| OracleAdapterError::Cancelled)?;
        receiver.await.map_err(|_| OracleAdapterError::Cancelled)?
    }
}

/// Cancellation shared only between one Oracle blocking worker and its local receiver.
#[derive(Clone, Default)]
pub struct OracleCancellation {
    state: Arc<Mutex<CancellationState>>,
}

#[derive(Default)]
struct CancellationState {
    requested: bool,
    connection: Option<Arc<Connection>>,
}

struct ActiveConnection {
    cancellation: OracleCancellation,
}

impl ActiveConnection {
    /// Publishes the active connection so cancellation can interrupt only this operation.
    fn register(
        cancellation: &OracleCancellation,
        connection: Arc<Connection>,
    ) -> Result<Self, OracleAdapterError> {
        let mut state = cancellation
            .state
            .lock()
            .map_err(|_| OracleAdapterError::CancellationState)?;
        if state.requested {
            return Err(OracleAdapterError::Cancelled);
        }
        state.connection = Some(connection);
        Ok(Self {
            cancellation: cancellation.clone(),
        })
    }
}

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        // The guard scopes cancellation to the operation that registered this connection.
        if let Ok(mut state) = self.cancellation.state.lock() {
            state.connection = None;
        }
    }
}

impl OracleCancellation {
    /// Prevents a cancelled operation from opening a connection after cancellation won.
    fn ensure_not_requested(&self) -> Result<(), OracleAdapterError> {
        let state = self
            .state
            .lock()
            .map_err(|_| OracleAdapterError::CancellationState)?;
        if state.requested {
            Err(OracleAdapterError::Cancelled)
        } else {
            Ok(())
        }
    }
}

#[async_trait(?Send)]
impl DriverCancellation for OracleCancellation {
    type Error = OracleAdapterError;

    /// Interrupts the currently published Oracle call on the blocking pool.
    async fn cancel(&self) -> Result<(), Self::Error> {
        let connection = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| OracleAdapterError::CancellationState)?;
            state.requested = true;
            state.connection.clone()
        };
        let Some(connection) = connection else {
            return Ok(());
        };
        tokio::task::spawn_blocking(move || connection.break_execution())
            .await
            .map_err(OracleAdapterError::CancellationWorker)?
            .map_err(OracleAdapterError::Cancellation)
    }
}

#[async_trait(?Send)]
impl DriverAdapter for OracleAdapter {
    type Error = OracleAdapterError;
    type Cancellation = OracleCancellation;

    /// Identifies rows from this adapter as Oracle data.
    fn system(&self) -> DatabaseSystem {
        DatabaseSystem::Oracle
    }

    /// Resets operation-local cancellation state before starting native work.
    fn begin_operation(&mut self) -> Result<Self::Cancellation, Self::Error> {
        let mut state = self
            .cancellation
            .state
            .lock()
            .map_err(|_| OracleAdapterError::CancellationState)?;
        state.requested = false;
        state.connection = None;
        drop(state);
        Ok(self.cancellation.clone())
    }

    /// Prepares the query and validates its live result metadata without fetching rows.
    async fn validate_query(
        &mut self,
        query: &CompiledQuery,
    ) -> Result<Vec<ColumnMetadata>, Self::Error> {
        // rust-oracle is synchronous. Moving native calls to the blocking pool
        // keeps the engine's local async control loop responsive.
        let initial = query.watermark().initial.clone();
        self.run_blocking(query, &initial, validate_blocking).await
    }

    /// Fetches and normalizes one bounded page after the supplied cursor.
    async fn execute(
        &mut self,
        query: &CompiledQuery,
        cursor: &CompositeCursor,
    ) -> Result<QueryPage, Self::Error> {
        self.run_blocking(query, cursor, execute_blocking).await
    }

    async fn shutdown(&mut self) -> Result<(), Self::Error> {
        // Close the job channel and await worker exit, including session cleanup.
        // This does not interrupt a running native call. The controller bounds
        // this wait on normal/error loop exits and retains the lease if cleanup
        // cannot be joined.
        drop(self.worker.take());
        if let Some(worker) = self.worker_join.take() {
            worker.await.map_err(OracleAdapterError::Worker)?;
        }
        Ok(())
    }

    /// Maps adapter failures into stable receiver error categories.
    fn classify_error(error: &Self::Error) -> ReceiverErrorKind {
        match error {
            OracleAdapterError::Connect(_) => ReceiverErrorKind::Connect,
            OracleAdapterError::Credential { .. }
            | OracleAdapterError::CredentialNotRegularFile(_)
            | OracleAdapterError::CredentialTooLarge(_)
            | OracleAdapterError::InvalidCredentialEncoding(_)
            | OracleAdapterError::EmptyCredential(_)
            | OracleAdapterError::Initialize(_)
            | OracleAdapterError::ClientAlreadyInitialized
            | OracleAdapterError::ClientDirectoryConflict
            | OracleAdapterError::ClientInitializationLock
            | OracleAdapterError::ConnectDescriptorUnsupported
            | OracleAdapterError::ConnectTimeoutOverride
            | OracleAdapterError::ConnectRetryUnsupported
            | OracleAdapterError::MultipleAddressUnsupported
            | OracleAdapterError::MissingCursorColumn(_)
            | OracleAdapterError::NullableCursorColumn
            | OracleAdapterError::UnsupportedCursorTimestamp { .. }
            | OracleAdapterError::UnsupportedCursorTieBreaker { .. }
            | OracleAdapterError::InvalidCursorTimestamp(_)
            | OracleAdapterError::NormalizedByteLimit { .. }
            | OracleAdapterError::ResultMetadataChanged
            | OracleAdapterError::UnsupportedType(_) => ReceiverErrorKind::Configuration,
            OracleAdapterError::Configure(_)
            | OracleAdapterError::Prepare(_)
            | OracleAdapterError::Query(_)
            | OracleAdapterError::Fetch(_)
            | OracleAdapterError::Convert(_)
            | OracleAdapterError::NullCursorValue(_) => ReceiverErrorKind::Transport,
            OracleAdapterError::CancellationState
            | OracleAdapterError::CancellationWorker(_)
            | OracleAdapterError::Cancellation(_)
            | OracleAdapterError::Cancelled => ReceiverErrorKind::Shutdown,
            OracleAdapterError::NonFiniteFloat | OracleAdapterError::Worker(_) => {
                ReceiverErrorKind::Other
            }
        }
    }
}

/// Performs startup preparation and returns the validated public column metadata.
fn validate_blocking(
    session: Option<OracleSession>,
    config: &OracleAdapterConfig,
    query: &CompiledQuery,
    cursor: &CompositeCursor,
    cancellation: &OracleCancellation,
) -> Result<(OracleSession, Vec<ColumnMetadata>), OracleAdapterError> {
    // Executing the prepared SELECT is required because Oracle exposes result
    // metadata on the result set. No row is fetched during startup validation.
    let (mut session, _active) = prepare_session(session, config, query, cancellation)?;
    ensure_prepared(&mut session, query, cursor)?;
    let columns = session
        .prepared
        .as_ref()
        .expect("query was prepared")
        .columns
        .clone();
    finish_session(&session.connection)?;
    Ok((session, columns))
}

/// Executes one prepared page fetch and enforces row and normalized-byte bounds.
fn execute_blocking(
    session: Option<OracleSession>,
    config: &OracleAdapterConfig,
    query: &CompiledQuery,
    cursor: &CompositeCursor,
    cancellation: &OracleCancellation,
) -> Result<(OracleSession, QueryPage), OracleAdapterError> {
    let (mut session, _active) = prepare_session(session, config, query, cancellation)?;
    ensure_prepared(&mut session, query, cursor)?;
    let prepared = session.prepared.as_mut().expect("query was prepared");
    let mut result_set = bind_cursor(&mut prepared.statement, query, cursor)?;
    if !metadata_matches(result_set.column_info(), &prepared.columns, &prepared.types) {
        return Err(OracleAdapterError::ResultMetadataChanged);
    }
    let columns = prepared.columns.clone();

    let watermark = query.watermark();
    let mut rows = Vec::new();
    let mut normalized_bytes = size_of::<Vec<CursorRow>>() as u64;
    for _ in 0..query.max_rows() {
        cancellation.ensure_not_requested()?;
        let Some(row) = result_set.next() else { break };
        let row = row.map_err(OracleAdapterError::Fetch)?;
        cancellation.ensure_not_requested()?;
        let cursor = extract_cursor(
            &row,
            prepared.timestamp_index,
            prepared.tie_breaker_index,
            watermark,
        )?;
        let normalized = normalize_row(&row, &prepared.types)?;
        let row_bytes = normalized
            .normalized_size()
            .saturating_add(cursor.timestamp.capacity() as u64);
        // Reserve exactly one slot to make spare page capacity explicit in the
        // normalized budget. The fixed Row header was already counted above.
        let next_bytes = normalized_bytes
            .saturating_add(row_bytes)
            .saturating_add((size_of::<CursorRow>() - size_of::<Row>()) as u64);
        if next_bytes > query.max_normalized_bytes() {
            if rows.is_empty() {
                // Skipping the row would silently lose data, so fail instead.
                return Err(OracleAdapterError::NormalizedByteLimit {
                    normalized_bytes: normalized.normalized_size(),
                    limit: query.max_normalized_bytes(),
                });
            }
            // Return the fitting prefix; later valid rows arrive next poll.
            break;
        }
        normalized_bytes = next_bytes;
        rows.reserve_exact(1);
        rows.push(CursorRow {
            row: normalized,
            cursor,
        });
    }
    drop(result_set);
    finish_session(&session.connection)?;

    Ok((session, QueryPage { columns, rows }))
}

/// Binds the committed cursor through Oracle named parameters.
///
/// The cursor is never interpolated into SQL text, and the timestamp bind uses
/// an explicit timezone-aware type so its source offset survives round trips.
fn bind_cursor<'a>(
    statement: &'a mut oracle::Statement,
    query: &CompiledQuery,
    cursor: &CompositeCursor,
) -> Result<oracle::ResultSet<'a, OracleRow>, OracleAdapterError> {
    let watermark = query.watermark();
    let timestamp = parse_cursor_timestamp(&cursor.timestamp)?;
    // Bind with timezone information even for DATE and timezone-naive
    // TIMESTAMP columns. The session is UTC, so a naive cursor binds as
    // +00:00, while a TIMESTAMP WITH TIME ZONE cursor retains its source
    // offset instead of silently shifting the checkpoint boundary.
    let timestamp_type = cursor_bind_type();
    let timestamp_bind = (&timestamp, &timestamp_type);
    let tie_breaker = cursor.tie_breaker;
    statement
        .query_named(&[
            (watermark.timestamp_bind.as_str(), &timestamp_bind),
            (watermark.tie_breaker_bind.as_str(), &tie_breaker),
        ])
        .map_err(OracleAdapterError::Query)
}

/// Parses cursor text without overflow or lossy numeric narrowing in the driver.
pub(super) fn parse_cursor_timestamp(text: &str) -> Result<Timestamp, OracleAdapterError> {
    // oracle 0.6.3 uses unchecked numeric accumulation and narrowing casts.
    // Nine digits fit every intermediate type and avoid fractional truncation.
    let mut digits = 0;
    for byte in text.bytes() {
        if byte.is_ascii_digit() {
            digits += 1;
            if digits > MAX_TIMESTAMP_COMPONENT_DIGITS {
                return Err(OracleAdapterError::InvalidCursorTimestamp(
                    "timestamp numeric component exceeds nine digits".to_owned(),
                ));
            }
        } else {
            digits = 0;
        }
    }
    Timestamp::from_str(text)
        .map_err(|error| OracleAdapterError::InvalidCursorTimestamp(error.to_string()))
}

/// Uses a timezone-aware bind so checkpoint offsets survive round trips.
fn cursor_bind_type() -> OracleType {
    OracleType::TimestampTZ(9)
}

/// Builds and caches the statement and decode plan once per connection.
fn ensure_prepared(
    session: &mut OracleSession,
    query: &CompiledQuery,
    cursor: &CompositeCursor,
) -> Result<(), OracleAdapterError> {
    if session.prepared.is_some() {
        return Ok(());
    }

    // Oracle exposes result metadata only after execution. Discover it with a
    // one-row buffer, then rebuild once with a byte-bounded fetch array.
    let mut discovery = session
        .connection
        .statement(query.sql())
        .fetch_array_size(1)
        .prefetch_rows(0)
        .build()
        .map_err(OracleAdapterError::Prepare)?;
    let result_set = bind_cursor(&mut discovery, query, cursor)?;
    let (columns, types) = result_metadata(result_set.column_info())?;
    let (timestamp_index, tie_breaker_index) =
        validate_cursor_columns(result_set.column_info(), query)?;
    drop(result_set);
    drop(discovery);

    let fetch_rows = bounded_fetch_array_size(&types, query);
    let statement = session
        .connection
        .statement(query.sql())
        .fetch_array_size(fetch_rows)
        // Prefetch owns a second native row buffer outside the calculated byte budget.
        .prefetch_rows(0)
        .build()
        .map_err(OracleAdapterError::Prepare)?;
    session.prepared = Some(OraclePreparedQuery {
        statement,
        columns,
        types,
        timestamp_index,
        tie_breaker_index,
    });
    Ok(())
}

/// Caps native fetch rows by configured row, fetch, and normalized-memory limits.
fn bounded_fetch_array_size(types: &[OracleType], query: &CompiledQuery) -> u32 {
    let row_bytes = types.iter().fold(
        (size_of::<Row>() + types.len() * size_of::<CellValue>()) as u64,
        |total, data_type| total.saturating_add(max_normalized_value_bytes(data_type)),
    );
    let byte_limited = query.max_normalized_bytes() / row_bytes.max(1);
    let rows = (query.max_rows() as u64)
        .min(query.fetch_size() as u64)
        .min(byte_limited)
        .max(1);
    rows as u32
}

/// Returns a conservative maximum normalized payload for one supported Oracle value.
fn max_normalized_value_bytes(data_type: &OracleType) -> u64 {
    match data_type {
        OracleType::Varchar2(bytes) | OracleType::Char(bytes) | OracleType::Raw(bytes) => {
            u64::from(*bytes)
        }
        OracleType::NVarchar2(characters) | OracleType::NChar(characters) => {
            u64::from(*characters).saturating_mul(4)
        }
        OracleType::Rowid => 128,
        OracleType::Number(_, _)
        | OracleType::Float(_)
        | OracleType::Date
        | OracleType::Timestamp(_)
        | OracleType::TimestampTZ(_)
        | OracleType::TimestampLTZ(_)
        | OracleType::IntervalDS(_, _)
        | OracleType::IntervalYM(_) => 128,
        OracleType::BinaryFloat
        | OracleType::BinaryDouble
        | OracleType::Int64
        | OracleType::UInt64 => 8,
        OracleType::Boolean => 1,
        // Unsupported types are rejected before this estimate is used.
        _ => 0,
    }
}

/// Validates that both cursor columns exist with deterministic supported types.
fn validate_cursor_columns(
    columns: &[oracle::ColumnInfo],
    query: &CompiledQuery,
) -> Result<(usize, usize), OracleAdapterError> {
    let described = columns
        .iter()
        .map(|column| (column.name().to_owned(), column.oracle_type().clone()))
        .collect::<Vec<_>>();
    let (timestamp, tie_breaker) =
        validate_described_cursor_columns(&described, query.watermark())?;
    if columns[timestamp].nullable() || columns[tie_breaker].nullable() {
        return Err(OracleAdapterError::NullableCursorColumn);
    }
    Ok((timestamp, tie_breaker))
}

/// Pure cursor-metadata validation over adapter-independent column descriptions.
fn validate_described_cursor_columns(
    columns: &[(String, OracleType)],
    watermark: &otel_arrow_dfe_scraper::database::CompositeWatermark,
) -> Result<(usize, usize), OracleAdapterError> {
    let timestamp_index = cursor_column_index(columns, &watermark.timestamp_column)?;
    let tie_breaker_index = cursor_column_index(columns, &watermark.tie_breaker_column)?;
    let timestamp_type = &columns[timestamp_index].1;
    if !matches!(
        timestamp_type,
        OracleType::Date
            | OracleType::Timestamp(_)
            | OracleType::TimestampTZ(_)
            | OracleType::TimestampLTZ(_)
    ) {
        return Err(OracleAdapterError::UnsupportedCursorTimestamp {
            column: watermark.timestamp_column.clone(),
            data_type: timestamp_type.to_string(),
        });
    }
    let tie_breaker_type = &columns[tie_breaker_index].1;
    if !matches!(
        tie_breaker_type,
        OracleType::Int64 | OracleType::Number(1..=18, 0)
    ) {
        return Err(OracleAdapterError::UnsupportedCursorTieBreaker {
            column: watermark.tie_breaker_column.clone(),
            data_type: tie_breaker_type.to_string(),
        });
    }
    Ok((timestamp_index, tie_breaker_index))
}

/// Resolves a configured cursor column using Oracle's case-insensitive identifier rules.
fn cursor_column_index(
    columns: &[(String, OracleType)],
    name: &str,
) -> Result<usize, OracleAdapterError> {
    columns
        .iter()
        .position(|(column, _)| column.eq_ignore_ascii_case(name))
        .ok_or_else(|| OracleAdapterError::MissingCursorColumn(name.to_owned()))
}

/// Extracts the composite cursor of one row, rejecting null components.
fn extract_cursor(
    row: &OracleRow,
    timestamp_index: usize,
    tie_breaker_index: usize,
    watermark: &otel_arrow_dfe_scraper::database::CompositeWatermark,
) -> Result<CompositeCursor, OracleAdapterError> {
    let timestamp = row
        .get::<_, Option<Timestamp>>(timestamp_index)
        .map_err(OracleAdapterError::Convert)?
        .ok_or_else(|| OracleAdapterError::NullCursorValue(watermark.timestamp_column.clone()))?;
    let tie_breaker = row
        .get::<_, Option<i64>>(tie_breaker_index)
        .map_err(OracleAdapterError::Convert)?
        .ok_or_else(|| OracleAdapterError::NullCursorValue(watermark.tie_breaker_column.clone()))?;
    // The text form round-trips through Timestamp::from_str on the next bind,
    // so the durable checkpoint keeps full source precision.
    Ok(CompositeCursor::new(timestamp.to_string(), tie_breaker))
}

/// Opens or reuses the single session, publishes it for cancellation, and starts read-only work.
fn prepare_session(
    session: Option<OracleSession>,
    config: &OracleAdapterConfig,
    query: &CompiledQuery,
    cancellation: &OracleCancellation,
) -> Result<(OracleSession, ActiveConnection), OracleAdapterError> {
    let session = match session {
        Some(session) => session,
        None => {
            cancellation.ensure_not_requested()?;
            OracleSession {
                connection: Arc::new(connect(config, query.timeout())?),
                prepared: None,
            }
        }
    };
    let active = ActiveConnection::register(cancellation, Arc::clone(&session.connection))?;
    session
        .connection
        .set_call_timeout(Some(query.timeout()))
        .map_err(OracleAdapterError::Configure)?;
    begin_read_only(&session.connection)?;
    Ok((session, active))
}

/// Compiles stable public metadata and the per-column Oracle decode types.
fn result_metadata(
    columns: &[oracle::ColumnInfo],
) -> Result<(Vec<ColumnMetadata>, Vec<OracleType>), OracleAdapterError> {
    let metadata = columns.iter().map(column_metadata).collect();
    let types = columns
        .iter()
        .map(|column| column.oracle_type().clone())
        .collect::<Vec<_>>();
    validate_types(&types)?;
    Ok((metadata, types))
}

/// Detects result-shape changes before applying a cached decode plan.
fn metadata_matches(
    columns: &[oracle::ColumnInfo],
    metadata: &[ColumnMetadata],
    types: &[OracleType],
) -> bool {
    columns.len() == metadata.len()
        && columns
            .iter()
            .zip(metadata.iter().zip(types))
            .all(|(column, (metadata, data_type))| {
                column.name() == metadata.name && column.oracle_type() == data_type
            })
}

/// Ends the read-only transaction without retaining database-side state between polls.
fn finish_session(connection: &Connection) -> Result<(), OracleAdapterError> {
    connection.rollback().map_err(OracleAdapterError::Configure)
}

/// Converts Oracle metadata into the vendor-neutral scraper representation.
fn column_metadata(column: &oracle::ColumnInfo) -> ColumnMetadata {
    ColumnMetadata {
        name: column.name().to_owned(),
        source_type: column.oracle_type().to_string(),
        nullable: column.nullable(),
    }
}

/// Initializes the client, reads mounted credentials, and establishes a UTC Oracle session.
fn connect(
    config: &OracleAdapterConfig,
    timeout: std::time::Duration,
) -> Result<Connection, OracleAdapterError> {
    initialize_client(&config.instant_client_dir)?;
    // Mounted files are read for each new connection so secret rotation takes
    // effect after a reconnect without placing credentials in configuration.
    let username = read_credential(&config.username_file, "username")?;
    let password = read_credential(&config.password_file, "password")?;
    let connect_string = bounded_connect_string(&config.connect_string, timeout)?;
    let connection = Connection::connect(
        username.expose_secret(),
        password.expose_secret(),
        connect_string,
    )
    .map_err(OracleAdapterError::Connect)?;
    connection
        .set_call_timeout(Some(timeout))
        .map_err(OracleAdapterError::Configure)?;
    connection.ping().map_err(OracleAdapterError::Connect)?;
    _ = connection
        .execute("ALTER SESSION SET TIME_ZONE = 'UTC'", &[])
        .map_err(OracleAdapterError::Configure)?;
    Ok(connection)
}

/// Makes the database enforce the receiver's read-only query contract.
fn begin_read_only(connection: &Connection) -> Result<(), OracleAdapterError> {
    // Static SQL inspection is intentionally conservative but cannot classify
    // every Oracle function; the database enforces the final read-only boundary.
    _ = connection
        .execute("SET TRANSACTION READ ONLY", &[])
        .map_err(OracleAdapterError::Configure)?;
    Ok(())
}

/// Injects fixed startup timeouts while rejecting options that could multiply attempts.
fn bounded_connect_string(
    connect_string: &str,
    timeout: std::time::Duration,
) -> Result<String, OracleAdapterError> {
    let normalized = connect_string.to_ascii_lowercase();
    if connect_string.trim_start().starts_with('(') {
        return Err(OracleAdapterError::ConnectDescriptorUnsupported);
    }
    if normalized.contains("connect_timeout=") || normalized.contains("transport_connect_timeout=")
    {
        return Err(OracleAdapterError::ConnectTimeoutOverride);
    }
    if normalized.contains("retry_count=") || normalized.contains("retry_delay=") {
        return Err(OracleAdapterError::ConnectRetryUnsupported);
    }
    if connect_string
        .split('?')
        .next()
        .is_some_and(|address| address.contains(','))
    {
        return Err(OracleAdapterError::MultipleAddressUnsupported);
    }
    let separator = if connect_string.contains('?') {
        '&'
    } else {
        '?'
    };
    let seconds = timeout.min(MAX_CONNECT_TIMEOUT).as_secs().max(1);
    Ok(format!(
        "{connect_string}{separator}connect_timeout={seconds}&transport_connect_timeout={seconds}"
    ))
}

/// Applies the process-global Instant Client directory exactly once.
fn initialize_client(directory: &str) -> Result<(), OracleAdapterError> {
    let selected = ORACLE_CLIENT_DIRECTORY.get_or_init(|| Mutex::new(None));
    let mut selected = selected
        .lock()
        .map_err(|_| OracleAdapterError::ClientInitializationLock)?;
    if let Some(existing) = selected.as_deref() {
        return if existing == directory {
            Ok(())
        } else {
            Err(OracleAdapterError::ClientDirectoryConflict)
        };
    }
    if oracle::InitParams::is_initialized() {
        return Err(OracleAdapterError::ClientAlreadyInitialized);
    }
    let mut params = oracle::InitParams::new();
    _ = params
        .oracle_client_lib_dir(directory)
        .and_then(|params| params.init())
        .map_err(OracleAdapterError::Initialize)?;
    *selected = Some(directory.to_owned());
    Ok(())
}

/// Reads one bounded UTF-8 credential and zeroizes its storage on drop.
fn read_credential(path: &str, kind: &'static str) -> Result<SecretString, OracleAdapterError> {
    let path = Path::new(path);
    let metadata = std::fs::metadata(path)
        .map_err(|source| OracleAdapterError::Credential { kind, source })?;
    if !metadata.is_file() {
        return Err(OracleAdapterError::CredentialNotRegularFile(kind));
    }
    if metadata.len() > MAX_CREDENTIAL_BYTES {
        return Err(OracleAdapterError::CredentialTooLarge(kind));
    }
    let file = std::fs::File::open(path)
        .map_err(|source| OracleAdapterError::Credential { kind, source })?;
    if !file
        .metadata()
        .map_err(|source| OracleAdapterError::Credential { kind, source })?
        .is_file()
    {
        return Err(OracleAdapterError::CredentialNotRegularFile(kind));
    }
    let mut bytes = Zeroizing::new(Vec::with_capacity(metadata.len() as usize));
    _ = file
        .take(MAX_CREDENTIAL_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|source| OracleAdapterError::Credential { kind, source })?;
    if bytes.len() as u64 > MAX_CREDENTIAL_BYTES {
        return Err(OracleAdapterError::CredentialTooLarge(kind));
    }
    let mut value = Zeroizing::new(
        std::str::from_utf8(&bytes)
            .map_err(|_| OracleAdapterError::InvalidCredentialEncoding(kind))?
            .to_owned(),
    );
    while value.ends_with(['\r', '\n']) {
        _ = value.pop();
    }
    if value.is_empty() {
        return Err(OracleAdapterError::EmptyCredential(kind));
    }
    Ok(value.as_str().to_owned().into())
}

/// Rejects result types that lack a precision-preserving normalization path.
fn validate_types(types: &[OracleType]) -> Result<(), OracleAdapterError> {
    // There is no catch-all string fallback. Every admitted vendor type has an
    // explicit, precision-preserving CellValue conversion below.
    for source_type in types {
        match source_type {
            OracleType::Varchar2(_)
            | OracleType::NVarchar2(_)
            | OracleType::Char(_)
            | OracleType::NChar(_)
            | OracleType::Rowid
            | OracleType::Raw(_)
            | OracleType::BinaryFloat
            | OracleType::BinaryDouble
            | OracleType::Number(_, _)
            | OracleType::Float(_)
            | OracleType::Date
            | OracleType::Timestamp(_)
            | OracleType::TimestampTZ(_)
            | OracleType::TimestampLTZ(_)
            | OracleType::IntervalDS(_, _)
            | OracleType::IntervalYM(_)
            | OracleType::Int64
            | OracleType::UInt64
            | OracleType::Boolean => {}
            unsupported => {
                return Err(OracleAdapterError::UnsupportedType(unsupported.to_string()));
            }
        }
    }
    Ok(())
}

/// Applies the cached type plan to every cell in one Oracle row.
fn normalize_row(row: &OracleRow, types: &[OracleType]) -> Result<Row, OracleAdapterError> {
    let values = types
        .iter()
        .enumerate()
        .map(|(index, source_type)| normalize_cell(row, index, source_type))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Row { values })
}

/// Converts one nullable Oracle scalar into the closed neutral value model.
fn normalize_cell(
    row: &OracleRow,
    index: usize,
    source_type: &OracleType,
) -> Result<CellValue, OracleAdapterError> {
    // rust-oracle returns conversion failures, including invalid text decoding,
    // as explicit errors. The receiver's query error policy then scopes them.
    macro_rules! optional {
        ($rust_type:ty, $variant:expr) => {
            row.get::<_, Option<$rust_type>>(index)
                .map(|value| value.map_or(CellValue::Null, $variant))
                .map_err(OracleAdapterError::Convert)
        };
    }

    match source_type {
        OracleType::Varchar2(_)
        | OracleType::NVarchar2(_)
        | OracleType::Char(_)
        | OracleType::NChar(_)
        | OracleType::Rowid => optional!(String, CellValue::String),
        OracleType::Raw(_) => optional!(Vec<u8>, CellValue::Bytes),
        OracleType::BinaryFloat => {
            optional!(f32, |value| CellValue::Float64(f64::from(value))).and_then(finite_float)
        }
        OracleType::BinaryDouble => optional!(f64, CellValue::Float64).and_then(finite_float),
        OracleType::Number(_, _) | OracleType::Float(_) => {
            optional!(String, CellValue::Decimal)
        }
        OracleType::Date | OracleType::Timestamp(_) => {
            optional!(Timestamp, |value: Timestamp| CellValue::Timestamp(
                format_oracle_timestamp(&value, false)
            ))
        }
        OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => {
            optional!(Timestamp, |value: Timestamp| CellValue::TimestampTz(
                format_oracle_timestamp(&value, true)
            ))
        }
        OracleType::IntervalDS(_, _) => {
            optional!(IntervalDS, |value: IntervalDS| CellValue::Interval(
                value.to_string()
            ))
        }
        OracleType::IntervalYM(_) => {
            optional!(IntervalYM, |value: IntervalYM| CellValue::Interval(
                value.to_string()
            ))
        }
        OracleType::Int64 => optional!(i64, CellValue::Int64),
        OracleType::UInt64 => optional!(u64, CellValue::UInt64),
        OracleType::Boolean => optional!(bool, CellValue::Bool),
        unsupported => Err(OracleAdapterError::UnsupportedType(unsupported.to_string())),
    }
}

/// Formats full timestamp precision and includes an offset only for zoned source types.
fn format_oracle_timestamp(value: &Timestamp, with_timezone: bool) -> String {
    let base = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:09}",
        value.year(),
        value.month(),
        value.day(),
        value.hour(),
        value.minute(),
        value.second(),
        value.nanosecond()
    );
    if !with_timezone {
        return base;
    }
    let sign = if value.tz_offset() < 0 { '-' } else { '+' };
    format!(
        "{base}{sign}{:02}:{:02}",
        value.tz_hour_offset().unsigned_abs(),
        value.tz_minute_offset().unsigned_abs()
    )
}

/// Rejects NaN and infinity because OTLP cannot preserve them portably.
fn finite_float(value: CellValue) -> Result<CellValue, OracleAdapterError> {
    match value {
        CellValue::Float64(value) if !value.is_finite() => Err(OracleAdapterError::NonFiniteFloat),
        value => Ok(value),
    }
}

/// Oracle connection, query, or conversion failure.
///
/// Native error messages may contain SQL, endpoint, row, or cursor data.
/// Expose only the operation and numeric codes, never native Debug/source chains.
#[derive(thiserror::Error)]
pub enum OracleAdapterError {
    /// A mounted credential file could not be read.
    #[error("failed to read Oracle {kind} file")]
    Credential {
        /// Credential kind without its configured path.
        kind: &'static str,
        /// Underlying file error.
        #[source]
        source: std::io::Error,
    },
    /// A mounted credential path is not a regular file.
    #[error("Oracle {0} path must reference a regular file")]
    CredentialNotRegularFile(&'static str),
    /// A mounted credential file exceeds the fixed allocation bound.
    #[error("Oracle {0} file must not exceed 64 KiB")]
    CredentialTooLarge(&'static str),
    /// A mounted credential file is not UTF-8.
    #[error("Oracle {0} file must contain valid UTF-8")]
    InvalidCredentialEncoding(&'static str),
    /// A mounted credential file was empty.
    #[error("Oracle {0} file must not be empty")]
    EmptyCredential(&'static str),
    /// Oracle client initialization failed.
    #[error("Oracle client initialization failed (OCI {oci:?}, DPI {dpi:?})", oci = .0.oci_code(), dpi = .0.dpi_code())]
    Initialize(oracle::Error),
    /// Oracle was already initialized outside this adapter.
    #[error("Oracle client was initialized before instant_client_dir was applied")]
    ClientAlreadyInitialized,
    /// Another adapter selected a different process-global client directory.
    #[error("instant_client_dir conflicts with the initialized Oracle client")]
    ClientDirectoryConflict,
    /// Oracle client initialization state was poisoned.
    #[error("Oracle client initialization lock was poisoned")]
    ClientInitializationLock,
    /// Connection establishment or validation failed.
    #[error("Oracle connection failed (OCI {oci:?}, DPI {dpi:?})", oci = .0.oci_code(), dpi = .0.dpi_code())]
    Connect(oracle::Error),
    /// The first slice cannot safely inject bounds into a connect descriptor.
    #[error("Oracle connect descriptors are not supported; use an Easy Connect string")]
    ConnectDescriptorUnsupported,
    /// Connection timeout properties are owned by the receiver's query timeout.
    #[error("Oracle connect string must not override receiver connection timeouts")]
    ConnectTimeoutOverride,
    /// Connection retry controls would defeat the bounded startup attempt.
    #[error("Oracle connect string must not configure retry_count or retry_delay")]
    ConnectRetryUnsupported,
    /// Multiple addresses would multiply the per-attempt startup timeout.
    #[error("Oracle connect string must contain exactly one database address")]
    MultipleAddressUnsupported,
    /// Session or timeout setup failed.
    #[error("Oracle session configuration failed (OCI {oci:?}, DPI {dpi:?})", oci = .0.oci_code(), dpi = .0.dpi_code())]
    Configure(oracle::Error),
    /// Statement preparation failed.
    #[error("Oracle query preparation failed (OCI {oci:?}, DPI {dpi:?})", oci = .0.oci_code(), dpi = .0.dpi_code())]
    Prepare(oracle::Error),
    /// Query execution failed.
    #[error("Oracle query execution failed (OCI {oci:?}, DPI {dpi:?})", oci = .0.oci_code(), dpi = .0.dpi_code())]
    Query(oracle::Error),
    /// Row fetching failed.
    #[error("Oracle row fetch failed (OCI {oci:?}, DPI {dpi:?})", oci = .0.oci_code(), dpi = .0.dpi_code())]
    Fetch(oracle::Error),
    /// Native value conversion failed.
    #[error("Oracle value conversion failed (OCI {oci:?}, DPI {dpi:?})", oci = .0.oci_code(), dpi = .0.dpi_code())]
    Convert(oracle::Error),
    /// A floating-point result cannot be represented faithfully.
    #[error("Oracle returned a non-finite floating-point value")]
    NonFiniteFloat,
    /// The result type does not have bounded conversion support.
    #[error("Oracle result type '{0}' is not supported")]
    UnsupportedType(String),
    /// A cached statement was invalidated with a different result shape.
    #[error("Oracle query result metadata changed after startup validation")]
    ResultMetadataChanged,
    /// A configured cursor column is absent from live result metadata.
    #[error("watermark cursor column '{0}' is not present in the query result")]
    MissingCursorColumn(String),
    /// Cursor nullability must be excluded by the source schema.
    #[error("watermark cursor columns must be declared NOT NULL")]
    NullableCursorColumn,
    /// The timestamp cursor column is not an Oracle date or timestamp type.
    #[error(
        "watermark timestamp column '{column}' has unsupported type '{data_type}'; DATE and TIMESTAMP family types are required"
    )]
    UnsupportedCursorTimestamp {
        /// Configured cursor column.
        column: String,
        /// Live Oracle type name.
        data_type: String,
    },
    /// The tie-breaker cursor column is not an integral Oracle type.
    #[error(
        "watermark tie-breaker column '{column}' has unsupported type '{data_type}'; a scale-zero integral type is required"
    )]
    UnsupportedCursorTieBreaker {
        /// Configured cursor column.
        column: String,
        /// Live Oracle type name.
        data_type: String,
    },
    /// A row's cursor component was SQL NULL.
    #[error("watermark cursor column '{0}' returned NULL; composite cursors must be non-null")]
    NullCursorValue(String),
    /// The committed cursor timestamp cannot be bound to Oracle.
    #[error("committed watermark timestamp is not a valid Oracle timestamp")]
    InvalidCursorTimestamp(String),
    /// The first row alone exceeds the normalized in-memory ceiling.
    #[error(
        "the first database row normalizes to {normalized_bytes} bytes, exceeding the {limit}-byte budget from query.max_batch_bytes"
    )]
    NormalizedByteLimit {
        /// Normalized size of the single row.
        normalized_bytes: u64,
        /// Configured normalized-byte ceiling.
        limit: u64,
    },
    /// Blocking Oracle execution could not be joined.
    #[error("Oracle worker failed")]
    Worker(tokio::task::JoinError),
    /// Cancellation state could not be synchronized with the blocking worker.
    #[error("Oracle cancellation state is unavailable")]
    CancellationState,
    /// Native Oracle cancellation could not be joined.
    #[error("Oracle cancellation worker failed")]
    CancellationWorker(tokio::task::JoinError),
    /// Oracle rejected a request to interrupt the active call.
    #[error("Oracle cancellation failed (OCI {oci:?}, DPI {dpi:?})", oci = .0.oci_code(), dpi = .0.dpi_code())]
    Cancellation(oracle::Error),
    /// An operation was cancelled before it registered its connection.
    #[error("Oracle operation was cancelled")]
    Cancelled,
}

impl std::fmt::Debug for OracleAdapterError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, formatter)
    }
}

#[cfg(test)]
#[path = "adapter_tests.rs"]
mod tests;
