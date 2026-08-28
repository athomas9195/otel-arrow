// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Dedicated blocking Oracle connection worker.

use crate::receivers::database::driver::Credentials;
use crate::receivers::database::query::{BindValue, CompiledQuery, ExecutionLimits};
use crate::receivers::database::row::{
    CellValue, ColumnMetadata, Row, RowError, RowPage, RowPageBuilder,
};
use oracle::sql_type::{IntervalDS, IntervalYM, OracleType, Timestamp, ToSql};
use oracle::{Connection, Row as OracleRow};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

/// Async handle for one Oracle connection owned by one OS worker thread.
pub(crate) struct OracleWorker {
    commands: mpsc::Sender<Command>,
    // Arc is required because Oracle cancellation must call the same
    // connection concurrently with its dedicated blocking query worker.
    break_connection: Arc<Connection>,
    thread: Option<JoinHandle<()>>,
    created_at: Instant,
}

enum Command {
    Execute {
        query: Box<CompiledQuery>,
        limits: ExecutionLimits,
        reply: oneshot::Sender<Result<RowPage, OracleWorkerError>>,
    },
    Shutdown,
}

impl OracleWorker {
    /// Opens, validates, and starts one dedicated Oracle connection worker.
    pub(crate) async fn connect(
        connect_string: String,
        credentials: Credentials,
    ) -> Result<Self, OracleWorkerError> {
        let username = credentials.username().to_owned();
        let password = credentials.password().to_owned();
        let (commands, mut command_rx) = mpsc::channel(1);
        let (startup_tx, startup_rx) = oneshot::channel();

        // Oracle OCI calls are synchronous. A dedicated worker owns each
        // connection so pipeline-local futures never block an OTAP core.
        let thread = std::thread::Builder::new()
            .name("otap-oracle-connection".to_owned())
            .spawn(move || {
                let connection = match Connection::connect(username, password, connect_string) {
                    Ok(connection) => connection,
                    Err(source) => {
                        _ = startup_tx.send(Err(OracleWorkerError::Oracle {
                            operation: OracleOperation::Connect,
                            source,
                        }));
                        return;
                    }
                };
                if let Err(source) = initialize_connection(&connection) {
                    _ = startup_tx.send(Err(OracleWorkerError::Oracle {
                        operation: OracleOperation::Initialize,
                        source,
                    }));
                    return;
                }
                let connection = Arc::new(connection);
                if startup_tx.send(Ok(Arc::clone(&connection))).is_err() {
                    return;
                }

                while let Some(command) = command_rx.blocking_recv() {
                    match command {
                        Command::Execute {
                            query,
                            limits,
                            reply,
                        } => {
                            _ = reply.send(execute_query(&connection, &query, limits));
                        }
                        Command::Shutdown => break,
                    }
                }
            })
            .map_err(OracleWorkerError::Spawn)?;

        let break_connection = startup_rx
            .await
            .map_err(|_| OracleWorkerError::ChannelClosed)??;
        Ok(Self {
            commands,
            break_connection,
            thread: Some(thread),
            created_at: Instant::now(),
        })
    }

    /// Executes one page and maps cancellation to Oracle connection break.
    pub(crate) async fn execute(
        &self,
        query: &CompiledQuery,
        limits: ExecutionLimits,
        cancel: CancellationToken,
    ) -> Result<RowPage, OracleWorkerError> {
        if cancel.is_cancelled() {
            return Err(OracleWorkerError::Cancelled);
        }
        let (reply_tx, mut reply_rx) = oneshot::channel();
        self.commands
            .send(Command::Execute {
                query: Box::new(query.clone()),
                limits,
                reply: reply_tx,
            })
            .await
            .map_err(|_| OracleWorkerError::ChannelClosed)?;

        tokio::select! {
            result = &mut reply_rx => result.map_err(|_| OracleWorkerError::ChannelClosed)?,
            _ = cancel.cancelled() => {
                let connection = Arc::clone(&self.break_connection);
                let break_result = tokio::task::spawn_blocking(move || connection.break_execution())
                    .await
                    .map_err(OracleWorkerError::CancellationWorker)
                    .and_then(|result| {
                        result.map_err(|source| OracleWorkerError::Oracle {
                            operation: OracleOperation::Cancel,
                            source,
                        })
                    });
                // Keep ownership of the connection worker until its bounded
                // call timeout returns, even when the native break call fails.
                _ = reply_rx.await;
                break_result?;
                Err(OracleWorkerError::Cancelled)
            }
        }
    }

    /// Returns how long this worker's credentials and TLS state have been active.
    pub(crate) fn age(&self) -> Duration {
        self.created_at.elapsed()
    }

    /// Stops and joins the dedicated worker.
    pub(crate) async fn shutdown(mut self) -> Result<(), OracleWorkerError> {
        _ = self.commands.send(Command::Shutdown).await;
        let Some(thread) = self.thread.take() else {
            return Ok(());
        };
        tokio::task::spawn_blocking(move || thread.join())
            .await
            .map_err(OracleWorkerError::ShutdownWorker)?
            .map_err(|_| OracleWorkerError::WorkerPanicked)
    }
}

fn initialize_connection(connection: &Connection) -> oracle::Result<()> {
    connection.ping()?;
    _ = connection.execute("ALTER SESSION SET TIME_ZONE = 'UTC'", &[])?;
    Ok(())
}

fn execute_query(
    connection: &Connection,
    query: &CompiledQuery,
    limits: ExecutionLimits,
) -> Result<RowPage, OracleWorkerError> {
    connection
        .set_call_timeout(Some(query.timeout))
        .map_err(|source| OracleWorkerError::Oracle {
            operation: OracleOperation::Configure,
            source,
        })?;
    let mut statement = connection
        .statement(&query.sql)
        .fetch_array_size(u32::try_from(limits.fetch_size).unwrap_or(u32::MAX).max(1))
        .build()
        .map_err(|source| OracleWorkerError::Oracle {
            operation: OracleOperation::Prepare,
            source,
        })?;
    let bind_values = oracle_binds(&query.binds)?;
    let bind_refs = bind_values
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_to_sql()))
        .collect::<Vec<_>>();
    let mut rows =
        statement
            .query_named(&bind_refs)
            .map_err(|source| OracleWorkerError::Oracle {
                operation: OracleOperation::Query,
                source,
            })?;
    let columns = rows
        .column_info()
        .iter()
        .map(|column| ColumnMetadata {
            name: column.name().to_owned(),
            source_type: column.oracle_type().to_string(),
            nullable: column.nullable(),
        })
        .collect::<Vec<_>>();
    let oracle_types = rows
        .column_info()
        .iter()
        .map(|column| column.oracle_type().clone())
        .collect::<Vec<_>>();
    let mut builder = RowPageBuilder::new(columns, limits, query.output.oversize_policy)?;

    for row in rows.by_ref().take(limits.max_rows.saturating_add(1)) {
        let row = row.map_err(|source| OracleWorkerError::Oracle {
            operation: OracleOperation::Fetch,
            source,
        })?;
        let values = oracle_types
            .iter()
            .enumerate()
            .map(|(index, oracle_type)| oracle_cell(&row, index, oracle_type))
            .collect::<Result<Vec<_>, OracleWorkerError>>()?;
        builder.push(Row { values })?;
    }
    Ok(builder.finish())
}

enum OracleBind {
    Bool(i32),
    Int64(i64),
    UInt64(u64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
}

impl OracleBind {
    fn as_to_sql(&self) -> &dyn ToSql {
        match self {
            Self::Bool(value) => value,
            Self::Int64(value) => value,
            Self::UInt64(value) => value,
            Self::Float64(value) => value,
            Self::String(value) => value,
            Self::Bytes(value) => value,
        }
    }
}

fn oracle_binds(binds: &[BindValue]) -> Result<Vec<(String, OracleBind)>, OracleWorkerError> {
    binds
        .iter()
        .map(|bind| {
            let value = match &bind.value {
                CellValue::Null => {
                    return Err(OracleWorkerError::UntypedNullBind {
                        name: bind.name.clone(),
                    });
                }
                CellValue::Bool(value) => OracleBind::Bool(i32::from(*value)),
                CellValue::Int64(value) => OracleBind::Int64(*value),
                CellValue::UInt64(value) => OracleBind::UInt64(*value),
                CellValue::Float64(value) if value.is_finite() => OracleBind::Float64(*value),
                CellValue::Float64(_) => return Err(RowError::NonFiniteFloat.into()),
                CellValue::Bytes(value) => OracleBind::Bytes(value.clone()),
                CellValue::Decimal(value)
                | CellValue::String(value)
                | CellValue::Date(value)
                | CellValue::Timestamp(value)
                | CellValue::TimestampTz(value)
                | CellValue::Interval(value)
                | CellValue::Json(value)
                | CellValue::Uuid(value) => OracleBind::String(value.clone()),
            };
            Ok((bind.name.clone(), value))
        })
        .collect()
}

fn oracle_cell(
    row: &OracleRow,
    index: usize,
    oracle_type: &OracleType,
) -> Result<CellValue, OracleWorkerError> {
    macro_rules! optional {
        ($rust_type:ty, $variant:expr) => {
            row.get::<_, Option<$rust_type>>(index)
                .map(|value| value.map_or(CellValue::Null, $variant))
                .map_err(|source| OracleWorkerError::Oracle {
                    operation: OracleOperation::Convert,
                    source,
                })
        };
    }

    match oracle_type {
        OracleType::Varchar2(_)
        | OracleType::NVarchar2(_)
        | OracleType::Char(_)
        | OracleType::NChar(_)
        | OracleType::Rowid
        | OracleType::CLOB
        | OracleType::NCLOB
        | OracleType::Long => optional!(String, CellValue::String),
        OracleType::Raw(_) | OracleType::BLOB | OracleType::LongRaw => {
            optional!(Vec<u8>, CellValue::Bytes)
        }
        OracleType::BinaryFloat => optional!(f32, |value| {
            let value = f64::from(value);
            CellValue::Float64(value)
        })
        .and_then(validate_float),
        OracleType::BinaryDouble => optional!(f64, CellValue::Float64).and_then(validate_float),
        OracleType::Number(_, _) | OracleType::Float(_) => {
            optional!(String, CellValue::Decimal)
        }
        OracleType::Date | OracleType::Timestamp(_) => {
            optional!(Timestamp, |value: Timestamp| CellValue::Timestamp(
                value.to_string()
            ))
        }
        OracleType::TimestampTZ(_) | OracleType::TimestampLTZ(_) => {
            optional!(Timestamp, |value: Timestamp| CellValue::TimestampTz(
                value.to_string()
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
        OracleType::Json => optional!(String, CellValue::Json).and_then(validate_json),
        OracleType::Int64 => optional!(i64, CellValue::Int64),
        OracleType::UInt64 => optional!(u64, CellValue::UInt64),
        OracleType::BFILE
        | OracleType::RefCursor
        | OracleType::Boolean
        | OracleType::Object(_)
        | OracleType::Xml => Err(RowError::UnsupportedType {
            source_type: oracle_type.to_string(),
        }
        .into()),
    }
}

fn validate_float(value: CellValue) -> Result<CellValue, OracleWorkerError> {
    if matches!(value, CellValue::Float64(number) if !number.is_finite()) {
        Err(RowError::NonFiniteFloat.into())
    } else {
        Ok(value)
    }
}

fn validate_json(value: CellValue) -> Result<CellValue, OracleWorkerError> {
    if let CellValue::Json(text) = &value {
        _ = serde_json::from_str::<serde_json::Value>(text).map_err(RowError::InvalidJson)?;
    }
    Ok(value)
}

/// Oracle operation associated with a sanitized driver error.
#[derive(Clone, Copy, Debug)]
pub(crate) enum OracleOperation {
    /// Connection establishment.
    Connect,
    /// Session validation/setup.
    Initialize,
    /// Per-query call-timeout setup.
    Configure,
    /// Statement preparation.
    Prepare,
    /// Query execution.
    Query,
    /// Row fetch.
    Fetch,
    /// Native value conversion.
    Convert,
    /// Server-side cancellation.
    Cancel,
}

impl std::fmt::Display for OracleOperation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

/// Sanitized dedicated-worker error.
#[derive(Debug, thiserror::Error)]
pub(crate) enum OracleWorkerError {
    /// Oracle returned an error for a bounded operation class.
    #[error("Oracle {operation} operation failed")]
    Oracle {
        /// Safe operation category.
        operation: OracleOperation,
        /// Native error retained as a source but never used as telemetry dimensions.
        #[source]
        source: oracle::Error,
    },
    /// Typed row conversion or resource-bound failure.
    #[error(transparent)]
    Row(#[from] RowError),
    /// Dedicated worker thread could not start.
    #[error("Oracle connection worker could not start")]
    Spawn(#[source] std::io::Error),
    /// Worker command or reply channel closed unexpectedly.
    #[error("Oracle connection worker channel closed")]
    ChannelClosed,
    /// Query execution was cancelled.
    #[error("Oracle query was cancelled")]
    Cancelled,
    /// A null bind reached the adapter without an explicit native type.
    #[error("Oracle bind parameter '{name}' is null and has no explicit native type")]
    UntypedNullBind {
        /// Logical bind name.
        name: String,
    },
    /// Cancellation helper could not be joined.
    #[error("Oracle cancellation worker failed")]
    CancellationWorker(#[source] tokio::task::JoinError),
    /// Shutdown helper could not be joined.
    #[error("Oracle shutdown worker failed")]
    ShutdownWorker(#[source] tokio::task::JoinError),
    /// Dedicated worker panicked.
    #[error("Oracle connection worker panicked")]
    WorkerPanicked,
}
