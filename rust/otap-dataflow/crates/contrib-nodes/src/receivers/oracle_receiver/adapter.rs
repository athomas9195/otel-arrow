// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Oracle implementation of the database-neutral adapter contract.

use super::config::OracleConnectionConfig;
use super::worker::{OracleOperation, OracleWorker, OracleWorkerError};
use crate::receivers::database::driver::{
    Credentials, DatabaseSystem, DriverAdapter, DriverCapabilities, ErrorClass, ErrorDisposition,
    SessionDisposition,
};
use crate::receivers::database::query::{CompiledQuery, ExecutionLimits};
use crate::receivers::database::row::RowPage;
use async_trait::async_trait;
use oracle::ErrorKind;
use tokio_util::sync::CancellationToken;

/// Oracle adapter with no query-job or OTLP mapping state.
pub(crate) struct OracleAdapter {
    config: OracleConnectionConfig,
}

impl OracleAdapter {
    /// Creates an Oracle adapter from validated connection configuration.
    pub(crate) fn new(config: OracleConnectionConfig) -> Self {
        Self { config }
    }
}

#[async_trait(?Send)]
impl DriverAdapter for OracleAdapter {
    type Session = OracleWorker;
    type Error = OracleWorkerError;

    fn system(&self) -> DatabaseSystem {
        DatabaseSystem::Oracle
    }

    fn capabilities(&self) -> DriverCapabilities {
        DriverCapabilities {
            cancellation: true,
            result_metadata: true,
            parameter_binding: true,
        }
    }

    fn max_connections(&self) -> usize {
        self.config.pool.max_connections
    }

    fn min_connections(&self) -> usize {
        self.config.pool.min_connections
    }

    fn session_expired(&self, session: &Self::Session) -> bool {
        session.age() >= self.config.pool.max_connection_lifetime
    }

    async fn connect(&self, credentials: Credentials) -> Result<Self::Session, Self::Error> {
        OracleWorker::connect(self.config.resolved_connect_string(), credentials).await
    }

    async fn execute_page(
        &self,
        session: &mut Self::Session,
        query: &CompiledQuery,
        limits: ExecutionLimits,
        cancel: CancellationToken,
    ) -> Result<RowPage, Self::Error> {
        session.execute(query, limits, cancel).await
    }

    async fn shutdown(&self, session: Self::Session) -> Result<(), Self::Error> {
        session.shutdown().await
    }

    fn classify_error(&self, error: &Self::Error) -> ErrorDisposition {
        let class = classify_worker_error(error);
        ErrorDisposition {
            class,
            retryable: matches!(
                class,
                ErrorClass::TransientTransport | ErrorClass::TimeoutCancel
            ),
            session: if matches!(
                class,
                ErrorClass::Query | ErrorClass::Conversion | ErrorClass::Configuration
            ) {
                SessionDisposition::Reuse
            } else {
                SessionDisposition::Discard
            },
        }
    }
}

fn classify_worker_error(error: &OracleWorkerError) -> ErrorClass {
    match error {
        OracleWorkerError::Cancelled => ErrorClass::TimeoutCancel,
        OracleWorkerError::Row(_) => ErrorClass::Conversion,
        OracleWorkerError::UntypedNullBind { .. } => ErrorClass::Configuration,
        OracleWorkerError::ChannelClosed
        | OracleWorkerError::Spawn(_)
        | OracleWorkerError::CancellationWorker(_)
        | OracleWorkerError::ShutdownWorker(_)
        | OracleWorkerError::WorkerPanicked => ErrorClass::Internal,
        OracleWorkerError::Oracle { operation, source } => {
            classify_oracle_error(*operation, source)
        }
    }
}

fn classify_oracle_error(operation: OracleOperation, error: &oracle::Error) -> ErrorClass {
    if let Some(database_error) = error.db_error() {
        match database_error.code() {
            1013 | 3136 | 12170 => return ErrorClass::TimeoutCancel,
            1017 | 28000 | 28001 => return ErrorClass::Authentication,
            _ if database_error.is_recoverable() => return ErrorClass::TransientTransport,
            3113 | 3114 | 12153 | 12514 | 12537 | 12541 | 12543 | 12545 => {
                return ErrorClass::TransientTransport;
            }
            _ => {}
        }
    }

    match operation {
        OracleOperation::Connect | OracleOperation::Initialize => ErrorClass::TransientTransport,
        OracleOperation::Cancel => ErrorClass::TimeoutCancel,
        OracleOperation::Convert => ErrorClass::Conversion,
        OracleOperation::Configure
        | OracleOperation::Prepare
        | OracleOperation::Query
        | OracleOperation::Fetch => match error.kind() {
            ErrorKind::InvalidArgument
            | ErrorKind::InvalidBindIndex
            | ErrorKind::InvalidBindName
            | ErrorKind::InvalidOperation => ErrorClass::Configuration,
            ErrorKind::InvalidTypeConversion
            | ErrorKind::OutOfRange
            | ErrorKind::ParseError
            | ErrorKind::NullValue => ErrorClass::Conversion,
            _ => ErrorClass::Query,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: Oracle cancellation interrupts an in-progress call.
    /// Guarantees: cancellation is retryable and the potentially unsafe session is discarded.
    #[test]
    fn cancellation_discards_session() {
        let adapter = OracleAdapter::new(
            serde_json::from_value(serde_json::json!({
                "connect_string": "//localhost/FREEPDB1",
                "username": "reader"
            }))
            .expect("adapter config"),
        );
        let disposition = adapter.classify_error(&OracleWorkerError::Cancelled);
        assert_eq!(disposition.class, ErrorClass::TimeoutCancel);
        assert!(disposition.retryable);
        assert_eq!(disposition.session, SessionDisposition::Discard);
    }
}
