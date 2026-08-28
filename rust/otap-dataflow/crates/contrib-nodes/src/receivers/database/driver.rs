// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Database adapter contract and common execution error policy.

use super::query::{CompiledQuery, ExecutionLimits};
use super::row::RowPage;
use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};
use std::error::Error as StdError;
use tokio_util::sync::CancellationToken;

/// Resolved credentials supplied only while opening a database session.
pub(crate) struct Credentials {
    username: String,
    password: SecretString,
}

impl Credentials {
    /// Creates redacted username/password credentials.
    pub(crate) fn new(username: String, password: String) -> Self {
        Self {
            username,
            password: SecretString::from(password),
        }
    }

    /// Borrows the username.
    pub(crate) fn username(&self) -> &str {
        &self.username
    }

    /// Borrows the password through the explicit secret-exposure API.
    pub(crate) fn password(&self) -> &str {
        self.password.expose_secret()
    }
}

/// Environment-backed credential reference resolved on every new connection.
#[derive(Clone)]
pub(crate) struct EnvironmentCredentialProvider {
    username: String,
    password_env: String,
}

impl EnvironmentCredentialProvider {
    /// Creates a provider without resolving secret material.
    pub(crate) fn new(username: String, password_env: String) -> Self {
        Self {
            username,
            password_env,
        }
    }

    /// Resolves fresh credentials, allowing rotation on reconnect.
    pub(crate) fn resolve(&self) -> Result<Credentials, CredentialError> {
        let password =
            std::env::var(&self.password_env).map_err(|source| CredentialError::Environment {
                name: self.password_env.clone(),
                source,
            })?;
        Ok(Credentials::new(self.username.clone(), password))
    }
}

/// Credential resolution failure that never includes secret material.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CredentialError {
    /// Referenced environment variable was absent or invalid.
    #[error("failed to read database password from environment variable {name}")]
    Environment {
        /// Safe environment variable name.
        name: String,
        /// Environment lookup failure.
        #[source]
        source: std::env::VarError,
    },
}

/// Stable database system identity used in OTLP and internal telemetry.
#[allow(dead_code)] // Variants are consumed as the remaining adapters are added.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DatabaseSystem {
    /// PostgreSQL.
    PostgreSql,
    /// Oracle Database.
    Oracle,
    /// Microsoft SQL Server.
    SqlServer,
    /// MySQL.
    MySql,
}

impl DatabaseSystem {
    /// Returns the OpenTelemetry database system name.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::PostgreSql => "postgresql",
            Self::Oracle => "oracle",
            Self::SqlServer => "sql_server",
            Self::MySql => "mysql",
        }
    }
}

/// Adapter capabilities used for startup validation and runtime branching.
#[derive(Clone, Copy, Debug)]
pub(crate) struct DriverCapabilities {
    /// Driver supports cooperative server-side cancellation.
    pub(crate) cancellation: bool,
    /// Driver exposes source database types and nullability.
    pub(crate) result_metadata: bool,
    /// Driver supports logical named or positional bind values.
    pub(crate) parameter_binding: bool,
}

/// Bounded receiver-wide database error taxonomy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ErrorClass {
    /// Invalid configuration or unsupported requested capability.
    Configuration,
    /// Credential or authentication failure.
    Authentication,
    /// Recoverable connection or network failure.
    TransientTransport,
    /// Query deadline or cancellation.
    TimeoutCancel,
    /// SQL, authorization, or source-object failure.
    Query,
    /// Native value normalization or mapping failure.
    Conversion,
    /// Downstream pipeline failure.
    Downstream,
    /// Adapter or worker invariant failure.
    Internal,
}

impl ErrorClass {
    /// Returns a stable low-cardinality telemetry value.
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Configuration => "configuration",
            Self::Authentication => "authentication",
            Self::TransientTransport => "transient_transport",
            Self::TimeoutCancel => "timeout_cancel",
            Self::Query => "query",
            Self::Conversion => "conversion",
            Self::Downstream => "downstream",
            Self::Internal => "internal",
        }
    }
}

/// Whether a failed session can safely execute another query.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SessionDisposition {
    /// Session is safe for reuse.
    Reuse,
    /// Session must be closed and recreated.
    Discard,
}

/// Runtime response to one adapter failure.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ErrorDisposition {
    /// Bounded failure category.
    pub(crate) class: ErrorClass,
    /// Whether bounded retry policy may retry this operation.
    pub(crate) retryable: bool,
    /// Whether the current session remains safe.
    pub(crate) session: SessionDisposition,
}

/// Database-specific operations required by the common polling runtime.
#[async_trait(?Send)]
pub(crate) trait DriverAdapter {
    /// Adapter-owned session, pool, or dedicated worker handle.
    type Session: 'static;
    /// Adapter-specific error retained for diagnostics.
    type Error: StdError + 'static;

    /// Stable database system.
    fn system(&self) -> DatabaseSystem;

    /// Declares adapter features required for startup validation.
    fn capabilities(&self) -> DriverCapabilities;

    /// Maximum simultaneously open sessions chosen by this adapter's pool policy.
    fn max_connections(&self) -> usize;

    /// Minimum sessions opened and validated before polling starts.
    fn min_connections(&self) -> usize;

    /// Returns whether an idle session should be rotated before reuse.
    fn session_expired(&self, session: &Self::Session) -> bool;

    /// Opens and validates one session using freshly resolved credentials.
    async fn connect(&self, credentials: Credentials) -> Result<Self::Session, Self::Error>;

    /// Executes one bounded operator-authored query page.
    async fn execute_page(
        &self,
        session: &mut Self::Session,
        query: &CompiledQuery,
        limits: ExecutionLimits,
        cancel: CancellationToken,
    ) -> Result<RowPage, Self::Error>;

    /// Closes one adapter session.
    async fn shutdown(&self, session: Self::Session) -> Result<(), Self::Error>;

    /// Maps an adapter error to bounded retry and session-safety policy.
    fn classify_error(&self, error: &Self::Error) -> ErrorDisposition;
}
