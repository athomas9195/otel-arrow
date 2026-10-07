// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Closed diagnostic vocabulary; native errors never escape this module.

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum Error {
    #[error("postgresql: invalid configuration")]
    Config,
    #[error("postgresql: unsupported SQL contract")]
    Sql,
    #[error("postgresql: incompatible source metadata")]
    Metadata,
    #[error("postgresql: invalid or unsupported source value")]
    Value,
    #[error("postgresql: protocol or memory limit exceeded")]
    Limit,
    #[error("postgresql: invalid credential or trust material")]
    Credential,
    #[error("postgresql: TLS verification or negotiation failed")]
    Tls,
    #[error("postgresql: database operation rejected")]
    Database,
    #[error("postgresql: transport unavailable after cleanup")]
    Unavailable,
    #[error("postgresql: cancelled operation stopped")]
    Cancelled,
    #[error("postgresql: cleanup unconfirmed; source requires process restart")]
    Cleanup,
}

pub(crate) type Result<T> = std::result::Result<T, Error>;

pub(crate) fn database(error: tokio_postgres::Error) -> Error {
    if let Some(code) = error.code() {
        if code.code() == "57014" {
            Error::Cancelled
        } else if code.code().starts_with("08")
            || matches!(code.code(), "57P01" | "57P02" | "57P03")
        {
            Error::Unavailable
        } else {
            Error::Database
        }
    } else if error.is_closed() {
        Error::Unavailable
    } else {
        // Only actual transport I/O is retryable. TLS errors fail closed.
        use std::error::Error as _;
        let mut source = error.source();
        while let Some(value) = source {
            if value.is::<rustls::Error>() {
                return Error::Tls;
            }
            if let Some(io) = value.downcast_ref::<std::io::Error>()
                && matches!(
                    io.kind(),
                    std::io::ErrorKind::ConnectionRefused
                        | std::io::ErrorKind::ConnectionReset
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::NotConnected
                        | std::io::ErrorKind::TimedOut
                        | std::io::ErrorKind::UnexpectedEof
                )
            {
                return Error::Unavailable;
            }
            source = value.source();
        }
        Error::Database
    }
}
