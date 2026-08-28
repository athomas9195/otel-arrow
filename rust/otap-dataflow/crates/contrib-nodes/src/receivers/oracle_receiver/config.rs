// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Oracle connection, pool, TLS, and credential-reference configuration.

use serde::Deserialize;
use std::fmt;
use std::time::Duration;

const DEFAULT_PASSWORD_ENV: &str = "ORACLE_PWD";
const DEFAULT_MIN_CONNECTIONS: usize = 1;
const DEFAULT_MAX_CONNECTIONS: usize = 1;
const DEFAULT_CONNECTION_LIFETIME: Duration = Duration::from_secs(3_600);

/// Oracle adapter configuration nested under `database`.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OracleConnectionConfig {
    /// Oracle Easy Connect string or connect descriptor.
    pub(crate) connect_string: String,
    /// Dedicated read-only database principal.
    pub(crate) username: String,
    /// Environment variable containing the password.
    #[serde(default = "default_password_env")]
    pub(crate) password_env: String,
    /// Bounded worker pool policy.
    #[serde(default)]
    pub(crate) pool: OraclePoolConfig,
    /// Explicit Oracle Net TLS policy.
    #[serde(default)]
    pub(crate) tls: OracleTlsConfig,
}

impl OracleConnectionConfig {
    /// Validates Oracle-specific connection and security settings.
    pub(crate) fn validate(&self) -> Result<(), String> {
        required("database.connect_string", &self.connect_string)?;
        required("database.username", &self.username)?;
        required("database.password_env", &self.password_env)?;
        self.pool.validate()?;
        self.tls.validate(&self.connect_string)
    }

    /// Produces an Oracle Easy Connect string with explicit TLS properties.
    pub(crate) fn resolved_connect_string(&self) -> String {
        self.tls.apply(&self.connect_string)
    }
}

impl fmt::Debug for OracleConnectionConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OracleConnectionConfig")
            .field("connect_string", &"<redacted>")
            .field("username", &self.username)
            .field("password_env", &self.password_env)
            .field("pool", &self.pool)
            .field("tls", &self.tls)
            .finish()
    }
}

/// Oracle dedicated-worker pool limits.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OraclePoolConfig {
    /// Sessions validated before the receiver begins polling.
    #[serde(default = "default_min_connections")]
    pub(crate) min_connections: usize,
    /// Maximum dedicated Oracle connection workers.
    #[serde(default = "default_max_connections")]
    pub(crate) max_connections: usize,
    /// Maximum age before a connection is replaced to pick up rotated credentials/certificates.
    #[serde(default = "default_connection_lifetime", with = "humantime_serde")]
    pub(crate) max_connection_lifetime: Duration,
}

impl Default for OraclePoolConfig {
    fn default() -> Self {
        Self {
            min_connections: DEFAULT_MIN_CONNECTIONS,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_connection_lifetime: DEFAULT_CONNECTION_LIFETIME,
        }
    }
}

impl OraclePoolConfig {
    fn validate(&self) -> Result<(), String> {
        if self.max_connections == 0 {
            return Err("database.pool.max_connections must be greater than zero".to_owned());
        }
        if self.min_connections > self.max_connections {
            return Err("database.pool.min_connections must not exceed max_connections".to_owned());
        }
        if self.max_connection_lifetime.is_zero() {
            return Err(
                "database.pool.max_connection_lifetime must be greater than zero".to_owned(),
            );
        }
        Ok(())
    }
}

/// Oracle Net TLS identity-verification policy.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OracleTlsConfig {
    /// Explicit encryption and identity-verification mode.
    #[serde(default)]
    pub(crate) mode: OracleTlsMode,
    /// Oracle wallet containing trusted CA certificates.
    #[serde(default)]
    pub(crate) wallet_location: Option<String>,
}

impl OracleTlsConfig {
    fn validate(&self, connect_string: &str) -> Result<(), String> {
        match self.mode {
            OracleTlsMode::Disabled => Ok(()),
            OracleTlsMode::VerifyFull | OracleTlsMode::Insecure => {
                if !connect_string.to_ascii_lowercase().starts_with("tcps://") {
                    return Err(
                        "database.tls requires a tcps:// Easy Connect string so TLS properties can be applied"
                            .to_owned(),
                    );
                }
                if self.mode == OracleTlsMode::VerifyFull {
                    let wallet = self.wallet_location.as_deref().ok_or_else(|| {
                        "database.tls.wallet_location is required for verify_full".to_owned()
                    })?;
                    required("database.tls.wallet_location", wallet)?;
                }
                Ok(())
            }
        }
    }

    fn apply(&self, connect_string: &str) -> String {
        if !connect_string.to_ascii_lowercase().starts_with("tcps://") {
            return connect_string.to_owned();
        }
        let separator = if connect_string.contains('?') {
            '&'
        } else {
            '?'
        };
        match self.mode {
            OracleTlsMode::Disabled => connect_string.to_owned(),
            OracleTlsMode::VerifyFull => format!(
                "{connect_string}{separator}wallet_location={}&ssl_server_dn_match=yes",
                urlencoding::encode(
                    self.wallet_location
                        .as_deref()
                        .expect("verify_full validation requires a wallet")
                )
            ),
            OracleTlsMode::Insecure => {
                format!("{connect_string}{separator}ssl_server_dn_match=no")
            }
        }
    }
}

/// Explicit Oracle TLS mode.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OracleTlsMode {
    /// Plain Oracle Net connection.
    #[default]
    Disabled,
    /// TCPS with trusted wallet and server identity verification.
    VerifyFull,
    /// TCPS encryption without server identity verification.
    Insecure,
}

fn required(name: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        Err(format!("{name} must not be empty"))
    } else {
        Ok(())
    }
}

fn default_password_env() -> String {
    DEFAULT_PASSWORD_ENV.to_owned()
}

fn default_min_connections() -> usize {
    DEFAULT_MIN_CONNECTIONS
}

fn default_max_connections() -> usize {
    DEFAULT_MAX_CONNECTIONS
}

fn default_connection_lifetime() -> Duration {
    DEFAULT_CONNECTION_LIFETIME
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: production Oracle TLS requests full server identity verification.
    /// Guarantees: validation requires TCPS and a trusted Oracle wallet.
    #[test]
    fn verify_full_requires_tcps_and_wallet() {
        let tls = OracleTlsConfig {
            mode: OracleTlsMode::VerifyFull,
            wallet_location: None,
        };
        assert!(tls.validate("//localhost:1521/FREEPDB1").is_err());
        assert!(tls.validate("tcps://localhost:2484/FREEPDB1").is_err());

        let tls = OracleTlsConfig {
            mode: OracleTlsMode::VerifyFull,
            wallet_location: Some("C:\\wallet".to_owned()),
        };
        assert!(tls.validate("tcps://localhost:2484/FREEPDB1").is_ok());
        let resolved = tls.apply("tcps://localhost:2484/FREEPDB1");
        assert!(resolved.contains("ssl_server_dn_match=yes"));
    }
}
