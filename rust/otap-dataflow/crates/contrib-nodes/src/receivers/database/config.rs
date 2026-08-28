// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Database-neutral receiver configuration.

use otap_df_config::byte_units;
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

const DEFAULT_MAX_CONCURRENT_QUERIES: usize = 1;
const DEFAULT_MAX_BATCH_ROWS: usize = 1_000;
const DEFAULT_MAX_BATCH_BYTES: u64 = 8 * 1024 * 1024;
const DEFAULT_MAX_IN_FLIGHT_BYTES: u64 = 16 * 1024 * 1024;
const DEFAULT_FETCH_SIZE: usize = 100;
const MAX_QUERY_COUNT: usize = 1_024;

/// Complete configuration for one database receiver and its named query jobs.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DatabaseReceiverConfig<C> {
    /// Adapter-specific database connection configuration.
    pub(crate) database: C,
    /// Receiver-wide scheduling and memory limits.
    #[serde(default)]
    pub(crate) limits: LimitsConfig,
    /// Scheduling policy shared by all query jobs.
    #[serde(default)]
    pub(crate) scheduling: SchedulingConfig,
    /// Optional retry policy for transient failures.
    #[serde(default)]
    pub(crate) retry: RetryConfig,
    /// Operator-approved resource attributes emitted with database logs.
    #[serde(default)]
    pub(crate) resource_attributes: BTreeMap<String, Value>,
    /// Uniquely named polling jobs.
    pub(crate) queries: BTreeMap<String, QueryConfig>,
}

impl<C> DatabaseReceiverConfig<C> {
    /// Validates database-neutral receiver configuration.
    pub(crate) fn validate(&self) -> Result<(), String> {
        self.limits.validate()?;
        self.scheduling.validate()?;
        self.retry.validate()?;

        if self.queries.is_empty() {
            return Err("queries must contain at least one named query".to_owned());
        }
        if self.queries.len() > MAX_QUERY_COUNT {
            return Err(format!(
                "queries must contain no more than {MAX_QUERY_COUNT} entries"
            ));
        }

        for (name, query) in &self.queries {
            validate_query_name(name)?;
            query.validate(name, &self.limits)?;
        }
        validate_resource_attributes(&self.resource_attributes)
    }
}

/// Receiver-wide resource ceilings.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LimitsConfig {
    /// Maximum query executions active in this receiver.
    #[serde(default = "default_max_concurrent_queries")]
    pub(crate) max_concurrent_queries: usize,
    /// Maximum rows in one downstream batch.
    #[serde(default = "default_max_batch_rows")]
    pub(crate) max_batch_rows: usize,
    /// Maximum normalized bytes in one downstream batch.
    #[serde(default, deserialize_with = "byte_units::deserialize_u64")]
    max_batch_bytes: Option<u64>,
    /// Maximum normalized bytes reserved by all active polls.
    #[serde(default, deserialize_with = "byte_units::deserialize_u64")]
    max_in_flight_bytes: Option<u64>,
    /// Maximum driver rows prefetched per fetch operation.
    #[serde(default = "default_fetch_size")]
    pub(crate) fetch_size: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            max_concurrent_queries: DEFAULT_MAX_CONCURRENT_QUERIES,
            max_batch_rows: DEFAULT_MAX_BATCH_ROWS,
            max_batch_bytes: None,
            max_in_flight_bytes: None,
            fetch_size: DEFAULT_FETCH_SIZE,
        }
    }
}

impl LimitsConfig {
    /// Returns the normalized downstream batch-byte limit.
    pub(crate) fn max_batch_bytes(&self) -> u64 {
        self.max_batch_bytes.unwrap_or(DEFAULT_MAX_BATCH_BYTES)
    }

    /// Returns the receiver-local in-flight byte reservation limit.
    pub(crate) fn max_in_flight_bytes(&self) -> u64 {
        self.max_in_flight_bytes
            .unwrap_or(DEFAULT_MAX_IN_FLIGHT_BYTES)
    }

    /// Returns the maximum byte reservation for one poll.
    pub(crate) fn max_page_bytes(&self) -> u64 {
        self.max_in_flight_bytes() / self.max_concurrent_queries as u64
    }

    fn validate(&self) -> Result<(), String> {
        if self.max_concurrent_queries == 0 {
            return Err("limits.max_concurrent_queries must be greater than zero".to_owned());
        }
        if self.max_batch_rows == 0 {
            return Err("limits.max_batch_rows must be greater than zero".to_owned());
        }
        if self.max_batch_bytes() == 0 {
            return Err("limits.max_batch_bytes must be greater than zero".to_owned());
        }
        if self.max_in_flight_bytes() < self.max_batch_bytes() {
            return Err(
                "limits.max_in_flight_bytes must be at least limits.max_batch_bytes".to_owned(),
            );
        }
        let minimum_in_flight = self
            .max_batch_bytes()
            .checked_mul(self.max_concurrent_queries as u64)
            .ok_or_else(|| "configured limits overflow the byte accounting range".to_owned())?;
        if self.max_in_flight_bytes() < minimum_in_flight {
            return Err(
                "limits.max_in_flight_bytes must allow one batch per concurrent query".to_owned(),
            );
        }
        if self.fetch_size == 0 {
            return Err("limits.fetch_size must be greater than zero".to_owned());
        }
        Ok(())
    }
}

/// Shared scheduling behavior.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SchedulingConfig {
    /// Random delay applied before each query's first execution.
    #[serde(default, with = "humantime_serde")]
    pub(crate) startup_jitter_max: Duration,
}

impl SchedulingConfig {
    fn validate(&self) -> Result<(), String> {
        if self.startup_jitter_max > Duration::from_secs(3_600) {
            return Err("scheduling.startup_jitter_max must not exceed 1h".to_owned());
        }
        Ok(())
    }
}

/// Optional bounded exponential retry policy.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RetryConfig {
    /// Enables retries for transient transport and timeout failures.
    #[serde(default)]
    pub(crate) enabled: bool,
    /// Initial retry delay.
    #[serde(default = "default_initial_backoff", with = "humantime_serde")]
    pub(crate) initial_backoff: Duration,
    /// Exponential multiplier.
    #[serde(default = "default_backoff_multiplier")]
    pub(crate) multiplier: u32,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            initial_backoff: default_initial_backoff(),
            multiplier: default_backoff_multiplier(),
        }
    }
}

impl RetryConfig {
    fn validate(&self) -> Result<(), String> {
        if self.initial_backoff.is_zero() {
            return Err("retry.initial_backoff must be greater than zero".to_owned());
        }
        if self.multiplier < 2 {
            return Err("retry.multiplier must be at least 2".to_owned());
        }
        Ok(())
    }
}

/// One named snapshot query job.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct QueryConfig {
    /// Read-only operator-authored SQL.
    pub(crate) sql: String,
    /// Delay between completed query executions.
    #[serde(with = "humantime_serde")]
    pub(crate) interval: Duration,
    /// Query execution deadline.
    #[serde(with = "humantime_serde")]
    pub(crate) timeout: Duration,
    /// Snapshot pagination limits.
    #[serde(default)]
    pub(crate) pagination: PaginationConfig,
    /// Static typed bind values keyed by logical bind name.
    #[serde(default)]
    pub(crate) parameters: BTreeMap<String, Value>,
    /// OTLP body, attribute, and timestamp mapping.
    #[serde(default)]
    pub(crate) output: OutputConfig,
    /// Action for a permanent query-job failure.
    #[serde(default)]
    pub(crate) error_policy: ErrorPolicy,
}

impl QueryConfig {
    fn validate(&self, name: &str, limits: &LimitsConfig) -> Result<(), String> {
        if self.sql.trim().is_empty() {
            return Err(format!("queries.{name}.sql must not be empty"));
        }
        if !is_read_only_query(&self.sql) {
            return Err(format!("queries.{name}.sql must start with SELECT or WITH"));
        }
        if self.interval.is_zero() {
            return Err(format!("queries.{name}.interval must be greater than zero"));
        }
        if self.timeout.is_zero() {
            return Err(format!("queries.{name}.timeout must be greater than zero"));
        }
        if self.pagination.max_rows == 0 {
            return Err(format!(
                "queries.{name}.pagination.max_rows must be greater than zero"
            ));
        }
        if self.pagination.max_rows > limits.max_batch_rows.saturating_mul(1_024) {
            return Err(format!(
                "queries.{name}.pagination.max_rows exceeds the bounded page limit"
            ));
        }
        self.output.validate(name)?;
        validate_parameter_names(name, &self.parameters)
    }
}

/// Snapshot query row bound.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PaginationConfig {
    /// Hard client-side maximum rows per query execution.
    #[serde(default = "default_max_batch_rows")]
    pub(crate) max_rows: usize,
}

impl Default for PaginationConfig {
    fn default() -> Self {
        Self {
            max_rows: DEFAULT_MAX_BATCH_ROWS,
        }
    }
}

/// OTLP row mapping policy.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OutputConfig {
    /// Columns included in the JSON row body. Empty includes all columns.
    #[serde(default)]
    pub(crate) include_columns: Vec<String>,
    /// Source-column to OTLP attribute-name mappings.
    #[serde(default)]
    pub(crate) attributes: BTreeMap<String, String>,
    /// Optional source event-time column.
    #[serde(default)]
    pub(crate) timestamp: Option<TimestampConfig>,
    /// Behavior for a single row larger than the batch-byte limit.
    #[serde(default)]
    pub(crate) oversize_policy: OversizePolicy,
}

impl OutputConfig {
    fn validate(&self, query_name: &str) -> Result<(), String> {
        let mut included = BTreeSet::new();
        for column in &self.include_columns {
            let normalized = normalize_name(column)?;
            if !included.insert(normalized) {
                return Err(format!(
                    "queries.{query_name}.output.include_columns contains a duplicate column"
                ));
            }
        }

        let mut attribute_names = BTreeSet::new();
        for (column, attribute) in &self.attributes {
            _ = normalize_name(column)?;
            let normalized = normalize_name(attribute)?;
            if !attribute_names.insert(normalized) {
                return Err(format!(
                    "queries.{query_name}.output.attributes contains a duplicate target name"
                ));
            }
        }
        if let Some(timestamp) = &self.timestamp {
            _ = normalize_name(&timestamp.column)?;
        }
        Ok(())
    }
}

/// Source event-time configuration.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TimestampConfig {
    /// Result column containing the source event time.
    pub(crate) column: String,
}

/// Explicit behavior for a single oversized row.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum OversizePolicy {
    /// Fail conversion without truncating the value.
    #[default]
    Error,
    /// Admit one explicit oversized record as its own batch.
    AllowSingleRow,
}

/// Scope of a permanent query failure.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ErrorPolicy {
    /// Stop only the affected named query.
    StopQuery,
    /// Stop the receiver so the failure is visible to orchestration.
    #[default]
    StopReceiver,
}

fn default_max_concurrent_queries() -> usize {
    DEFAULT_MAX_CONCURRENT_QUERIES
}

fn default_max_batch_rows() -> usize {
    DEFAULT_MAX_BATCH_ROWS
}

fn default_fetch_size() -> usize {
    DEFAULT_FETCH_SIZE
}

fn default_initial_backoff() -> Duration {
    Duration::from_secs(1)
}

fn default_backoff_multiplier() -> u32 {
    2
}

fn validate_query_name(name: &str) -> Result<(), String> {
    if name.is_empty()
        || !name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || matches!(character, '_' | '-'))
    {
        return Err(format!(
            "query name '{name}' must contain only ASCII letters, digits, '_' or '-'"
        ));
    }
    Ok(())
}

fn validate_parameter_names(
    query_name: &str,
    parameters: &BTreeMap<String, Value>,
) -> Result<(), String> {
    for name in parameters.keys() {
        if name.is_empty()
            || !name
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            return Err(format!(
                "queries.{query_name}.parameters key '{name}' is not a valid logical bind name"
            ));
        }
    }
    Ok(())
}

fn validate_resource_attributes(attributes: &BTreeMap<String, Value>) -> Result<(), String> {
    const FORBIDDEN: [&str; 4] = [
        "db.connection_string",
        "db.endpoint",
        "server.address",
        "server.port",
    ];
    for (key, value) in attributes {
        _ = normalize_name(key)?;
        if FORBIDDEN.contains(&key.as_str()) {
            return Err(format!(
                "resource attribute '{key}' may expose database connection identity"
            ));
        }
        if !(value.is_string() || value.is_boolean() || value.is_number()) {
            return Err(format!(
                "resource attribute '{key}' must be a string, boolean, or number"
            ));
        }
    }
    Ok(())
}

fn normalize_name(value: &str) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() {
        Err("configured names must not be empty".to_owned())
    } else {
        Ok(value.to_ascii_lowercase())
    }
}

fn is_read_only_query(query: &str) -> bool {
    query.split_whitespace().next().is_some_and(|keyword| {
        keyword.eq_ignore_ascii_case("SELECT") || keyword.eq_ignore_ascii_case("WITH")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct TestConnection {
        endpoint: String,
    }

    /// Scenario: a receiver declares two independently scheduled snapshot queries.
    /// Guarantees: shared defaults are bounded and both named jobs pass semantic validation.
    #[test]
    fn validates_named_query_configuration() {
        let config: DatabaseReceiverConfig<TestConnection> =
            serde_json::from_value(serde_json::json!({
                "database": {"endpoint": "database"},
                "queries": {
                    "orders": {
                        "sql": "SELECT order_id FROM orders",
                        "interval": "30s",
                        "timeout": "10s"
                    },
                    "health": {
                        "sql": "SELECT status FROM health",
                        "interval": "5m",
                        "timeout": "10s"
                    }
                }
            }))
            .expect("configuration should deserialize");

        assert_eq!(config.database.endpoint, "database");
        assert_eq!(config.limits.max_batch_bytes(), 8 * 1024 * 1024);
        assert_eq!(config.queries.len(), 2);
        config.validate().expect("configuration should validate");
    }

    /// Scenario: receiver memory cannot reserve one batch for every permitted concurrent query.
    /// Guarantees: configuration rejects limits that could overcommit the in-flight byte budget.
    #[test]
    fn rejects_inconsistent_memory_limits() {
        let config: DatabaseReceiverConfig<TestConnection> =
            serde_json::from_value(serde_json::json!({
                "database": {"endpoint": "database"},
                "limits": {
                    "max_concurrent_queries": 2,
                    "max_batch_bytes": "8 MiB",
                    "max_in_flight_bytes": "8 MiB"
                },
                "queries": {
                    "orders": {
                        "sql": "SELECT order_id FROM orders",
                        "interval": "30s",
                        "timeout": "10s"
                    }
                }
            }))
            .expect("configuration should deserialize");

        assert!(config.validate().is_err());
    }
}
