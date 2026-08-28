// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Compiled database query plans and typed logical bind values.

use super::config::{ErrorPolicy, OutputConfig, QueryConfig};
use super::row::CellValue;
use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

/// Immutable database-neutral plan for one named query job.
#[derive(Clone)]
pub(crate) struct CompiledQuery {
    /// Stable configured query identity.
    pub(crate) name: String,
    /// Read-only operator-authored SQL.
    pub(crate) sql: String,
    /// Delay between completed polls.
    pub(crate) interval: Duration,
    /// Per-execution deadline.
    pub(crate) timeout: Duration,
    /// Hard client-side row limit.
    pub(crate) max_rows: usize,
    /// Static logical bind values.
    pub(crate) binds: Vec<BindValue>,
    /// Shared OTLP mapping policy.
    pub(crate) output: OutputConfig,
    /// Permanent failure scope.
    pub(crate) error_policy: ErrorPolicy,
}

impl CompiledQuery {
    /// Compiles one validated query configuration into an adapter-ready plan.
    pub(crate) fn compile(name: String, config: QueryConfig) -> Result<Self, QueryCompileError> {
        let binds = config
            .parameters
            .into_iter()
            .map(|(name, value)| {
                Ok(BindValue {
                    value: bind_value(&name, value)?,
                    name,
                })
            })
            .collect::<Result<Vec<_>, QueryCompileError>>()?;

        Ok(Self {
            name,
            sql: config.sql,
            interval: config.interval,
            timeout: config.timeout,
            max_rows: config.pagination.max_rows,
            binds,
            output: config.output,
            error_policy: config.error_policy,
        })
    }
}

impl fmt::Debug for CompiledQuery {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CompiledQuery")
            .field("name", &self.name)
            .field("sql", &"<redacted>")
            .field("interval", &self.interval)
            .field("timeout", &self.timeout)
            .field("max_rows", &self.max_rows)
            .field("bind_count", &self.binds.len())
            .field("output", &self.output)
            .field("error_policy", &self.error_policy)
            .finish()
    }
}

/// One logical named parameter whose value remains separate from SQL text.
#[derive(Clone)]
pub(crate) struct BindValue {
    /// Logical bind name.
    pub(crate) name: String,
    /// Database-neutral typed value.
    pub(crate) value: CellValue,
}

/// Hard execution bounds enforced independently of operator-authored SQL.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ExecutionLimits {
    /// Maximum rows returned by this query execution.
    pub(crate) max_rows: usize,
    /// Maximum rows placed in one downstream batch.
    pub(crate) max_batch_rows: usize,
    /// Maximum normalized bytes placed in one downstream batch.
    pub(crate) max_batch_bytes: u64,
    /// Maximum normalized bytes materialized by this execution.
    pub(crate) max_page_bytes: u64,
    /// Maximum rows prefetched by the driver.
    pub(crate) fetch_size: usize,
}

/// Failure while compiling static query configuration.
#[derive(Debug, thiserror::Error)]
pub(crate) enum QueryCompileError {
    /// SQL NULL has no source type, so adapters cannot bind it portably.
    #[error("bind parameter '{name}' is null and requires an explicit database type")]
    UntypedNullBind {
        /// Logical parameter name.
        name: String,
    },
    /// Static parameters must be scalar values with explicit conversion behavior.
    #[error("bind parameter '{name}' must be a scalar JSON value")]
    UnsupportedBindValue {
        /// Logical parameter name.
        name: String,
    },
    /// JSON numbers outside supported typed forms are not silently rounded.
    #[error("bind parameter '{name}' is outside the supported numeric range")]
    NumericBindOverflow {
        /// Logical parameter name.
        name: String,
    },
}

fn bind_value(name: &str, value: serde_json::Value) -> Result<CellValue, QueryCompileError> {
    match value {
        serde_json::Value::Null => Err(QueryCompileError::UntypedNullBind {
            name: name.to_owned(),
        }),
        serde_json::Value::Bool(value) => Ok(CellValue::Bool(value)),
        serde_json::Value::String(value) => Ok(CellValue::String(value)),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Ok(CellValue::Int64(value))
            } else if let Some(value) = value.as_u64() {
                Ok(CellValue::UInt64(value))
            } else if let Some(value) = value.as_f64().filter(|value| value.is_finite()) {
                Ok(CellValue::Float64(value))
            } else {
                Err(QueryCompileError::NumericBindOverflow {
                    name: name.to_owned(),
                })
            }
        }
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => {
            Err(QueryCompileError::UnsupportedBindValue {
                name: name.to_owned(),
            })
        }
    }
}

/// Compiles every configured query while retaining stable map order.
pub(crate) fn compile_queries(
    queries: BTreeMap<String, QueryConfig>,
) -> Result<Vec<CompiledQuery>, QueryCompileError> {
    queries
        .into_iter()
        .map(|(name, config)| CompiledQuery::compile(name, config))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: one query contains an unsupported structured bind value.
    /// Guarantees: the compile error identifies the bind rather than incorrectly naming the query.
    #[test]
    fn compile_error_identifies_bind_name() {
        let config = serde_json::from_value(serde_json::json!({
            "sql": "SELECT 1 FROM DUAL WHERE :customer_id = 1",
            "interval": "30s",
            "timeout": "10s",
            "parameters": {"customer_id": [42]}
        }))
        .expect("query config");

        let error = CompiledQuery::compile("customers".to_owned(), config)
            .expect_err("array bind should fail");
        assert!(matches!(
            error,
            QueryCompileError::UnsupportedBindValue { name } if name == "customer_id"
        ));
    }

    /// Scenario: a compiled query is formatted for diagnostics.
    /// Guarantees: SQL text and bind values are not exposed through Debug output.
    #[test]
    fn debug_output_redacts_sql_and_bind_values() {
        let config = serde_json::from_value(serde_json::json!({
            "sql": "SELECT * FROM secret_table WHERE token = :token",
            "interval": "30s",
            "timeout": "10s",
            "parameters": {"token": "sensitive-value"}
        }))
        .expect("query config");
        let query =
            CompiledQuery::compile("secrets".to_owned(), config).expect("query should compile");

        let debug = format!("{query:?}");
        assert!(!debug.contains("secret_table"));
        assert!(!debug.contains("sensitive-value"));
        assert!(debug.contains("<redacted>"));
    }
}
