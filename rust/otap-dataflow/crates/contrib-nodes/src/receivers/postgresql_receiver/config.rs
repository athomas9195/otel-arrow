// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Strict native configuration and restart compatibility identity.

use super::{
    adapter::{Error, Result},
    query::{MAX_PAGE_ROWS, Plan},
};
use otel_arrow_dfe_scraper::database::{
    CatchUpConfig, CheckpointConfig, CompiledQuery, OnNack, OnPermanentNack, OutputConfig,
    PollingConfig, TieBreakerCursorConfig, TimestampCursorConfig, WatermarkConfig,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, net::IpAddr, time::Duration};

pub(super) const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_BATCH_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PostgreSqlReceiverConfig {
    pub source_id: String,
    pub connection: Connection,
    pub query: Query,
    pub watermark: Watermark,
    pub checkpoint: Checkpoint,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Connection {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub database: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Query {
    pub statement: String,
    #[serde(default = "default_interval", with = "humantime_serde")]
    pub interval: Duration,
    #[serde(default = "default_fetch_size_rows")]
    pub fetch_size_rows: usize,
    #[serde(default)]
    pub catch_up: CatchUpConfig,
    pub result_schema: Vec<ExpectedColumn>,
    #[serde(default)]
    pub output: Output,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExpectedColumn {
    pub name: String,
    pub source_type: String,
    pub nullable: bool,
    pub type_modifier: i32,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Watermark {
    Composite {
        timestamp: Timestamp,
        tie_breaker: Tie,
    },
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Timestamp {
    pub column: String,
    #[serde(default = "default_timestamp_bind")]
    pub bind: String,
    pub initial: String,
    pub timezone: String,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Tie {
    pub column: String,
    #[serde(default = "default_tie_breaker_bind")]
    pub bind: String,
    pub initial: i64,
}
#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Output {
    pub timestamp_column: Option<String>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Checkpoint {
    pub directory: String,
    #[serde(default = "default_on_nack")]
    pub on_nack: OnNack,
    #[serde(default)]
    pub on_permanent_nack: OnPermanentNack,
    #[serde(default = "default_nack_backoff", with = "humantime_serde")]
    pub nack_backoff: Duration,
    #[serde(default = "default_max_consecutive_failures")]
    pub max_consecutive_failures: u32,
}

const fn default_port() -> u16 {
    5432
}
const fn default_interval() -> Duration {
    Duration::from_secs(60)
}
const fn default_fetch_size_rows() -> usize {
    300
}

const fn default_nack_backoff() -> Duration {
    Duration::from_secs(1)
}
const fn default_max_consecutive_failures() -> u32 {
    5
}
const fn default_on_nack() -> OnNack {
    OnNack::Rewind
}
fn default_timestamp_bind() -> String {
    "last_timestamp".into()
}
fn default_tie_breaker_bind() -> String {
    "last_tie_breaker".into()
}

pub(crate) struct Validated {
    pub config: PostgreSqlReceiverConfig,
    pub common: CompiledQuery,
    pub plan: Plan,
    pub fingerprint: String,
}

pub(crate) fn validate_name(value: &str, maximum: usize) -> Result<()> {
    if value.is_empty() || value.len() > maximum || value.chars().any(char::is_control) {
        return Err(Error::Config);
    }
    Ok(())
}

impl PostgreSqlReceiverConfig {
    pub fn parse(value: &serde_json::Value) -> Result<Self> {
        fn has_null(value: &serde_json::Value) -> bool {
            match value {
                serde_json::Value::Null => true,
                serde_json::Value::Array(values) => values.iter().any(has_null),
                serde_json::Value::Object(values) => values.values().any(has_null),
                _ => false,
            }
        }
        if has_null(value) {
            return Err(Error::Config);
        }
        serde_json::from_value(value.clone()).map_err(|_| Error::Config)
    }

    pub fn validate(mut self) -> Result<Validated> {
        validate_name(&self.source_id, 128)?;
        validate_name(&self.connection.database, 63)?;
        let connection = &mut self.connection;
        validate_name(&connection.host, 253)?;
        if connection.port == 0 {
            return Err(Error::Config);
        }
        if connection.host.parse::<IpAddr>().is_err() {
            if !connection.host.is_ascii()
                || connection.host.split('.').any(|label| {
                    label.is_empty()
                        || label.len() > 63
                        || label.starts_with('-')
                        || label.ends_with('-')
                        || !label
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b == b'-')
                })
            {
                return Err(Error::Config);
            }
            connection.host.make_ascii_lowercase();
        }
        let query = &self.query;
        if !(60..=86400).contains(&query.interval.as_secs()) || query.interval.subsec_nanos() != 0 {
            return Err(Error::Config);
        }
        if query.result_schema.is_empty() || query.result_schema.len() > 128 {
            return Err(Error::Config);
        }
        let mut names = HashSet::new();
        for column in &query.result_schema {
            validate_name(&column.name, 63)?;
            if !names.insert(column.name.to_ascii_lowercase()) || column.type_modifier < -1 {
                return Err(Error::Config);
            }
            let _ = super::value::native_type(&column.source_type)?;
        }
        let Watermark::Composite {
            timestamp,
            tie_breaker,
        } = &self.watermark;
        validate_name(&timestamp.column, 63)?;
        validate_name(&tie_breaker.column, 63)?;
        if timestamp.timezone != "UTC" || !timestamp.initial.ends_with('Z') {
            return Err(Error::Config);
        }
        let column_index = |name: &str| {
            query
                .result_schema
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(name))
                .ok_or(Error::Config)
        };
        let timestamp_index = column_index(&timestamp.column)?;
        let tie_breaker_index = column_index(&tie_breaker.column)?;
        if timestamp_index == tie_breaker_index
            || query.result_schema[timestamp_index].nullable
            || query.result_schema[tie_breaker_index].nullable
        {
            return Err(Error::Config);
        }
        let timestamp_column = &query.result_schema[timestamp_index];
        if !matches!(
            timestamp_column.source_type.as_str(),
            "timestamp" | "timestamptz"
        ) || !matches!(
            query.result_schema[tie_breaker_index].source_type.as_str(),
            "int2" | "int4" | "int8"
        ) {
            return Err(Error::Config);
        }
        let _ = super::value::cursor_time(&timestamp.initial, timestamp_column.type_modifier)?;
        super::value::check_tie(
            tie_breaker.initial,
            &query.result_schema[tie_breaker_index].source_type,
        )?;
        if let Some(output) = &query.output.timestamp_column {
            validate_name(output, 63)?;
            if !matches!(
                query.result_schema[column_index(output)?]
                    .source_type
                    .as_str(),
                "timestamp" | "timestamptz"
            ) {
                return Err(Error::Config);
            }
        }
        let checkpoint = CheckpointConfig {
            directory: self.checkpoint.directory.clone(),
            on_nack: self.checkpoint.on_nack,
            on_permanent_nack: self.checkpoint.on_permanent_nack,
            nack_backoff: self.checkpoint.nack_backoff,
            max_consecutive_failures: self.checkpoint.max_consecutive_failures,
        };
        let watermark = WatermarkConfig::Composite {
            timestamp: TimestampCursorConfig {
                column: timestamp.column.clone(),
                bind: timestamp.bind.clone(),
                initial: timestamp.initial.clone(),
                timezone: timestamp.timezone.clone(),
            },
            tie_breaker: TieBreakerCursorConfig {
                column: tie_breaker.column.clone(),
                bind: tie_breaker.bind.clone(),
                initial: tie_breaker.initial,
            },
        };
        let common = CompiledQuery::compile(
            query.statement.clone(),
            PollingConfig {
                interval: query.interval,
                timeout: OPERATION_TIMEOUT,
                max_rows_per_poll: MAX_PAGE_ROWS,
                fetch_size_rows: query.fetch_size_rows,
                max_batch_bytes: MAX_BATCH_BYTES,
                catch_up: query.catch_up,
            },
            &watermark,
            &checkpoint,
            OutputConfig {
                timestamp_column: query.output.timestamp_column.clone(),
                validation_columns: vec![],
            },
        )
        .map_err(|_| Error::Config)?;
        let plan = Plan::compile(
            &common,
            &query.result_schema,
            timestamp_index,
            tie_breaker_index,
        )?;
        let identity = serde_json::to_vec(&(
            "postgresql/v3",
            "postgresql",
            &self.connection.host,
            self.connection.port,
            &self.connection.database,
            &query.statement,
            "named-parameters/v2",
            &self.watermark,
            &query.output,
            &query.result_schema,
            "UTC/microseconds/native-v2",
        ))
        .map_err(|_| Error::Config)?;
        let fingerprint = blake3::hash(&identity).to_hex().to_string();
        Ok(Validated {
            config: self,
            common,
            plan,
            fingerprint,
        })
    }
}
