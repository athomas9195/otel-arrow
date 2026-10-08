// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Strict native configuration and restart compatibility identity.

use super::{
    adapter::{Error, Result},
    query::Plan,
};
use otel_arrow_dfe_scraper::database::{
    CatchUpConfig, CheckpointConfig, CompiledQuery, OnNack, OnPermanentNack, OutputConfig,
    PollingConfig, TieBreakerCursorConfig, TimestampCursorConfig, WatermarkConfig,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, net::IpAddr, time::Duration};

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
    #[serde(default = "port")]
    pub port: u16,
    pub database: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Query {
    pub statement: String,
    #[serde(default = "interval", with = "humantime_serde")]
    pub interval: Duration,
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
    #[serde(default = "timestamp_bind")]
    pub bind: String,
    pub initial: String,
    pub timezone: String,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Tie {
    pub column: String,
    #[serde(default = "tie_bind")]
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
    #[serde(default = "rewind")]
    pub on_nack: OnNack,
    #[serde(default)]
    pub on_permanent_nack: OnPermanentNack,
    #[serde(default = "backoff", with = "humantime_serde")]
    pub nack_backoff: Duration,
    #[serde(default = "failures")]
    pub max_consecutive_failures: u32,
}

fn port() -> u16 {
    5432
}
fn interval() -> Duration {
    Duration::from_secs(60)
}
fn backoff() -> Duration {
    Duration::from_secs(1)
}
fn failures() -> u32 {
    5
}
fn rewind() -> OnNack {
    OnNack::Rewind
}
fn timestamp_bind() -> String {
    "last_timestamp".into()
}
fn tie_bind() -> String {
    "last_tie_breaker".into()
}

pub(crate) struct Validated {
    pub config: PostgreSqlReceiverConfig,
    pub common: CompiledQuery,
    pub plan: Plan,
    pub fingerprint: String,
}

pub(crate) fn name(value: &str, maximum: usize) -> Result<()> {
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
        name(&self.source_id, 128)?;
        name(&self.connection.database, 63)?;
        let conn = &mut self.connection;
        name(&conn.host, 253)?;
        if conn.port == 0 {
            return Err(Error::Config);
        }
        if conn.host.parse::<IpAddr>().is_err() {
            if !conn.host.is_ascii()
                || conn.host.split('.').any(|label| {
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
            conn.host.make_ascii_lowercase();
        }
        let q = &self.query;
        if !(60..=86400).contains(&q.interval.as_secs()) || q.interval.subsec_nanos() != 0 {
            return Err(Error::Config);
        }
        if q.result_schema.is_empty() || q.result_schema.len() > 128 {
            return Err(Error::Config);
        }
        let mut names = HashSet::new();
        for column in &q.result_schema {
            name(&column.name, 63)?;
            if !names.insert(column.name.to_ascii_lowercase()) || column.type_modifier < -1 {
                return Err(Error::Config);
            }
            let _ = super::value::native_type(&column.source_type)?;
        }
        let Watermark::Composite {
            timestamp,
            tie_breaker,
        } = &self.watermark;
        name(&timestamp.column, 63)?;
        name(&tie_breaker.column, 63)?;
        if timestamp.timezone != "UTC" || !timestamp.initial.ends_with('Z') {
            return Err(Error::Config);
        }
        let find = |name: &str| {
            q.result_schema
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(name))
                .ok_or(Error::Config)
        };
        let ts = find(&timestamp.column)?;
        let tie = find(&tie_breaker.column)?;
        if ts == tie || q.result_schema[ts].nullable || q.result_schema[tie].nullable {
            return Err(Error::Config);
        }
        let ts_type = &q.result_schema[ts];
        if !matches!(ts_type.source_type.as_str(), "timestamp" | "timestamptz")
            || !matches!(
                q.result_schema[tie].source_type.as_str(),
                "int2" | "int4" | "int8"
            )
        {
            return Err(Error::Config);
        }
        let _ = super::value::cursor_time(&timestamp.initial, ts_type.type_modifier)?;
        super::value::check_tie(tie_breaker.initial, &q.result_schema[tie].source_type)?;
        if let Some(output) = &q.output.timestamp_column {
            name(output, 63)?;
            if !matches!(
                q.result_schema[find(output)?].source_type.as_str(),
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
            q.statement.clone(),
            PollingConfig {
                interval: q.interval,
                timeout: Duration::from_secs(30),
                max_rows_per_poll: 1000,
                fetch_size_rows: 300,
                max_batch_bytes: 8 * 1024 * 1024,
                catch_up: CatchUpConfig::default(),
            },
            &watermark,
            &checkpoint,
            OutputConfig {
                timestamp_column: q.output.timestamp_column.clone(),
                validation_columns: vec![],
            },
        )
        .map_err(|_| Error::Config)?;
        let plan = Plan::compile(&common, &q.result_schema, ts, tie)?;
        let identity = serde_json::to_vec(&(
            "postgresql/v3",
            "postgresql",
            &self.connection.host,
            self.connection.port,
            &self.connection.database,
            &q.statement,
            "named-parameters/v2",
            &self.watermark,
            &q.output,
            &q.result_schema,
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
