// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Shared typed database-row to OTLP log conversion.

use super::config::OutputConfig;
use super::driver::DatabaseSystem;
use super::row::{CellValue, ColumnMetadata, Row, RowBatch};
use base64::Engine;
use chrono::{DateTime, NaiveDateTime, Utc};
use otap_df_otap::pdata::OtapPdata;
use otap_df_pdata::proto::OtlpProtoMessage;
use otap_df_pdata::proto::opentelemetry::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList, any_value,
};
use otap_df_pdata::proto::opentelemetry::logs::v1::{
    LogRecord, LogsData, ResourceLogs, ScopeLogs, SeverityNumber,
};
use otap_df_pdata::proto::opentelemetry::resource::v1::Resource;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

const QUERY_NAME_ATTRIBUTE: &str = "receiver.database.query.name";
const DATABASE_SCOPE: &str = "otel-arrow.database_receiver";

/// Shared immutable context for OTLP row mapping.
pub(crate) struct OtlpMapping<'a> {
    /// Database vendor.
    pub(crate) system: DatabaseSystem,
    /// Stable configured query name.
    pub(crate) query_name: &'a str,
    /// Query output policy.
    pub(crate) output: &'a OutputConfig,
    /// Operator-approved resource attributes.
    pub(crate) resource_attributes: &'a BTreeMap<String, Value>,
    /// Receiver observation timestamp.
    pub(crate) observed_time_unix_nano: u64,
}

/// Converts one bounded row batch into OTAP pipeline data.
pub(crate) fn rows_to_pdata(
    batch: RowBatch,
    mapping: OtlpMapping<'_>,
) -> Result<OtapPdata, OtlpMappingError> {
    validate_mapping(&batch.columns, mapping.output)?;
    let logs = rows_to_logs(batch, mapping)?;
    let payload = OtlpProtoMessage::Logs(logs).try_into()?;
    Ok(OtapPdata::new_todo_context(payload))
}

fn rows_to_logs(batch: RowBatch, mapping: OtlpMapping<'_>) -> Result<LogsData, OtlpMappingError> {
    let _normalized_bytes = batch.normalized_bytes;
    let records = batch
        .rows
        .iter()
        .map(|row| row_to_log(row, &batch.columns, &mapping))
        .collect::<Result<Vec<_>, OtlpMappingError>>()?;

    let mut resource_attributes = vec![KeyValue {
        key: "db.system.name".to_owned(),
        value: Some(string_value(mapping.system.as_str())),
    }];
    for (key, value) in mapping.resource_attributes {
        resource_attributes.push(KeyValue {
            key: key.clone(),
            value: Some(json_scalar_to_any(value)?),
        });
    }

    Ok(LogsData {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: resource_attributes,
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: DATABASE_SCOPE.to_owned(),
                    version: env!("CARGO_PKG_VERSION").to_owned(),
                    ..Default::default()
                }),
                log_records: records,
                ..Default::default()
            }],
            ..Default::default()
        }],
    })
}

fn row_to_log(
    row: &Row,
    columns: &[ColumnMetadata],
    mapping: &OtlpMapping<'_>,
) -> Result<LogRecord, OtlpMappingError> {
    let included = mapping
        .output
        .include_columns
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    let include_all = included.is_empty();
    let mut body = Vec::new();
    let mut attributes = vec![KeyValue {
        key: QUERY_NAME_ATTRIBUTE.to_owned(),
        value: Some(string_value(mapping.query_name)),
    }];
    let mut event_time = None;

    for (column, cell) in columns.iter().zip(&row.values) {
        let normalized = column.name.to_ascii_lowercase();
        if include_all || included.contains(&normalized) {
            body.push(KeyValue {
                key: column.name.clone(),
                value: Some(cell_to_body_any(cell)?),
            });
        }
        if let Some((_, attribute_name)) = mapping
            .output
            .attributes
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(&column.name))
        {
            if !matches!(cell, CellValue::Null) {
                attributes.push(KeyValue {
                    key: attribute_name.clone(),
                    value: Some(cell_to_any(cell)?),
                });
            }
        }
        if mapping
            .output
            .timestamp
            .as_ref()
            .is_some_and(|timestamp| timestamp.column.eq_ignore_ascii_case(&column.name))
            && !matches!(cell, CellValue::Null)
        {
            event_time = Some(parse_event_time(cell, &column.name)?);
        }
    }

    Ok(LogRecord {
        time_unix_nano: event_time.unwrap_or(mapping.observed_time_unix_nano),
        observed_time_unix_nano: mapping.observed_time_unix_nano,
        severity_number: SeverityNumber::Info as i32,
        severity_text: "INFO".to_owned(),
        body: Some(AnyValue {
            value: Some(any_value::Value::KvlistValue(KeyValueList { values: body })),
        }),
        attributes,
        event_name: "database.query.row".to_owned(),
        ..Default::default()
    })
}

fn validate_mapping(
    columns: &[ColumnMetadata],
    output: &OutputConfig,
) -> Result<(), OtlpMappingError> {
    let available = columns
        .iter()
        .map(|column| column.name.to_ascii_lowercase())
        .collect::<BTreeSet<_>>();
    for name in &output.include_columns {
        if !available.contains(&name.to_ascii_lowercase()) {
            return Err(OtlpMappingError::UnknownColumn { name: name.clone() });
        }
    }
    for name in output.attributes.keys() {
        if !available.contains(&name.to_ascii_lowercase()) {
            return Err(OtlpMappingError::UnknownColumn { name: name.clone() });
        }
    }
    if let Some(timestamp) = &output.timestamp
        && !available.contains(&timestamp.column.to_ascii_lowercase())
    {
        return Err(OtlpMappingError::UnknownColumn {
            name: timestamp.column.clone(),
        });
    }
    Ok(())
}

fn cell_to_body_any(value: &CellValue) -> Result<AnyValue, OtlpMappingError> {
    match value {
        CellValue::Json(value) => json_to_any(&serde_json::from_str(value)?),
        _ => cell_to_any(value),
    }
}

fn json_to_any(value: &Value) -> Result<AnyValue, OtlpMappingError> {
    match value {
        Value::Null => Ok(AnyValue::default()),
        Value::Bool(value) => Ok(AnyValue {
            value: Some(any_value::Value::BoolValue(*value)),
        }),
        Value::String(value) => Ok(string_value(value)),
        Value::Number(value) if value.is_i64() => Ok(int_value(
            value
                .as_i64()
                .ok_or(OtlpMappingError::InvalidResourceAttribute)?,
        )),
        Value::Number(value) if value.is_u64() => {
            let value = value
                .as_u64()
                .ok_or(OtlpMappingError::InvalidResourceAttribute)?;
            i64::try_from(value)
                .map(int_value)
                .or_else(|_| Ok(string_value(value.to_string())))
        }
        Value::Number(value) => Ok(AnyValue {
            value: Some(any_value::Value::DoubleValue(
                value
                    .as_f64()
                    .filter(|value| value.is_finite())
                    .ok_or(OtlpMappingError::NonFiniteFloat)?,
            )),
        }),
        Value::Array(values) => Ok(AnyValue {
            value: Some(any_value::Value::ArrayValue(ArrayValue {
                values: values
                    .iter()
                    .map(json_to_any)
                    .collect::<Result<Vec<_>, _>>()?,
            })),
        }),
        Value::Object(values) => Ok(AnyValue {
            value: Some(any_value::Value::KvlistValue(KeyValueList {
                values: values
                    .iter()
                    .map(|(key, value)| {
                        Ok(KeyValue {
                            key: key.clone(),
                            value: Some(json_to_any(value)?),
                        })
                    })
                    .collect::<Result<Vec<_>, OtlpMappingError>>()?,
            })),
        }),
    }
}

fn cell_to_any(value: &CellValue) -> Result<AnyValue, OtlpMappingError> {
    match value {
        CellValue::Null => Ok(AnyValue::default()),
        CellValue::Bool(value) => Ok(AnyValue {
            value: Some(any_value::Value::BoolValue(*value)),
        }),
        CellValue::Int64(value) => Ok(int_value(*value)),
        CellValue::UInt64(value) => i64::try_from(*value)
            .map(int_value)
            .or_else(|_| Ok(string_value(value.to_string()))),
        CellValue::Float64(value) if value.is_finite() => Ok(AnyValue {
            value: Some(any_value::Value::DoubleValue(*value)),
        }),
        CellValue::Float64(_) => Err(OtlpMappingError::NonFiniteFloat),
        CellValue::Bytes(value) => Ok(string_value(
            base64::engine::general_purpose::STANDARD.encode(value),
        )),
        CellValue::Decimal(value)
        | CellValue::String(value)
        | CellValue::Date(value)
        | CellValue::Timestamp(value)
        | CellValue::TimestampTz(value)
        | CellValue::Interval(value)
        | CellValue::Json(value)
        | CellValue::Uuid(value) => Ok(string_value(value)),
    }
}

fn json_scalar_to_any(value: &Value) -> Result<AnyValue, OtlpMappingError> {
    match value {
        Value::Bool(value) => Ok(AnyValue {
            value: Some(any_value::Value::BoolValue(*value)),
        }),
        Value::String(value) => Ok(string_value(value)),
        Value::Number(value) if value.is_i64() => Ok(int_value(
            value
                .as_i64()
                .ok_or(OtlpMappingError::InvalidResourceAttribute)?,
        )),
        Value::Number(value) if value.is_u64() => {
            let value = value
                .as_u64()
                .ok_or(OtlpMappingError::InvalidResourceAttribute)?;
            i64::try_from(value)
                .map(int_value)
                .or_else(|_| Ok(string_value(value.to_string())))
        }
        Value::Number(value) => {
            let value = value
                .as_f64()
                .filter(|value| value.is_finite())
                .ok_or(OtlpMappingError::InvalidResourceAttribute)?;
            Ok(AnyValue {
                value: Some(any_value::Value::DoubleValue(value)),
            })
        }
        _ => Err(OtlpMappingError::InvalidResourceAttribute),
    }
}

fn parse_event_time(value: &CellValue, column: &str) -> Result<u64, OtlpMappingError> {
    let text = match value {
        CellValue::Date(value)
        | CellValue::Timestamp(value)
        | CellValue::TimestampTz(value)
        | CellValue::String(value) => value,
        _ => {
            return Err(OtlpMappingError::InvalidEventTimeType {
                column: column.to_owned(),
            });
        }
    };

    if let Ok(timestamp) = DateTime::parse_from_rfc3339(text) {
        return timestamp_to_nanos(timestamp.with_timezone(&Utc), column);
    }
    let timestamp = NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f")
        .map_err(|source| OtlpMappingError::InvalidEventTime {
            column: column.to_owned(),
            source,
        })?
        .and_utc();
    timestamp_to_nanos(timestamp, column)
}

fn timestamp_to_nanos(timestamp: DateTime<Utc>, column: &str) -> Result<u64, OtlpMappingError> {
    u64::try_from(timestamp.timestamp_nanos_opt().ok_or_else(|| {
        OtlpMappingError::EventTimeOutOfRange {
            column: column.to_owned(),
        }
    })?)
    .map_err(|_| OtlpMappingError::EventTimeOutOfRange {
        column: column.to_owned(),
    })
}

fn string_value(value: impl Into<String>) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::StringValue(value.into())),
    }
}

fn int_value(value: i64) -> AnyValue {
    AnyValue {
        value: Some(any_value::Value::IntValue(value)),
    }
}

/// Shared row-to-OTLP mapping failure.
#[derive(Debug, thiserror::Error)]
pub(crate) enum OtlpMappingError {
    /// Output configuration references a result column that does not exist.
    #[error("configured output column '{name}' is not present in the result metadata")]
    UnknownColumn {
        /// Missing column.
        name: String,
    },
    /// A float cannot be represented in OTLP or JSON.
    #[error("non-finite floating-point values are not supported")]
    NonFiniteFloat,
    /// JSON source text or body serialization failed.
    #[error("JSON row mapping failed")]
    Json(#[from] serde_json::Error),
    /// Event-time column has an unsupported value type.
    #[error("event-time column '{column}' is not a date, timestamp, or string")]
    InvalidEventTimeType {
        /// Configured column.
        column: String,
    },
    /// Event-time text could not be parsed.
    #[error("event-time column '{column}' contains an invalid timestamp")]
    InvalidEventTime {
        /// Configured column.
        column: String,
        /// Timestamp parse failure.
        #[source]
        source: chrono::ParseError,
    },
    /// Event timestamp cannot fit the OTLP unsigned nanosecond field.
    #[error("event-time column '{column}' is outside the supported OTLP time range")]
    EventTimeOutOfRange {
        /// Configured column.
        column: String,
    },
    /// Resource attribute escaped semantic validation.
    #[error("resource attribute is not a supported scalar value")]
    InvalidResourceAttribute,
    /// OTLP protobuf conversion failed.
    #[error("OTLP protobuf encoding failed")]
    Otlp(#[from] prost::EncodeError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::receivers::database::config::{OversizePolicy, TimestampConfig};
    use otap_df_pdata::proto::opentelemetry::common::v1::any_value;

    /// Scenario: a typed database row is mapped with source time and selected attributes.
    /// Guarantees: one log preserves JSON scalar types, query identity, and both OTLP timestamps.
    #[test]
    fn maps_typed_rows_to_shared_otlp_schema() {
        let batch = RowBatch {
            columns: vec![
                ColumnMetadata {
                    name: "ORDER_ID".to_owned(),
                    source_type: "NUMBER".to_owned(),
                    nullable: false,
                },
                ColumnMetadata {
                    name: "UPDATED_AT".to_owned(),
                    source_type: "TIMESTAMP".to_owned(),
                    nullable: false,
                },
            ],
            rows: vec![Row {
                values: vec![
                    CellValue::Int64(42),
                    CellValue::Timestamp("2026-08-26 16:40:00".to_owned()),
                ],
            }],
            normalized_bytes: 32,
        };
        let output = OutputConfig {
            include_columns: Vec::new(),
            attributes: BTreeMap::from([("order_id".to_owned(), "order.id".to_owned())]),
            timestamp: Some(TimestampConfig {
                column: "UPDATED_AT".to_owned(),
            }),
            oversize_policy: OversizePolicy::Error,
        };
        let logs = rows_to_logs(
            batch,
            OtlpMapping {
                system: DatabaseSystem::Oracle,
                query_name: "orders",
                output: &output,
                resource_attributes: &BTreeMap::new(),
                observed_time_unix_nano: 123,
            },
        )
        .expect("row should map");

        let record = &logs.resource_logs[0].scope_logs[0].log_records[0];
        assert_ne!(record.time_unix_nano, 123);
        assert_eq!(record.observed_time_unix_nano, 123);
        assert!(matches!(
            record.body,
            Some(AnyValue {
                value: Some(any_value::Value::KvlistValue(_))
            })
        ));
        assert!(record.attributes.iter().any(|attribute| {
            attribute.key == "order.id"
                && matches!(
                    attribute.value,
                    Some(AnyValue {
                        value: Some(any_value::Value::IntValue(42))
                    })
                )
        }));
    }
}
