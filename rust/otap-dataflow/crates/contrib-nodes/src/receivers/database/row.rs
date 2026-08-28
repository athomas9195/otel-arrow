// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Database-neutral values, metadata validation, and bounded row batching.

use super::config::OversizePolicy;
use super::query::ExecutionLimits;
use std::collections::BTreeSet;
use std::fmt;

/// Closed database-neutral scalar value representation.
#[allow(dead_code)] // Some variants are produced by adapters added after Oracle.
#[derive(Clone, PartialEq)]
pub(crate) enum CellValue {
    /// SQL NULL.
    Null,
    /// Boolean.
    Bool(bool),
    /// Signed integer.
    Int64(i64),
    /// Unsigned integer.
    UInt64(u64),
    /// Exact decimal text preserving source precision.
    Decimal(String),
    /// Finite IEEE-754 value.
    Float64(f64),
    /// UTF-8 text.
    String(String),
    /// Binary bytes.
    Bytes(Vec<u8>),
    /// Calendar date text.
    Date(String),
    /// Timestamp without source timezone.
    Timestamp(String),
    /// Timestamp with source timezone.
    TimestampTz(String),
    /// Database interval text.
    Interval(String),
    /// Valid JSON text.
    Json(String),
    /// UUID text.
    Uuid(String),
}

impl fmt::Debug for CellValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Null => formatter.write_str("Null"),
            Self::Bool(_) => formatter.write_str("Bool(<redacted>)"),
            Self::Int64(_) => formatter.write_str("Int64(<redacted>)"),
            Self::UInt64(_) => formatter.write_str("UInt64(<redacted>)"),
            Self::Float64(_) => formatter.write_str("Float64(<redacted>)"),
            Self::Decimal(value) => redacted_text(formatter, "Decimal", value),
            Self::String(value) => redacted_text(formatter, "String", value),
            Self::Bytes(value) => formatter
                .debug_tuple("Bytes")
                .field(&format_args!("<redacted:{} bytes>", value.len()))
                .finish(),
            Self::Date(value) => redacted_text(formatter, "Date", value),
            Self::Timestamp(value) => redacted_text(formatter, "Timestamp", value),
            Self::TimestampTz(value) => redacted_text(formatter, "TimestampTz", value),
            Self::Interval(value) => redacted_text(formatter, "Interval", value),
            Self::Json(value) => redacted_text(formatter, "Json", value),
            Self::Uuid(value) => redacted_text(formatter, "Uuid", value),
        }
    }
}

fn redacted_text(formatter: &mut fmt::Formatter<'_>, name: &str, value: &str) -> fmt::Result {
    formatter
        .debug_tuple(name)
        .field(&format_args!("<redacted:{} bytes>", value.len()))
        .finish()
}

impl CellValue {
    /// Returns an approximate normalized payload size for admission accounting.
    pub(crate) fn normalized_size(&self) -> u64 {
        match self {
            Self::Null => 1,
            Self::Bool(_) => 1,
            Self::Int64(_) | Self::UInt64(_) | Self::Float64(_) => 8,
            Self::Decimal(value)
            | Self::String(value)
            | Self::Date(value)
            | Self::Timestamp(value)
            | Self::TimestampTz(value)
            | Self::Interval(value)
            | Self::Json(value)
            | Self::Uuid(value) => value.len() as u64,
            Self::Bytes(value) => value.len() as u64,
        }
    }
}

/// Metadata shared by every row in a result page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ColumnMetadata {
    /// Result-set column name.
    pub(crate) name: String,
    /// Stable adapter-reported database type name.
    pub(crate) source_type: String,
    /// Whether the database metadata reports this column as nullable.
    pub(crate) nullable: bool,
}

/// One database-neutral row whose values correspond to page metadata.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Row {
    /// Ordered values matching `RowBatch::columns`.
    pub(crate) values: Vec<CellValue>,
}

impl Row {
    fn normalized_size(&self, columns: &[ColumnMetadata]) -> u64 {
        self.values
            .iter()
            .zip(columns)
            .map(|(value, column)| {
                value
                    .normalized_size()
                    .saturating_add(column.name.len() as u64)
            })
            .sum()
    }
}

/// One downstream-admissible bounded row batch.
#[derive(Clone, Debug)]
pub(crate) struct RowBatch {
    /// Shared result schema for the batch.
    pub(crate) columns: Vec<ColumnMetadata>,
    /// Rows in this batch.
    pub(crate) rows: Vec<Row>,
    /// Approximate normalized payload bytes.
    pub(crate) normalized_bytes: u64,
}

/// Fully bounded result of one page execution.
#[derive(Clone, Debug, Default)]
pub(crate) struct RowPage {
    /// Batches split before downstream admission.
    pub(crate) batches: Vec<RowBatch>,
    /// Total rows normalized in this page.
    pub(crate) row_count: usize,
    /// Total normalized bytes materialized in this page.
    pub(crate) normalized_bytes: u64,
}

/// Incrementally validates and splits a driver result into bounded batches.
pub(crate) struct RowPageBuilder {
    columns: Vec<ColumnMetadata>,
    limits: ExecutionLimits,
    oversize_policy: OversizePolicy,
    batches: Vec<RowBatch>,
    current_rows: Vec<Row>,
    current_bytes: u64,
    total_rows: usize,
    total_bytes: u64,
}

impl RowPageBuilder {
    /// Creates a page builder after validating stable result metadata.
    pub(crate) fn new(
        columns: Vec<ColumnMetadata>,
        limits: ExecutionLimits,
        oversize_policy: OversizePolicy,
    ) -> Result<Self, RowError> {
        validate_columns(&columns)?;
        Ok(Self {
            columns,
            limits,
            oversize_policy,
            batches: Vec::new(),
            current_rows: Vec::new(),
            current_bytes: 0,
            total_rows: 0,
            total_bytes: 0,
        })
    }

    /// Adds one row while enforcing row, batch-byte, and page-byte bounds.
    pub(crate) fn push(&mut self, row: Row) -> Result<(), RowError> {
        if row.values.len() != self.columns.len() {
            return Err(RowError::ColumnCount {
                expected: self.columns.len(),
                actual: row.values.len(),
            });
        }
        if self.total_rows >= self.limits.max_rows {
            return Err(RowError::RowLimitExceeded {
                limit: self.limits.max_rows,
            });
        }

        let row_bytes = row.normalized_size(&self.columns);
        if row_bytes > self.limits.max_batch_bytes && self.oversize_policy == OversizePolicy::Error
        {
            return Err(RowError::OversizedRow {
                bytes: row_bytes,
                limit: self.limits.max_batch_bytes,
            });
        }
        if self.total_bytes.saturating_add(row_bytes) > self.limits.max_page_bytes {
            return Err(RowError::PageByteLimitExceeded {
                bytes: self.total_bytes.saturating_add(row_bytes),
                limit: self.limits.max_page_bytes,
            });
        }

        let row_limit_reached = self.current_rows.len() >= self.limits.max_batch_rows;
        let byte_limit_reached = !self.current_rows.is_empty()
            && self.current_bytes.saturating_add(row_bytes) > self.limits.max_batch_bytes;
        if row_limit_reached || byte_limit_reached {
            self.flush_batch();
        }

        self.current_rows.push(row);
        self.current_bytes = self.current_bytes.saturating_add(row_bytes);
        self.total_rows += 1;
        self.total_bytes = self.total_bytes.saturating_add(row_bytes);

        if row_bytes > self.limits.max_batch_bytes {
            self.flush_batch();
        }
        Ok(())
    }

    /// Completes the bounded page.
    pub(crate) fn finish(mut self) -> RowPage {
        self.flush_batch();
        RowPage {
            batches: self.batches,
            row_count: self.total_rows,
            normalized_bytes: self.total_bytes,
        }
    }

    fn flush_batch(&mut self) {
        if self.current_rows.is_empty() {
            return;
        }
        self.batches.push(RowBatch {
            columns: self.columns.clone(),
            rows: std::mem::take(&mut self.current_rows),
            normalized_bytes: std::mem::take(&mut self.current_bytes),
        });
    }
}

/// Explicit row normalization or resource-bound failure.
#[derive(Debug, thiserror::Error)]
pub(crate) enum RowError {
    /// Result column names are ambiguous after case-insensitive normalization.
    #[error("result column '{name}' is not unique after normalization")]
    DuplicateColumn {
        /// Ambiguous result name.
        name: String,
    },
    /// A database row did not match the result metadata.
    #[error("row has {actual} values but metadata describes {expected} columns")]
    ColumnCount {
        /// Expected values.
        expected: usize,
        /// Actual values.
        actual: usize,
    },
    /// The driver returned more rows than the client-side hard bound.
    #[error("query returned more than the hard row limit of {limit}")]
    RowLimitExceeded {
        /// Configured limit.
        limit: usize,
    },
    /// One row cannot fit in a normal batch and no explicit override permits it.
    #[error("one normalized row is {bytes} bytes and exceeds the {limit}-byte batch limit")]
    OversizedRow {
        /// Row size.
        bytes: u64,
        /// Configured limit.
        limit: u64,
    },
    /// The page would exceed its receiver-local memory reservation.
    #[error("normalized page would use {bytes} bytes and exceed its {limit}-byte reservation")]
    PageByteLimitExceeded {
        /// Prospective page size.
        bytes: u64,
        /// Reserved page limit.
        limit: u64,
    },
    /// A float cannot be represented by OTLP/JSON without losing validity.
    #[error("non-finite floating-point values are not supported")]
    NonFiniteFloat,
    /// JSON source text is malformed.
    #[error("database JSON value is invalid")]
    InvalidJson(#[source] serde_json::Error),
    /// A vendor type has no approved database-neutral conversion.
    #[error("database type '{source_type}' is not supported")]
    UnsupportedType {
        /// Adapter-reported type.
        source_type: String,
    },
}

fn validate_columns(columns: &[ColumnMetadata]) -> Result<(), RowError> {
    let mut names = BTreeSet::new();
    for column in columns {
        let normalized = column.name.trim().to_ascii_lowercase();
        if normalized.is_empty() || !names.insert(normalized) {
            return Err(RowError::DuplicateColumn {
                name: column.name.clone(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits() -> ExecutionLimits {
        ExecutionLimits {
            max_rows: 10,
            max_batch_rows: 10,
            max_batch_bytes: 8,
            max_page_bytes: 32,
            fetch_size: 10,
        }
    }

    /// Scenario: normalized rows collectively exceed one batch-byte limit.
    /// Guarantees: rows are split into bounded batches before downstream admission.
    #[test]
    fn splits_rows_by_normalized_bytes() {
        let columns = vec![ColumnMetadata {
            name: "value".to_owned(),
            source_type: "VARCHAR2".to_owned(),
            nullable: false,
        }];
        let mut builder =
            RowPageBuilder::new(columns, limits(), OversizePolicy::Error).expect("metadata");
        builder
            .push(Row {
                values: vec![CellValue::String("a".to_owned())],
            })
            .expect("first row");
        builder
            .push(Row {
                values: vec![CellValue::String("b".to_owned())],
            })
            .expect("second row");

        let page = builder.finish();
        assert_eq!(page.batches.len(), 2);
        assert_eq!(page.row_count, 2);
    }

    /// Scenario: one source value is larger than the configured batch-byte ceiling.
    /// Guarantees: default conversion fails explicitly and never truncates source data.
    #[test]
    fn rejects_oversized_single_row() {
        let columns = vec![ColumnMetadata {
            name: "v".to_owned(),
            source_type: "VARCHAR2".to_owned(),
            nullable: false,
        }];
        let mut builder =
            RowPageBuilder::new(columns, limits(), OversizePolicy::Error).expect("metadata");
        let error = builder
            .push(Row {
                values: vec![CellValue::String("0123456789".to_owned())],
            })
            .expect_err("oversized row must fail");

        assert!(matches!(error, RowError::OversizedRow { .. }));
    }
}
