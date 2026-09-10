// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Deterministic neutral-row to OTLP-log mapping.

use crate::row::{DatabaseRow, ResultSchema};

pub struct MappingConfig {
    pub event_time_column: Option<String>,
    pub reserved_attribute_prefix: String,
}

pub struct OtlpLogRecord {
    // Placeholder for the repository pdata/OTLP representation selected by the
    // implementation PR.
}

pub fn map_row(
    _schema: &ResultSchema,
    _row: DatabaseRow,
    _config: &MappingConfig,
) -> Result<OtlpLogRecord, MappingError> {
    // Map every selected column to a typed attribute, set observed time, apply
    // the configured event-time column, and reject collisions.
    todo!("design-only pseudocode")
}

pub enum MappingError {
    ColumnCountMismatch,
    DuplicateColumn,
    ReservedNameCollision,
    InvalidEventTime,
    UnsupportedValue,
    EncodedSizeLimit,
}
