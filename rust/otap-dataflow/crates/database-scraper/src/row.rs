// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Closed database-neutral row model.

pub struct ResultSchema {
    pub columns: Vec<Column>,
}

pub struct Column {
    pub name: String,
    pub nullable: bool,
    pub source_type: String,
}

pub struct DatabaseRow {
    pub values: Vec<CellValue>,
}

pub enum CellValue {
    Null,
    Bool(bool),
    Int64(i64),
    UInt64(u64),
    Decimal(String),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Date(String),
    Timestamp(String),
    TimestampWithTimezone(String),
    Interval(String),
    Json(String),
    Uuid(String),
}

pub trait RowSink {
    fn try_push(&mut self, row: DatabaseRow) -> Result<(), RowSinkError>;
}

pub enum RowSinkError {
    RowLimitReached,
    NormalizedByteLimitReached,
    OversizedValue,
    Cancelled,
}
