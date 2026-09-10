// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Source progress independent of a database driver's native value types.

use crate::row::CellValue;

pub struct Cursor {
    pub fields: Vec<CursorField>,
}

pub struct CursorField {
    pub column: String,
    pub value: CellValue,
}

pub struct CursorDefinition {
    pub ordered_columns: Vec<String>,
    pub initial_position: Option<Cursor>,
}

pub struct CheckpointFingerprint {
    pub version: u32,
    pub digest: [u8; 32],
}

pub fn validate_strictly_after(
    _committed: Option<&Cursor>,
    _returned: &Cursor,
) -> Result<(), CursorError> {
    // Compare values using the semantic types established during preflight.
    todo!("design-only pseudocode")
}

pub enum CursorError {
    MissingColumn,
    NullValue,
    TypeChanged,
    NotStrictlyOrdered,
    NotRepresentable,
}
