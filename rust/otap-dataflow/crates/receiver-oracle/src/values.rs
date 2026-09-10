// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Oracle native-to-neutral scalar mapping.

use otel_arrow_dfe_database_scraper::row::CellValue;

pub fn convert_oracle_value(_value: OracleValue) -> Result<CellValue, OracleValueError> {
    // Preserve NUMBER precision, timezone semantics, binary values, and LOB
    // limits without forcing Oracle types into the shared crate.
    todo!("design-only pseudocode")
}

pub struct OracleValue;

pub enum OracleValueError {
    UnsupportedType,
    PrecisionLoss,
    InvalidEncoding,
    OversizedLob,
}
