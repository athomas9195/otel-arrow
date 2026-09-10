// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use otel_arrow_dfe_database_scraper::row::CellValue;

pub fn convert_postgresql_value(
    _value: PostgresqlValue,
) -> Result<CellValue, PostgresqlValueError> {
    todo!("preserve NUMERIC, TIMESTAMPTZ, BYTEA, JSONB, UUID, and arrays by policy")
}

pub struct PostgresqlValue;

pub enum PostgresqlValueError {
    UnsupportedType,
    PrecisionLoss,
    InvalidEncoding,
    OversizedValue,
}
