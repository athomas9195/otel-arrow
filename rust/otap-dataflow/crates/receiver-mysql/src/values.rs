// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use otel_arrow_dfe_database_scraper::row::CellValue;

pub fn convert_mysql_value(_value: MySqlValue) -> Result<CellValue, MySqlValueError> {
    todo!("preserve DECIMAL, temporal, BIT, binary, JSON, and unsigned values")
}

pub struct MySqlValue;

pub enum MySqlValueError {
    UnsupportedType,
    PrecisionLoss,
    InvalidEncoding,
    OversizedValue,
}
