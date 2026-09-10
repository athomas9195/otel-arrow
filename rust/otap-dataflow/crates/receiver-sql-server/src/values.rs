// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use otel_arrow_dfe_database_scraper::row::CellValue;

pub fn convert_sql_server_value(_value: SqlServerValue) -> Result<CellValue, SqlServerValueError> {
    todo!("preserve DECIMAL, DATETIMEOFFSET, UNIQUEIDENTIFIER, binary, and XML")
}

pub struct SqlServerValue;

pub enum SqlServerValueError {
    UnsupportedType,
    PrecisionLoss,
    InvalidEncoding,
    OversizedValue,
}
