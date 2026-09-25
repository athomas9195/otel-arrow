// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL scalar normalization without decimal or timestamp precision loss.

use super::adapter::PgError;
use chrono::{DateTime, NaiveDate, NaiveDateTime, SecondsFormat, Utc};
use otel_arrow_dfe_scraper::database::CellValue;
use tokio_postgres::{
    Row,
    types::{FromSql, Type},
};

pub(super) struct RawValue<'a>(pub &'a [u8]);
impl<'a> FromSql<'a> for RawValue<'a> {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Self(raw))
    }
    fn accepts(_: &Type) -> bool {
        true
    }
}

pub(super) fn supported(ty: &Type) -> bool {
    matches!(
        *ty,
        Type::BOOL
            | Type::INT2
            | Type::INT4
            | Type::INT8
            | Type::FLOAT4
            | Type::FLOAT8
            | Type::NUMERIC
            | Type::TEXT
            | Type::VARCHAR
            | Type::BPCHAR
            | Type::NAME
            | Type::BYTEA
            | Type::DATE
            | Type::TIMESTAMP
            | Type::TIMESTAMPTZ
            | Type::JSON
            | Type::JSONB
            | Type::UUID
            | Type::INTERVAL
    )
}

pub(super) fn decode(row: &Row, index: usize, limit: u64) -> Result<CellValue, PgError> {
    let raw = row
        .try_get::<_, Option<RawValue<'_>>>(index)
        .map_err(|_| PgError::Conversion)?;
    let Some(RawValue(raw)) = raw else {
        return Ok(CellValue::Null);
    };
    if raw.len() as u64 > limit {
        return Err(PgError::RowTooLarge);
    }
    macro_rules! get {
        ($ty:ty) => {
            row.try_get::<_, $ty>(index)
                .map_err(|_| PgError::Conversion)?
        };
    }
    Ok(match *row.columns()[index].type_() {
        Type::BOOL => CellValue::Bool(get!(bool)),
        Type::INT2 => CellValue::Int64(i64::from(get!(i16))),
        Type::INT4 => CellValue::Int64(i64::from(get!(i32))),
        Type::INT8 => CellValue::Int64(get!(i64)),
        Type::FLOAT4 | Type::FLOAT8 => {
            let value = if *row.columns()[index].type_() == Type::FLOAT4 {
                f64::from(get!(f32))
            } else {
                get!(f64)
            };
            if !value.is_finite() {
                return Err(PgError::Conversion);
            }
            CellValue::Float64(value)
        }
        Type::NUMERIC => CellValue::Decimal(numeric(raw, limit)?),
        Type::BYTEA => CellValue::Bytes(raw.to_vec()),
        Type::UUID => CellValue::String(get!(uuid::Uuid).to_string()),
        Type::DATE => CellValue::String(get!(NaiveDate).to_string()),
        Type::TIMESTAMP => CellValue::Timestamp(
            get!(NaiveDateTime)
                .and_utc()
                .to_rfc3339_opts(SecondsFormat::Micros, true),
        ),
        Type::TIMESTAMPTZ => {
            CellValue::TimestampTz(get!(DateTime<Utc>).to_rfc3339_opts(SecondsFormat::Micros, true))
        }
        Type::INTERVAL => {
            if raw.len() != 16 {
                return Err(PgError::Conversion);
            }
            let micros = i64::from_be_bytes(raw[..8].try_into().map_err(|_| PgError::Conversion)?);
            let days = i32::from_be_bytes(raw[8..12].try_into().map_err(|_| PgError::Conversion)?);
            let months = i32::from_be_bytes(raw[12..].try_into().map_err(|_| PgError::Conversion)?);
            CellValue::Interval(format!("{months} mons {days} days {micros} microseconds"))
        }
        Type::JSONB => {
            if raw.first() != Some(&1) {
                return Err(PgError::Conversion);
            }
            CellValue::String(
                std::str::from_utf8(&raw[1..])
                    .map_err(|_| PgError::Conversion)?
                    .to_owned(),
            )
        }
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME | Type::JSON => CellValue::String(
            std::str::from_utf8(raw)
                .map_err(|_| PgError::Conversion)?
                .to_owned(),
        ),
        _ => return Err(PgError::UnsupportedType),
    })
}

// PostgreSQL binary NUMERIC stores base-10000 digits, a weight and a decimal scale.
fn numeric(raw: &[u8], limit: u64) -> Result<String, PgError> {
    if raw.len() < 8 {
        return Err(PgError::Conversion);
    }
    let word = |offset| u16::from_be_bytes([raw[offset], raw[offset + 1]]);
    let count = usize::from(word(0));
    let weight = i32::from(i16::from_be_bytes([raw[2], raw[3]]));
    let sign = word(4);
    let scale = usize::from(word(6));
    if raw.len() != 8 + count * 2 || !matches!(sign, 0 | 0x4000) || scale > 16383 {
        return Err(PgError::Conversion);
    }
    let integers = (weight + 1).max(0) as usize;
    let capacity = integers.max(1) * 4 + scale + 2;
    if capacity as u64 > limit {
        return Err(PgError::RowTooLarge);
    }
    let digits = raw[8..]
        .as_chunks::<2>()
        .0
        .iter()
        .map(|word| u16::from_be_bytes([word[0], word[1]]))
        .collect::<Vec<_>>();
    if digits.iter().any(|digit| *digit > 9999) {
        return Err(PgError::Conversion);
    }
    let group = |position: i32| -> u16 {
        let index = weight - position;
        if index >= 0 {
            digits.get(index as usize).copied().unwrap_or(0)
        } else {
            0
        }
    };
    let mut value = String::with_capacity(capacity);
    if sign == 0x4000 {
        value.push('-');
    }
    if integers == 0 {
        value.push('0');
    } else {
        use std::fmt::Write;
        for index in 0..integers {
            let digit = group(weight - index as i32);
            if index == 0 {
                write!(value, "{digit}").map_err(|_| PgError::Conversion)?;
            } else {
                write!(value, "{digit:04}").map_err(|_| PgError::Conversion)?;
            }
        }
    }
    if scale > 0 {
        use std::fmt::Write;
        value.push('.');
        let end = value.len() + scale;
        for index in 0..scale.div_ceil(4) {
            write!(value, "{:04}", group(-1 - index as i32)).map_err(|_| PgError::Conversion)?;
        }
        value.truncate(end);
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: Binary NUMERIC carries fractional leading zero groups, sign and trailing scale.
    /// Guarantees: Exact decimal text is retained without conversion through floating point.
    #[test]
    fn exact_numeric_text() {
        for (words, expected) in [
            (vec![3, 1, 0, 4, 12, 3456, 7800], "123456.7800"),
            (vec![1, 65535, 0x4000, 6, 12], "-0.001200"),
            (vec![0, 0, 0, 3], "0.000"),
            (vec![1, 2, 0, 0, 1], "100000000"),
        ] {
            let raw = words
                .into_iter()
                .flat_map(u16::to_be_bytes)
                .collect::<Vec<_>>();
            assert_eq!(numeric(&raw, 1024).expect("decimal"), expected);
        }
    }

    /// Scenario: Numeric data is malformed, non-finite or expands beyond the permitted bytes.
    /// Guarantees: No truncation, silent numeric rounding or unbounded output allocation occurs.
    #[test]
    fn rejects_invalid_or_oversized_numeric() {
        assert!(numeric(&[0; 3], 1024).is_err());
        for words in [
            vec![0, 0, 0xC000, 0],
            vec![1, 0, 0, 0, 10000],
            vec![0, 32767, 0, 0],
        ] {
            let raw = words
                .into_iter()
                .flat_map(u16::to_be_bytes)
                .collect::<Vec<_>>();
            assert!(numeric(&raw, 128).is_err());
        }
    }
}
