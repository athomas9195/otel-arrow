// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use otel_arrow_dfe_database_scraper::ScraperConfig;
use std::path::PathBuf;

pub struct OracleConfig {
    pub common: ScraperConfig,
    pub connection: OracleConnectionConfig,
    pub source: OracleSourceConfig,
}

pub struct OracleConnectionConfig {
    pub endpoint: String,
    pub service_name: String,
    pub credential_file: PathBuf,
    pub wallet_directory: Option<PathBuf>,
    pub server_identity: String,
    pub max_secret_bytes: usize,
    pub max_connections: usize,
    pub blocking_concurrency: usize,
}

pub struct OracleSourceConfig {
    pub query_file: PathBuf,
    pub key_columns: Vec<OracleKeyColumn>,
    pub cursor_binds: Vec<OracleCursorBind>,
    pub fetch_array_size: usize,
    pub statement_cache_capacity: usize,
}

pub struct OracleKeyColumn {
    pub name: String,
    pub value_type: OracleValueType,
}

pub struct OracleCursorBind {
    pub cursor_index: usize,
    pub placeholder: String,
}

pub enum OracleValueType {
    Bool,
    Int64,
    Float64,
    Text,
    Bytes,
    Timestamp,
}
