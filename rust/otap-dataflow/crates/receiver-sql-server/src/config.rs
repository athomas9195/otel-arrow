// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use otel_arrow_dfe_database_scraper::ScraperConfig;
use std::path::PathBuf;

pub struct SqlServerConfig {
    pub common: ScraperConfig,
    pub connection: SqlServerConnectionConfig,
    pub source: SqlServerSourceConfig,
}

pub struct SqlServerConnectionConfig {
    pub endpoint: String,
    pub database: String,
    pub credential_file: PathBuf,
    pub ca_file: Option<PathBuf>,
    pub server_name: String,
}

pub struct SqlServerSourceConfig {
    pub query_file: PathBuf,
    pub cursor_bind_names: Vec<String>,
    pub fetch_rows: usize,
}
