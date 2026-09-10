// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use otel_arrow_dfe_database_scraper::ScraperConfig;
use std::path::PathBuf;

pub struct MySqlConfig {
    pub common: ScraperConfig,
    pub connection: MySqlConnectionConfig,
    pub source: MySqlSourceConfig,
}

pub struct MySqlConnectionConfig {
    pub endpoint: String,
    pub database: String,
    pub credential_file: PathBuf,
    pub ca_file: Option<PathBuf>,
    pub server_name: String,
}

pub struct MySqlSourceConfig {
    pub query_file: PathBuf,
    pub cursor_bind_names: Vec<String>,
    pub fetch_rows: usize,
}
