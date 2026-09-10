// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use crate::config::MySqlConfig;
use otel_arrow_dfe_database_scraper::driver::{
    DatabaseScraper, PreflightRequest, ScrapeError, ScrapeOutcome, ScrapeRequest,
};
use otel_arrow_dfe_database_scraper::row::{ResultSchema, RowSink};

pub struct MySqlScraper {
    // Owns the MySQL client and query cancellation state.
}

#[async_trait::async_trait(?Send)]
impl DatabaseScraper for MySqlScraper {
    type Config = MySqlConfig;

    async fn open(_config: &Self::Config) -> Result<Self, ScrapeError> {
        todo!("open a verified-TLS read-only MySQL session")
    }

    async fn preflight(
        &mut self,
        _request: PreflightRequest<'_>,
    ) -> Result<ResultSchema, ScrapeError> {
        todo!("prepare SQL and validate MySQL result metadata")
    }

    async fn scrape(
        &mut self,
        _request: ScrapeRequest<'_>,
        _sink: &mut dyn RowSink,
    ) -> Result<ScrapeOutcome, ScrapeError> {
        todo!("stream bounded MySQL rows into the shared sink")
    }

    async fn shutdown(&mut self) -> Result<(), ScrapeError> {
        todo!("cancel active work and dispose uncertain MySQL connections")
    }
}
