// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use crate::config::PostgresqlConfig;
use otel_arrow_dfe_database_scraper::driver::{
    DatabaseScraper, PreflightRequest, ScrapeError, ScrapeOutcome, ScrapeRequest,
};
use otel_arrow_dfe_database_scraper::row::{ResultSchema, RowSink};

pub struct PostgresqlScraper {
    // Owns the PostgreSQL client and query cancellation handle.
}

#[async_trait::async_trait(?Send)]
impl DatabaseScraper for PostgresqlScraper {
    type Config = PostgresqlConfig;

    async fn open(_config: &Self::Config) -> Result<Self, ScrapeError> {
        todo!("open a verified-TLS read-only PostgreSQL session")
    }

    async fn preflight(
        &mut self,
        _request: PreflightRequest<'_>,
    ) -> Result<ResultSchema, ScrapeError> {
        todo!("prepare SQL and validate PostgreSQL result metadata")
    }

    async fn scrape(
        &mut self,
        _request: ScrapeRequest<'_>,
        _sink: &mut dyn RowSink,
    ) -> Result<ScrapeOutcome, ScrapeError> {
        todo!("stream bounded PostgreSQL rows into the shared sink")
    }

    async fn shutdown(&mut self) -> Result<(), ScrapeError> {
        todo!("cancel active work and close the PostgreSQL session")
    }
}
