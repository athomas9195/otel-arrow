// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Oracle-specific connection, query, cancellation, and row conversion.

use crate::config::OracleConfig;
use otel_arrow_dfe_database_scraper::driver::{
    DatabaseScraper, PreflightRequest, ScrapeError, ScrapeOutcome, ScrapeRequest,
};
use otel_arrow_dfe_database_scraper::row::{ResultSchema, RowSink};

pub struct OracleScraper {
    // Owns bounded blocking and session permits plus a bounded statement cache.
    // The Dataflow core never executes ODPI-C calls directly.
}

#[async_trait::async_trait(?Send)]
impl DatabaseScraper for OracleScraper {
    type Config = OracleConfig;

    async fn open(_config: &Self::Config) -> Result<Self, ScrapeError> {
        // Read size-bounded mounted credentials, create a read-only Oracle
        // session pool, and start the bounded vendor-owned blocking worker.
        todo!("design-only pseudocode")
    }

    async fn preflight(
        &mut self,
        _request: PreflightRequest<'_>,
    ) -> Result<ResultSchema, ScrapeError> {
        // Accept only the supported read-only query shape, validate every
        // declared cursor bind and key column, prepare the statement, and
        // build a reusable metadata decode plan.
        todo!("design-only pseudocode")
    }

    async fn scrape(
        &mut self,
        _request: ScrapeRequest<'_>,
        _sink: &mut dyn RowSink,
    ) -> Result<ScrapeOutcome, ScrapeError> {
        // The request deadline includes waiting for worker/session admission,
        // statement preparation, execute, fetch, and decode. Bind the committed
        // cursor, fetch bounded arrays, normalize each row, and stop when the
        // shared sink rejects further admission.
        todo!("design-only pseudocode")
    }

    async fn shutdown(&mut self) -> Result<(), ScrapeError> {
        // Retire the active session before requesting break_execution. Reconcile
        // both the query and break operations, and never return that session to
        // the pool unless normal completion is confirmed.
        todo!("design-only pseudocode")
    }
}
