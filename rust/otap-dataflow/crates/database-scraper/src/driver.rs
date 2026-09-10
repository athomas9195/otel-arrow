// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Narrow contract implemented by each vendor receiver crate.

use crate::cursor::Cursor;
use crate::row::{ResultSchema, RowSink};
use std::time::Instant;

#[async_trait::async_trait(?Send)]
pub trait DatabaseScraper {
    type Config;

    async fn open(config: &Self::Config) -> Result<Self, ScrapeError>
    where
        Self: Sized;

    async fn preflight(
        &mut self,
        request: PreflightRequest<'_>,
    ) -> Result<ResultSchema, ScrapeError>;

    async fn scrape(
        &mut self,
        request: ScrapeRequest<'_>,
        sink: &mut dyn RowSink,
    ) -> Result<ScrapeOutcome, ScrapeError>;

    async fn shutdown(&mut self) -> Result<(), ScrapeError>;
}

pub struct PreflightRequest<'a> {
    pub query: &'a str,
    pub cursor_columns: &'a [String],
}

pub struct ScrapeRequest<'a> {
    pub query: &'a str,
    pub committed_cursor: Option<&'a Cursor>,
    pub max_rows: usize,
    pub max_normalized_bytes: usize,
    // Covers worker/session admission, prepare, execute, fetch, and decode.
    pub deadline: Instant,
}

pub struct ScrapeOutcome {
    pub rows_emitted: usize,
    pub normalized_bytes: usize,
    pub candidate_cursor: Option<Cursor>,
    pub reached_source_tail: bool,
}

pub enum ScrapeError {
    Authentication,
    Authorization,
    InvalidQuery,
    SchemaChanged,
    Timeout,
    Cancelled,
    ConnectionLost,
    UnsupportedValue,
    ResourceLimit,
    Other,
}
