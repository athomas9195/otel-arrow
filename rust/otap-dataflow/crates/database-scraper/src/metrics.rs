// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Common low-cardinality telemetry contract.

pub struct DatabaseReceiverMetrics {
    pub polls_started: u64,
    pub polls_completed: u64,
    pub polls_failed: u64,
    pub rows_read: u64,
    pub normalized_bytes: u64,
    pub publications_sent: u64,
    pub acknowledgements: u64,
    pub negative_acknowledgements: u64,
    pub replays: u64,
    pub checkpoint_commits: u64,
    pub checkpoint_failures: u64,
}

// SQL, table names, endpoints, row values, bind values, and cursor values are
// never metric attributes or event fields.
