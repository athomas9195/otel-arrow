// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Database-neutral contracts for OTAP receiver scraping.
//!
//! Database-neutral contracts and runtime behavior belong here. Vendor drivers,
//! receiver factory registration, executable startup, and deployment assets do
//! not. This layer defines validated configuration, values, cursors, pages, and
//! local async driver contracts. The polling controller uses persistence and
//! ownership interfaces; concrete filesystem implementations remain separate.

mod controller;
pub mod database;
mod progress;
mod telemetry;

pub use controller::DatabaseReceiver;
pub use progress::{CheckpointBackend, CheckpointState, SourceOwnership, WriteOutcome};
pub use telemetry::DatabaseReceiverMetrics;
