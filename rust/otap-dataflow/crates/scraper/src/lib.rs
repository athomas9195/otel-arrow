// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Database-neutral contracts for OTAP receiver scraping.
//!
//! Database-neutral contracts and runtime behavior belong here. Vendor drivers,
//! receiver factory registration, executable startup, and deployment assets do
//! not. This layer defines validated configuration, values, cursors, pages, and
//! local async driver contracts. The polling controller uses persistence and
//! ownership interfaces backed by durable filesystem checkpoints and source
//! leases. Database adapters and receiver factories remain vendor-specific.

mod checkpoint;
mod controller;
pub mod database;
mod partition;
mod progress;
mod telemetry;

pub use checkpoint::{CheckpointError, CheckpointStore};
pub use controller::DatabaseReceiver;
pub use partition::{LeaseError, SourceLease};
pub use progress::{CheckpointBackend, CheckpointState, SourceOwnership, WriteOutcome};
pub use telemetry::DatabaseReceiverMetrics;
