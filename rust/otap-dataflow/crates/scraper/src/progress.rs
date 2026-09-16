// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Progress and ownership contracts, independent of filesystem persistence.

use crate::database::CompositeCursor;

/// Last acknowledged cursor and its checkpoint revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointState {
    /// Monotonic revision of the committed checkpoint.
    pub revision: u64,
    /// Last durably acknowledged cursor.
    pub cursor: CompositeCursor,
}

/// Non-fatal outcome details of one checkpoint write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WriteOutcome {
    /// Number of stale revision files that could not be removed.
    pub cleanup_failures: usize,
}

/// Persistence operations required by the ACK-driven polling controller.
///
/// Clones must address the same checkpoint identity. Reads must fail closed on
/// invalid state. A successful write must install the next revision durably
/// before returning; a failed write must not authorize in-memory advancement.
///
/// Handles and errors are `Send` because persistence runs on blocking workers,
/// not on the local async engine core. This does not require a `Send` receiver,
/// database adapter, or database-operation future.
pub trait CheckpointBackend: Clone + Send + 'static {
    /// Read or write failure reported to the controller.
    type Error: std::error::Error + Send + 'static;

    /// Loads the last committed state, or `None` for a new checkpoint.
    fn read(&self) -> Result<Option<CheckpointState>, Self::Error>;

    /// Durably installs the cursor after the supplied committed revision.
    fn write(
        &self,
        revision: u64,
        cursor: &CompositeCursor,
    ) -> Result<(CheckpointState, WriteOutcome), Self::Error>;
}

/// Ownership guard retained for the lifetime of a source's polling controller.
///
/// The caller must acquire this guard for the same identity as its checkpoint.
/// Dropping it releases ownership only after active database work has ended.
/// No `Send` bound is needed: the guard stays on the local engine core.
pub trait SourceOwnership {
    /// Monotonic generation identifying this acquisition of source ownership.
    fn generation(&self) -> u64;
}
