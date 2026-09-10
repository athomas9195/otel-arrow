// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Revisioned checkpoint contract used by local and future shared stores.

use crate::cursor::{CheckpointFingerprint, Cursor};

pub struct Checkpoint {
    pub format_version: u32,
    pub revision: u64,
    pub identity: CheckpointIdentity,
    pub committed_cursor: Option<Cursor>,
    pub checksum: String,
}

pub struct CheckpointIdentity {
    pub source_id: String,
    // Covers database/query identity, cursor definition, mapping, and delivery
    // profile so incompatible state is never silently reused.
    pub compatibility_fingerprint: CheckpointFingerprint,
}

#[async_trait::async_trait(?Send)]
pub trait CheckpointStore {
    async fn load(
        &self,
        identity: &CheckpointIdentity,
    ) -> Result<Option<Checkpoint>, CheckpointError>;

    async fn compare_and_set(
        &self,
        identity: &CheckpointIdentity,
        expected_revision: u64,
        next: &Checkpoint,
    ) -> Result<(), CheckpointError>;

    async fn probe(&self) -> Result<(), CheckpointError>;
}

pub struct LocalFileCheckpointStore {
    // Bounds serialized state, serializes writers with a local lock, retains
    // the previous revision for crash recovery, writes a temporary sibling
    // file, fsyncs it, atomically renames it, and fsyncs the directory.
}

pub enum CheckpointError {
    NotFound,
    Corrupt,
    UnsupportedVersion,
    FingerprintMismatch,
    RevisionConflict,
    Io,
    DeadlineExceeded,
}
