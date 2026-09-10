// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Correlates Dataflow Ack/Nack feedback with candidate source progress.

use crate::cursor::Cursor;

pub struct DeliveryToken {
    pub publication_id: u64,
    pub checkpoint_revision: u64,
    pub candidate_cursor: Cursor,
    pub expected_batch_count: usize,
}

pub enum Completion {
    Ack { batch_index: usize },
    Nack { batch_index: usize, retryable: bool },
    Timeout,
}

pub enum CompletionDecision {
    Pending,
    Commit(DeliveryToken),
    ReplayFromCommitted,
    FailPermanent,
}

pub struct CompletionTracker {
    // V1 should permit one candidate page at a time. This type preserves the
    // boundary for future contiguous-prefix completion tracking.
}

impl CompletionTracker {
    pub fn begin(&mut self, _token: DeliveryToken) -> Result<(), CompletionError> {
        todo!("design-only pseudocode")
    }

    pub fn apply(&mut self, _completion: Completion) -> CompletionDecision {
        todo!("design-only pseudocode")
    }
}

pub enum CompletionError {
    CandidateAlreadyPending,
    InvalidBatchCount,
}
