// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Host-neutral polling state machine wrapped by each vendor receiver.

use crate::checkpoint::CheckpointStore;
use crate::completion::CompletionTracker;
use crate::config::ScraperConfig;
use crate::driver::DatabaseScraper;

pub struct DatabaseReceiver<D, S>
where
    D: DatabaseScraper,
    S: CheckpointStore,
{
    pub config: ScraperConfig,
    pub driver: D,
    pub checkpoint_store: S,
    pub completions: CompletionTracker,
    pub state: PollState,
}

pub enum PollState {
    Starting,
    Idle,
    Querying,
    AwaitingDelivery,
    Committing,
    BackingOff,
    Draining,
}

impl<D, S> DatabaseReceiver<D, S>
where
    D: DatabaseScraper,
    S: CheckpointStore,
{
    pub async fn run_one_cycle(&mut self) -> Result<PollOutcome, PollError> {
        // 1. Load the committed checkpoint.
        // 2. Ask the vendor scraper for one bounded page.
        // 3. Map accepted rows into one or more bounded OTLP batches.
        // 4. Subscribe each batch to Dataflow Ack/Nack feedback.
        // 5. Retain the candidate cursor until all required batches Ack.
        // 6. Commit the candidate with the expected checkpoint revision.
        // 7. On Nack, timeout, cancellation, or commit failure, do not advance.
        todo!("design-only pseudocode")
    }
}

pub enum PollOutcome {
    NoRows,
    PublicationPending,
    CheckpointCommitted,
    RetryScheduled,
}

pub enum PollError {
    Driver,
    Mapping,
    Completion,
    Checkpoint,
    ShutdownDeadline,
}
