// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Independent query scheduling and receiver-local admission accounting.

use super::config::RetryConfig;
use std::time::Duration;
use tokio::time::Instant;

/// Delay-based schedule state for one named query.
#[derive(Clone, Debug)]
pub(crate) struct QuerySchedule {
    interval: Duration,
    next_due: Instant,
    running: bool,
    stopped: bool,
    retry_attempt: u32,
    reserved_bytes: u64,
}

impl QuerySchedule {
    /// Creates a schedule whose first poll is delayed by bounded startup jitter.
    pub(crate) fn new(interval: Duration, startup_jitter_max: Duration) -> Self {
        Self {
            interval,
            next_due: Instant::now() + random_jitter(startup_jitter_max),
            running: false,
            stopped: false,
            retry_attempt: 0,
            reserved_bytes: 0,
        }
    }

    /// Returns whether this query is ready and not already active.
    pub(crate) fn is_due(&self, now: Instant) -> bool {
        !self.running && !self.stopped && self.next_due <= now
    }

    /// Returns this query's next deadline when it can run.
    pub(crate) fn next_due(&self) -> Option<Instant> {
        (!self.running && !self.stopped).then_some(self.next_due)
    }

    /// Marks one admitted execution and its byte reservation.
    pub(crate) fn start(&mut self, reserved_bytes: u64) {
        debug_assert!(!self.running);
        self.running = true;
        self.reserved_bytes = reserved_bytes;
    }

    /// Completes an execution and applies Delay missed-tick semantics.
    pub(crate) fn complete(&mut self, completed_at: Instant) -> u64 {
        self.running = false;
        self.retry_attempt = 0;
        self.next_due = completed_at + self.interval;
        std::mem::take(&mut self.reserved_bytes)
    }

    /// Schedules a bounded transient retry capped by the normal interval.
    pub(crate) fn retry(&mut self, completed_at: Instant, policy: &RetryConfig) -> u64 {
        self.running = false;
        let exponent = self.retry_attempt.min(31);
        self.retry_attempt = self.retry_attempt.saturating_add(1);
        let multiplier = policy.multiplier.saturating_pow(exponent);
        let delay = policy
            .initial_backoff
            .saturating_mul(multiplier)
            .min(self.interval);
        self.next_due = completed_at + jitter_retry(delay);
        std::mem::take(&mut self.reserved_bytes)
    }

    /// Stops this query after an explicitly configured permanent failure.
    pub(crate) fn stop(&mut self) -> u64 {
        self.running = false;
        self.stopped = true;
        std::mem::take(&mut self.reserved_bytes)
    }
}

/// Receiver-wide active-query and byte-reservation accounting.
#[derive(Debug)]
pub(crate) struct AdmissionController {
    max_concurrent_queries: usize,
    max_in_flight_bytes: u64,
    active_queries: usize,
    reserved_bytes: u64,
}

impl AdmissionController {
    /// Creates bounded local admission state.
    pub(crate) fn new(max_concurrent_queries: usize, max_in_flight_bytes: u64) -> Self {
        Self {
            max_concurrent_queries,
            max_in_flight_bytes,
            active_queries: 0,
            reserved_bytes: 0,
        }
    }

    /// Attempts to reserve one query slot and its maximum page bytes.
    pub(crate) fn try_acquire(&mut self, bytes: u64) -> bool {
        if self.active_queries >= self.max_concurrent_queries
            || self.reserved_bytes.saturating_add(bytes) > self.max_in_flight_bytes
        {
            return false;
        }
        self.active_queries += 1;
        self.reserved_bytes += bytes;
        true
    }

    /// Releases a reservation after downstream admission or failure.
    pub(crate) fn release(&mut self, bytes: u64) {
        self.active_queries = self.active_queries.saturating_sub(1);
        self.reserved_bytes = self.reserved_bytes.saturating_sub(bytes);
    }

    /// Returns whether another query slot is available.
    pub(crate) fn has_capacity(&self) -> bool {
        self.active_queries < self.max_concurrent_queries
    }
}

fn random_jitter(max: Duration) -> Duration {
    if max.is_zero() {
        return Duration::ZERO;
    }
    let max_nanos = u64::try_from(max.as_nanos()).unwrap_or(u64::MAX);
    Duration::from_nanos(rand::random::<u64>() % max_nanos.saturating_add(1))
}

fn jitter_retry(delay: Duration) -> Duration {
    let half = delay / 2;
    half.saturating_add(random_jitter(delay.saturating_sub(half)))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario: a completed query has missed one or more original interval boundaries.
    /// Guarantees: Delay semantics schedule the next run relative to completion, not catch-up time.
    #[test]
    fn completion_uses_delay_semantics() {
        let mut schedule = QuerySchedule::new(Duration::from_secs(30), Duration::ZERO);
        schedule.start(100);
        let completed = Instant::now() + Duration::from_secs(90);
        assert_eq!(schedule.complete(completed), 100);
        assert_eq!(
            schedule.next_due(),
            Some(completed + Duration::from_secs(30))
        );
    }

    /// Scenario: query and memory budgets are both exhausted.
    /// Guarantees: no additional poll is admitted until an existing reservation is released.
    #[test]
    fn admission_bounds_concurrency_and_bytes() {
        let mut admission = AdmissionController::new(2, 200);
        assert!(admission.try_acquire(100));
        assert!(admission.try_acquire(100));
        assert!(!admission.try_acquire(1));
        admission.release(100);
        assert!(admission.try_acquire(100));
    }
}
