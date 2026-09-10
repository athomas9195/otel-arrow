// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Explicit lifecycle states shared by all vendor receivers.

pub enum LifecycleState {
    Starting,
    Idle,
    Querying,
    AwaitingDelivery,
    CommittingCheckpoint,
    BackingOff,
    Draining,
    Stopped,
    Failed,
}

pub enum LifecycleCommand {
    PollEligible,
    MemoryPressureChanged,
    CollectTelemetry,
    Drain { deadline: std::time::Instant },
    Shutdown { deadline: std::time::Instant },
}
