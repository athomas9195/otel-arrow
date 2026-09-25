// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

/// PostgreSQL incremental query receiver.
#[cfg(feature = "postgresql-receiver")]
pub mod postgresql_receiver;

/// ETW (Event Tracing for Windows) receiver.
#[cfg(all(feature = "etw", target_os = "windows"))]
pub mod etw_receiver;

/// Kafka receiver.
#[cfg(feature = "kafka")]
pub mod kafka_receiver;

/// Linux user_events receiver.
#[cfg(all(feature = "user-events", target_os = "linux"))]
pub mod user_events_receiver;
