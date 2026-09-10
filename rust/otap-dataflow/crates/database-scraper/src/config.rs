// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Vendor-neutral configuration owned by the shared polling runtime.

use std::path::PathBuf;
use std::time::Duration;

pub struct ScraperConfig {
    pub source_id: String,
    pub schedule: ScheduleConfig,
    pub limits: LimitsConfig,
    pub checkpoint: CheckpointConfig,
    pub delivery: DeliveryConfig,
}

pub struct ScheduleConfig {
    pub interval: Duration,
    pub query_timeout: Duration,
    pub retry_backoff: Duration,
}

pub struct LimitsConfig {
    pub max_rows_per_page: usize,
    pub max_normalized_bytes_per_page: usize,
    pub max_encoded_bytes_per_batch: usize,
    pub max_pending_publications: usize,
}

pub struct CheckpointConfig {
    pub directory: PathBuf,
    pub schema_version: u32,
    pub max_serialized_bytes: usize,
}

pub struct DeliveryConfig {
    pub acknowledgement_timeout: Duration,
    pub boundary: DeliveryBoundary,
}

pub enum DeliveryBoundary {
    RemoteOtlpReceiver,
    DurableBuffer,
    FinalExporter,
}
