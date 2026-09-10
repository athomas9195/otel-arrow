// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Converts local deployment inputs into semantic receiver inputs.

use crate::error::AgentError;
use crate::input::{AgentInput, ReceiverSelection};
use std::path::PathBuf;

pub struct BootstrapConfig {
    pub receiver: ReceiverSelection,
    pub receiver_config: ReceiverBootstrapConfig,
    pub exporter_config: OtlpBootstrapConfig,
}

pub struct ReceiverBootstrapConfig {
    pub endpoint: String,
    pub database_or_service: String,
    pub query_file: PathBuf,
    pub credential_file: PathBuf,
    pub checkpoint_directory: PathBuf,
}

pub struct OtlpBootstrapConfig {
    pub endpoint: String,
}

impl TryFrom<AgentInput> for BootstrapConfig {
    type Error = AgentError;

    fn try_from(_input: AgentInput) -> Result<Self, Self::Error> {
        // Validate file ownership/permissions, endpoint schemes, selected
        // receiver availability, and non-secret configuration invariants.
        todo!("design-only pseudocode")
    }
}
