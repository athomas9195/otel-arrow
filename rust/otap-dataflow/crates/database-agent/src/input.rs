// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Small, versioned customer-facing input contract.

use crate::error::AgentError;
use std::path::PathBuf;

pub struct AgentInput {
    pub schema_version: u32,
    pub receiver: ReceiverSelection,
    pub endpoint: String,
    pub database_or_service: String,
    pub query_file: PathBuf,
    pub credential_file: PathBuf,
    pub checkpoint_directory: PathBuf,
    pub otlp_endpoint: String,
}

pub enum ReceiverSelection {
    Oracle,
    Postgresql,
    SqlServer,
    MySql,
}

impl AgentInput {
    pub fn from_environment() -> Result<Self, AgentError> {
        // Read only documented non-secret values and mounted-file references.
        // Reject unknown receiver names and unsupported input schema versions.
        // Do not fetch DCR or AMCS configuration in this delivery profile.
        todo!("design-only pseudocode")
    }
}
