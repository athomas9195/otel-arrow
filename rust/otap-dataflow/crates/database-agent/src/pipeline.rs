// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Builds exactly one database receiver to OTLP exporter pipeline.

use crate::bootstrap::BootstrapConfig;
use crate::error::AgentError;
use otel_arrow_dfe_config::engine::OtelDataflowSpec;

pub fn build_constrained_pipeline(
    _bootstrap: &BootstrapConfig,
) -> Result<OtelDataflowSpec, AgentError> {
    // Translate the selected receiver into its vendor URN and semantic config.
    // Add the existing OTLP gRPC exporter.
    // Connect receiver -> exporter.
    // Force the singleton source pipeline onto one core.
    // Do not accept arbitrary customer-supplied pipeline nodes or connections.
    todo!("design-only pseudocode")
}

pub fn validate_selected_components(_spec: &OtelDataflowSpec) -> Result<(), AgentError> {
    // Use the normal OTAP_PIPELINE_FACTORY validation. Fail when the requested
    // receiver was not compiled into this agent distribution.
    todo!("design-only pseudocode")
}

pub fn run_controller(_spec: OtelDataflowSpec) -> Result<(), AgentError> {
    // Install the configured crypto provider, construct the normal Controller,
    // and run with standard signal, drain, telemetry, and shutdown behavior.
    todo!("design-only pseudocode")
}
