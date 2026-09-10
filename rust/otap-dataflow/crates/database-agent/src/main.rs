// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Thin local-config agent host with no DCR or AMCS bootstrap dependency.
//! All polling behavior remains in linked receiver crates.

// The design skeleton declares types before their implementations use every
// field. Remove this allowance when the crate enters the workspace.
#![allow(dead_code)]

mod bootstrap;
mod error;
mod input;
mod pipeline;

// These side-effect imports retain the selected ReceiverFactory registrations.
#[cfg(feature = "mysql")]
use otel_arrow_dfe_receiver_mysql as _;
#[cfg(feature = "oracle")]
use otel_arrow_dfe_receiver_oracle as _;
#[cfg(feature = "postgresql")]
use otel_arrow_dfe_receiver_postgresql as _;
#[cfg(feature = "sql-server")]
use otel_arrow_dfe_receiver_sql_server as _;

// Retain the existing OTLP exporter registration.
use otel_arrow_dfe_core_nodes as _;

fn main() -> Result<(), error::AgentError> {
    let local_input = input::AgentInput::from_environment()?;
    let bootstrap = bootstrap::BootstrapConfig::try_from(local_input)?;
    let dataflow_spec = pipeline::build_constrained_pipeline(&bootstrap)?;

    pipeline::validate_selected_components(&dataflow_spec)?;
    pipeline::run_controller(dataflow_spec)
}
