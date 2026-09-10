// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Design-only PostgreSQL receiver component boundary.

// The design skeleton declares private adapter types before the factory uses
// them. Remove this allowance when the crate enters the workspace.
#![allow(dead_code)]

mod config;
mod driver;
mod error;
mod factory;
mod values;

pub use config::PostgresqlConfig;
pub use factory::POSTGRESQL_RECEIVER_URN;
