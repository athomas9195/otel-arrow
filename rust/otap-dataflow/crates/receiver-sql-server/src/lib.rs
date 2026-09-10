// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Design-only SQL Server receiver component boundary.

// The design skeleton declares private adapter types before the factory uses
// them. Remove this allowance when the crate enters the workspace.
#![allow(dead_code)]

mod config;
mod driver;
mod error;
mod factory;
mod values;

pub use config::SqlServerConfig;
pub use factory::SQL_SERVER_RECEIVER_URN;
