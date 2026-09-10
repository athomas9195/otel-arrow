// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Design-only public surface for the shared database scraper runtime.
//!
//! This crate is a library, not a registered Dataflow receiver. Vendor crates
//! bind a database-specific scraper to the shared receiver runtime and register
//! the resulting component.

pub mod checkpoint;
pub mod completion;
pub mod config;
pub mod cursor;
pub mod driver;
pub mod lifecycle;
pub mod mapping;
pub mod metrics;
pub mod poller;
pub mod row;

pub use config::ScraperConfig;
pub use driver::DatabaseScraper;
pub use poller::DatabaseReceiver;
