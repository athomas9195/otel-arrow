// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Database-neutral polling runtime shared by SQL receiver adapters.

pub(crate) mod config;
pub(crate) mod driver;
pub(crate) mod metrics;
pub(crate) mod otlp;
pub(crate) mod query;
pub(crate) mod receiver;
pub(crate) mod row;
pub(crate) mod scheduler;
