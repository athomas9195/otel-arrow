// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL query receiver backed by the shared scraper runtime.

mod adapter;
mod config;
#[cfg(test)]
mod tests;
mod value;

use linkme::distributed_slice;
use otel_arrow_dfe_config::{error::Error as ConfigError, node::NodeUserConfig};
use otel_arrow_dfe_engine::{
    ReceiverFactory, config::ReceiverConfig, context::PipelineContext,
    memory_limiter::LocalReceiverAdmissionState, node::NodeId, receiver::ReceiverWrapper,
};
use otel_arrow_dfe_otap::{OTAP_RECEIVER_FACTORIES, pdata::OtapPdata};
use otel_arrow_dfe_scraper::{
    CheckpointStore, DatabaseReceiver, DatabaseReceiverMetrics, SourceLease,
};
use serde_json::Value;
use std::{path::Path, sync::Arc};

pub use adapter::{PgError, PostgreSqlAdapter};
pub use config::PostgreSqlConfig;

/// Registry identity for this opt-in receiver.
pub const POSTGRESQL_RECEIVER_URN: &str = "urn:otel:receiver:postgresql";

fn invalid(error: impl std::fmt::Display) -> ConfigError {
    ConfigError::InvalidUserConfig {
        error: error.to_string(),
    }
}

fn parse(value: &Value) -> Result<PostgreSqlConfig, ConfigError> {
    serde_json::from_value(value.clone()).map_err(invalid)
}

fn build(
    pipeline: &PipelineContext,
    name: &str,
    value: &Value,
) -> Result<DatabaseReceiver<PostgreSqlAdapter>, ConfigError> {
    if pipeline.num_cores() != 1 {
        return Err(invalid(
            "the PostgreSQL receiver requires a single-core pipeline",
        ));
    }
    let config = parse(value)?;
    let store = CheckpointStore::new(
        Path::new(&config.checkpoint.directory),
        pipeline.pipeline_group_id().as_ref(),
        pipeline.pipeline_id().as_ref(),
        name,
        &config.source_id,
        config.fingerprint.clone(),
    );
    let lease = SourceLease::acquire(store.lease_key()).map_err(invalid)?;
    Ok(DatabaseReceiver::new(
        config.adapter(),
        config.query,
        store,
        lease,
        config.checkpoint.nack_backoff,
        config.checkpoint.max_consecutive_failures,
        config.source_id,
        LocalReceiverAdmissionState::from_process_state(&pipeline.memory_pressure_state()),
        Some(DatabaseReceiverMetrics::register(pipeline)),
    ))
}

/// Registers the vendor adapter without duplicating scheduling, encoding or checkpoint logic.
#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Receiver)]
#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
pub static POSTGRESQL_RECEIVER: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: POSTGRESQL_RECEIVER_URN,
    create: |pipeline: PipelineContext,
             node: NodeId,
             user: Arc<NodeUserConfig>,
             receiver: &ReceiverConfig,
             _| {
        let component = build(&pipeline, receiver.name.as_ref(), &user.config)?;
        Ok(ReceiverWrapper::local(component, node, user, receiver))
    },
    validate_config: |value| parse(value).map(|_| ()),
    context_declarations: None,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
};
