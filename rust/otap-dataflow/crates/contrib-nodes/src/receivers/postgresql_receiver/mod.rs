// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL 15 bounded composite-cursor receiver.

mod adapter;
mod config;
mod convert;
mod credentials;
mod error;
mod query;
#[cfg(test)]
mod tests;
mod transport;
mod worker;

use linkme::distributed_slice;
use otel_arrow_dfe_config::{error::Error as ConfigError, node::NodeUserConfig};
use otel_arrow_dfe_engine::{
    ReceiverFactory, config::ReceiverConfig, context::PipelineContext,
    memory_limiter::LocalReceiverAdmissionState, node::NodeId, receiver::ReceiverWrapper,
};
use otel_arrow_dfe_otap::{OTAP_RECEIVER_FACTORIES, pdata::OtapPdata};
use otel_arrow_dfe_scraper::{
    CheckpointStore, DatabaseReceiver, DatabaseReceiverMetrics, SourceBinding,
};
use std::{path::Path, sync::Arc};

otel_arrow_dfe_telemetry::otel_component_scope!(
    urn = POSTGRESQL_RECEIVER_URN,
    target = "otel.receiver.postgresql",
);

/// PostgreSQL receiver registration identity.
pub const POSTGRESQL_RECEIVER_URN: &str = "urn:otel:receiver:postgresql";

#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Receiver)]
#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
/// Registers the opt-in local PostgreSQL receiver.
pub static POSTGRESQL_RECEIVER: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: POSTGRESQL_RECEIVER_URN,
    create: |pipeline, node, node_config, receiver_config, _capabilities| {
        create(pipeline, node, node_config, receiver_config)
    },
    validate_config: |value| {
        config::Config::parse(value)
            .and_then(config::Config::validate)
            .map(|_| ())
            .map_err(config_error)
    },
    context_declarations: None,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
};

fn config_error(error: error::Error) -> ConfigError {
    ConfigError::InvalidUserConfig {
        error: error.to_string(),
    }
}

fn create(
    pipeline: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    receiver_config: &ReceiverConfig,
) -> Result<ReceiverWrapper<OtapPdata>, ConfigError> {
    if pipeline.num_cores() > 1 {
        return Err(ConfigError::InvalidUserConfig {
            error: "postgresql requires a one-core source pipeline".into(),
        });
    }
    let validated = config::Config::parse(&node_config.config)
        .and_then(config::Config::validate)
        .map_err(config_error)?;
    let store = CheckpointStore::new(
        Path::new(&validated.config.checkpoint.directory),
        pipeline.pipeline_group_id().as_ref(),
        pipeline.pipeline_id().as_ref(),
        node.name.as_ref(),
        &validated.config.source_id,
        validated.fingerprint.clone(),
    );
    let binding = SourceBinding::acquire(store).map_err(|_| config_error(error::Error::Config))?;
    let query = validated.common.clone();
    let backoff = validated.config.checkpoint.nack_backoff;
    let failures = validated.config.checkpoint.max_consecutive_failures;
    let adapter = adapter::Adapter::new(validated).map_err(config_error)?;
    let metrics = DatabaseReceiverMetrics::register(&pipeline);
    let admission =
        LocalReceiverAdmissionState::from_process_state(&pipeline.memory_pressure_state());
    let receiver = DatabaseReceiver::new(
        adapter,
        query,
        binding,
        backoff,
        failures,
        admission,
        Some(metrics),
    );
    Ok(ReceiverWrapper::local(
        receiver,
        node,
        node_config,
        receiver_config,
    ))
}
