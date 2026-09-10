// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use otel_arrow_dfe_config::error::Error;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::ReceiverFactory;
use otel_arrow_dfe_engine::capability::registry::Capabilities;
use otel_arrow_dfe_engine::config::ReceiverConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::receiver::ReceiverWrapper;
use otel_arrow_dfe_otap::OTAP_RECEIVER_FACTORIES;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use serde_json::Value;
use std::sync::Arc;

pub const POSTGRESQL_RECEIVER_URN: &str = "urn:otel:receiver:postgresql";

#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Receiver)]
#[linkme::distributed_slice(OTAP_RECEIVER_FACTORIES)]
pub static POSTGRESQL_RECEIVER: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: POSTGRESQL_RECEIVER_URN,
    create: create_postgresql_receiver,
    validate_config: validate_postgresql_config,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
};

fn create_postgresql_receiver(
    _pipeline: PipelineContext,
    _node: NodeId,
    _node_config: Arc<NodeUserConfig>,
    _receiver_config: &ReceiverConfig,
    _capabilities: &Capabilities,
) -> Result<ReceiverWrapper<OtapPdata>, Error> {
    todo!("bind PostgresqlScraper to the shared DatabaseReceiver")
}

fn validate_postgresql_config(_config: &Value) -> Result<(), Error> {
    todo!("validate shared and PostgreSQL-specific configuration")
}
