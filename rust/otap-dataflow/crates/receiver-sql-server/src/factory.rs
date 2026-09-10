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

pub const SQL_SERVER_RECEIVER_URN: &str = "urn:otel:receiver:sql_server";

#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Receiver)]
#[linkme::distributed_slice(OTAP_RECEIVER_FACTORIES)]
pub static SQL_SERVER_RECEIVER: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: SQL_SERVER_RECEIVER_URN,
    create: create_sql_server_receiver,
    validate_config: validate_sql_server_config,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
};

fn create_sql_server_receiver(
    _pipeline: PipelineContext,
    _node: NodeId,
    _node_config: Arc<NodeUserConfig>,
    _receiver_config: &ReceiverConfig,
    _capabilities: &Capabilities,
) -> Result<ReceiverWrapper<OtapPdata>, Error> {
    todo!("bind SqlServerScraper to the shared DatabaseReceiver")
}

fn validate_sql_server_config(_config: &Value) -> Result<(), Error> {
    todo!("validate shared and SQL Server-specific configuration")
}
