// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Link-time registration for the Oracle receiver component.

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

pub const ORACLE_RECEIVER_URN: &str = "urn:otel:receiver:oracle";

#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Receiver)]
#[linkme::distributed_slice(OTAP_RECEIVER_FACTORIES)]
pub static ORACLE_RECEIVER: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: ORACLE_RECEIVER_URN,
    create: create_oracle_receiver,
    validate_config: validate_oracle_config,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED,
};

fn create_oracle_receiver(
    _pipeline: PipelineContext,
    _node: NodeId,
    _node_config: Arc<NodeUserConfig>,
    _receiver_config: &ReceiverConfig,
    _capabilities: &Capabilities,
) -> Result<ReceiverWrapper<OtapPdata>, Error> {
    // Parse OracleConfig, construct OracleScraper, bind it to the shared
    // DatabaseReceiver, and return ReceiverWrapper<OtapPdata>.
    todo!("design-only pseudocode")
}

fn validate_oracle_config(_config: &Value) -> Result<(), Error> {
    // Validate shared limits and Oracle-specific connection/source settings.
    todo!("design-only pseudocode")
}
