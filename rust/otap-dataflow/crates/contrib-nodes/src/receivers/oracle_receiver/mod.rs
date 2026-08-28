// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Oracle database polling receiver.

use self::adapter::OracleAdapter;
use self::config::OracleConnectionConfig;
use crate::receivers::database::config::DatabaseReceiverConfig;
use crate::receivers::database::receiver::DatabaseReceiver;
use linkme::distributed_slice;
use otap_df_config::error::Error as ConfigError;
use otap_df_config::node::NodeUserConfig;
use otap_df_config::validation::validate_typed_config;
use otap_df_engine::ReceiverFactory;
use otap_df_engine::config::ReceiverConfig;
use otap_df_engine::context::PipelineContext;
use otap_df_engine::node::NodeId;
use otap_df_engine::receiver::ReceiverWrapper;
use otap_df_otap::OTAP_RECEIVER_FACTORIES;
use otap_df_otap::pdata::OtapPdata;
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;

mod adapter;
mod config;
mod worker;

otap_df_telemetry::otel_component_scope!(
    urn = ORACLE_RECEIVER_URN,
    target = "otel.receiver.oracle",
);

/// URN for the Oracle database receiver.
pub const ORACLE_RECEIVER_URN: &str = "urn:otel:receiver:oracle";

type OracleReceiver = DatabaseReceiver<OracleAdapter>;

#[derive(Deserialize)]
#[serde(try_from = "DatabaseReceiverConfig<OracleConnectionConfig>")]
struct OracleReceiverConfig {
    inner: DatabaseReceiverConfig<OracleConnectionConfig>,
}

impl TryFrom<DatabaseReceiverConfig<OracleConnectionConfig>> for OracleReceiverConfig {
    type Error = String;

    fn try_from(
        inner: DatabaseReceiverConfig<OracleConnectionConfig>,
    ) -> Result<Self, Self::Error> {
        inner.validate()?;
        inner.database.validate()?;
        if inner.limits.max_concurrent_queries > inner.database.pool.max_connections {
            return Err(
                "limits.max_concurrent_queries must not exceed database.pool.max_connections"
                    .to_owned(),
            );
        }
        Ok(Self { inner })
    }
}

impl OracleReceiverConfig {
    fn create_receiver(
        pipeline: PipelineContext,
        config: &Value,
    ) -> Result<OracleReceiver, ConfigError> {
        if pipeline.num_cores() != 1 {
            return Err(ConfigError::InvalidUserConfig {
                error: "the Oracle receiver requires a single-core pipeline".to_owned(),
            });
        }
        let config: Self = serde_json::from_value(config.clone()).map_err(|error| {
            ConfigError::InvalidUserConfig {
                error: error.to_string(),
            }
        })?;
        let username = config.inner.database.username.clone();
        let password_env = config.inner.database.password_env.clone();
        let adapter = OracleAdapter::new(config.inner.database.clone());
        DatabaseReceiver::new(&pipeline, config.inner, adapter, username, password_env).map_err(
            |error| ConfigError::InvalidUserConfig {
                error: error.to_string(),
            },
        )
    }
}

/// Declares the Oracle receiver as a local receiver factory.
#[allow(unsafe_code)]
#[otap_df_engine::component_inventory(category = Receiver)]
#[distributed_slice(OTAP_RECEIVER_FACTORIES)]
pub static ORACLE_RECEIVER: ReceiverFactory<OtapPdata> = ReceiverFactory {
    name: ORACLE_RECEIVER_URN,
    create: |pipeline: PipelineContext,
             node: NodeId,
             node_config: Arc<NodeUserConfig>,
             receiver_config: &ReceiverConfig,
             _capabilities: &otap_df_engine::capability::registry::Capabilities| {
        Ok(ReceiverWrapper::local(
            OracleReceiverConfig::create_receiver(pipeline, &node_config.config)?,
            node,
            node_config,
            receiver_config,
        ))
    },
    validate_config: validate_typed_config::<OracleReceiverConfig>,
    wiring_contract: otap_df_engine::wiring_contract::WiringContract::UNRESTRICTED,
};

#[cfg(test)]
mod tests {
    use super::*;
    use otap_df_engine::receiver::ReceiverWrapper;
    use otap_df_engine::testing::{receiver::TestRuntime, test_node, test_pipeline_ctx};
    use std::time::{Duration, Instant};

    fn test_config() -> OracleReceiverConfig {
        serde_json::from_value(serde_json::json!({
            "database": {
                "connect_string": "//localhost:1521/FREEPDB1",
                "username": "PDBADMIN"
            },
            "queries": {
                "current_time": {
                    "sql": "SELECT SYSDATE AS CURRENT_TIME FROM DUAL",
                    "interval": "30s",
                    "timeout": "10s",
                    "pagination": {"max_rows": 100}
                }
            }
        }))
        .expect("test config should deserialize")
    }

    /// Scenario: Oracle configuration declares one named query and omits optional limits.
    /// Guarantees: bounded shared and Oracle-specific defaults are applied.
    #[test]
    fn config_applies_bounded_defaults() {
        let config = test_config();
        assert_eq!(config.inner.database.password_env, "ORACLE_PWD");
        assert_eq!(config.inner.limits.max_batch_rows, 1_000);
        assert_eq!(config.inner.database.pool.max_connections, 1);
        assert_eq!(config.inner.queries.len(), 1);
    }

    /// Scenario: receiver concurrency exceeds the bounded Oracle worker pool.
    /// Guarantees: semantic validation rejects a configuration that cannot honor admission limits.
    #[test]
    fn config_rejects_concurrency_above_pool_limit() {
        let result = serde_json::from_value::<OracleReceiverConfig>(serde_json::json!({
            "database": {
                "connect_string": "//localhost:1521/FREEPDB1",
                "username": "PDBADMIN",
                "pool": {"max_connections": 1}
            },
            "limits": {"max_concurrent_queries": 2},
            "queries": {
                "current_time": {
                    "sql": "SELECT SYSDATE AS CURRENT_TIME FROM DUAL",
                    "interval": "30s",
                    "timeout": "10s"
                }
            }
        }));
        assert!(result.is_err());
    }

    /// Scenario: local Oracle credentials opt in to a live receiver test.
    /// Guarantees: the shared runtime polls through the dedicated OCI worker and emits OTAP data.
    #[test]
    fn oracle_receiver_emits_rows_when_configured() {
        if std::env::var_os("OTAP_ORACLE_RECEIVER_E2E").is_none() {
            return;
        }
        let config = serde_json::json!({
            "database": {
                "connect_string": std::env::var("ORACLE_CONNECT_STRING")
                    .unwrap_or_else(|_| "//localhost:1521/FREEPDB1".to_owned()),
                "username": std::env::var("ORACLE_USERNAME")
                    .unwrap_or_else(|_| "PDBADMIN".to_owned())
            },
            "queries": {
                "current_time": {
                    "sql": "SELECT SYSDATE AS CURRENT_TIME FROM DUAL",
                    "interval": "100ms",
                    "timeout": "10s",
                    "pagination": {"max_rows": 10}
                }
            }
        });
        let (pipeline, _registry) = test_pipeline_ctx();
        let receiver =
            OracleReceiverConfig::create_receiver(pipeline, &config).expect("receiver config");
        let test_runtime = TestRuntime::<OtapPdata>::new();
        let node_config = Arc::new(NodeUserConfig::new_receiver_config(ORACLE_RECEIVER_URN));
        let receiver_wrapper = ReceiverWrapper::local(
            receiver,
            test_node(test_runtime.config().name.clone()),
            node_config,
            test_runtime.config(),
        );

        test_runtime
            .set_receiver(receiver_wrapper)
            .run_test(|ctx| async move {
                ctx.sleep(Duration::from_millis(250)).await;
                ctx.send_shutdown(Instant::now(), "Oracle receiver E2E complete")
                    .await
                    .expect("shutdown should enqueue");
            })
            .run_validation(|mut ctx| async move {
                let pdata = ctx.recv().await.expect("receiver should emit pdata");
                assert_eq!(pdata.num_items(), 1);
            });
    }
}
