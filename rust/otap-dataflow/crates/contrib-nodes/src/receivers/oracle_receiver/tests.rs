// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use otel_arrow_dfe_engine::context::ControllerContext;
use otel_arrow_dfe_engine::receiver::ReceiverWrapper;
use otel_arrow_dfe_engine::testing::{receiver::TestRuntime, test_node};
use otel_arrow_dfe_scraper::database::OnNack;
use otel_arrow_dfe_telemetry::registry::TelemetryRegistryHandle;
use std::time::{Duration, Instant};

const COMPOSITE_STATEMENT: &str = "SELECT AUDIT_ID, LAST_UPDATED, PAYLOAD FROM AUDIT_LOGS \
     WHERE (LAST_UPDATED > :last_timestamp \
     OR (LAST_UPDATED = :last_timestamp AND AUDIT_ID > :last_tie_breaker)) \
     ORDER BY LAST_UPDATED ASC, AUDIT_ID ASC";

fn documented_config() -> Value {
    serde_json::json!({
        "source_id": "oracle-audit",
        "connection": {
            "connect_string": "database.contoso.com:1521/ORCL",
            "instant_client_dir": "/opt/oracle/instantclient"
        },
        "authentication": {
            "username_file": "/var/run/secrets/oracle/oracle-audit/username",
            "password_file": "/var/run/secrets/oracle/oracle-audit/password"
        },
        "query": {
            "statement": COMPOSITE_STATEMENT,
            "interval": "5m",
            "fetch_size": 1000,
            "max_rows_per_poll": 10000,
            "max_batch_bytes": "10 MiB",
            "timeout": "2m"
        },
        "watermark": {
            "mode": "composite",
            "timestamp": {
                "column": "LAST_UPDATED",
                "bind": "last_timestamp",
                "initial": "1970-01-01 00:00:00",
                "timezone": "UTC"
            },
            "tie_breaker": {
                "column": "AUDIT_ID",
                "bind": "last_tie_breaker",
                "initial": 0
            }
        },
        "checkpoint": {
            "directory": "${engine.state_dir}/oracle",
            "on_nack": "rewind",
            "nack_backoff": "1s",
            "max_consecutive_failures": 5
        }
    })
}

fn with_statement(statement: &str) -> Value {
    let mut config = documented_config();
    config["query"]["statement"] = serde_json::json!(statement);
    config
}

/// Scenario: SQL contains all watermark comparisons but combines them incorrectly or unions rows.
/// Guarantees: Unwatermarked branches and predicates that skip timestamp ties fail before connecting.
#[test]
fn rejects_composite_predicate_lookalikes_and_set_operations() {
    for statement in [
        "SELECT AUDIT_ID, LAST_UPDATED FROM AUDIT_LOGS WHERE LAST_UPDATED > :last_timestamp AND (LAST_UPDATED = :last_timestamp OR AUDIT_ID > :last_tie_breaker) ORDER BY LAST_UPDATED ASC, AUDIT_ID ASC",
        "SELECT AUDIT_ID, LAST_UPDATED FROM AUDIT_LOGS WHERE LAST_UPDATED > :last_timestamp OR (LAST_UPDATED = :last_timestamp AND AUDIT_ID > :last_tie_breaker) OR 1 = 1 ORDER BY LAST_UPDATED ASC, AUDIT_ID ASC",
    ] {
        assert!(parsed(with_statement(statement)).is_err());
    }
    for operation in ["UNION", "UNION ALL", "INTERSECT", "MINUS", "EXCEPT"] {
        let statement = COMPOSITE_STATEMENT.replacen(
            " ORDER BY",
            &format!(
                " {operation} SELECT AUDIT_ID, LAST_UPDATED, PAYLOAD FROM AUDIT_LOGS ORDER BY"
            ),
            1,
        );
        for statement in [statement.clone(), statement.to_ascii_lowercase()] {
            assert!(parsed(with_statement(&statement)).is_err(), "{statement}");
        }
    }
}

/// Scenario: Native Oracle configuration supplies only the original max_batch_bytes setting.
/// Guarantees: It parses without an additional field and uses that limit for both row storage and encoding.
#[test]
fn uses_one_byte_limit_for_rows_and_encoding() {
    let config = parsed(documented_config()).expect("configuration");
    assert_eq!(config.query().max_batch_bytes(), 10 * 1024 * 1024);
    assert_eq!(
        config.query().max_normalized_bytes(),
        config.query().max_batch_bytes()
    );
}

/// Scenario: Oracle configuration omits or overrides the shared catch-up budgets.
/// Guarantees: Shared defaults are preserved and explicit native settings reach the query plan.
#[test]
fn forwards_catch_up_configuration() {
    let defaults = parsed(documented_config())
        .expect("defaults")
        .query()
        .catch_up();
    assert_eq!(defaults.max_pages, 32);
    assert_eq!(defaults.max_duration, Duration::from_secs(10));
    let mut config = documented_config();
    config["query"]["catch_up"] = serde_json::json!({"max_pages": 1, "max_duration": "2s"});
    let configured = parsed(config).expect("override").query().catch_up();
    assert_eq!(configured.max_pages, 1);
    assert_eq!(configured.max_duration, Duration::from_secs(2));
}

/// Scenario: Oracle configuration supplies an invalid shared catch-up page budget.
/// Guarantees: The vendor wrapper applies shared validation instead of bypassing it.
#[test]
fn rejects_invalid_catch_up_budget() {
    for pages in [0, 1025] {
        let mut config = documented_config();
        config["query"]["catch_up"] = serde_json::json!({"max_pages": pages});
        assert!(parsed(config).is_err());
    }
}

/// Scenario: Configuration includes the removed independent normalized-row byte setting.
/// Guarantees: The obsolete field is rejected rather than silently ignored.
#[test]
fn rejects_obsolete_normalized_byte_limit() {
    let mut config = documented_config();
    config["query"]["max_normalized_bytes"] = serde_json::json!("5 MiB");
    let error = parsed(config).err().expect("obsolete field must fail");
    assert!(
        error
            .to_string()
            .contains("invalid Oracle receiver configuration")
    );
}

/// Scenario: Oracle accepts date-only and timezone-naive ISO initial timestamps.
/// Guarantees: Initial cursors are normalized to the same text representation as fetched cursors.
#[test]
fn normalizes_all_accepted_initial_timestamp_spellings() {
    for initial in ["1970-01-01T00:00:00", "1970-01-01"] {
        let mut config = documented_config();
        config["watermark"]["timestamp"]["initial"] = serde_json::json!(initial);
        let plan = parsed(config).expect("Oracle timestamp").query();
        assert!(
            plan.watermark()
                .initial
                .timestamp
                .starts_with("1970-01-01 00:00:00")
        );
    }
}

fn parsed(config: Value) -> Result<OracleReceiverConfig, serde_json::Error> {
    serde_json::from_value(config)
}

fn pipeline_context() -> PipelineContext {
    ControllerContext::new(TelemetryRegistryHandle::new()).pipeline_context_with(
        "group".into(),
        "pipeline".into(),
        0,
        1,
        0,
    )
}

/// Scenario: The engine attempts to create the unpartitioned Oracle receiver on two cores.
/// Guarantees: Factory validation rejects duplicate per-core pollers before acquiring a lease.
#[test]
fn factory_rejects_multi_core_placement() {
    let pipeline = ControllerContext::new(TelemetryRegistryHandle::new()).pipeline_context_with(
        "group".into(),
        "pipeline".into(),
        0,
        2,
        0,
    );
    let runtime = TestRuntime::<OtapPdata>::new();
    let result = (ORACLE_RECEIVER.create)(
        pipeline,
        test_node("oracle-test"),
        Arc::new(NodeUserConfig::new_receiver_config(ORACLE_RECEIVER_URN)),
        runtime.config(),
        &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
    );
    assert!(matches!(
        result,
        Err(ConfigError::InvalidUserConfig { error }) if error.contains("single-core")
    ));
}

/// Scenario: The receiver loads the complete documented composite configuration.
/// Guarantees: The public schema builds a query plan whose cursor binds, checkpoint policy, and
/// source identity are all present, so the documented example remains runnable.
#[test]
fn accepts_the_documented_composite_configuration() {
    let config = parsed(documented_config()).expect("configuration should deserialize");

    assert_eq!(config.source_id(), "oracle-audit");
    assert_eq!(config.checkpoint().on_nack, OnNack::Rewind);
    assert_eq!(config.checkpoint().nack_backoff, Duration::from_secs(1));
    let query = config.query();
    assert_eq!(query.watermark().timestamp_bind, "last_timestamp");
    assert_eq!(query.watermark().tie_breaker_bind, "last_tie_breaker");
    assert_eq!(query.watermark().initial.tie_breaker, 0);
}

/// Scenario: Required watermark, checkpoint, or byte-limit sections are omitted.
/// Guarantees: Every operational bound and cursor field stays explicit, so a receiver can never
/// silently run without a durable checkpoint or a byte ceiling.
#[test]
fn requires_every_operational_and_cursor_field() {
    for section in ["watermark", "checkpoint"] {
        let mut config = documented_config();
        _ = config
            .as_object_mut()
            .expect("config object")
            .remove(section);
        assert!(
            parsed(config).is_err(),
            "required section '{section}' must not be optional"
        );
    }
    for field in [
        "interval",
        "fetch_size",
        "max_rows_per_poll",
        "max_batch_bytes",
        "timeout",
    ] {
        let mut config = documented_config();
        _ = config["query"]
            .as_object_mut()
            .expect("query object")
            .remove(field);
        assert!(
            parsed(config).is_err(),
            "required query field '{field}' must not be optional"
        );
    }
    let mut config = documented_config();
    _ = config["checkpoint"]
        .as_object_mut()
        .expect("checkpoint object")
        .remove("nack_backoff");
    assert!(parsed(config).is_err());
}

/// Scenario: A statement omits a bind, or references one only inside a literal or as a prefix.
/// Guarantees: Both committed cursor components must appear as real bind markers, so a query
/// cannot silently ignore the checkpoint and re-read the full table every poll.
#[test]
fn requires_both_cursor_binds_as_real_markers() {
    let missing_bind = "SELECT AUDIT_ID, LAST_UPDATED FROM AUDIT_LOGS \
         WHERE LAST_UPDATED > :last_timestamp \
         ORDER BY LAST_UPDATED ASC, AUDIT_ID ASC";
    assert!(parsed(with_statement(missing_bind)).is_err());

    let prefix_only = "SELECT AUDIT_ID, LAST_UPDATED FROM AUDIT_LOGS \
         WHERE LAST_UPDATED > :last_timestamp_extra AND AUDIT_ID > :last_tie_breaker \
         ORDER BY LAST_UPDATED ASC, AUDIT_ID ASC";
    assert!(parsed(with_statement(prefix_only)).is_err());

    let literal_only = "SELECT AUDIT_ID, LAST_UPDATED FROM AUDIT_LOGS \
         WHERE ':last_timestamp' = ':last_timestamp' AND AUDIT_ID > :last_tie_breaker \
         ORDER BY LAST_UPDATED ASC, AUDIT_ID ASC";
    assert!(parsed(with_statement(literal_only)).is_err());
}

/// Scenario: Both bind names exist, but the query does not use the strict composite predicate.
/// Guarantees: A full-table or inclusive-boundary query is rejected before polling so an ACKed
/// page cannot repeatedly commit the same cursor.
#[test]
fn requires_the_strict_composite_predicate() {
    let no_predicate = "SELECT AUDIT_ID, LAST_UPDATED FROM AUDIT_LOGS \
         WHERE :last_timestamp IS NOT NULL AND :last_tie_breaker IS NOT NULL \
         ORDER BY LAST_UPDATED ASC, AUDIT_ID ASC";
    assert!(parsed(with_statement(no_predicate)).is_err());

    let inclusive = "SELECT AUDIT_ID, LAST_UPDATED FROM AUDIT_LOGS \
         WHERE (LAST_UPDATED >= :last_timestamp OR (LAST_UPDATED = :last_timestamp \
         AND AUDIT_ID > :last_tie_breaker)) ORDER BY LAST_UPDATED ASC, AUDIT_ID ASC";
    assert!(parsed(with_statement(inclusive)).is_err());
}

/// Scenario: A cursor column is configured with quoted, qualified, or unsafe identifier syntax.
/// Guarantees: Only plain Oracle identifiers can participate in validated paging SQL, preventing
/// configuration text from becoming executable SQL syntax.
#[test]
fn rejects_unsafe_cursor_identifiers() {
    for column in ["AUDIT.ID", "\"AUDIT_ID\"", "AUDIT ID", "AUDIT_ID;DELETE"] {
        let mut config = documented_config();
        config["watermark"]["tie_breaker"]["column"] = serde_json::json!(column);
        assert!(parsed(config).is_err(), "unsafe identifier '{column}'");
    }
}

/// Scenario: The configured initial timestamp cannot be represented by Oracle's timestamp type.
/// Guarantees: Invalid initial state fails configuration instead of surfacing only after the
/// receiver has acquired its source lease and started database work.
#[test]
fn rejects_invalid_initial_timestamp() {
    let mut config = documented_config();
    config["watermark"]["timestamp"]["initial"] = serde_json::json!("not-a-timestamp");

    assert!(parsed(config).is_err());
}

/// Scenario: Initial timestamp components overflow or narrow in the native parser, or exceed its precision.
/// Guarantees: Configuration returns a field-specific error without panicking or changing the initial position.
#[test]
fn rejects_oversized_or_wrapping_initial_timestamps() {
    for initial in [
        "9".repeat(40),
        "4294969322-01-01 00:00:00".to_owned(),
        "2026-4294967297-01 00:00:00".to_owned(),
        "2026-01-01 00:00:00.1234567890".to_owned(),
        "2026-01-01 00:00:00 +4294967297:00".to_owned(),
    ] {
        let mut config = documented_config();
        config["watermark"]["timestamp"]["initial"] = serde_json::json!(initial);
        let error = parsed(config).err().expect("unsafe timestamp must fail");
        assert!(error.to_string().contains("watermark.timestamp.initial"));
    }
}

/// Scenario: Nested comparisons satisfy token checks but the only top-level WHERE occurs after ORDER BY.
/// Guarantees: Invalid clause ordering returns an error instead of panicking during predicate slicing.
#[test]
fn rejects_where_after_order_without_panicking() {
    let mut config = documented_config();
    config["watermark"]["timestamp"]["column"] = serde_json::json!("WHERE");
    config["query"]["statement"] = serde_json::json!(
        "SELECT (WHERE > :last_timestamp), (WHERE = :last_timestamp), \
         (AUDIT_ID > :last_tie_breaker) FROM AUDIT_LOGS \
         ORDER BY WHERE ASC, AUDIT_ID ASC"
    );
    let error = parsed(config).err().expect("misordered clauses must fail");
    assert!(
        error
            .to_string()
            .contains("WHERE predicate before ORDER BY")
    );
}

/// Scenario: Malformed fields, enum values, and unknown keys contain sensitive configuration text.
/// Guarantees: Neither deserialization errors nor engine diagnostics expose those supplied values.
#[test]
fn configuration_errors_redact_supplied_values() {
    use otel_arrow_dfe_engine::error::{Error, error_summary_from};

    const SENTINEL: &str = "PRIVATE_CONFIGURATION_SENTINEL";
    let mut invalid_cursor = documented_config();
    invalid_cursor["watermark"]["tie_breaker"]["initial"] = serde_json::json!(SENTINEL);
    let mut invalid_mode = documented_config();
    invalid_mode["watermark"]["mode"] = serde_json::json!(SENTINEL);
    let mut invalid_authentication = documented_config();
    invalid_authentication["authentication"] = serde_json::json!(SENTINEL);
    let mut unknown_field = documented_config();
    unknown_field["query"][SENTINEL] = serde_json::json!(SENTINEL);

    for config in [
        invalid_cursor,
        invalid_mode,
        invalid_authentication,
        unknown_field,
    ] {
        let direct_error = parsed(config.clone()).err().expect("invalid schema");
        assert!(!direct_error.to_string().contains(SENTINEL));
        let error = (ORACLE_RECEIVER.validate_config)(&config).expect_err("invalid schema");
        let engine = Error::ConfigError(Box::new(error));
        for rendered in [
            engine.to_string(),
            format!("{engine:?}"),
            serde_json::to_string(&error_summary_from(&engine)).expect("diagnostic JSON"),
        ] {
            assert!(!rendered.contains(SENTINEL), "{rendered}");
        }
    }
}

/// Scenario: A statement's final ordering is descending, reordered, missing, or only nested
/// inside a subquery.
/// Guarantees: The outer result must be ascending by timestamp then tie-breaker, so paging by the
/// composite cursor cannot skip rows an unordered or differently ordered result would return.
#[test]
fn requires_the_final_outer_ascending_ordering() {
    let descending = "SELECT AUDIT_ID, LAST_UPDATED FROM AUDIT_LOGS \
         WHERE LAST_UPDATED > :last_timestamp OR (LAST_UPDATED = :last_timestamp \
         AND AUDIT_ID > :last_tie_breaker) ORDER BY LAST_UPDATED DESC, AUDIT_ID ASC";
    assert!(parsed(with_statement(descending)).is_err());

    let reversed = "SELECT AUDIT_ID, LAST_UPDATED FROM AUDIT_LOGS \
         WHERE LAST_UPDATED > :last_timestamp OR (LAST_UPDATED = :last_timestamp \
         AND AUDIT_ID > :last_tie_breaker) ORDER BY AUDIT_ID ASC, LAST_UPDATED ASC";
    assert!(parsed(with_statement(reversed)).is_err());

    let missing = "SELECT AUDIT_ID, LAST_UPDATED FROM AUDIT_LOGS \
         WHERE LAST_UPDATED > :last_timestamp OR (LAST_UPDATED = :last_timestamp \
         AND AUDIT_ID > :last_tie_breaker)";
    assert!(parsed(with_statement(missing)).is_err());

    let nested_only = "SELECT AUDIT_ID, LAST_UPDATED FROM \
         (SELECT AUDIT_ID, LAST_UPDATED FROM AUDIT_LOGS WHERE LAST_UPDATED > :last_timestamp \
         OR (LAST_UPDATED = :last_timestamp AND AUDIT_ID > :last_tie_breaker) \
         ORDER BY LAST_UPDATED ASC, AUDIT_ID ASC)";
    assert!(parsed(with_statement(nested_only)).is_err());

    let nested_then_wrong_outer = "SELECT AUDIT_ID, LAST_UPDATED FROM \
         (SELECT AUDIT_ID, LAST_UPDATED FROM AUDIT_LOGS WHERE LAST_UPDATED > :last_timestamp \
         OR (LAST_UPDATED = :last_timestamp AND AUDIT_ID > :last_tie_breaker) \
         ORDER BY LAST_UPDATED ASC, AUDIT_ID ASC) ORDER BY AUDIT_ID DESC";
    assert!(parsed(with_statement(nested_then_wrong_outer)).is_err());
}

/// Scenario: A statement contains SQL comments or appends a second SELECT or DELETE.
/// Guarantees: Extra statements and comments are rejected before any database execution.
#[test]
fn rejects_comments_and_multiple_statements() {
    let commented = "SELECT AUDIT_ID, LAST_UPDATED FROM AUDIT_LOGS \
         WHERE LAST_UPDATED > :last_timestamp OR (LAST_UPDATED = :last_timestamp \
         AND AUDIT_ID > :last_tie_breaker) ORDER BY LAST_UPDATED ASC, AUDIT_ID ASC -- trailing";
    assert!(parsed(with_statement(commented)).is_err());

    for statement in [
        format!("{COMPOSITE_STATEMENT}; SELECT 1 FROM DUAL"),
        format!("{COMPOSITE_STATEMENT}; DELETE FROM AUDIT_LOGS"),
        "SELECT 1; DELETE FROM audit_logs".to_owned(),
    ] {
        assert!(parsed(with_statement(&statement)).is_err(), "{statement}");
    }
}

/// Scenario: An otherwise valid watermark query adds an Oracle row-locking clause.
/// Guarantees: FOR UPDATE variants are rejected before connecting or acquiring row locks.
#[test]
fn rejects_row_locking_selects() {
    for clause in [
        "FOR UPDATE",
        "FOR UPDATE OF AUDIT_ID",
        "FOR UPDATE NOWAIT",
        "FOR UPDATE SKIP LOCKED",
        "for update wait 1",
    ] {
        let statement = format!("{COMPOSITE_STATEMENT} {clause}");
        assert!(parsed(with_statement(&statement)).is_err(), "{statement}");
    }
}

/// Scenario: A valid composite statement uses a trailing semicolon and extra surrounding spacing.
/// Guarantees: Ordinary operator formatting is accepted, so validation rejects unsafe SQL rather
/// than merely unusual whitespace.
#[test]
fn accepts_ordinary_statement_formatting() {
    let formatted = format!("  {COMPOSITE_STATEMENT} ;  ");

    assert!(parsed(with_statement(&formatted)).is_ok());
}

/// Scenario: An Oracle query contains a field absent from the closed public schema.
/// Guarantees: Undocumented fields are rejected rather than silently accepting behavior the
/// receiver does not implement.
#[test]
fn rejects_undocumented_oracle_query_fields() {
    for (field, value) in [
        ("name", serde_json::json!("audit-query")),
        ("error_policy", serde_json::json!("fail_batch")),
        (
            "output",
            serde_json::json!({"include_columns": ["AUDIT_ID"]}),
        ),
    ] {
        let mut config = documented_config();
        config["query"][field] = value;
        assert!(
            parsed(config).is_err(),
            "undocumented query field '{field}' must be rejected"
        );
    }
}

/// Scenario: Oracle configuration uses an unsupported call timeout.
/// Guarantees: Sub-millisecond and excessively long timeouts fail before opening a connection.
#[test]
fn rejects_unsupported_oracle_timeouts() {
    for timeout in ["500us", "6m"] {
        let mut config = documented_config();
        config["query"]["timeout"] = serde_json::json!(timeout);
        assert!(
            parsed(config).is_err(),
            "unsupported timeout '{timeout}' must be rejected"
        );
    }
}

/// Scenario: A source identifier is large enough to amplify every emitted row.
/// Guarantees: Repeated OTLP resource identity remains bounded by configuration validation.
#[test]
fn rejects_oversized_source_id() {
    let mut config = documented_config();
    config["source_id"] = serde_json::json!("x".repeat(257));

    assert!(parsed(config).is_err());
}

/// Scenario: Two configurations differ only in mounted credential paths, or only in semantics.
/// Guarantees: Rotating a secret preserves the durable checkpoint, while changing the query or a
/// cursor definition invalidates it so an unrelated position is never resumed.
#[test]
fn fingerprint_tracks_semantics_and_ignores_credential_paths() {
    let baseline = parsed(documented_config()).expect("baseline should parse");

    let mut rotated = documented_config();
    rotated["authentication"]["password_file"] = serde_json::json!("/var/run/secrets/rotated");
    rotated["connection"]["instant_client_dir"] = serde_json::json!("/opt/oracle/ic-23");
    let rotated = parsed(rotated).expect("rotated credentials should parse");
    assert_eq!(baseline.config_fingerprint(), rotated.config_fingerprint());

    let mut different_cursor = documented_config();
    different_cursor["watermark"]["tie_breaker"]["initial"] = serde_json::json!(100);
    let different_cursor = parsed(different_cursor).expect("changed cursor should parse");
    assert_ne!(
        baseline.config_fingerprint(),
        different_cursor.config_fingerprint()
    );

    let mut different_source = documented_config();
    different_source["source_id"] = serde_json::json!("oracle-orders");
    let different_source = parsed(different_source).expect("changed source should parse");
    assert_ne!(
        baseline.config_fingerprint(),
        different_source.config_fingerprint()
    );
}

/// Scenario: A pipeline validates a documented Oracle node before instantiating it.
/// Guarantees: Configuration validation succeeds repeatedly without acquiring a source lease, so
/// validating a pipeline never blocks the receiver that later runs it.
#[test]
fn validation_does_not_acquire_a_source_lease() {
    validate(&documented_config()).expect("documented configuration should validate");
    validate(&documented_config()).expect("validation must be repeatable");
}

/// Scenario: Two receivers in one process are built against the same checkpoint source.
/// Guarantees: The second build fails while the first owner lives, so two receivers can never
/// race to advance one durable checkpoint and duplicate or lose rows.
#[test]
fn duplicate_source_receivers_cannot_be_built() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut config = documented_config();
    config["checkpoint"]["directory"] =
        serde_json::json!(directory.path().to_str().expect("UTF-8 path"));
    let context = pipeline_context();

    let first = build(&context, "oracle-audit", &config).expect("first receiver should build");
    assert!(build(&context, "oracle-audit", &config).is_err());

    drop(first);
    _ = build(&context, "oracle-audit", &config)
        .expect("the source becomes available after the owner is dropped");
}

/// Scenario: A live Oracle database is explicitly configured for an end-to-end smoke test.
/// Guarantees: The registered receiver validates cursor metadata, binds the committed composite
/// cursor, and emits OTLP logs for the selected rows.
#[test]
fn emits_oracle_rows_when_live_test_is_enabled() {
    if std::env::var_os("OTAP_ORACLE_RECEIVER_E2E").is_none() {
        return;
    }
    let state_directory = tempfile::tempdir().expect("temporary directory");
    let config = serde_json::json!({
        "source_id": "otap-oracle-events",
        "connection": {
            "connect_string": std::env::var("ORACLE_CONNECT_STRING")
                .unwrap_or_else(|_| "//localhost:1521/FREEPDB1".to_owned()),
            "instant_client_dir": std::env::var("ORACLE_INSTANT_CLIENT_DIR")
                .unwrap_or_else(|_| "C:\\oracle\\instantclient".to_owned())
        },
        "authentication": {
            "username_file": std::env::var("ORACLE_USERNAME_FILE")
                .unwrap_or_else(|_| "C:\\secrets\\oracle-username".to_owned()),
            "password_file": std::env::var("ORACLE_PASSWORD_FILE")
                .unwrap_or_else(|_| "C:\\secrets\\oracle-password".to_owned())
        },
        "query": {
            "statement": "SELECT EVENT_ID, EVENT_TS, PAYLOAD FROM OTAP_ORACLE_EVENTS \
                WHERE (EVENT_TS > :last_timestamp OR (EVENT_TS = :last_timestamp \
                AND EVENT_ID > :last_tie_breaker)) ORDER BY EVENT_TS ASC, EVENT_ID ASC",
            "interval": "100ms",
            "fetch_size": 10,
            "max_rows_per_poll": 10,
            "max_batch_bytes": "10 MiB",
            "timeout": "10s"
        },
        "watermark": {
            "mode": "composite",
            "timestamp": {
                "column": "EVENT_TS",
                "bind": "last_timestamp",
                "initial": "1970-01-01 00:00:00",
                "timezone": "UTC"
            },
            "tie_breaker": {
                "column": "EVENT_ID",
                "bind": "last_tie_breaker",
                "initial": 0
            }
        },
        "checkpoint": {
            "directory": state_directory.path().to_str().expect("UTF-8 path"),
            "on_nack": "rewind",
            "nack_backoff": "1s",
            "max_consecutive_failures": 5
        }
    });
    let receiver =
        build(&pipeline_context(), "oracle-e2e", &config).expect("receiver config should build");
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
            ctx.sleep(Duration::from_millis(500)).await;
            ctx.send_shutdown(
                Instant::now() + Duration::from_secs(5),
                "Oracle receiver E2E complete",
            )
            .await
            .expect("shutdown should enqueue");
        })
        .run_validation(|mut ctx| async move {
            let mut pdata = ctx.recv().await.expect("receiver should emit pdata");
            assert!(pdata.num_items() >= 1);
        });
}
