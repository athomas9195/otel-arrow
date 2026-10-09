// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Private adapter tests remain colocated logically without exposing implementation internals.
macro_rules! postgresql_module_tests {
    (value) => {
        mod tests {
            use super::*;

            /// Scenario: Native PostgreSQL timestamps cover years before the epoch, leap days, and microsecond boundaries.
            /// Guarantees: Both native timestamp types emit exact ISO 8601 UTC text with six fractional digits and a Z suffix.
            #[test]
            fn direct_timestamp_format_matches_native_text_contract() {
                for (year, month, day) in [
                    (1, 1, 1),
                    (1960, 2, 29),
                    (2000, 1, 1),
                    (2026, 12, 31),
                    (9999, 12, 31),
                ] {
                    for micros in [0, 1, 1000, 123456, 999999] {
                        let dt = NaiveDate::from_ymd_opt(year, month, day)
                            .expect("date")
                            .and_hms_micro_opt(23, 59, 59, micros)
                            .expect("time");
                        let bytes = dt
                            .signed_duration_since(epoch().expect("epoch"))
                            .num_microseconds()
                            .expect("native range")
                            .to_be_bytes();
                        assert_eq!(
                            decode(&Type::TIMESTAMP, &bytes).expect("timestamp"),
                            CellValue::Timestamp(dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
                        );
                        assert_eq!(
                            decode(&Type::TIMESTAMPTZ, &bytes).expect("timestamptz"),
                            CellValue::TimestampTz(dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
                        );
                    }
                }
            }

            /// Scenario: Sorted rows share timestamps or continuously advance to a new timestamp.
            /// Guarantees: The single-entry cache preserves exact values and reuses fixed storage instead of growing with row count.
            #[test]
            fn timestamp_cache_is_exact_and_bounded() {
                let mut cache = CursorTimestampCache::default();
                let _ = cache.decode(&0i64.to_be_bytes(), 6).expect("initial");
                let capacity = cache.text.capacity();
                for micros in [-1i64, 0, 0, 1, 1, 123456, 1_000_000, 86_400_000_000] {
                    let bytes = micros.to_be_bytes();
                    let dt = timestamp(&bytes).expect("native timestamp");
                    let (cached, text) = cache.decode(&bytes, 6).expect("decoded");
                    assert_eq!(cached, dt);
                    assert_eq!(text, dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string());
                    assert_eq!(cache.text.capacity(), capacity);
                }
                let micros = 1i64.to_be_bytes();
                assert!(cache.decode(&micros, 6).is_ok());
                assert!(
                    cache.decode(&micros, 3).is_err(),
                    "precision is part of the cache key"
                );
                assert!(cache.decode(&i64::MAX.to_be_bytes(), 6).is_err());
                assert!(cache.decode(&[0; 7], 6).is_err());
                assert_eq!(
                    cache.decode(&micros, 6).expect("valid").1,
                    "2000-01-01T00:00:00.000001Z"
                );
            }

            /// Scenario: A native cursor timestamp is checked without parsing its formatted string.
            /// Guarantees: Typmod precision, invalid modifiers, and leap-second rejection match checkpoint parsing.
            #[test]
            fn native_cursor_validation_preserves_precision_checks() {
                let date = NaiveDate::from_ymd_opt(2026, 1, 1).expect("date");
                for micros in [0, 1, 1000, 123000, 123001, 999999] {
                    let dt = date.and_hms_micro_opt(0, 0, 0, micros).expect("time");
                    for modifier in [-2, -1, 0, 1, 2, 3, 4, 5, 6, 7] {
                        assert_eq!(
                            validate_timestamp(dt, modifier).is_ok(),
                            cursor_time(&timestamp_text(dt).expect("formatted"), modifier).is_ok()
                        );
                    }
                }
                assert!(
                    validate_timestamp(
                        date.and_hms_nano_opt(23, 59, 59, 1_000_000_000)
                            .expect("leap second"),
                        6
                    )
                    .is_err()
                );
            }
        }
    };
    (worker) => {
        mod tests {
            use super::*;

            /// Scenario: Native cursor positions cross equal timestamps, integer boundaries, and timestamp transitions.
            /// Guarantees: The allocation-free ordering check agrees with the shared durable-cursor comparison.
            #[test]
            fn native_cursor_order_matches_shared_order() {
                use otel_arrow_dfe_scraper::database::Cursor;
                let timestamp = convert::cursor_time("2026-01-01T00:00:00.123456Z", 6)
                    .expect("timestamp")
                    .naive_utc();
                for (previous, next) in [
                    ((timestamp, 1), (timestamp, 1)),
                    ((timestamp, 1), (timestamp, 2)),
                    ((timestamp, 0), (timestamp, -1)),
                    ((timestamp, i64::MIN), (timestamp, i64::MAX)),
                    (
                        (timestamp, i64::MAX),
                        (timestamp + chrono::Duration::microseconds(1), i64::MIN),
                    ),
                    (
                        (timestamp, 1),
                        (timestamp - chrono::Duration::microseconds(1), 2),
                    ),
                ] {
                    let cursor = |position: convert::CursorPosition| {
                        Cursor::composite(
                            position.0.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string(),
                            position.1,
                        )
                    };
                    assert_eq!(
                        ensure_advance(&previous, &next).is_ok(),
                        cursor(next).compare(&cursor(previous)).expect("compatible")
                            == std::cmp::Ordering::Greater
                    );
                }
            }

            /// Scenario: Row sizes change from narrow to wide while pages approach their byte ceiling.
            /// Guarantees: The first fetch probes one row; subsequent fetches respect remaining bytes and the caller's configured row limit.
            #[test]
            fn fetch_size_adapts_to_observed_rows() {
                let mut size = FetchSize::default();
                let budget = 8 * 1024 * 1024;
                assert_eq!(size.target(MAX_PAGE_ROWS, budget), 1);
                size.observe(512);
                assert_eq!(size.target(300, budget), 300);
                assert_eq!(size.target(17, budget), 17);
                assert_eq!(size.target(MAX_PAGE_ROWS, budget), MAX_PAGE_ROWS);
                assert_eq!(size.target(12, budget), 12);
                size.observe(900 * 1024);
                assert_eq!(size.target(MAX_PAGE_ROWS, budget), 9);
                assert_eq!(size.target(MAX_PAGE_ROWS, 900 * 1024 - 1), 0);
                size.observe(1);
                assert_eq!(size.target(MAX_PAGE_ROWS, budget), 9);
                assert_eq!(size.target(0, budget), 0);
            }

            /// Scenario: Sixty-four valid 900-KiB rows span several 8-MiB pages.
            /// Guarantees: Fetch estimates survive page boundaries without repeatedly requesting discarded rows.
            #[test]
            fn large_rows_do_not_repeat_full_fetches() {
                let mut size = FetchSize::default();
                let row_bytes = 900 * 1024;
                let mut remaining = 64usize;
                let mut requested = 0;
                while remaining > 0 {
                    let mut used = 128 * 1024;
                    let mut page_rows = 0;
                    loop {
                        let count = size.target(
                            (MAX_PAGE_ROWS - page_rows).min(remaining),
                            8 * 1024 * 1024 - used,
                        );
                        if count == 0 {
                            break;
                        }
                        requested += count;
                        for _ in 0..count {
                            size.observe(row_bytes);
                            used += row_bytes;
                            assert!(used <= 8 * 1024 * 1024);
                            page_rows += 1;
                            remaining -= 1;
                        }
                    }
                    assert!(page_rows > 0);
                }
                assert_eq!(requested, 64);
            }

            /// Scenario: A connection-driving task panics before worker cleanup joins it.
            /// Guarantees: Cleanup reports failure once, removes the session, and never polls the completed join handle again.
            #[tokio::test]
            async fn panicked_driver_cleanup_is_not_repolled() {
                tokio::task::LocalSet::new()
                    .run_until(async {
                        let driver: JoinHandle<Result<()>> = tokio::task::spawn_local(async {
                            panic!("injected driver failure");
                        });
                        let mut worker = Worker {
                            validated: super::super::tests::test_validated(),
                            session: Some(Session {
                                client: None,
                                driver,
                                statement: None,
                                catalog: None,
                                receive_failure: ReceiveFailure::default(),
                            }),
                            signature: None,
                            cancel: Rc::new(RefCell::new(None)),
                            poisoned: false,
                            principal: None,
                            fetch_size: FetchSize::default(),
                            timestamp_cache: convert::CursorTimestampCache::default(),
                        };
                        assert_eq!(worker.close().await, Err(Error::Cleanup));
                        assert!(worker.session.is_none());
                        assert!(worker.poisoned);
                        assert_eq!(worker.close().await, Ok(()));
                        assert!(
                            worker.poisoned,
                            "cleanup cannot clear the quarantined state"
                        );
                    })
                    .await;
            }
        }
    };
    () => {
        mod tests {
            use super::*;

            /// Scenario: PostgreSQL reports overload, transaction conflicts, timeout, or permanent failures.
            /// Guarantees: Transient SQLSTATEs and timeouts retry through the shared policy; auth/schema errors stay terminal.
            #[test]
            fn sqlstate_retry_policy() {
                for code in [
                    "53300", "08006", "57P01", "57P02", "57P03", "40001", "40P01",
                ] {
                    let error = sqlstate(code);
                    assert_eq!(error, Error::Unavailable);
                    assert!(PostgreSqlAdapter::is_retryable(&error));
                }
                let timeout = sqlstate("57014");
                assert_eq!(timeout, Error::Timeout);
                assert!(PostgreSqlAdapter::is_retryable(&timeout));
                for code in ["28P01", "42501", "42P01", "23505", "53200", "XX000"] {
                    let error = sqlstate(code);
                    assert_eq!(error, Error::Database);
                    assert!(!PostgreSqlAdapter::is_retryable(&error));
                }
                for error in [
                    Error::Limit,
                    Error::Value,
                    Error::Credential,
                    Error::Cleanup,
                ] {
                    assert!(!PostgreSqlAdapter::is_retryable(&error));
                }
            }

            /// Scenario: A worker panics after acknowledging cleanup but before its thread exits.
            /// Guarantees: A failed join remains terminal across repeated shutdown calls.
            #[tokio::test]
            async fn failed_join_remains_terminal() {
                let (commands, receiver) = mpsc::channel(1);
                let (sender, exit) = oneshot::channel();
                drop(receiver);
                let thread = std::thread::spawn(move || {
                    sender.send(Ok(())).expect("exit acknowledgment");
                    panic!("test worker exit failure");
                });
                let mut adapter = PostgreSqlAdapter {
                    credentials: crate::receivers::postgresql_receiver::tests::test_provider(),
                    commands,
                    operation: Arc::new(Operation::default()),
                    busy: Arc::new(AtomicBool::new(false)),
                    thread: Some(thread),
                    exit,
                    exit_result: None,
                    stopped: false,
                };
                for _ in 0..3 {
                    assert_eq!(adapter.shutdown().await, Err(Error::Cleanup));
                    assert!(!adapter.stopped);
                }
            }

            /// Scenario: The worker exits without sending its cleanup acknowledgment.
            /// Guarantees: Every shutdown attempt reports unconfirmed cleanup instead of repolling a consumed oneshot.
            #[tokio::test]
            async fn lost_exit_acknowledgment_remains_terminal() {
                let (commands, receiver) = mpsc::channel(1);
                let (sender, exit) = oneshot::channel();
                drop(receiver);
                drop(sender);
                let mut adapter = PostgreSqlAdapter {
                    credentials: crate::receivers::postgresql_receiver::tests::test_provider(),
                    commands,
                    operation: Arc::new(Operation::default()),
                    busy: Arc::new(AtomicBool::new(false)),
                    thread: None,
                    exit,
                    exit_result: None,
                    stopped: false,
                };
                for _ in 0..3 {
                    assert_eq!(adapter.shutdown().await, Err(Error::Cleanup));
                    assert!(!adapter.stopped);
                    assert!(adapter.begin_operation().is_err());
                }
            }
        }
    };
}

use super::{
    adapter::Error, adapter::Operation, config::PostgreSqlReceiverConfig as Config,
    query::compile_parameters, value as convert,
};
use otel_arrow_dfe_engine::capability::{
    CapabilityError, CapabilityErrorSource,
    auth::{
        BasicAuthCredential,
        basic_auth_provider::{
            BasicAuthCredentialStream, BasicAuthProvider as BasicAuthCapability,
        },
    },
};
use otel_arrow_dfe_engine::local::capability::auth::basic_auth_provider::BasicAuthProvider;
use otel_arrow_dfe_scraper::database::{CellValue, DatabaseSystem, DriverAdapter};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_postgres::types::Type;

enum TestCredentials {
    Fixed,
    Pending,
    Failed,
}

fn fixed_credential() -> BasicAuthCredential {
    BasicAuthCredential::new("reader", "password").expect("fixture")
}

#[async_trait::async_trait(?Send)]
impl BasicAuthProvider for TestCredentials {
    async fn get_credential(&self) -> Result<BasicAuthCredential, CapabilityError> {
        match self {
            Self::Fixed => Ok(fixed_credential()),
            Self::Pending => futures::future::pending().await,
            Self::Failed => Err(CapabilityErrorSource::<BasicAuthCapability>::new(
                "PRIVATE_PROVIDER".into(),
            )
            .error("PRIVATE_PASSWORD")),
        }
    }

    fn credential_stream(&self) -> BasicAuthCredentialStream {
        match self {
            Self::Fixed => Box::pin(futures::stream::iter([fixed_credential()])),
            Self::Pending | Self::Failed => Box::pin(futures::stream::pending()),
        }
    }
}

pub(super) fn test_provider() -> Box<dyn BasicAuthProvider> {
    Box::new(TestCredentials::Fixed)
}

const KEY: &str =
    "((e.ts > :last_timestamp) OR (e.ts = :last_timestamp AND e.id > :last_tie_breaker))";

fn config(sql: &str) -> Value {
    let root = std::env::current_dir().expect("working directory");
    json!({
        "source_id": "test",
        "connection": {
            "host": "LOCALHOST", "database": "fixture"
        },
        "watermark": {
            "mode":"composite",
            "timestamp":{"column":"ts","initial":"2026-10-05T00:00:00.000000Z","timezone":"UTC"},
            "tie_breaker":{"column":"id","initial":0}
        },
        "query": {
            "statement": sql,
            "interval": "60s",
            "result_schema": [
                {"name":"ts","source_type":"timestamptz","nullable":false,"type_modifier":6},
                {"name":"id","source_type":"int8","nullable":false,"type_modifier":-1}
            ]
        },
        "checkpoint":{"directory":root.join("checkpoint"),"on_permanent_nack":"pause"}
    })
}
fn sql(predicate: &str) -> String {
    format!("SELECT e.ts, e.id FROM public.events AS e WHERE {predicate} ORDER BY e.ts, e.id")
}
fn validate(value: &Value) -> super::adapter::Result<super::config::Validated> {
    Config::parse(value)?.validate()
}

pub(super) fn test_validated() -> super::config::Validated {
    validate(&config(&sql(KEY))).expect("worker fixture")
}

/// Scenario: Both vendor identities are emitted by the existing shared mapper.
/// Guarantees: Adding PostgreSQL leaves the Oracle semantic identity unchanged.
#[test]
fn system_identity() {
    assert_eq!(DatabaseSystem::PostgreSQL.as_str(), "postgresql");
    assert_eq!(DatabaseSystem::Oracle.as_str(), "oracle.db");
}

/// Scenario: Named, custom, repeated, and native parameters occur in real token positions.
/// Guarantees: Roles are fixed and all unrelated UTF-8, CRLF, strings, comments, and casts survive byte-for-byte.
#[test]
fn parameter_compilation_preserves_bytes() {
    for source in [
        "SELECT '\u{e9}:ts', :id, :ts, :ts\r\n-- :id\n",
        "SELECT E'\\\\:ts', $$:id$$, \"x:ts\", :ts::timestamp, :id /* :ts */",
        "SELECT :ts /* a /* :id */ b */, :id",
    ] {
        let result = compile_parameters(source, "ts", "id").expect("compile");
        assert!(result.contains("$1"));
        assert!(result.contains("$2"));
        assert_eq!(
            compile_parameters(&result, "ts", "id").expect("native"),
            result
        );
    }
    assert_eq!(
        compile_parameters("SELECT '\u{e9}'\r\n, :id, :ts, :ts", "ts", "id").expect("compile"),
        "SELECT '\u{e9}'\r\n, $2, $1, $1"
    );
}

/// Scenario: Placeholders are missing, malformed, mixed, or present only in literals/comments.
/// Guarantees: No guessed mapping or accidental bind-looking text can authorize execution.
#[test]
fn invalid_parameters_fail_closed() {
    for text in [
        "SELECT :ts, $2",
        "SELECT :ts, :unknown",
        "SELECT :ts, : id",
        "SELECT :ts, :\"id\"",
        "SELECT :ts, ':id'",
        "SELECT :ts /* :id */",
        "SELECT $1, $0",
        "SELECT $1, $3",
        "SELECT $01, $2",
        "SELECT $1, ?",
        "SELECT :ts, :id;",
        "SELECT $$:ts$$, $$:id$$",
    ] {
        assert!(compile_parameters(text, "ts", "id").is_err(), "{text}");
    }
}

/// Scenario: Supported keysets, static predicates, aliases, and unique-side joins are authored.
/// Guarantees: The complete AST is admitted locally before later catalog proof.
#[test]
fn supported_sql_shapes() {
    for predicate in [
        KEY.to_owned(),
        "(e.ts, e.id) > (:last_timestamp, :last_tie_breaker)".into(),
        format!("{KEY} AND e.id >= -10"),
        format!("e.id IS NOT NULL AND ({KEY})"),
    ] {
        assert!(validate(&config(&sql(&predicate))).is_ok(), "{predicate}");
    }
    let joined = sql(KEY).replace(
        "public.events AS e",
        "public.events AS e INNER JOIN public.lookup AS l ON e.id = l.id",
    );
    assert!(validate(&config(&joined)).is_ok());
}

/// Scenario: Exact OR keysets occur alone, in parentheses, among filters, and with an inner join.
/// Guarantees: Only the keyset AST becomes a tuple comparison; all other SQL and the page limit match.
#[test]
fn keyset_optimization_preserves_query_tree() {
    use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

    let tuple = "((e.ts, e.id) > (:last_timestamp, :last_tie_breaker))";
    for predicate in [
        KEY.to_owned(),
        format!("(({KEY}))"),
        format!("e.id >= -10 AND {KEY}"),
        format!("{KEY} AND e.id IS NOT NULL"),
        format!("e.id >= -10 AND (({KEY})) AND e.id < 42"),
        format!("{KEY} AND e.note = 'caf\u{e9} :last_timestamp it''s $2'"),
    ] {
        for joined in [false, true] {
            let mut source = sql(&predicate);
            if joined {
                source = source.replace(
                    "public.events AS e",
                    "public.events AS e INNER JOIN public.lookup AS l ON e.id = l.id",
                );
            }
            let expected_source = source.replace(KEY, tuple);
            let original = validate(&config(&source)).expect("OR query");
            let native = validate(&config(&expected_source)).expect("tuple query");
            let expected = format!(
                "{} LIMIT {}",
                compile_parameters(&expected_source, "last_timestamp", "last_tie_breaker")
                    .expect("parameters"),
                super::query::MAX_PAGE_ROWS
            );
            let parse =
                |text: &str| Parser::parse_sql(&PostgreSqlDialect {}, text).expect("query tree");
            assert_eq!(parse(&original.plan.sql), parse(&expected), "{source}");
            assert_eq!(parse(&original.plan.sql), parse(&native.plan.sql));
            assert_eq!(original.common.sql(), source);
            assert_eq!(original.plan.referenced, native.plan.referenced);
            assert_eq!(original.plan.filters, native.plan.filters);
        }
    }
}

/// Scenario: Quoted aliases, cursor columns, and UTF-8 or escaped strings surround a nested OR keyset.
/// Guarantees: AST rewriting retains identifier spelling, literal values, projections, filters, and order.
#[test]
fn keyset_optimization_preserves_quoted_identifiers() {
    use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

    let source = concat!(
        "SELECT \"Event\".\"Ts\" AS ts, \"Event\".\"Id\" AS id ",
        "FROM \"public\".\"Events\" AS \"Event\" ",
        "WHERE \"Event\".\"Note\" = 'it''s :last_timestamp' AND ",
        "(((\"Event\".\"Ts\") > (:last_timestamp)) OR ",
        "(((\"Event\".\"Ts\") = (:last_timestamp)) AND ",
        "((\"Event\".\"Id\") > (:last_tie_breaker)))) ",
        "AND \"Event\".\"Other\" = '"
    );
    let source = format!("{source}\u{e9} $2' ORDER BY \"Event\".\"Ts\" ASC, \"Event\".\"Id\"");
    let optimized = validate(&config(&source)).expect("quoted query");
    let expected = format!(
        "SELECT \"Event\".\"Ts\" AS ts, \"Event\".\"Id\" AS id \
         FROM \"public\".\"Events\" AS \"Event\" \
         WHERE \"Event\".\"Note\" = 'it''s :last_timestamp' AND \
         ((\"Event\".\"Ts\", \"Event\".\"Id\") > ($1, $2)) \
         AND \"Event\".\"Other\" = '\u{e9} $2' \
         ORDER BY \"Event\".\"Ts\" ASC, \"Event\".\"Id\" LIMIT {}",
        super::query::MAX_PAGE_ROWS
    );
    assert_eq!(
        Parser::parse_sql(&PostgreSqlDialect {}, &optimized.plan.sql).expect("optimized"),
        Parser::parse_sql(&PostgreSqlDialect {}, &expected).expect("expected")
    );
    assert_eq!(optimized.common.sql(), source);
}

/// Scenario: Different authored keysets compile to the same bounded tuple query.
/// Guarantees: Checkpoint identity still hashes the original SQL and existing version identifiers.
#[test]
fn optimized_query_retains_original_checkpoint_identity() {
    let source = sql(KEY);
    let validated = validate(&config(&source)).expect("original");
    let cfg = &validated.config;
    let identity = serde_json::to_vec(&(
        "postgresql/v3",
        "postgresql",
        &cfg.connection.host,
        cfg.connection.port,
        &cfg.connection.database,
        &source,
        "named-parameters/v2",
        &cfg.watermark,
        &cfg.query.output,
        &cfg.query.result_schema,
        "UTC/microseconds/native-v2",
    ))
    .expect("identity");
    assert_eq!(
        validated.fingerprint,
        blake3::hash(&identity).to_hex().to_string()
    );
    let tuple = validate(&config(&sql(
        "((e.ts, e.id) > (:last_timestamp, :last_tie_breaker))",
    )))
    .expect("tuple");
    assert_eq!(validated.plan.sql, tuple.plan.sql);
    assert_ne!(validated.fingerprint, tuple.fingerprint);
}

/// Scenario: A bounded query projects 128 columns with additional filter and join references.
/// Guarantees: Catalog requests can deduplicate each relation's complete metadata without losing provenance.
#[test]
fn wide_query_tracks_all_catalog_references() {
    let projection = std::iter::once("e.ts".to_owned())
        .chain(std::iter::once("e.id".to_owned()))
        .chain((0..126).map(|i| format!("e.c{i}")))
        .collect::<Vec<_>>()
        .join(", ");
    let source = format!(
        "SELECT {projection} FROM public.events AS e \
         INNER JOIN public.lookup AS l ON e.id = l.id \
         WHERE {KEY} AND e.c0 > 0 AND e.extra IS NOT NULL AND l.enabled = TRUE \
         ORDER BY e.ts, e.id"
    );
    let mut cfg = config(&source);
    let schema = cfg["query"]["result_schema"]
        .as_array_mut()
        .expect("schema");
    for i in 0..126 {
        schema.push(json!({
            "name": format!("c{i}"), "source_type": "int8",
            "nullable": true, "type_modifier": -1
        }));
    }
    let plan = validate(&cfg).expect("wide query").plan;
    assert_eq!(plan.projections.len(), 128);
    assert_eq!(plan.referenced.len(), 131);
    for (alias, expected) in [("e", 129), ("l", 2)] {
        assert_eq!(
            plan.referenced.iter().filter(|c| c.alias == alias).count(),
            expected
        );
    }
    assert_eq!(plan.relations[1].join[0].0.alias, "e");
    assert_eq!(plan.relations[1].join[0].1.alias, "l");
    assert!(
        plan.sql
            .ends_with(&format!(" LIMIT {}", super::query::MAX_PAGE_ROWS))
    );
}

/// Scenario: Legal SQL outside the experiment or an OR-bypass weakens the keyset contract.
/// Guarantees: Rejected statements never reach native prepare, including all extra query/select clauses.
#[test]
fn unsafe_and_unsupported_sql_rejected() {
    let base = sql(KEY);
    for text in [
        sql(&format!("{KEY} OR TRUE")),
        sql(&format!("{KEY} AND ({KEY})")),
        sql(&format!(
            "{KEY} AND (e.ts, e.id) > (:last_timestamp, :last_tie_breaker)"
        )),
        sql("e.ts >= :last_timestamp OR (e.ts = :last_timestamp AND e.id > :last_tie_breaker)"),
        sql("e.ts > :last_timestamp OR (e.ts = :last_timestamp AND e.id >= :last_tie_breaker)"),
        sql("e.ts > :last_timestamp OR (e.id > :last_tie_breaker AND e.ts = :last_timestamp)"),
        sql("e.ts > :last_timestamp AND e.id > :last_tie_breaker"),
        base.replace("e.ts, e.id FROM", "e.ts, abs(e.id) AS id FROM"),
        base.replace("SELECT", "SELECT DISTINCT"),
        base.replace(" FROM", " INTO public.copy FROM"),
        base.replace("e.ts, e.id FROM", "* FROM"),
        base.replace("public.events AS e", "(SELECT * FROM public.events) AS e"),
        base.replace(
            "public.events AS e",
            "public.events AS e LEFT JOIN public.x AS x ON e.id=x.id",
        ),
        base.replace("ORDER BY e.ts, e.id", "ORDER BY e.id, e.ts"),
        base.replace("ORDER BY e.ts, e.id", "ORDER BY e.ts DESC, e.id"),
        base.replace("ORDER BY e.ts, e.id", "ORDER BY e.ts NULLS FIRST, e.id"),
        format!("{base} LIMIT 1"),
        format!("{base} LIMIT {}", super::query::MAX_PAGE_ROWS),
        format!("{base} OFFSET 1"),
        format!("{base} FETCH FIRST 1 ROW ONLY"),
        format!("{base} FOR UPDATE"),
        format!("{base}; SELECT 1"),
        format!("{base} UNION {base}"),
        base.replace("ORDER BY", "GROUP BY e.ts, e.id ORDER BY"),
        format!("WITH x AS (DELETE FROM public.events) {base}"),
        sql(&format!(
            "{KEY} AND e.id = pg_catalog.set_config('a', 'b', false)"
        )),
    ] {
        assert!(validate(&config(&text)).is_err(), "{text}");
    }
}

/// Scenario: SQL exceeds bytes, lexical tokens, or nesting limits.
/// Guarantees: Resource bounds reject before recursive AST processing.
#[test]
fn parser_budgets() {
    assert!(
        compile_parameters(
            &format!("SELECT :ts, :id {}", " ".repeat(16384)),
            "ts",
            "id"
        )
        .is_err()
    );
    assert!(
        compile_parameters(
            &format!("SELECT :ts, :id {}", "x ".repeat(4096)),
            "ts",
            "id"
        )
        .is_err()
    );
    assert!(
        compile_parameters(
            &format!("SELECT {}:ts{}, :id", "(".repeat(17), ")".repeat(17)),
            "ts",
            "id"
        )
        .is_err()
    );
}

/// Scenario: Configured intervals are at, below, above, or fractional to the whole-second bounds.
/// Guarantees: 90s and 1.5m agree; no fractional result is rounded into acceptance.
#[test]
fn interval_boundaries() {
    for (interval, allowed) in [
        ("59s", false),
        ("60s", true),
        ("90s", true),
        ("1.5m", true),
        ("24h", true),
        ("24h1s", false),
        ("60.5s", false),
    ] {
        let mut cfg = config(&sql(KEY));
        cfg["query"]["interval"] = json!(interval);
        assert_eq!(validate(&cfg).is_ok(), allowed, "{interval}");
    }
}

/// Scenario: PostgreSQL omits catch-up settings or overrides either shared budget.
/// Guarantees: Oracle's 32-page/10-second defaults apply; overrides preserve checkpoints and per-page resource bounds.
#[test]
fn catch_up_matches_oracle_defaults_and_overrides() {
    let base = config(&sql(KEY));
    let fingerprint = validate(&base).expect("default config").fingerprint;
    for (override_value, pages, millis) in [
        (None, 32, 10_000),
        (Some(json!({"max_pages": 2})), 2, 10_000),
        (Some(json!({"max_duration": "50ms"})), 32, 50),
        (
            Some(json!({"max_pages": 1024, "max_duration": "5m"})),
            1024,
            300_000,
        ),
    ] {
        let mut value = base.clone();
        if let Some(override_value) = override_value {
            value["query"]["catch_up"] = override_value;
        }
        let validated = validate(&value).expect("valid catch-up");
        assert_eq!(validated.fingerprint, fingerprint);
        let query = validated.common;
        assert_eq!(query.catch_up().max_pages, pages);
        assert_eq!(
            query.catch_up().max_duration,
            std::time::Duration::from_millis(millis)
        );
        assert_eq!(query.fetch_size_rows(), 300);
        assert_eq!(query.max_rows(), 1000);
        assert_eq!(query.max_batch_bytes(), 8 * 1024 * 1024);
        assert_eq!(query.max_normalized_bytes(), 8 * 1024 * 1024);
    }
}

/// Scenario: PostgreSQL supplies catch-up values outside Oracle's shared limits or adds unknown fields.
/// Guarantees: Invalid budgets are rejected instead of disabling limits or silently falling back.
#[test]
fn invalid_catch_up_settings_are_rejected() {
    for catch_up in [
        json!({"max_pages": 0}),
        json!({"max_pages": 1025}),
        json!({"max_duration": "0ms"}),
        json!({"max_duration": "5m1ms"}),
        json!({"unlimited": true}),
        Value::Null,
    ] {
        let mut value = config(&sql(KEY));
        value["query"]["catch_up"] = catch_up;
        assert!(validate(&value).is_err());
    }
}

/// Scenario: PostgreSQL omits or overrides the Oracle-style fetch-size setting.
/// Guarantees: The default remains 300, valid overrides reach the compiled query, and changing fetch size preserves checkpoint identity.
#[test]
fn fetch_size_defaults_and_overrides() {
    let base = config(&sql(KEY));
    let default = validate(&base).expect("default");
    assert_eq!(default.common.fetch_size_rows(), 300);
    for rows in [1, 17, 300, 1000] {
        let mut value = base.clone();
        value["query"]["fetch_size_rows"] = json!(rows);
        let configured = validate(&value).expect("configured fetch size");
        assert_eq!(configured.common.fetch_size_rows(), rows);
        assert_eq!(configured.fingerprint, default.fingerprint);
    }
}

/// Scenario: Fetch size is zero, exceeds the fixed page size, or has an invalid type.
/// Guarantees: Invalid values fail explicitly rather than being clamped or silently replaced with a default.
#[test]
fn invalid_fetch_sizes_are_rejected() {
    for rows in [
        json!(0),
        json!(1001),
        json!(10000),
        json!(-1),
        json!(1.5),
        json!("300"),
        Value::Null,
    ] {
        let mut value = config(&sql(KEY));
        value["query"]["fetch_size_rows"] = rows;
        assert!(validate(&value).is_err());
    }
}

/// Scenario: An optional event timestamp is NULL or outside OTLP's unsigned nanosecond range.
/// Guarantees: PostgreSQL values reach the shared mapper unchanged and use observation time without changing the cursor.
#[test]
fn optional_event_time_uses_shared_fallback() {
    use otel_arrow_dfe_pdata::{
        PayloadData,
        otlp::OtlpProtoBytes,
        proto::opentelemetry::{common::v1::any_value, logs::v1::LogsData},
    };
    use otel_arrow_dfe_scraper::database::{
        ColumnMetadata, Cursor, CursorRow, OutputConfig, QueryPage, Row, encode_page,
    };
    use prost::Message;
    let epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1)
        .expect("epoch")
        .and_hms_opt(0, 0, 0)
        .expect("midnight");
    let mut values = vec![CellValue::Null];
    for year in [1960, 3000] {
        let time = chrono::NaiveDate::from_ymd_opt(year, 1, 1)
            .expect("date")
            .and_hms_opt(0, 0, 0)
            .expect("time");
        let bytes = time
            .signed_duration_since(epoch)
            .num_microseconds()
            .expect("micros")
            .to_be_bytes();
        values.push(convert::decode(&Type::TIMESTAMPTZ, &bytes).expect("native timestamp"));
    }
    for value in values {
        let is_null = matches!(value, CellValue::Null);
        let expected = match &value {
            CellValue::Null => None,
            CellValue::TimestampTz(text) => Some(any_value::Value::StringValue(text.clone())),
            _ => panic!("timestamp fixture"),
        };
        let cursor = Cursor::composite("2026-10-05T00:00:00Z".into(), 1);
        let encoded = encode_page(
            QueryPage {
                columns: vec![ColumnMetadata {
                    name: "occurred".into(),
                    source_type: "timestamptz".into(),
                    nullable: true,
                }],
                rows: vec![CursorRow {
                    row: Row {
                        values: vec![value],
                    },
                    cursor: cursor.clone(),
                }],
            },
            DatabaseSystem::PostgreSQL,
            "fixture",
            &OutputConfig {
                timestamp_column: Some("occurred".into()),
                validation_columns: vec![],
            },
            123,
            1024,
        )
        .expect("mapping")
        .expect("one row");
        assert_eq!(encoded.row_count, 1);
        assert_eq!(encoded.candidate, cursor);
        assert_eq!(encoded.event_time_fallbacks, usize::from(!is_null));
        let PayloadData::OtlpBytes(OtlpProtoBytes::ExportLogsRequest(bytes)) =
            encoded.pdata.payload().into_data()
        else {
            panic!("OTLP logs");
        };
        let logs = LogsData::decode(bytes).expect("logs");
        let record = &logs.resource_logs[0].scope_logs[0].log_records[0];
        assert_eq!(record.time_unix_nano, 123);
        let Some(any_value::Value::KvlistValue(body)) =
            record.body.as_ref().and_then(|body| body.value.as_ref())
        else {
            panic!("structured row");
        };
        assert_eq!(
            body.values[0]
                .value
                .as_ref()
                .and_then(|value| value.value.clone()),
            expected
        );
    }
}

/// Scenario: A PostgreSQL cursor is configured using UTC ISO 8601 text or another timestamp notation.
/// Guarantees: Only the T/Z form is accepted, with exact native precision rather than offset conversion or truncation.
#[test]
fn cursor_configuration_requires_iso_utc() {
    for (initial, valid) in [
        ("2026-01-01T00:00:00Z", true),
        ("2026-01-01T00:00:00.123456Z", true),
        ("2026-01-01T00:00:00.123456000Z", true),
        ("2026-01-01 00:00:00", false),
        ("2026-01-01 00:00:00Z", false),
        ("2026-01-01T00:00:00+00:00", false),
        ("2026-01-01T05:30:00+05:30", false),
        ("2026-01-01T00:00:00", false),
        ("2026-01-01", false),
        ("2026-01-01T00:00:00.123456789Z", false),
    ] {
        let mut value = config(&sql(KEY));
        value["watermark"]["timestamp"]["initial"] = json!(initial);
        assert_eq!(validate(&value).is_ok(), valid, "{initial}");
    }
}

/// Scenario: PostgreSQL's console example obtains both credentials through the shared provider.
/// Guarantees: The example references username/password files and preserves the required capability binding.
#[test]
fn console_example_uses_shared_file_credentials() {
    let value: Value = serde_yaml::from_str(include_str!(
        "../../../../../configs/postgresql-console.yaml"
    ))
    .expect("example YAML");
    let pipeline = &value["groups"]["default"]["pipelines"]["main"];
    let provider = &pipeline["extensions"]["pg-credentials"];
    assert_eq!(
        provider["type"],
        "urn:otel:extension:flat_file_user_pass_auth"
    );
    assert!(provider["config"].get("username").is_none());
    assert_eq!(
        provider["config"]["username_file"],
        "${env:PG_USERNAME_FILE:-/run/secrets/postgresql/username}"
    );
    assert_eq!(
        provider["config"]["password_secret_file"],
        "${env:PG_PASSWORD_FILE:-/run/secrets/postgresql/password}"
    );
    assert_eq!(
        pipeline["nodes"]["pg"]["capabilities"]["basic_auth_provider"],
        "pg-credentials"
    );
}

/// Scenario: Operators add unsupported fields or omit required lower-bound and schema information.
/// Guarantees: Strict objects reject unsafe settings without printing their values.
#[test]
fn strict_configuration() {
    let base = config(&sql(KEY));
    for pointer in [
        "/connection/password",
        "/connection/tls",
        "/query/output/attributes",
        "/query/queries",
        "/checkpoint/unknown",
        "/unknown",
    ] {
        let mut cfg = base.clone();
        let (parent, key) = pointer.rsplit_once('/').expect("pointer");
        if parent == "/query/output" {
            cfg["query"]["output"] = json!({});
        }
        let _ = cfg
            .pointer_mut(parent)
            .expect("parent")
            .as_object_mut()
            .expect("object")
            .insert(key.into(), json!("diagnostic-canary"));
        assert!(validate(&cfg).is_err(), "{pointer}");
    }
    for field in ["initial", "timezone"] {
        let mut cfg = base.clone();
        let _ = cfg["watermark"]["timestamp"]
            .as_object_mut()
            .expect("object")
            .remove(field);
        assert!(validate(&cfg).is_err());
    }
    for host in [
        "postgres://user:pass@host/db",
        "a,b",
        "/socket",
        "a b",
        "a@b",
        "-bad",
    ] {
        let mut cfg = base.clone();
        cfg["connection"]["host"] = json!(host);
        assert!(validate(&cfg).is_err());
    }
}

/// Scenario: Policy-only edits differ from source compatibility edits.
/// Guarantees: Cadence does not enter identity; native schema, SQL, binds, and lower bounds do.
#[test]
fn compatibility_fingerprint() {
    let cfg = config(&sql(KEY));
    let original = validate(&cfg).expect("valid").fingerprint;
    for (pointer, value) in [
        ("/query/interval", json!("90s")),
        ("/checkpoint/on_permanent_nack", json!("retry")),
    ] {
        let mut other = cfg.clone();
        *other
            .pointer_mut(pointer)
            .unwrap_or_else(|| panic!("pointer {pointer}")) = value;
        assert_eq!(validate(&other).expect("valid").fingerprint, original);
    }
    for (pointer, value) in [
        ("/watermark/tie_breaker/initial", json!(-1)),
        (
            "/query/statement",
            json!(sql(KEY).replace("SELECT", "SELECT ")),
        ),
        ("/query/result_schema/1/source_type", json!("int4")),
    ] {
        let mut other = cfg.clone();
        *other.pointer_mut(pointer).expect("pointer") = value;
        assert_ne!(validate(&other).expect("valid").fingerprint, original);
    }
}

fn number(weight: i16, sign: u16, scale: u16, digits: &[u16]) -> Vec<u8> {
    let mut bytes = vec![];
    bytes.extend_from_slice(&(digits.len() as i16).to_be_bytes());
    bytes.extend_from_slice(&weight.to_be_bytes());
    bytes.extend_from_slice(&sign.to_be_bytes());
    bytes.extend_from_slice(&scale.to_be_bytes());
    for d in digits {
        bytes.extend_from_slice(&d.to_be_bytes());
    }
    bytes
}

/// Scenario: NUMERIC encodes large exact integers, preserved dscale, fractions, and malformed groups.
/// Guarantees: No float rounding, silent scale truncation, special-value acceptance, or huge allocation occurs.
#[test]
fn exact_numeric() {
    for (bytes, expected) in [
        (
            number(3, 0, 0, &[9007, 1992, 5474, 993]),
            "9007199254740993",
        ),
        (number(0, 0x4000, 4, &[12, 3400]), "-12.3400"),
        (number(-2, 0, 8, &[1]), "0.00000001"),
        (number(0, 0, 3, &[]), "0.000"),
        (number(1, 0, 0, &[1]), "10000"),
    ] {
        assert_eq!(convert::numeric(&bytes).expect("numeric"), expected);
    }
    for bytes in [
        number(0, 0xC000, 0, &[]),
        number(0, 0, 0, &[10000]),
        number(-1, 0, 2, &[1234]),
        number(32767, 0, 0, &[1]),
        vec![0; 7],
    ] {
        assert!(convert::numeric(&bytes).is_err());
    }
    assert_eq!(
        convert::numeric(&number(4095, 0, 0, &[1000]))
            .expect("boundary")
            .len(),
        16384
    );
    assert!(convert::numeric(&number(4096, 0, 0, &[1])).is_err());
}

/// Scenario: Temporal cursors and native integer ties approach their precision/range boundaries.
/// Guarantees: Native binding cannot silently truncate submicroseconds, typmod digits, leap seconds, or integers.
#[test]
fn cursor_precision_and_range() {
    for (text, precision, valid) in [
        ("2026-10-05T00:00:00.123000Z", 3, true),
        ("2026-10-05T00:00:00.123001Z", 3, false),
        ("2026-10-05T00:00:00.123456789Z", 6, false),
        ("2026-10-05T00:00:00.123456000Z", 6, true),
        ("2026-02-30T00:00:00Z", 6, false),
        ("2026-10-05T00:00:60Z", 6, false),
        ("infinity", 6, false),
    ] {
        assert_eq!(convert::cursor_time(text, precision).is_ok(), valid);
    }
    assert!(convert::check_tie(32767, "int2").is_ok());
    assert!(convert::check_tie(32768, "int2").is_err());
    assert!(convert::check_tie(i32::MIN as i64, "int4").is_ok());
    assert!(convert::check_tie(i32::MIN as i64 - 1, "int4").is_err());
    assert!(convert::check_tie(i64::MAX, "int8").is_ok());
}

/// Scenario: Supported exact codecs and unsupported/nonfinite source values are decoded.
/// Guarantees: JSON lexemes, bytes, padding and interval units survive; invalid data never stringifies.
#[test]
fn exact_values() {
    let json = br#"{"n":9007199254740993,"x":1.2345678901234567890123456789}"#;
    assert_eq!(
        convert::decode(&Type::JSON, json).expect("json"),
        CellValue::String(std::str::from_utf8(json).expect("utf8").into())
    );
    assert_eq!(
        convert::decode(&Type::BYTEA, &[0, 255]).expect("bytes"),
        CellValue::Bytes(vec![0, 255])
    );
    assert_eq!(
        convert::decode(&Type::BPCHAR, b"x   ").expect("padding"),
        CellValue::String("x   ".into())
    );
    let interval = [
        (-9i64).to_be_bytes().to_vec(),
        2i32.to_be_bytes().to_vec(),
        (-1i32).to_be_bytes().to_vec(),
    ]
    .concat();
    assert_eq!(
        convert::decode(&Type::INTERVAL, &interval).expect("interval"),
        CellValue::Interval("months=-1;days=2;microseconds=-9".into())
    );
    assert_eq!(
        convert::decode(&Type::UUID, &[0; 16]).expect("uuid"),
        CellValue::String("00000000-0000-0000-0000-000000000000".into())
    );
    for (ty, bytes) in [
        (Type::JSONB, vec![2, b'{', b'}']),
        (Type::TEXT, vec![255]),
        (Type::FLOAT8, f64::NAN.to_be_bytes().to_vec()),
        (Type::OID, vec![0; 4]),
        (Type::TIMESTAMP, i64::MAX.to_be_bytes().to_vec()),
    ] {
        assert!(convert::decode(&ty, &bytes).is_err());
    }
}

/// Scenario: Cancellation is requested before a connection or client token exists.
/// Guarantees: The operation generation is independently cancellable and later work observes it.
#[test]
fn disconnected_cancellation() {
    let first = Operation::default();
    first.cancel();
    assert_eq!(first.check(), Err(Error::Cancelled));
    assert!(Operation::default().check().is_ok());
    assert!(!super::adapter::PostgreSqlAdapter::is_retryable(
        &Error::Cleanup
    ));
}

/// Scenario: Diagnostics format typed errors whose underlying inputs could contain secrets.
/// Guarantees: Only static bounded messages are published, without a native source chain.
#[test]
fn redacted_errors() {
    use std::error::Error as _;
    for error in [
        Error::Config,
        Error::Sql,
        Error::Metadata,
        Error::Value,
        Error::Limit,
        Error::Credential,
        Error::Database,
        Error::Unavailable,
        Error::Cancelled,
        Error::Timeout,
        Error::Cleanup,
    ] {
        assert!(error.to_string().len() < 100);
        assert!(error.source().is_none());
    }
}

/// Scenario: A worker receives shutdown without ever connecting to a database.
/// Guarantees: Exit is acknowledged, the thread is joined, and repeated shutdown is idempotent.
#[tokio::test]
async fn disconnected_worker_shutdown() {
    let mut adapter =
        super::adapter::PostgreSqlAdapter::new(test_validated(), test_provider()).expect("worker");
    tokio::time::timeout(std::time::Duration::from_secs(2), adapter.shutdown())
        .await
        .expect("bounded shutdown")
        .expect("confirmed exit");
    adapter.shutdown().await.expect("idempotent shutdown");
    assert!(adapter.begin_operation().is_err());
}

/// Scenario: A provider waits indefinitely before the first database connection and shutdown cancels its operation.
/// Guarantees: The credential wait ends promptly and the idle worker can be joined without opening a connection.
#[tokio::test]
async fn provider_wait_is_cancellable() {
    use otel_arrow_dfe_scraper::database::DriverCancellation;
    let validated = test_validated();
    let query = validated.common.clone();
    let mut adapter =
        super::adapter::PostgreSqlAdapter::new(validated, Box::new(TestCredentials::Pending))
            .expect("worker");
    let cancellation = adapter.begin_operation().expect("operation");
    {
        let operation = adapter.validate_query(&query);
        tokio::pin!(operation);
        assert!(futures::poll!(&mut operation).is_pending());
        cancellation.cancel().await.expect("cancel");
        assert!(matches!(
            tokio::time::timeout(std::time::Duration::from_secs(1), operation)
                .await
                .expect("prompt cancel"),
            Err(Error::Cancelled),
        ));
    }
    adapter.shutdown().await.expect("worker joined");
}

/// Scenario: A provider never produces credentials and no cancellation arrives.
/// Guarantees: The bounded acquisition wait fails explicitly rather than stalling the receiver indefinitely.
#[tokio::test(start_paused = true)]
async fn provider_wait_has_a_deadline() {
    let validated = test_validated();
    let query = validated.common.clone();
    let mut adapter =
        super::adapter::PostgreSqlAdapter::new(validated, Box::new(TestCredentials::Pending))
            .expect("worker");
    _ = adapter.begin_operation().expect("operation");
    assert!(matches!(
        adapter.validate_query(&query).await,
        Err(Error::Credential)
    ));
    adapter.shutdown().await.expect("worker joined");
}

/// Scenario: A credential capability returns sensitive provider details in its error.
/// Guarantees: PostgreSQL exposes only a terminal credential category with no secret text or source chain.
#[tokio::test]
async fn provider_errors_are_redacted() {
    use std::error::Error as _;
    let validated = test_validated();
    let query = validated.common.clone();
    let mut adapter =
        super::adapter::PostgreSqlAdapter::new(validated, Box::new(TestCredentials::Failed))
            .expect("worker");
    _ = adapter.begin_operation().expect("operation");
    let error = adapter
        .validate_query(&query)
        .await
        .expect_err("credential error");
    assert_eq!(error, Error::Credential);
    assert!(!super::adapter::PostgreSqlAdapter::is_retryable(&error));
    assert!(error.source().is_none());
    assert!(!format!("{error:?} {error}").contains("PRIVATE"));
    adapter.shutdown().await.expect("worker joined");
}

/// Scenario: PostgreSQL configuration supplies a TLS block, old inline credentials, or a scalar watermark.
/// Guarantees: Unsupported transport/authentication fields and cursor modes fail rather than being ignored or reinterpreted.
#[test]
fn unsupported_transport_and_cursor_modes_fail_closed() {
    for (field, value) in [
        ("tls", json!({"ca_file": "ca.pem"})),
        ("sslmode", json!("require")),
        ("username", json!("user")),
        ("password_file", json!("password")),
    ] {
        let mut cfg = config(&sql(KEY));
        cfg["connection"][field] = value;
        assert!(validate(&cfg).is_err());
    }
    let mut cfg = config(&sql(KEY));
    cfg["watermark"] = json!({"mode":"scalar","column":"id","bind":"last_id","initial":{"type":"int64","value":0}});
    assert!(validate(&cfg).is_err());
}

/// Scenario: A PostgreSQL receiver is constructed without a credential capability.
/// Guarantees: The required binding is reported before source leases or database connections are acquired.
#[test]
fn factory_requires_basic_auth_provider() {
    use otel_arrow_dfe_engine::testing::{receiver::TestRuntime, test_node};
    let runtime = TestRuntime::<otel_arrow_dfe_otap::pdata::OtapPdata>::new();
    let result = (super::POSTGRESQL_RECEIVER.create)(
        otel_arrow_dfe_engine::context::ControllerContext::new(
            otel_arrow_dfe_telemetry::registry::TelemetryRegistryHandle::new(),
        )
        .pipeline_context_with("group".into(), "pipeline".into(), 0, 1, 0),
        test_node("postgresql"),
        Arc::new(
            otel_arrow_dfe_config::node::NodeUserConfig::new_receiver_config(
                super::POSTGRESQL_RECEIVER_URN,
            ),
        ),
        runtime.config(),
        &otel_arrow_dfe_engine::capability::registry::Capabilities::empty(),
    );
    assert!(
        matches!(result, Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig { error }) if error.contains("basic_auth_provider"))
    );
}

/// Scenario: A caller supplies a scalar cursor to a composite PostgreSQL query.
/// Guarantees: The current shared Cursor API is checked before credential or database work begins.
#[tokio::test]
async fn scalar_cursor_cannot_execute_as_composite() {
    use otel_arrow_dfe_scraper::database::{Cursor, ScalarValue};
    let validated = test_validated();
    let query = validated.common.clone();
    let mut adapter =
        super::adapter::PostgreSqlAdapter::new(validated, test_provider()).expect("worker");
    assert!(matches!(
        adapter
            .execute(&query, &Cursor::Scalar(ScalarValue::Int64(1)))
            .await,
        Err(Error::Value),
    ));
    adapter.shutdown().await.expect("worker joined");
}
