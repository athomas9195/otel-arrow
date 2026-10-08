// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Private adapter tests remain colocated logically without exposing implementation internals.
macro_rules! postgresql_module_tests {
    () => {
        mod tests {
            use super::*;

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
    adapter::Error,
    adapter::Operation,
    config::PostgreSqlReceiverConfig as Config,
    query::compile_parameters,
    transport::{Guard, GuardState},
    value as convert,
};
use otel_arrow_dfe_scraper::database::{CellValue, DatabaseSystem, DriverAdapter};
use serde_json::{Value, json};
use std::sync::{Arc, atomic::Ordering};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_postgres::types::Type;

struct FixedCredentials;

#[async_trait::async_trait(?Send)]
impl otel_arrow_dfe_engine::local::capability::auth::basic_auth_provider::BasicAuthProvider
    for FixedCredentials
{
    async fn get_credential(
        &self,
    ) -> Result<
        otel_arrow_dfe_engine::capability::auth::BasicAuthCredential,
        otel_arrow_dfe_engine::capability::CapabilityError,
    > {
        Ok(
            otel_arrow_dfe_engine::capability::auth::BasicAuthCredential::new("reader", "password")
                .expect("fixture"),
        )
    }

    fn credential_stream(
        &self,
    ) -> otel_arrow_dfe_engine::capability::auth::basic_auth_provider::BasicAuthCredentialStream
    {
        Box::pin(futures::stream::iter([
            otel_arrow_dfe_engine::capability::auth::BasicAuthCredential::new("reader", "password")
                .expect("fixture"),
        ]))
    }
}

pub(super) fn test_provider()
-> Box<dyn otel_arrow_dfe_engine::local::capability::auth::basic_auth_provider::BasicAuthProvider> {
    Box::new(FixedCredentials)
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

/// Scenario: Legal SQL outside the experiment or an OR-bypass weakens the keyset contract.
/// Guarantees: Rejected statements never reach native prepare, including all extra query/select clauses.
#[test]
fn unsafe_and_unsupported_sql_rejected() {
    let base = sql(KEY);
    for text in [
        sql(&format!("{KEY} OR TRUE")),
        sql(&format!("{KEY} AND ({KEY})")),
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

/// Scenario: Backend length headers advertise invalid and over-budget message sizes.
/// Guarantees: The guard reads only its fixed header and exposes no bytes before rejecting.
#[tokio::test]
async fn frame_limit_before_exposure() {
    for length in [-1i32, 0, 3, 1024 * 1024 + 1] {
        let header = [vec![b'D'], length.to_be_bytes().to_vec()].concat();
        let state = Arc::new(GuardState::default());
        let mut guard = Guard::new(header.as_slice(), state.clone());
        let mut buf = [0u8; 64];
        assert!(guard.read(&mut buf).await.is_err());
        assert_eq!(buf, [0; 64]);
        assert!(state.limit.load(Ordering::Acquire));
    }
}

/// Scenario: Headers/bodies arrive one byte at a time and consumers request tiny buffers.
/// Guarantees: Valid frames remain byte-exact and truncated headers/bodies fail explicitly.
#[tokio::test]
async fn fragmented_frames_and_eof() {
    let expected = [
        vec![b'D'],
        8i32.to_be_bytes().to_vec(),
        vec![1, 2, 3, 4],
        vec![b'Z'],
        5i32.to_be_bytes().to_vec(),
        vec![b'I'],
    ]
    .concat();
    let (mut sender, receiver) = tokio::io::duplex(8);
    let sent = expected.clone();
    let task = tokio::spawn(async move {
        for byte in sent {
            sender.write_all(&[byte]).await.expect("write");
        }
    });
    let mut guard = Guard::new(receiver, Arc::new(GuardState::default()));
    let mut actual = vec![];
    let mut buf = [0; 2];
    loop {
        let n = guard.read(&mut buf).await.expect("read");
        if n == 0 {
            break;
        }
        actual.extend_from_slice(&buf[..n]);
    }
    task.await.expect("writer");
    assert_eq!(actual, expected);
    for bytes in [&expected[..3], &expected[..8]] {
        let mut guard = Guard::new(bytes, Arc::new(GuardState::default()));
        assert!(guard.read_to_end(&mut vec![]).await.is_err());
    }
}

/// Scenario: A certified server sends unlimited startup state, idle notices, or excessive column metadata.
/// Guarantees: Side-channel traffic cannot bypass message and metadata bounds.
#[tokio::test]
async fn metadata_and_side_channels() {
    for input in [
        [
            vec![b'T'],
            6i32.to_be_bytes().to_vec(),
            129u16.to_be_bytes().to_vec(),
        ]
        .concat(),
        [vec![b'N'], 4i32.to_be_bytes().to_vec()].concat(),
        [vec![b'S'], 4i32.to_be_bytes().to_vec()]
            .concat()
            .repeat(65),
    ] {
        let mut guard = Guard::new(input.as_slice(), Arc::new(GuardState::default()));
        assert!(guard.read_to_end(&mut vec![]).await.is_err());
    }
}

/// Scenario: Startup status traffic reaches its bound, then a server sends a status change after ReadyForQuery.
/// Guarantees: The startup allowance cannot authorize unsolicited idle state changes or expose their headers.
#[tokio::test]
async fn startup_status_is_not_an_idle_allowance() {
    let status = [vec![b'S'], 4i32.to_be_bytes().to_vec()].concat();
    let ready = [vec![b'Z'], 5i32.to_be_bytes().to_vec(), vec![b'I']].concat();
    let prefix = [status.repeat(64), ready].concat();
    let input = [prefix.clone(), status].concat();
    let state = Arc::new(GuardState::default());
    let mut guard = Guard::new(input.as_slice(), state.clone());
    let mut received = vec![0; prefix.len()];
    let _ = guard
        .read_exact(&mut received)
        .await
        .expect("bounded startup");
    assert_eq!(received, prefix);
    let mut rejected = [0; 5];
    assert!(guard.read(&mut rejected).await.is_err());
    assert_eq!(rejected, [0; 5]);
    assert!(state.limit.load(Ordering::Acquire));
}

/// Scenario: An active operation receives exactly 128 notices followed by a 129th.
/// Guarantees: The fixed per-operation notice allowance is enforced before the excess frame is exposed.
#[tokio::test]
async fn active_notice_bound() {
    let notice = [vec![b'N'], 4i32.to_be_bytes().to_vec()].concat();
    let input = notice.repeat(129);
    let state = Arc::new(GuardState::default());
    state.active.store(true, Ordering::Release);
    let mut guard = Guard::new(input.as_slice(), state.clone());
    let mut prefix = vec![0; notice.len() * 128];
    let _ = guard
        .read_exact(&mut prefix)
        .await
        .expect("notice allowance");
    assert_eq!(prefix, notice.repeat(128));
    assert!(guard.read(&mut [0; 5]).await.is_err());
    assert!(state.limit.load(Ordering::Acquire));
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
    let mut adapter = super::adapter::PostgreSqlAdapter::new(
        validate(&config(&sql(KEY))).expect("valid"),
        test_provider(),
    )
    .expect("worker");
    tokio::time::timeout(std::time::Duration::from_secs(2), adapter.shutdown())
        .await
        .expect("bounded shutdown")
        .expect("confirmed exit");
    adapter.shutdown().await.expect("idempotent shutdown");
    assert!(adapter.begin_operation().is_err());
}

struct PendingCredentials;

#[async_trait::async_trait(?Send)]
impl otel_arrow_dfe_engine::local::capability::auth::basic_auth_provider::BasicAuthProvider
    for PendingCredentials
{
    async fn get_credential(
        &self,
    ) -> Result<
        otel_arrow_dfe_engine::capability::auth::BasicAuthCredential,
        otel_arrow_dfe_engine::capability::CapabilityError,
    > {
        futures::future::pending().await
    }

    fn credential_stream(
        &self,
    ) -> otel_arrow_dfe_engine::capability::auth::basic_auth_provider::BasicAuthCredentialStream
    {
        Box::pin(futures::stream::pending())
    }
}

/// Scenario: A provider waits indefinitely before the first database connection and shutdown cancels its operation.
/// Guarantees: The credential wait ends promptly and the idle worker can be joined without opening a connection.
#[tokio::test]
async fn provider_wait_is_cancellable() {
    use otel_arrow_dfe_scraper::database::DriverCancellation;
    let validated = validate(&config(&sql(KEY))).expect("config");
    let query = validated.common.clone();
    let mut adapter =
        super::adapter::PostgreSqlAdapter::new(validated, Box::new(PendingCredentials))
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
    let validated = validate(&config(&sql(KEY))).expect("config");
    let query = validated.common.clone();
    let mut adapter =
        super::adapter::PostgreSqlAdapter::new(validated, Box::new(PendingCredentials))
            .expect("worker");
    _ = adapter.begin_operation().expect("operation");
    assert!(matches!(
        adapter.validate_query(&query).await,
        Err(Error::Credential)
    ));
    adapter.shutdown().await.expect("worker joined");
}

struct FailedCredentials;

#[async_trait::async_trait(?Send)]
impl otel_arrow_dfe_engine::local::capability::auth::basic_auth_provider::BasicAuthProvider
    for FailedCredentials
{
    async fn get_credential(&self) -> Result<
        otel_arrow_dfe_engine::capability::auth::BasicAuthCredential,
        otel_arrow_dfe_engine::capability::CapabilityError,
    > {
        Err(otel_arrow_dfe_engine::capability::CapabilityErrorSource::<
            otel_arrow_dfe_engine::capability::auth::basic_auth_provider::BasicAuthProvider,
        >::new("PRIVATE_PROVIDER".into()).error("PRIVATE_PASSWORD"))
    }

    fn credential_stream(&self) -> otel_arrow_dfe_engine::capability::auth::basic_auth_provider::BasicAuthCredentialStream {
        Box::pin(futures::stream::pending())
    }
}

/// Scenario: A credential capability returns sensitive provider details in its error.
/// Guarantees: PostgreSQL exposes only a terminal credential category with no secret text or source chain.
#[tokio::test]
async fn provider_errors_are_redacted() {
    use std::error::Error as _;
    let validated = validate(&config(&sql(KEY))).expect("config");
    let query = validated.common.clone();
    let mut adapter = super::adapter::PostgreSqlAdapter::new(validated, Box::new(FailedCredentials)).expect("worker");
    _ = adapter.begin_operation().expect("operation");
    let error = adapter.validate_query(&query).await.expect_err("credential error");
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
    let validated = validate(&config(&sql(KEY))).expect("config");
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
