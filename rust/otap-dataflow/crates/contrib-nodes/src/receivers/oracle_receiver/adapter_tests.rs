// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::{
    CellValue, OracleAdapterError, OracleType, bounded_connect_string, cursor_bind_type,
    finite_float, parse_cursor_timestamp, read_credential, validate_described_cursor_columns,
    validate_types,
};
use oracle::sql_type::Timestamp;
use otel_arrow_dfe_scraper::database::{CompositeCursor, CompositeWatermark};
use secrecy::ExposeSecret;
use std::fs;
use std::str::FromStr;
use std::time::Duration;

/// Scenario: A native Oracle failure contains sentinel SQL, row, endpoint and nested-source text.
/// Guarantees: Adapter formatting and the complete engine diagnostic expose no native text.
#[test]
fn native_error_text_is_redacted_from_engine_diagnostics() {
    use otel_arrow_dfe_engine::error::{Error, error_summary_from, format_error_sources};
    use otel_arrow_dfe_scraper::database::DriverAdapter;
    const SENTINEL: &str = "secret-row SELECT-private endpoint-private checkpoint-private";
    let constructors: [fn(oracle::Error) -> OracleAdapterError; 8] = [
        OracleAdapterError::Initialize,
        OracleAdapterError::Connect,
        OracleAdapterError::Configure,
        OracleAdapterError::Prepare,
        OracleAdapterError::Query,
        OracleAdapterError::Fetch,
        OracleAdapterError::Convert,
        OracleAdapterError::Cancellation,
    ];
    for constructor in constructors {
        let native = oracle::Error::with_source(
            oracle::ErrorKind::InvalidOperation,
            std::io::Error::other(SENTINEL),
        );
        let error = constructor(native);
        let source_detail = format_error_sources(&error);
        assert!(source_detail.is_empty());
        assert!(!format!("{error:?}").contains(SENTINEL));
        let engine = Error::ReceiverError {
            receiver: otel_arrow_dfe_engine::testing::test_node("oracle-test"),
            kind: super::OracleAdapter::classify_error(&error),
            error: error.to_string(),
            source_detail,
        };
        for rendered in [
            engine.to_string(),
            format!("{engine:?}"),
            serde_json::to_string(&error_summary_from(&engine)).expect("diagnostic JSON"),
        ] {
            assert!(!rendered.contains(SENTINEL), "{rendered}");
            assert!(!rendered.contains("endpoint-private"), "{rendered}");
        }
    }
}

fn test_adapter() -> super::OracleAdapter {
    super::OracleAdapter::new(super::OracleAdapterConfig {
        connect_string: String::new(),
        instant_client_dir: String::new(),
        username_file: String::new(),
        password_file: String::new(),
    })
}

/// Scenario: An Oracle worker is idle after successfully completing an operation.
/// Guarantees: Adapter shutdown closes its work channel and confirms worker cleanup before returning.
#[tokio::test]
async fn shutdown_joins_worker_after_successful_operation() {
    let mut adapter = test_adapter();
    adapter.worker = Some(super::NativeWorker::new("oracle-adapter-test").expect("worker"));
    let operation = adapter
        .worker
        .as_ref()
        .expect("worker")
        .run(|_| std::thread::current().id())
        .expect("accepted operation");
    let worker_thread = super::receive(operation)
        .await
        .expect("successful operation");
    assert_ne!(worker_thread, std::thread::current().id());
    otel_arrow_dfe_scraper::database::DriverAdapter::shutdown(&mut adapter)
        .await
        .expect("shutdown");
    assert!(adapter.worker.is_none());
    assert!(adapter.cancellation.state.lock().expect("state").stopped);
}

/// Scenario: Native cancellation is still running after the query worker becomes idle.
/// Guarantees: Adapter shutdown does not confirm cleanup before the cancellation worker finishes.
#[tokio::test]
async fn shutdown_waits_for_cancellation_worker() {
    let mut adapter = test_adapter();
    adapter.worker = Some(super::NativeWorker::new("oracle-query-test").expect("worker"));
    let cancellation = super::NativeWorker::new("oracle-cancel-test").expect("worker");
    let (release, gate) = std::sync::mpsc::sync_channel(1);
    let (started, ready) = tokio::sync::oneshot::channel();
    let _operation = cancellation
        .run(move |_| {
            let _ = started.send(());
            gate.recv().expect("released");
        })
        .expect("cancel job");
    adapter.cancellation.state.lock().expect("state").worker = Some(cancellation);
    super::receive(ready).await.expect("cancellation started");
    let shutdown = otel_arrow_dfe_scraper::database::DriverAdapter::shutdown(&mut adapter);
    tokio::pin!(shutdown);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut shutdown)
            .await
            .is_err()
    );
    release.send(()).expect("release cancellation");
    tokio::time::timeout(Duration::from_secs(2), shutdown)
        .await
        .expect("cleanup completes")
        .expect("shutdown");
}

/// Scenario: Cancellation arrives before any connection exists, then shutdown closes the adapter.
/// Guarantees: The operation is cancelled without spawning native work and cannot restart after shutdown.
#[tokio::test]
async fn cancellation_before_connect_stops_admission() {
    use otel_arrow_dfe_scraper::database::{DriverAdapter, DriverCancellation};
    let mut adapter = test_adapter();
    let cancellation = adapter.begin_operation().expect("operation");
    cancellation.cancel().await.expect("no active connection");
    assert!(matches!(
        cancellation.ensure_not_requested(),
        Err(OracleAdapterError::Cancelled)
    ));
    adapter.shutdown().await.expect("shutdown");
    assert!(matches!(
        adapter.begin_operation(),
        Err(OracleAdapterError::Cancelled)
    ));
}

/// Scenario: Cancellation is requested during a native call that otherwise succeeds.
/// Guarantees: Its result is rejected and no subsequent call begins for that operation.
#[test]
fn cancellation_is_checked_between_native_calls() {
    let cancellation = super::OracleCancellation::default();
    let result = cancellation.native_call(|| {
        cancellation.state.lock().expect("state").requested = true;
        Ok(())
    });
    assert!(matches!(result, Err(OracleAdapterError::Cancelled)));
    assert!(matches!(
        cancellation.native_call::<()>(|| panic!("cancelled operation must not run")),
        Err(OracleAdapterError::Cancelled)
    ));
}

fn watermark() -> CompositeWatermark {
    CompositeWatermark {
        timestamp_column: "EVENT_TS".to_owned(),
        timestamp_bind: "last_timestamp".to_owned(),
        tie_breaker_column: "EVENT_ID".to_owned(),
        tie_breaker_bind: "last_tie_breaker".to_owned(),
        initial: CompositeCursor::new("1970-01-01 00:00:00".to_owned(), 0),
    }
}

fn columns(timestamp: OracleType, tie_breaker: OracleType) -> Vec<(String, OracleType)> {
    vec![
        ("PAYLOAD".to_owned(), OracleType::Varchar2(64)),
        ("EVENT_TS".to_owned(), timestamp),
        ("EVENT_ID".to_owned(), tie_breaker),
    ]
}

/// Scenario: live metadata reports supported cursor types under differing identifier case.
/// Guarantees: both cursor columns are resolved to their result positions, so the receiver reads
/// each row's cursor from the correct columns regardless of driver quoting behavior.
#[test]
fn resolves_supported_cursor_columns_case_insensitively() {
    let mut described = columns(OracleType::Timestamp(6), OracleType::Number(18, 0));
    described[1].0 = "event_ts".to_owned();

    let (timestamp_index, tie_breaker_index) =
        validate_described_cursor_columns(&described, &watermark()).expect("cursor columns");

    assert_eq!(timestamp_index, 1);
    assert_eq!(tie_breaker_index, 2);
}

/// Scenario: every supported Oracle DATE and TIMESTAMP family type is used as the cursor.
/// Guarantees: the documented supported timestamp types are all accepted, so a valid deployment
/// is not rejected because of a timestamp precision or timezone variant.
#[test]
fn accepts_the_supported_oracle_timestamp_family() {
    for timestamp in [
        OracleType::Date,
        OracleType::Timestamp(0),
        OracleType::Timestamp(9),
        OracleType::TimestampTZ(6),
        OracleType::TimestampLTZ(6),
    ] {
        assert!(
            validate_described_cursor_columns(
                &columns(timestamp.clone(), OracleType::Int64),
                &watermark(),
            )
            .is_ok(),
            "timestamp type '{timestamp}' must be supported"
        );
    }
}

/// Scenario: a cursor column has a non-temporal, fractional, or otherwise non-deterministic type.
/// Guarantees: unsupported cursor metadata fails before polling, so the receiver never paginates
/// on a column whose ordering or checkpoint round trip is not exact.
#[test]
fn rejects_unsupported_cursor_column_types() {
    for kind in [
        OracleType::UInt64,
        OracleType::Number(38, 0),
        OracleType::Number(19, 0),
    ] {
        assert!(
            validate_described_cursor_columns(
                &columns(OracleType::Timestamp(6), kind),
                &watermark(),
            )
            .is_err()
        );
    }
    assert!(matches!(
        validate_described_cursor_columns(
            &columns(OracleType::Varchar2(32), OracleType::Int64),
            &watermark(),
        ),
        Err(OracleAdapterError::UnsupportedCursorTimestamp { .. })
    ));
    assert!(matches!(
        validate_described_cursor_columns(
            &columns(OracleType::Timestamp(6), OracleType::Number(38, 2)),
            &watermark(),
        ),
        Err(OracleAdapterError::UnsupportedCursorTieBreaker { .. })
    ));
    assert!(matches!(
        validate_described_cursor_columns(
            &columns(OracleType::Timestamp(6), OracleType::BinaryDouble),
            &watermark(),
        ),
        Err(OracleAdapterError::UnsupportedCursorTieBreaker { .. })
    ));
}

/// Scenario: a configured cursor column is absent from the query's result metadata.
/// Guarantees: a mismatch between the statement and the cursor configuration fails at startup
/// rather than at the first row fetch.
#[test]
fn rejects_missing_cursor_columns() {
    let described = vec![("PAYLOAD".to_owned(), OracleType::Varchar2(64))];

    assert!(matches!(
        validate_described_cursor_columns(&described, &watermark()),
        Err(OracleAdapterError::MissingCursorColumn(column)) if column == "EVENT_TS"
    ));
}

/// Scenario: a committed cursor timestamp is bound back into Oracle after a restart.
/// Guarantees: the checkpointed text round-trips through the Oracle timestamp type without
/// losing sub-second precision, so replay resumes at the exact committed boundary.
#[test]
fn cursor_timestamp_round_trips_through_oracle() {
    let committed = Timestamp::from_str("2026-01-01 12:34:56.123456789")
        .expect("committed timestamp should parse")
        .to_string();

    let rebound = Timestamp::from_str(&committed).expect("committed text should rebind");

    assert_eq!(rebound.to_string(), committed);
    assert_eq!(rebound.nanosecond(), 123_456_789);
}

/// Scenario: a committed timezone-aware cursor is rebound after restart.
/// Guarantees: the bind type retains the cursor's UTC offset instead of coercing it to a
/// timezone-naive timestamp and moving the polling boundary.
#[test]
fn timezone_aware_cursor_uses_a_timezone_aware_bind() {
    let committed = Timestamp::from_str("2026-01-01 12:34:56.123456789 +05:30")
        .expect("timezone-aware cursor should parse");

    assert!(committed.with_tz());
    assert_eq!(committed.tz_offset(), 19_800);
    assert!(matches!(cursor_bind_type(), OracleType::TimestampTZ(9)));
}

/// Scenario: a checkpoint file holds a cursor timestamp Oracle cannot parse.
/// Guarantees: an invalid committed timestamp is reported explicitly instead of being
/// interpolated into SQL or silently reset to the initial cursor.
#[test]
fn rejects_uninterpretable_cursor_timestamps() {
    assert!(parse_cursor_timestamp("not-a-timestamp").is_err());
}

/// Scenario: Configured or checkpointed cursor text contains oversized numeric components.
/// Guarantees: The shared Oracle parsing boundary rejects overflow, narrowing, and precision loss.
#[test]
fn cursor_parser_rejects_oversized_numeric_components() {
    for text in [
        "9".repeat(40),
        "4294969322-01-01 00:00:00".to_owned(),
        "2026-01-01 00:00:00.1234567890".to_owned(),
    ] {
        assert!(matches!(
            parse_cursor_timestamp(&text),
            Err(OracleAdapterError::InvalidCursorTimestamp(_))
        ));
    }
}

/// Scenario: Oracle returns a non-finite binary floating-point value.
/// Guarantees: Driver normalization fails the batch instead of emitting invalid OTLP data.
#[test]
fn rejects_non_finite_float() {
    assert!(matches!(
        finite_float(CellValue::Float64(f64::NAN)),
        Err(OracleAdapterError::NonFiniteFloat)
    ));
}
/// Scenario: Oracle result metadata contains a vendor type without a CellValue mapping.
/// Guarantees: Metadata validation fails explicitly instead of using a lossy fallback.
#[test]
fn rejects_unsupported_vendor_type() {
    assert!(matches!(
        validate_types(&[OracleType::BLOB]),
        Err(OracleAdapterError::UnsupportedType(_))
    ));
}

/// Scenario: An Easy Connect string uses a query timeout longer than connection establishment.
/// Guarantees: Connection and transport attempts remain bounded while query calls retain their
/// independently configured timeout.
#[test]
fn adds_bounded_network_timeouts() {
    let connect_string =
        bounded_connect_string("database.contoso.com:1521/ORCL", Duration::from_secs(120))
            .expect("Easy Connect string should be supported");

    assert_eq!(
        connect_string,
        "database.contoso.com:1521/ORCL?connect_timeout=10&transport_connect_timeout=10"
    );
}

/// Scenario: An Easy Connect string adds retries or multiple database addresses.
/// Guarantees: Connection establishment cannot multiply the fixed per-attempt startup bound.
#[test]
fn rejects_unbounded_connection_attempts() {
    for connect_string in [
        "database.contoso.com:1521/ORCL?retry_count=10",
        "database.contoso.com:1521/ORCL?retry_delay=5",
        "db1.contoso.com,db2.contoso.com:1521/ORCL",
    ] {
        assert!(bounded_connect_string(connect_string, Duration::from_secs(120)).is_err());
    }
}

/// Scenario: A mounted credential contains a trailing newline.
/// Guarantees: Kubernetes-style secret files load without adding the line ending to the credential.
#[test]
fn trims_credential_line_endings() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("password");
    fs::write(&path, b"secret\r\n").expect("write credential");

    assert_eq!(
        read_credential(path.to_str().expect("UTF-8 path"), "password")
            .expect("credential should load")
            .expose_secret(),
        "secret"
    );
}

/// Scenario: A mounted credential exceeds the receiver's fixed secret-file ceiling.
/// Guarantees: The adapter rejects the file before allocating or retaining unbounded secret data.
#[test]
fn rejects_oversized_credential() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("password");
    fs::write(&path, vec![b'x'; 64 * 1024 + 1]).expect("write credential");

    assert!(matches!(
        read_credential(path.to_str().expect("UTF-8 path"), "password"),
        Err(OracleAdapterError::CredentialTooLarge("password"))
    ));
}

/// Scenario: A mounted credential is not valid UTF-8.
/// Guarantees: Invalid text is rejected without including credential bytes in diagnostics.
#[test]
fn rejects_non_utf8_credential() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("password");
    fs::write(&path, [0xff]).expect("write credential");

    assert!(matches!(
        read_credential(path.to_str().expect("UTF-8 path"), "password"),
        Err(OracleAdapterError::InvalidCredentialEncoding("password"))
    ));
}
