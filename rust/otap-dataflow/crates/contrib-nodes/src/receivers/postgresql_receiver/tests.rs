// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;

fn configuration() -> Value {
    serde_json::json!({
        "source_id": "audit",
        "connection": { "host": "localhost", "database": "events", "tls": { "ca_file": "ca.pem" } },
        "authentication": { "username_file": "username", "password_file": "password" },
        "query": { "statement": "SELECT event_ts, event_id, payload FROM events WHERE (event_ts > $1 OR (event_ts = $1 AND event_id > $2)) ORDER BY event_ts ASC, event_id ASC" },
        "watermark": {
            "mode": "composite",
            "timestamp": { "column": "event_ts", "bind": "last_timestamp", "initial": "2026-01-01T00:00:00Z", "timezone": "UTC" },
            "tie_breaker": { "column": "event_id", "bind": "last_id", "initial": 0 }
        },
        "checkpoint": { "directory": "state", "on_nack": "rewind", "nack_backoff": "1s", "max_consecutive_failures": 3 }
    })
}

/// Scenario: Operational knobs are omitted or supplied with their defaults.
/// Guarantees: Defaults are one minute, thirty seconds and 300 rows without changing stream identity.
#[test]
fn defaults_and_fingerprint() {
    let baseline = parse(&configuration()).expect("valid");
    assert_eq!(
        baseline.query.interval(),
        std::time::Duration::from_secs(60)
    );
    assert_eq!(baseline.query.timeout(), std::time::Duration::from_secs(30));
    assert_eq!(baseline.query.fetch_size_rows(), 300);
    let mut explicit = configuration();
    explicit["query"]["interval"] = "1m".into();
    explicit["query"]["timeout"] = "30s".into();
    explicit["query"]["fetch_size_rows"] = 300.into();
    explicit["authentication"]["password_file"] = "rotated-password".into();
    assert_eq!(
        baseline.fingerprint,
        parse(&explicit).expect("explicit").fingerprint
    );
    explicit["connection"]["database"] = "other".into();
    assert_ne!(
        baseline.fingerprint,
        parse(&explicit).expect("other").fingerprint
    );
}

/// Scenario: SQL attempts multiple statements, cursor bypasses, row locks, invalid ordering or subqueries.
/// Guarantees: Invalid statements fail before connecting and parser errors do not expose customer SQL.
#[test]
fn rejects_unsafe_or_non_incremental_sql() {
    for sql in [
        "DELETE FROM events",
        "SELECT 1; DELETE FROM events",
        "SELECT event_ts, event_id FROM events WHERE event_ts > $1 OR event_id > $2 ORDER BY event_ts, event_id",
        "SELECT event_ts, event_id FROM events WHERE (event_ts > $1 OR (event_ts = $1 AND event_id > $2)) OR true ORDER BY event_ts, event_id",
        "SELECT event_ts, event_id FROM events WHERE (event_ts > $1 OR (event_ts = $1 AND event_id > $2)) ORDER BY event_ts DESC, event_id",
        "SELECT event_ts, event_id FROM events WHERE (event_ts > $1 OR (event_ts = $1 AND event_id > $2)) ORDER BY event_ts, event_id FOR UPDATE",
        "SELECT event_ts, event_id FROM events WHERE (event_ts > $1 OR (event_ts = $1 AND event_id > $2)) ORDER BY event_ts, event_id LIMIT 1",
        "SELECT (SELECT 1), event_ts, event_id FROM events WHERE (event_ts > $1 OR (event_ts = $1 AND event_id > $2)) ORDER BY event_ts, event_id",
        "SELECT PRIVATE_SQL_SENTINEL broken",
        "SELECT payload AS event_ts, event_id FROM events WHERE (event_ts > $1 OR (event_ts = $1 AND event_id > $2)) ORDER BY event_ts, event_id",
        "SELECT event_ts, event_id INTO stolen FROM events WHERE (event_ts > $1 OR (event_ts = $1 AND event_id > $2)) ORDER BY event_ts, event_id",
        "SELECT event_ts, event_id FROM events WHERE (event_ts > $1 OR (event_ts = $1 AND event_id > $2)) AND event_id > $3 ORDER BY event_ts, event_id",
        "SELECT d.* FROM events e INNER JOIN details d ON d.event_id = e.event_id WHERE (e.event_ts > $1 OR (e.event_ts = $1 AND e.event_id > $2)) ORDER BY e.event_ts, e.event_id",
    ] {
        let mut config = configuration();
        config["query"]["statement"] = sql.into();
        let error = match parse(&config) {
            Ok(_) => panic!("accepted forbidden SQL"),
            Err(error) => error,
        };
        assert!(!error.to_string().contains("PRIVATE_SQL_SENTINEL"));
    }
}

/// Scenario: The same starting instant uses another offset and operational/CA settings change.
/// Guarantees: Equivalent stream identity remains resumable while a changed source identity is not.
#[test]
fn fingerprint_tracks_semantics_not_performance_or_trust_files() {
    let baseline = parse(&configuration()).expect("baseline");
    let mut changed = configuration();
    changed["watermark"]["timestamp"]["initial"] = "2026-01-01T02:00:00+02:00".into();
    changed["connection"]["tls"]["ca_file"] = "rotated-ca.pem".into();
    changed["query"]["interval"] = "1.5m".into();
    changed["query"]["fetch_size_rows"] = 50.into();
    assert_eq!(
        baseline.fingerprint,
        parse(&changed).expect("equivalent").fingerprint
    );
    changed["source_id"] = "different-stream".into();
    assert_ne!(
        baseline.fingerprint,
        parse(&changed).expect("new identity").fingerprint
    );
}

/// Scenario: A cancelled operation is followed by a new operation while the old token is retained.
/// Guarantees: Cancellation fails before connection work, and stale tokens cannot cancel the new operation.
#[tokio::test]
async fn cancellation_is_scoped_to_one_operation() {
    use otel_arrow_dfe_scraper::database::{DriverAdapter, DriverCancellation};
    let directory = tempfile::tempdir().expect("credential directory");
    let mut raw = configuration();
    raw["authentication"]["username_file"] = directory
        .path()
        .join("missing")
        .to_string_lossy()
        .into_owned()
        .into();
    let config = parse(&raw).expect("config");
    let mut adapter = config.adapter();
    let old = adapter.begin_operation().expect("first operation");
    old.cancel().await.expect("cancel");
    assert!(matches!(
        adapter.validate_query(&config.query).await,
        Err(PgError::Cancelled)
    ));
    let current = adapter.begin_operation().expect("new operation");
    old.cancel().await.expect("stale cancellation");
    // The intentionally missing credential file is reached only when the new token is uncancelled.
    assert!(matches!(
        adapter.validate_query(&config.query).await,
        Err(PgError::Credentials)
    ));
    current.cancel().await.expect("new cancellation");
    adapter.shutdown().await.expect("no session was leaked");
}

/// Scenario: A query has comments and an extra AND filter around the full keyset expression.
/// Guarantees: Parsed validation handles syntax rather than substring matching and preserves the bound.
#[test]
fn accepts_filters_comments_and_inner_joins() {
    let mut config = configuration();
    config["query"]["statement"] = "/* operator query */ SELECT e.event_ts, e.event_id, d.payload FROM events e INNER JOIN details d ON d.id = e.event_id WHERE (e.event_ts > $1 OR (e.event_ts = $1 AND e.event_id > $2)) AND d.enabled = true ORDER BY e.event_ts, e.event_id".into();
    assert!(parse(&config).is_ok());
}

/// Scenario: Invalid timing, unsupported transport options, cursor precision or leaked password fields are supplied.
/// Guarantees: Invalid configuration fails closed and does not echo secret values.
#[test]
fn validates_configuration_and_redacts_errors() {
    for (section, key, value) in [
        ("query", "interval", serde_json::json!("59s")),
        ("query", "timeout", serde_json::json!("0.5s")),
        ("query", "fetch_size_rows", serde_json::json!(0)),
        ("query", "fetch_size", serde_json::json!(300)),
        ("connection", "sslmode", serde_json::json!("disable")),
        (
            "authentication",
            "password",
            serde_json::json!("PRIVATE_PASSWORD_SENTINEL"),
        ),
    ] {
        let mut config = configuration();
        config[section][key] = value;
        let error = match parse(&config) {
            Ok(_) => panic!("invalid config accepted"),
            Err(error) => error,
        };
        assert!(!error.to_string().contains("PRIVATE_PASSWORD_SENTINEL"));
    }
    let mut config = configuration();
    config["watermark"]["timestamp"]["initial"] = "2026-01-01T00:00:00.000000001Z".into();
    assert!(parse(&config).is_err());
}
