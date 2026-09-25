// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! PostgreSQL configuration and parsed incremental-query validation.

use super::adapter::PostgreSqlAdapter;
use chrono::{DateTime, SecondsFormat, Utc};
use otel_arrow_dfe_scraper::database::{
    CatchUpConfig, CheckpointConfig, CompiledQuery, OutputConfig, PollingConfig, WatermarkConfig,
};
use serde::{Deserialize, Deserializer, de::Error as _};
use sqlparser::ast::{
    BinaryOperator, Expr, JoinOperator, OrderByKind, Query, SelectItem, SetExpr, Statement,
    TableFactor, Value, Visit, Visitor,
};
use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};
use std::{ops::ControlFlow, path::PathBuf, time::Duration};

/// Validated configuration for one PostgreSQL query and checkpoint stream.
pub struct PostgreSqlConfig {
    pub(super) source_id: String,
    pub(super) connection: ConnectionConfig,
    pub(super) authentication: AuthenticationConfig,
    pub(super) query: CompiledQuery,
    pub(super) checkpoint: CheckpointConfig,
    pub(super) fingerprint: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ConnectionConfig {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub database: String,
    pub tls: TlsConfig,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TlsConfig {
    /// Explicit CA bundle; an empty trust store or disabled verification is never accepted.
    pub ca_file: PathBuf,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AuthenticationConfig {
    pub username_file: PathBuf,
    pub password_file: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    source_id: String,
    connection: ConnectionConfig,
    authentication: AuthenticationConfig,
    query: QueryConfig,
    watermark: WatermarkConfig,
    checkpoint: CheckpointConfig,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryConfig {
    statement: String,
    #[serde(default = "default_interval", with = "humantime_serde")]
    interval: Duration,
    #[serde(default = "default_timeout", with = "humantime_serde")]
    timeout: Duration,
    #[serde(default = "default_fetch")]
    fetch_size_rows: usize,
    #[serde(default = "default_rows")]
    max_rows_per_poll: usize,
    #[serde(default = "default_bytes", deserialize_with = "byte_size")]
    max_batch_bytes: u64,
    #[serde(default)]
    catch_up: CatchUpConfig,
}

const fn default_port() -> u16 {
    5432
}
const fn default_interval() -> Duration {
    Duration::from_secs(60)
}
const fn default_timeout() -> Duration {
    Duration::from_secs(30)
}
const fn default_fetch() -> usize {
    300
}
const fn default_rows() -> usize {
    10_000
}
const fn default_bytes() -> u64 {
    10 * 1024 * 1024
}

fn byte_size<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    otel_arrow_dfe_config::byte_units::deserialize_u64(d)?
        .ok_or_else(|| D::Error::custom("max_batch_bytes must not be null"))
}

impl<'de> Deserialize<'de> for PostgreSqlConfig {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // Serde/parser diagnostics can contain operator SQL, cursor values or credentials.
        let raw = RawConfig::deserialize(d).map_err(|_| {
            D::Error::custom(
                "invalid PostgreSQL configuration: check field names, required fields and types",
            )
        })?;
        Self::compile(raw).map_err(D::Error::custom)
    }
}

impl PostgreSqlConfig {
    fn compile(mut raw: RawConfig) -> Result<Self, ConfigError> {
        let conn = &raw.connection;
        if raw.source_id.trim().is_empty() || raw.source_id.len() > 256 {
            return Err(ConfigError("source_id must contain 1..=256 bytes"));
        }
        if conn.host.trim().is_empty()
            || conn.host.contains(['/', '\\', '\0', ' ', '?', '='])
            || conn.database.trim().is_empty()
            || conn.port == 0
        {
            return Err(ConfigError(
                "connection requires a hostname, nonzero port and database",
            ));
        }
        if conn.tls.ca_file.as_os_str().is_empty()
            || raw.authentication.username_file.as_os_str().is_empty()
            || raw.authentication.password_file.as_os_str().is_empty()
        {
            return Err(ConfigError(
                "CA and credential file paths must not be empty",
            ));
        }
        if !(Duration::from_secs(60)..=Duration::from_secs(86400)).contains(&raw.query.interval)
            || raw.query.interval.subsec_nanos() != 0
        {
            return Err(ConfigError(
                "query.interval must be 1m..=24h in whole seconds",
            ));
        }
        if !(Duration::from_secs(1)..=Duration::from_secs(300)).contains(&raw.query.timeout)
            || raw.query.timeout.subsec_nanos() != 0
        {
            return Err(ConfigError(
                "query.timeout must be 1s..=5m in whole seconds",
            ));
        }
        raw.watermark
            .validate()
            .map_err(|_| ConfigError("invalid composite watermark"))?;
        for column in [
            &raw.watermark.timestamp().column,
            &raw.watermark.tie_breaker().column,
        ] {
            if !identifier(column) {
                return Err(ConfigError(
                    "cursor columns must be simple ASCII identifiers",
                ));
            }
        }
        let initial = parse_timestamp(&raw.watermark.timestamp().initial)?;
        let WatermarkConfig::Composite { timestamp, .. } = &mut raw.watermark;
        timestamp.initial = initial.to_rfc3339_opts(SecondsFormat::Micros, true);
        let sql = validate_sql(&raw.query.statement, &raw.watermark)?;
        let query = CompiledQuery::compile(
            sql.clone(),
            PollingConfig {
                interval: raw.query.interval,
                timeout: raw.query.timeout,
                fetch_size_rows: raw.query.fetch_size_rows,
                max_rows_per_poll: raw.query.max_rows_per_poll,
                max_batch_bytes: raw.query.max_batch_bytes,
                catch_up: raw.query.catch_up,
            },
            &raw.watermark,
            &raw.checkpoint,
            OutputConfig {
                timestamp_column: Some(raw.watermark.timestamp().column.clone()),
                validation_columns: vec![raw.watermark.tie_breaker().column.clone()],
            },
        )
        .map_err(|_| ConfigError("invalid polling, output or checkpoint configuration"))?;
        // Credentials, CA files and performance knobs are deliberately not stream identity.
        let semantic = serde_json::json!({
            "adapter": "postgresql", "mapping": "typed-body",
            "source": raw.source_id, "host": conn.host.to_ascii_lowercase(),
            "port": conn.port, "database": conn.database, "sql": sql,
            "timestamp_column": query.watermark().timestamp_column,
            "id_column": query.watermark().tie_breaker_column,
            "initial": query.watermark().initial,
        });
        let fingerprint = blake3::hash(semantic.to_string().as_bytes())
            .to_hex()
            .to_string();
        Ok(Self {
            source_id: raw.source_id,
            connection: raw.connection,
            authentication: raw.authentication,
            query,
            checkpoint: raw.checkpoint,
            fingerprint,
        })
    }

    pub(super) fn adapter(&self) -> PostgreSqlAdapter {
        PostgreSqlAdapter::new(
            self.connection.clone(),
            self.authentication.clone(),
            self.query.sql().to_owned(),
        )
    }
}

pub(super) fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, ConfigError> {
    let timestamp = DateTime::parse_from_rfc3339(value)
        .map_err(|_| ConfigError("timestamp must be an RFC3339 timestamp with an explicit offset"))?
        .with_timezone(&Utc);
    if timestamp.timestamp_subsec_nanos() % 1000 != 0 {
        return Err(ConfigError(
            "PostgreSQL timestamps require microsecond precision",
        ));
    }
    Ok(timestamp)
}

fn identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn unnest(mut expr: &Expr) -> &Expr {
    while let Expr::Nested(inner) = expr {
        expr = inner;
    }
    expr
}

fn binary(expr: &Expr, operator: BinaryOperator) -> Option<(&Expr, &Expr)> {
    match unnest(expr) {
        Expr::BinaryOp { left, op, right } if *op == operator => {
            Some((unnest(left), unnest(right)))
        }
        _ => None,
    }
}

fn comparison(expr: &Expr, col: &Expr, op: BinaryOperator, bind: &str) -> bool {
    binary(expr, op).is_some_and(|(left, right)| {
        left.to_string() == col.to_string()
            && matches!(right, Expr::Value(value) if matches!(&value.value, Value::Placeholder(p) if p == bind))
    })
}

fn keyset(expr: &Expr, timestamp: &Expr, id: &Expr) -> bool {
    if let Some((left, right)) = binary(expr, BinaryOperator::And) {
        return keyset(left, timestamp, id) || keyset(right, timestamp, id);
    }
    binary(expr, BinaryOperator::Or).is_some_and(|(left, right)| {
        comparison(left, timestamp, BinaryOperator::Gt, "$1")
            && binary(right, BinaryOperator::And).is_some_and(|(equal, greater)| {
                comparison(equal, timestamp, BinaryOperator::Eq, "$1")
                    && comparison(greater, id, BinaryOperator::Gt, "$2")
            })
    })
}

fn column_name(expr: &Expr) -> Option<String> {
    let ident = match expr {
        Expr::Identifier(ident) => ident,
        Expr::CompoundIdentifier(idents) => idents.last()?,
        _ => return None,
    };
    Some(if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_lowercase()
    })
}

struct ShapeVisitor {
    queries: usize,
    bad_bind: bool,
}
impl Visitor for ShapeVisitor {
    type Break = ();
    fn pre_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
        self.queries += 1;
        ControlFlow::Continue(())
    }
    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<()> {
        if let Expr::Value(value) = expr
            && let Value::Placeholder(bind) = &value.value
            && bind != "$1"
            && bind != "$2"
        {
            self.bad_bind = true;
        }
        ControlFlow::Continue(())
    }
}

pub(super) fn validate_sql(sql: &str, watermark: &WatermarkConfig) -> Result<String, ConfigError> {
    if sql.len() > 64 * 1024 {
        return Err(ConfigError("query.statement exceeds 64 KiB"));
    }
    let mut statements = Parser::parse_sql(&PostgreSqlDialect {}, sql)
        .map_err(|_| ConfigError("query.statement is not valid supported PostgreSQL SQL"))?;
    if statements.len() != 1 {
        return Err(ConfigError("exactly one SELECT is required"));
    }
    let Statement::Query(query) = statements.remove(0) else {
        return Err(ConfigError("only SELECT queries are supported"));
    };
    let mut visitor = ShapeVisitor {
        queries: 0,
        bad_bind: false,
    };
    let _ = query.visit(&mut visitor);
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Err(ConfigError("set operations are not supported"));
    };
    if visitor.queries != 1
        || visitor.bad_bind
        || query.with.is_some()
        || query.limit_clause.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || select.into.is_some()
        || select.distinct.is_some()
    {
        return Err(ConfigError(
            "unsupported query shape, bind, row lock or row limit",
        ));
    }
    if select.from.len() != 1
        || select.from.iter().any(|table| {
            !matches!(&table.relation, TableFactor::Table { args: None, .. })
                || table.joins.iter().any(|join| {
                    !matches!(&join.relation, TableFactor::Table { args: None, .. })
                        || !matches!(
                            join.join_operator,
                            JoinOperator::Inner(_) | JoinOperator::Join(_)
                        )
                })
        })
    {
        return Err(ConfigError("only tables and inner joins are supported"));
    }
    let order = query
        .order_by
        .as_ref()
        .ok_or(ConfigError("cursor ORDER BY is required"))?;
    let OrderByKind::Expressions(order) = &order.kind else {
        return Err(ConfigError("cursor ORDER BY expressions are required"));
    };
    if order.len() != 2
        || order
            .iter()
            .any(|item| item.options.asc == Some(false) || item.with_fill.is_some())
        || column_name(&order[0].expr).as_deref() != Some(watermark.timestamp().column.as_str())
        || column_name(&order[1].expr).as_deref() != Some(watermark.tie_breaker().column.as_str())
    {
        return Err(ConfigError(
            "ORDER BY must be ascending timestamp then tie-breaker",
        ));
    }
    if !select
        .selection
        .as_ref()
        .is_some_and(|expr| keyset(expr, &order[0].expr, &order[1].expr))
    {
        return Err(ConfigError(
            "WHERE must enforce timestamp > $1 OR (timestamp = $1 AND tie_breaker > $2)",
        ));
    }
    for ordered in order {
        let name = column_name(&ordered.expr).ok_or(ConfigError("invalid cursor identifier"))?;
        let mut found = false;
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) if column_name(expr).as_ref() == Some(&name) => {
                    if expr.to_string() != ordered.expr.to_string() {
                        return Err(ConfigError(
                            "cursor projection must match its ordering expression",
                        ));
                    }
                    found = true;
                }
                SelectItem::ExprWithAlias { alias, expr } if alias.value == name => {
                    if expr.to_string() != ordered.expr.to_string() {
                        return Err(ConfigError(
                            "cursor projection aliases cannot change cursor identity",
                        ));
                    }
                    found = true;
                }
                SelectItem::Wildcard(_) if select.from[0].joins.is_empty() => found = true,
                SelectItem::QualifiedWildcard(_, _) => {
                    if let Some((qualifier, _)) = ordered.expr.to_string().rsplit_once('.')
                        && item.to_string() == format!("{qualifier}.*")
                    {
                        found = true;
                    }
                }
                _ => {}
            }
        }
        if !found {
            return Err(ConfigError("cursor columns must be selected"));
        }
    }
    Ok(query.to_string())
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(super) struct ConfigError(pub &'static str);
