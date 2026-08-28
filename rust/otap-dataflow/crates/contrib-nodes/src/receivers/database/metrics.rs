// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Stable internal telemetry for named database query jobs.

use super::driver::{DatabaseSystem, ErrorClass};
use otap_df_engine::context::PipelineContext;
use otap_df_telemetry::error::Error as TelemetryError;
use otap_df_telemetry::instrument::{Counter, HistogramNormal};
use otap_df_telemetry::metrics::{MeasurementMetricSet, MetricSet, MetricSetSnapshot};
use otap_df_telemetry::reporter::MetricsReporter;
use otap_df_telemetry_macros::{AttributeEnum, attribute_set, metric_set};
use std::time::Duration;

/// Bounded internal error classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, AttributeEnum)]
pub(crate) enum DatabaseFailureClass {
    /// Invalid configuration.
    Configuration,
    /// Authentication failure.
    Authentication,
    /// Temporary connection or network failure.
    TransientTransport,
    /// Deadline or cancellation.
    TimeoutCancel,
    /// SQL or authorization failure.
    Query,
    /// Native value conversion failure.
    Conversion,
    /// Downstream pipeline failure.
    Downstream,
    /// Internal invariant failure.
    Internal,
}

impl From<ErrorClass> for DatabaseFailureClass {
    fn from(value: ErrorClass) -> Self {
        match value {
            ErrorClass::Configuration => Self::Configuration,
            ErrorClass::Authentication => Self::Authentication,
            ErrorClass::TransientTransport => Self::TransientTransport,
            ErrorClass::TimeoutCancel => Self::TimeoutCancel,
            ErrorClass::Query => Self::Query,
            ErrorClass::Conversion => Self::Conversion,
            ErrorClass::Downstream => Self::Downstream,
            ErrorClass::Internal => Self::Internal,
        }
    }
}

/// Low-cardinality database and configured query identity.
#[attribute_set(item, registration)]
#[derive(Clone, Debug)]
pub(crate) struct DatabaseQueryAttributes {
    /// Database vendor.
    #[attribute_key = "db.system.name"]
    pub(crate) database_system: String,
    /// Stable configured query name.
    #[attribute_key = "query.name"]
    pub(crate) query_name: String,
}

/// Bounded class for one failed poll.
#[attribute_set(item, measurement)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct DatabaseFailureAttributes {
    /// Stable failure class, never a raw error message.
    #[attribute_key = "error.type"]
    pub(crate) error_type: DatabaseFailureClass,
}

/// Primary `receiver.database` metric set for one named query.
#[metric_set(
    name = "receiver.database",
    registration_attributes = DatabaseQueryAttributes
)]
#[derive(Clone, Debug, Default)]
pub(crate) struct DatabaseQueryMetricSet {
    /// Query polls started.
    #[metric(unit = "{poll}")]
    pub(crate) polls_started: Counter<u64>,
    /// Query polls completed and converted.
    #[metric(unit = "{poll}")]
    pub(crate) polls_completed: Counter<u64>,
    /// Rows normalized.
    #[metric(unit = "{row}")]
    pub(crate) rows_read: Counter<u64>,
    /// Approximate normalized row bytes.
    #[metric(unit = "By")]
    pub(crate) bytes_read: Counter<u64>,
    /// OTLP batches admitted downstream.
    #[metric(unit = "{batch}")]
    pub(crate) batches_sent: Counter<u64>,
    /// End-to-end poll duration in nanoseconds.
    #[metric(unit = "ns")]
    pub(crate) poll_duration_ns: HistogramNormal,
}

/// Failure counts partitioned only by bounded error class.
#[metric_set(
    name = "receiver.database.failures",
    registration_attributes = DatabaseQueryAttributes,
    measurement_attributes = DatabaseFailureAttributes
)]
#[derive(Clone, Debug, Default)]
pub(crate) struct DatabaseFailureMetricSet {
    /// Failed query polls.
    #[metric(unit = "{poll}")]
    pub(crate) polls_failed: Counter<u64>,
}

/// Metrics owned by one configured query.
pub(crate) struct QueryMetrics {
    metrics: MetricSet<DatabaseQueryMetricSet>,
    failures: MeasurementMetricSet<DatabaseFailureMetricSet>,
}

impl QueryMetrics {
    /// Registers query metrics using only approved low-cardinality identities.
    pub(crate) fn register(
        pipeline: &PipelineContext,
        system: DatabaseSystem,
        query_name: &str,
    ) -> Self {
        let attributes = DatabaseQueryAttributes {
            database_system: system.as_str().to_owned(),
            query_name: query_name.to_owned(),
        };
        Self {
            metrics: DatabaseQueryMetricSet::register(pipeline, &attributes),
            failures: DatabaseFailureMetricSet::register(pipeline, &attributes),
        }
    }

    /// Records query admission.
    pub(crate) fn poll_started(&mut self) {
        self.metrics.polls_started.inc();
    }

    /// Records successful normalization.
    pub(crate) fn poll_completed(&mut self, rows: usize, bytes: u64, duration: Duration) {
        self.metrics.polls_completed.inc();
        self.metrics
            .rows_read
            .add(u64::try_from(rows).unwrap_or(u64::MAX));
        self.metrics.bytes_read.add(bytes);
        self.metrics
            .poll_duration_ns
            .record(duration.as_nanos() as f64);
    }

    /// Records one downstream-admitted batch.
    pub(crate) fn batch_sent(&mut self) {
        self.metrics.batches_sent.inc();
    }

    /// Records a failed poll using a bounded class.
    pub(crate) fn poll_failed(&mut self, class: ErrorClass, duration: Duration) {
        self.failures
            .with(DatabaseFailureAttributes {
                error_type: class.into(),
            })
            .polls_failed
            .inc();
        self.metrics
            .poll_duration_ns
            .record(duration.as_nanos() as f64);
    }

    pub(crate) fn report(&mut self, reporter: &mut MetricsReporter) -> Result<(), TelemetryError> {
        reporter.report(&mut self.metrics)?;
        reporter.report_measurement(&mut self.failures)
    }

    pub(crate) fn terminal_snapshots(&mut self) -> Vec<MetricSetSnapshot> {
        let mut snapshots = self.metrics.terminal_snapshots();
        snapshots.extend(self.failures.terminal_snapshots());
        snapshots
    }
}
