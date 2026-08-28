// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! OTAP lifecycle, scheduling, admission, and downstream sends.

use super::config::{DatabaseReceiverConfig, ErrorPolicy};
use super::driver::{
    CredentialError, DriverAdapter, EnvironmentCredentialProvider, ErrorClass, ErrorDisposition,
    SessionDisposition,
};
use super::metrics::QueryMetrics;
use super::otlp::{OtlpMapping, OtlpMappingError, rows_to_pdata};
use super::query::{CompiledQuery, ExecutionLimits, QueryCompileError, compile_queries};
use super::row::RowPage;
use super::scheduler::{AdmissionController, QuerySchedule};
use futures::future::LocalBoxFuture;
use futures::{FutureExt, StreamExt};
use otap_df_engine::MessageSourceLocalEffectHandlerExtension;
use otap_df_engine::control::NodeControlMsg;
use otap_df_engine::error::{Error as EngineError, ReceiverErrorKind};
use otap_df_engine::local::receiver as local;
use otap_df_engine::memory_limiter::LocalReceiverAdmissionState;
use otap_df_engine::terminal_state::TerminalState;
use otap_df_otap::pdata::OtapPdata;
use otap_df_telemetry::otel_error;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::{Duration, Instant as StdInstant, SystemTime, UNIX_EPOCH};
use tokio::time::{self, Instant};
use tokio_util::sync::CancellationToken;

/// Common local database receiver parameterized by one driver adapter.
pub(crate) struct DatabaseReceiver<A: DriverAdapter> {
    adapter: Rc<A>,
    credentials: EnvironmentCredentialProvider,
    resource_attributes: BTreeMap<String, serde_json::Value>,
    jobs: Vec<QueryJob<A::Session>>,
    limits: super::config::LimitsConfig,
    retry: super::config::RetryConfig,
    admission: AdmissionController,
    memory_pressure: LocalReceiverAdmissionState,
    idle_sessions: Vec<A::Session>,
    connection_slots: usize,
}

struct QueryJob<S> {
    query: CompiledQuery,
    schedule: QuerySchedule,
    metrics: QueryMetrics,
    _session: std::marker::PhantomData<S>,
}

struct PollCompletion<S, E: std::error::Error + 'static> {
    job_index: usize,
    session: Option<S>,
    duration: Duration,
    result: Result<RowPage, PollTaskError<E>>,
}

#[derive(Debug, thiserror::Error)]
enum PollTaskError<E: std::error::Error + 'static> {
    #[error(transparent)]
    Credentials(#[from] CredentialError),
    #[error(transparent)]
    Adapter(E),
}

type PollFuture<S, E> = LocalBoxFuture<'static, PollCompletion<S, E>>;

impl<A> DatabaseReceiver<A>
where
    A: DriverAdapter + 'static,
{
    /// Builds the shared runtime from validated config and one adapter.
    pub(crate) fn new<C>(
        pipeline: &otap_df_engine::context::PipelineContext,
        config: DatabaseReceiverConfig<C>,
        adapter: A,
        username: String,
        password_env: String,
    ) -> Result<Self, QueryCompileError> {
        let system = adapter.system();
        let startup_jitter_max = config.scheduling.startup_jitter_max;
        let queries = compile_queries(config.queries)?;
        let jobs = queries
            .into_iter()
            .map(|query| QueryJob {
                schedule: QuerySchedule::new(query.interval, startup_jitter_max),
                metrics: QueryMetrics::register(pipeline, system, &query.name),
                query,
                _session: std::marker::PhantomData,
            })
            .collect();
        let admission = AdmissionController::new(
            config.limits.max_concurrent_queries,
            config.limits.max_in_flight_bytes(),
        );

        Ok(Self {
            adapter: Rc::new(adapter),
            credentials: EnvironmentCredentialProvider::new(username, password_env),
            resource_attributes: config.resource_attributes,
            jobs,
            limits: config.limits,
            retry: config.retry,
            admission,
            memory_pressure: LocalReceiverAdmissionState::from_process_state(
                &pipeline.memory_pressure_state(),
            ),
            idle_sessions: Vec::new(),
            connection_slots: 0,
        })
    }

    fn execution_limits(&self, query: &CompiledQuery) -> ExecutionLimits {
        ExecutionLimits {
            max_rows: query.max_rows,
            max_batch_rows: self.limits.max_batch_rows.min(query.max_rows),
            max_batch_bytes: self.limits.max_batch_bytes(),
            max_page_bytes: self.limits.max_page_bytes(),
            fetch_size: self.limits.fetch_size,
        }
    }

    fn next_deadline(&self) -> Instant {
        self.jobs
            .iter()
            .filter_map(|job| job.schedule.next_due())
            .min()
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(86_400))
    }

    async fn open_min_connections(
        &mut self,
        effect_handler: &local::EffectHandler<OtapPdata>,
    ) -> Result<(), EngineError> {
        while self.connection_slots < self.adapter.min_connections() {
            let credentials = self.credentials.resolve().map_err(|error| {
                database_error(
                    effect_handler,
                    "<startup>",
                    ErrorClass::Authentication,
                    Some(error.to_string()),
                )
            })?;
            let session = self.adapter.connect(credentials).await.map_err(|error| {
                let disposition = self.adapter.classify_error(&error);
                database_error(effect_handler, "<startup>", disposition.class, None)
            })?;
            self.idle_sessions.push(session);
            self.connection_slots += 1;
        }
        Ok(())
    }

    fn admit_due(
        &mut self,
        inflight: &mut futures::stream::FuturesUnordered<PollFuture<A::Session, A::Error>>,
        cancel: &CancellationToken,
    ) {
        if self.memory_pressure.should_shed_ingress() {
            return;
        }

        let now = Instant::now();
        while self.admission.has_capacity() {
            let Some(job_index) = self.jobs.iter().position(|job| job.schedule.is_due(now)) else {
                break;
            };

            let existing_session = self.idle_sessions.pop();
            let needs_connection = existing_session.is_none();
            if needs_connection && self.connection_slots >= self.adapter.max_connections() {
                break;
            }
            let reservation = self.limits.max_page_bytes();
            if !self.admission.try_acquire(reservation) {
                if let Some(session) = existing_session {
                    self.idle_sessions.push(session);
                }
                break;
            }
            if needs_connection {
                self.connection_slots += 1;
            }

            self.jobs[job_index].schedule.start(reservation);
            self.jobs[job_index].metrics.poll_started();
            let adapter = Rc::clone(&self.adapter);
            let credentials = self.credentials.clone();
            let query = self.jobs[job_index].query.clone();
            let limits = self.execution_limits(&query);
            let cancel = cancel.child_token();
            inflight.push(
                async move {
                    let started = StdInstant::now();
                    let mut session = existing_session;
                    let result = async {
                        if session
                            .as_ref()
                            .is_some_and(|session| adapter.session_expired(session))
                        {
                            if let Some(expired) = session.take() {
                                _ = adapter.shutdown(expired).await;
                            }
                        }
                        if session.is_none() {
                            let resolved = credentials.resolve()?;
                            session = Some(
                                adapter
                                    .connect(resolved)
                                    .await
                                    .map_err(PollTaskError::Adapter)?,
                            );
                        }
                        adapter
                            .execute_page(
                                session.as_mut().expect("session established above"),
                                &query,
                                limits,
                                cancel,
                            )
                            .await
                            .map_err(PollTaskError::Adapter)
                    }
                    .await;
                    PollCompletion {
                        job_index,
                        session,
                        duration: started.elapsed(),
                        result,
                    }
                }
                .boxed_local(),
            );
        }
    }

    async fn handle_completion(
        &mut self,
        completion: PollCompletion<A::Session, A::Error>,
        effect_handler: &local::EffectHandler<OtapPdata>,
    ) -> Result<(), EngineError> {
        let index = completion.job_index;
        match completion.result {
            Ok(page) => {
                if let Some(session) = completion.session {
                    if self.adapter.session_expired(&session) {
                        _ = self.adapter.shutdown(session).await;
                        self.connection_slots = self.connection_slots.saturating_sub(1);
                    } else {
                        self.idle_sessions.push(session);
                    }
                }
                let row_count = page.row_count;
                let normalized_bytes = page.normalized_bytes;
                let observed_time = unix_time_nanos();
                for batch in page.batches {
                    let pdata = match rows_to_pdata(
                        batch,
                        OtlpMapping {
                            system: self.adapter.system(),
                            query_name: &self.jobs[index].query.name,
                            output: &self.jobs[index].query.output,
                            resource_attributes: &self.resource_attributes,
                            observed_time_unix_nano: observed_time,
                        },
                    ) {
                        Ok(pdata) => pdata,
                        Err(error) => {
                            self.jobs[index]
                                .metrics
                                .poll_failed(ErrorClass::Conversion, completion.duration);
                            let reservation = self.jobs[index].schedule.stop();
                            self.admission.release(reservation);
                            if self.jobs[index].query.error_policy == ErrorPolicy::StopQuery {
                                otel_error!(
                                    "database_receiver.query_stopped",
                                    query_name = self.jobs[index].query.name.as_str(),
                                    error_type = ErrorClass::Conversion.as_str(),
                                    message =
                                        "Database query stopped after an OTLP mapping failure"
                                );
                                return Ok(());
                            }
                            return Err(mapping_error(
                                effect_handler,
                                &self.jobs[index].query.name,
                                error,
                            ));
                        }
                    };

                    if let Err(error) = effect_handler.send_message_with_source_node(pdata).await {
                        self.jobs[index]
                            .metrics
                            .poll_failed(ErrorClass::Downstream, completion.duration);
                        let reservation = self.jobs[index].schedule.stop();
                        self.admission.release(reservation);
                        return Err(error.into());
                    }
                    self.jobs[index].metrics.batch_sent();
                }
                self.jobs[index].metrics.poll_completed(
                    row_count,
                    normalized_bytes,
                    completion.duration,
                );
                let reservation = self.jobs[index].schedule.complete(Instant::now());
                self.admission.release(reservation);
                Ok(())
            }
            Err(PollTaskError::Credentials(error)) => {
                if completion.session.is_none() {
                    self.connection_slots = self.connection_slots.saturating_sub(1);
                }
                let reservation = self.jobs[index].schedule.stop();
                self.admission.release(reservation);
                self.jobs[index]
                    .metrics
                    .poll_failed(ErrorClass::Authentication, completion.duration);
                Err(database_error(
                    effect_handler,
                    &self.jobs[index].query.name,
                    ErrorClass::Authentication,
                    Some(error.to_string()),
                ))
            }
            Err(PollTaskError::Adapter(error)) => {
                let disposition = self.adapter.classify_error(&error);
                self.jobs[index]
                    .metrics
                    .poll_failed(disposition.class, completion.duration);
                self.finish_failed_session(completion.session, disposition)
                    .await;

                if disposition.retryable && self.retry.enabled {
                    let reservation = self.jobs[index].schedule.retry(Instant::now(), &self.retry);
                    self.admission.release(reservation);
                    return Ok(());
                }

                let reservation = self.jobs[index].schedule.stop();
                self.admission.release(reservation);
                if self.jobs[index].query.error_policy == ErrorPolicy::StopQuery {
                    otel_error!(
                        "database_receiver.query_stopped",
                        query_name = self.jobs[index].query.name.as_str(),
                        error_type = disposition.class.as_str(),
                        message = "Database query stopped after a permanent failure"
                    );
                    Ok(())
                } else {
                    Err(database_error(
                        effect_handler,
                        &self.jobs[index].query.name,
                        disposition.class,
                        None,
                    ))
                }
            }
        }
    }

    async fn finish_failed_session(
        &mut self,
        session: Option<A::Session>,
        disposition: ErrorDisposition,
    ) {
        match (session, disposition.session) {
            (Some(session), SessionDisposition::Reuse) => self.idle_sessions.push(session),
            (Some(session), SessionDisposition::Discard) => {
                _ = self.adapter.shutdown(session).await;
                self.connection_slots = self.connection_slots.saturating_sub(1);
            }
            (None, _) => {
                self.connection_slots = self.connection_slots.saturating_sub(1);
            }
        }
    }

    async fn shutdown(
        &mut self,
        inflight: &mut futures::stream::FuturesUnordered<PollFuture<A::Session, A::Error>>,
        cancel: &CancellationToken,
        deadline: StdInstant,
    ) {
        cancel.cancel();
        let deadline = Instant::from_std(deadline);
        while !inflight.is_empty() {
            let Ok(Some(completion)) = time::timeout_at(deadline, inflight.next()).await else {
                break;
            };
            if let Some(session) = completion.session {
                _ = self.adapter.shutdown(session).await;
                self.connection_slots = self.connection_slots.saturating_sub(1);
            } else {
                self.connection_slots = self.connection_slots.saturating_sub(1);
            }
            let reservation = self.jobs[completion.job_index].schedule.stop();
            self.admission.release(reservation);
        }
        for session in self.idle_sessions.drain(..) {
            if Instant::now() >= deadline {
                break;
            }
            _ = self.adapter.shutdown(session).await;
            self.connection_slots = self.connection_slots.saturating_sub(1);
        }
    }
}

#[async_trait::async_trait(?Send)]
impl<A> local::Receiver<OtapPdata> for DatabaseReceiver<A>
where
    A: DriverAdapter + 'static,
{
    async fn start(
        self: Box<Self>,
        mut ctrl_chan: local::ControlChannel<OtapPdata>,
        effect_handler: local::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, EngineError> {
        let mut receiver = *self;
        let capabilities = receiver.adapter.capabilities();
        if !capabilities.cancellation
            || !capabilities.result_metadata
            || !capabilities.parameter_binding
        {
            return Err(database_error(
                &effect_handler,
                "<startup>",
                ErrorClass::Configuration,
                Some(
                    "adapter does not provide required cancellation, metadata, and bind capabilities"
                        .to_owned(),
                ),
            ));
        }
        let cancel = CancellationToken::new();
        let mut inflight = futures::stream::FuturesUnordered::new();
        if let Err(error) = receiver.open_min_connections(&effect_handler).await {
            receiver
                .shutdown(
                    &mut inflight,
                    &cancel,
                    StdInstant::now() + Duration::from_secs(5),
                )
                .await;
            return Err(error);
        }
        let telemetry_timer = effect_handler
            .start_periodic_telemetry(Duration::from_secs(1))
            .await?;

        loop {
            receiver.admit_due(&mut inflight, &cancel);
            let deadline = receiver.next_deadline();
            tokio::select! {
                biased;

                ctrl = ctrl_chan.recv() => {
                    match ctrl {
                        Ok(NodeControlMsg::CollectTelemetry { mut metrics_reporter }) => {
                            for job in &mut receiver.jobs {
                                _ = job.metrics.report(&mut metrics_reporter);
                            }
                        }
                        Ok(NodeControlMsg::MemoryPressureChanged { update }) => {
                            receiver.memory_pressure.apply(update);
                        }
                        Ok(NodeControlMsg::DrainIngress { deadline, .. }) => {
                            _ = telemetry_timer.cancel().await;
                            receiver.shutdown(&mut inflight, &cancel, deadline).await;
                            effect_handler.notify_receiver_drained().await?;
                            let snapshots = receiver.take_terminal_snapshots();
                            return Ok(TerminalState::new(deadline, snapshots));
                        }
                        Ok(NodeControlMsg::Shutdown { deadline, .. }) => {
                            _ = telemetry_timer.cancel().await;
                            receiver.shutdown(&mut inflight, &cancel, deadline).await;
                            let snapshots = receiver.take_terminal_snapshots();
                            return Ok(TerminalState::new(deadline, snapshots));
                        }
                        Err(error) => return Err(EngineError::ChannelRecvError(error)),
                        _ => {}
                    }
                }

                Some(completion) = inflight.next(), if !inflight.is_empty() => {
                    if let Err(error) = receiver.handle_completion(completion, &effect_handler).await {
                        _ = telemetry_timer.cancel().await;
                        receiver.shutdown(
                            &mut inflight,
                            &cancel,
                            StdInstant::now() + Duration::from_secs(5),
                        ).await;
                        return Err(error);
                    }
                }

                _ = time::sleep_until(deadline),
                    if receiver.admission.has_capacity()
                        && !receiver.memory_pressure.should_shed_ingress() => {}
            }
        }
    }
}

impl<A: DriverAdapter> DatabaseReceiver<A> {
    fn take_terminal_snapshots(&mut self) -> Vec<otap_df_telemetry::metrics::MetricSetSnapshot> {
        let mut snapshots = Vec::new();
        for job in &mut self.jobs {
            snapshots.extend(job.metrics.terminal_snapshots());
        }
        snapshots
    }
}

fn database_error(
    effect_handler: &local::EffectHandler<OtapPdata>,
    query_name: &str,
    class: ErrorClass,
    safe_detail: Option<String>,
) -> EngineError {
    EngineError::ReceiverError {
        receiver: effect_handler.receiver_id(),
        kind: receiver_error_kind(class),
        error: format!(
            "database query '{query_name}' failed with {}",
            class.as_str()
        ),
        source_detail: safe_detail.unwrap_or_default(),
    }
}

fn mapping_error(
    effect_handler: &local::EffectHandler<OtapPdata>,
    query_name: &str,
    _error: OtlpMappingError,
) -> EngineError {
    database_error(
        effect_handler,
        query_name,
        ErrorClass::Conversion,
        Some("database row failed the configured OTLP mapping".to_owned()),
    )
}

const fn receiver_error_kind(class: ErrorClass) -> ReceiverErrorKind {
    match class {
        ErrorClass::Configuration | ErrorClass::Authentication => ReceiverErrorKind::Configuration,
        ErrorClass::TransientTransport | ErrorClass::TimeoutCancel | ErrorClass::Query => {
            ReceiverErrorKind::Transport
        }
        ErrorClass::Downstream => ReceiverErrorKind::Transport,
        ErrorClass::Conversion | ErrorClass::Internal => ReceiverErrorKind::Other,
    }
}

fn unix_time_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
        })
}
