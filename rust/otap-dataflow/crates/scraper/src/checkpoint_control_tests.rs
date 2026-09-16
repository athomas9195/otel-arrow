// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::progress::WriteOutcome;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::receiver::ReceiverWrapper;
use otel_arrow_dfe_engine::testing::{receiver::TestRuntime, test_node};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

// Only test coordination crosses between the local runtime and blocking writer.
#[derive(Clone)]
struct FailingStore {
    delay: Duration,
    attempts: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
}

#[derive(Debug, thiserror::Error)]
#[error("injected checkpoint write failure")]
struct WriteFailure;

impl CheckpointBackend for FailingStore {
    type Error = WriteFailure;

    fn read(&self) -> Result<Option<CheckpointState>, Self::Error> {
        Ok(None)
    }

    fn write(
        &self,
        _: u64,
        _: &CompositeCursor,
    ) -> Result<(CheckpointState, WriteOutcome), Self::Error> {
        _ = self.attempts.fetch_add(1, Ordering::SeqCst);
        std::thread::sleep(self.delay);
        _ = self.completed.fetch_add(1, Ordering::SeqCst);
        Err(WriteFailure)
    }
}

struct Probe {
    store: FailingStore,
    already_draining: bool,
}

#[async_trait(?Send)]
impl local::Receiver<OtapPdata> for Probe {
    async fn start(
        self: Box<Self>,
        mut controls: local::ControlChannel<OtapPdata>,
        effects: local::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let abandoned = Cell::new(false);
        let deadline = self
            .already_draining
            .then(|| Instant::now() + Duration::from_millis(30));
        let outcome = commit_checkpoint(
            &self.store,
            0,
            &CompositeCursor::new("2026-01-01 00:00:00".into(), 1),
            1000,
            Duration::from_millis(1),
            &mut 0,
            "test",
            1,
            &effects,
            &mut None,
            &abandoned,
            &mut controls,
            deadline,
        )
        .await?;
        assert!(matches!(
            outcome,
            CommitOutcome::Stopped(StopRequest::Drain(_))
        ));
        if self.already_draining {
            assert!(!abandoned.get());
            assert!(self.store.attempts.load(Ordering::SeqCst) < 1000);
        } else {
            assert!(abandoned.get());
            // The controller observed drain and returned while fsync-like work
            // was still running, rather than waiting for the blocking write.
            assert_eq!(self.store.completed.load(Ordering::SeqCst), 0);
        }
        Ok(TerminalState::default())
    }
}

fn run_probe(already_draining: bool) {
    let store = FailingStore {
        delay: if already_draining {
            Duration::ZERO
        } else {
            Duration::from_millis(200)
        },
        attempts: Arc::new(AtomicUsize::new(0)),
        completed: Arc::new(AtomicUsize::new(0)),
    };
    let attempts = Arc::clone(&store.attempts);
    let runtime = TestRuntime::<OtapPdata>::new();
    let wrapper = ReceiverWrapper::local(
        Probe {
            store,
            already_draining,
        },
        test_node(runtime.config().name.clone()),
        Arc::new(NodeUserConfig::new_receiver_config(
            "urn:otel:receiver:checkpoint_test",
        )),
        runtime.config(),
    );
    runtime
        .set_receiver(wrapper)
        .run_test(move |ctx| async move {
            if !already_draining {
                while attempts.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
                ctx.send_control_msg(NodeControlMsg::DrainIngress {
                    deadline: Instant::now() + Duration::from_millis(10),
                    reason: "slow checkpoint".to_owned(),
                })
                .await
                .expect("drain");
            }
        })
        .run_validation(|_| async {});
}

/// Scenario: Drain arrives while a checkpoint write is blocked on filesystem work.
/// Guarantees: Control remains responsive and ownership is quarantined when the deadline expires.
#[test]
fn slow_checkpoint_write_remains_drainable() {
    run_probe(false);
}

/// Scenario: Repeated checkpoint failures occur after an earlier drain request.
/// Guarantees: Retries stop at the already-active deadline without requiring another control message.
#[test]
fn checkpoint_retries_honor_existing_drain_deadline() {
    run_probe(true);
}
