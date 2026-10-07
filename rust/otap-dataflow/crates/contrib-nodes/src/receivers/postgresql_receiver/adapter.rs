// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Capacity-one local proxy. The shared receiver owns polling and feedback.

use super::{
    config::Validated,
    error::{Error, Result},
    worker,
};
use async_trait::async_trait;
use otel_arrow_dfe_scraper::database::{
    ColumnMetadata, CompiledQuery, CompositeCursor, DatabaseSystem, DriverAdapter,
    DriverCancellation, QueryPage,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
};
use tokio::sync::{Notify, mpsc, oneshot};

#[derive(Default)]
pub(crate) struct Operation {
    pub cancelled: AtomicBool,
    pub notify: Notify,
}
impl Operation {
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.notify.notify_one();
    }
    pub fn check(&self) -> Result<()> {
        if self.cancelled.load(Ordering::Acquire) {
            Err(Error::Cancelled)
        } else {
            Ok(())
        }
    }
}

#[derive(Clone)]
pub(crate) struct Cancellation(Arc<Operation>);
#[async_trait(?Send)]
impl DriverCancellation for Cancellation {
    type Error = Error;
    async fn cancel(&self) -> Result<()> {
        self.0.cancel();
        Ok(())
    }
}

pub(crate) enum Action {
    Validate,
    Reconnect,
    Execute(CompositeCursor),
    Shutdown,
}
pub(crate) enum Reply {
    Columns(Vec<ColumnMetadata>),
    Page(QueryPage),
    Done,
}
pub(crate) struct Command {
    pub action: Action,
    pub operation: Arc<Operation>,
    pub reply: oneshot::Sender<Result<Reply>>,
}
pub(crate) struct Adapter {
    commands: mpsc::Sender<Command>,
    operation: Arc<Operation>,
    busy: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
    exit: oneshot::Receiver<Result<()>>,
    exit_result: Option<Result<()>>,
    stopped: bool,
}

impl Adapter {
    pub fn new(validated: Validated) -> Result<Self> {
        let (commands, rx) = mpsc::channel(1);
        let (exit_tx, exit) = oneshot::channel();
        let busy = Arc::new(AtomicBool::new(false));
        let worker_busy = busy.clone();
        let thread = std::thread::Builder::new()
            .name("postgresql-receiver".into())
            .spawn(move || {
                let outcome = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|_| Error::Cleanup)
                    .and_then(|runtime| {
                        tokio::task::LocalSet::new()
                            .block_on(&runtime, worker::run(validated, rx, worker_busy))
                    });
                let _ = exit_tx.send(outcome);
            })
            .map_err(|_| Error::Cleanup)?;
        Ok(Self {
            commands,
            operation: Arc::new(Operation::default()),
            busy,
            thread: Some(thread),
            exit,
            exit_result: None,
            stopped: false,
        })
    }

    async fn request(&mut self, action: Action) -> Result<Reply> {
        if self.busy.swap(true, Ordering::AcqRel) {
            return Err(Error::Cleanup);
        }
        let (reply, result) = oneshot::channel();
        self.commands
            .send(Command {
                action,
                operation: self.operation.clone(),
                reply,
            })
            .await
            .map_err(|_| Error::Cleanup)?;
        result.await.map_err(|_| Error::Cleanup)?
    }
}

#[async_trait(?Send)]
impl DriverAdapter for Adapter {
    type Error = Error;
    type Cancellation = Cancellation;
    fn system(&self) -> DatabaseSystem {
        DatabaseSystem::PostgreSQL
    }
    fn begin_operation(&mut self) -> Result<Cancellation> {
        if self.stopped || self.exit_result.is_some() || self.busy.load(Ordering::Acquire) {
            return Err(Error::Cleanup);
        }
        self.operation = Arc::new(Operation::default());
        Ok(Cancellation(self.operation.clone()))
    }
    fn is_retryable(error: &Error) -> bool {
        matches!(error, Error::Unavailable | Error::Cancelled)
    }
    async fn reconnect(&mut self, _: &CompiledQuery) -> Result<()> {
        match self.request(Action::Reconnect).await? {
            Reply::Done => Ok(()),
            _ => Err(Error::Cleanup),
        }
    }
    async fn validate_query(&mut self, _: &CompiledQuery) -> Result<Vec<ColumnMetadata>> {
        match self.request(Action::Validate).await? {
            Reply::Columns(c) => Ok(c),
            _ => Err(Error::Cleanup),
        }
    }
    async fn execute(&mut self, _: &CompiledQuery, cursor: &CompositeCursor) -> Result<QueryPage> {
        match self.request(Action::Execute(cursor.clone())).await? {
            Reply::Page(p) => Ok(p),
            _ => Err(Error::Cleanup),
        }
    }
    async fn shutdown(&mut self) -> Result<()> {
        if self.stopped {
            return Ok(());
        }
        self.operation.cancel();
        let (reply, _result) = oneshot::channel();
        if self.exit_result.is_none() {
            let _ = self
                .commands
                .send(Command {
                    action: Action::Shutdown,
                    operation: self.operation.clone(),
                    reply,
                })
                .await;
            self.exit_result = Some((&mut self.exit).await.unwrap_or(Err(Error::Cleanup)));
        }
        self.exit_result.ok_or(Error::Cleanup)??;
        // Exit acknowledgment precedes the final thread return by a few instructions.
        while self.thread.as_ref().is_some_and(|t| !t.is_finished()) {
            tokio::task::yield_now().await;
        }
        if let Some(thread) = self.thread.take()
            && thread.join().is_err()
        {
            self.exit_result = Some(Err(Error::Cleanup));
            return Err(Error::Cleanup);
        }
        self.stopped = true;
        Ok(())
    }
}

impl Drop for Adapter {
    fn drop(&mut self) {
        self.operation.cancel();
    }
}

#[cfg(test)]
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
        let mut adapter = Adapter {
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
        let mut adapter = Adapter {
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
