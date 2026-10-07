// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Explicit live integration runner, never a skipped/ignored unit test.
//! Usage: postgresql_e2e <df_engine executable> <native fixture pipeline>.
//! Requires the externally provisioned PostgreSQL fixture and mounted secrets.

use otel_arrow_dfe_pdata::proto::opentelemetry::{
    collector::logs::v1::{
        ExportLogsServiceRequest, ExportLogsServiceResponse,
        logs_service_server::{LogsService, LogsServiceServer},
    },
    common::v1::any_value,
};
use std::{collections::BTreeSet, path::Path, process::Stdio, time::Duration};
use tokio::{
    process::{Child, Command},
    sync::{mpsc, oneshot},
};
use tonic::{Request, Response, Status};

struct Pending {
    request: ExportLogsServiceRequest,
    accepted: oneshot::Sender<bool>,
}
struct Capture(mpsc::Sender<Pending>);

#[tonic::async_trait]
impl LogsService for Capture {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let (accepted, decision) = oneshot::channel();
        self.0
            .send(Pending {
                request: request.into_inner(),
                accepted,
            })
            .await
            .map_err(|_| Status::unavailable("capture stopped"))?;
        match decision.await {
            Ok(true) => Ok(Response::new(ExportLogsServiceResponse {
                partial_success: None,
            })),
            _ => Err(Status::unavailable("capture rejected")),
        }
    }
}

fn launch(engine: &Path, pipeline: &Path) -> Child {
    Command::new(engine)
        .arg("--config")
        .arg(pipeline)
        .arg("--http-admin-bind")
        .arg("127.0.0.1:0")
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("launch df_engine")
}

async fn receive(rx: &mut mpsc::Receiver<Pending>, child: &mut Child) -> Pending {
    tokio::select! {
        result = tokio::time::timeout(Duration::from_secs(90), rx.recv()) =>
            result.expect("no page within 90 seconds").expect("capture closed"),
        _ = child.wait() => panic!("df_engine exited before fixture delivery"),
    }
}

fn inspect(request: &ExportLogsServiceRequest) -> Vec<i64> {
    let mut ids = Vec::new();
    for resource in &request.resource_logs {
        assert!(
            resource
                .resource
                .as_ref()
                .expect("resource")
                .attributes
                .iter()
                .any(|a| a.key == "db.system.name"
                    && matches!(a.value.as_ref().and_then(|v| v.value.as_ref()),
                Some(any_value::Value::StringValue(v)) if v == "postgresql"))
        );
        for scope in &resource.scope_logs {
            for record in &scope.log_records {
                assert_eq!(record.time_unix_nano, 1791158400000000000);
                let Some(any_value::Value::KvlistValue(body)) =
                    record.body.as_ref().and_then(|v| v.value.as_ref())
                else {
                    panic!("typed body");
                };
                let field = |name: &str| {
                    body.values
                        .iter()
                        .find(|v| v.key == name)
                        .and_then(|v| v.value.as_ref())
                        .and_then(|v| v.value.as_ref())
                        .expect("field")
                };
                let any_value::Value::IntValue(id) = field("event_id") else {
                    panic!("integer ID");
                };
                assert!(
                    matches!(field("amount"), any_value::Value::StringValue(v) if v == "9007199254740993.1200")
                );
                assert!(matches!(field("actor"), any_value::Value::StringValue(v) if v == "actor"));
                assert!(
                    matches!(field("payload"), any_value::Value::StringValue(v) if v == "{\"n\": 9007199254740993}")
                );
                ids.push(*id);
            }
        }
    }
    assert!(!ids.is_empty() && ids.len() <= 1000);
    ids
}

/// Scenario: Real PostgreSQL pages traverse df_engine to a sink that withholds ACK, crashes, then accepts replay.
/// Guarantees: Unacknowledged first-page IDs replay exactly; accepted fixture IDs/types/multiplicities are complete.
#[tokio::main]
async fn main() {
    let args: Vec<_> = std::env::args_os().collect();
    assert_eq!(
        args.len(),
        3,
        "expected engine executable and pipeline path"
    );
    let engine = Path::new(&args[1]);
    assert!(engine.is_file(), "build df_engine first");
    let bytes = std::fs::read(&args[2]).expect("read pipeline");
    let mut config: serde_json::Value =
        serde_yaml::from_slice(&bytes).expect("native YAML pipeline");
    let temp = tempfile::tempdir().expect("isolated fixture checkpoint");
    let nodes = &mut config["groups"]["database"]["pipelines"]["pg_fixture"]["nodes"];
    assert_eq!(nodes["pg"]["type"], "urn:otel:receiver:postgresql");
    nodes["pg"]["config"]["checkpoint"]["directory"] = serde_json::json!(temp.path().join("state"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback");
    let address = listener.local_addr().expect("address");
    nodes["capture"]["config"]["grpc_endpoint"] = serde_json::json!(format!("http://{address}"));
    let pipeline = temp.path().join("pipeline.yaml");
    std::fs::write(
        &pipeline,
        serde_yaml::to_string(&config).expect("serialize"),
    )
    .expect("pipeline");
    let (send, mut receive_pages) = mpsc::channel(1);
    let (stop, stopped) = oneshot::channel::<()>();
    let incoming = futures::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(stream, _)| stream), listener))
    });
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(LogsServiceServer::new(Capture(send)))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = stopped.await;
            })
            .await
            .expect("capture server");
    });
    let mut child = launch(engine, &pipeline);
    let first = receive(&mut receive_pages, &mut child).await;
    let expected_replay = inspect(&first.request);
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(
        receive_pages.try_recv().is_err(),
        "prefetched while ACK blocked"
    );
    child.kill().await.expect("crash only the fixture process");
    let _ = child.wait().await.expect("observe process exit");
    let _ = first.accepted.send(false);

    let mut child = launch(engine, &pipeline);
    let replay = receive(&mut receive_pages, &mut child).await;
    assert_eq!(
        inspect(&replay.request),
        expected_replay,
        "unacknowledged replay"
    );
    let mut all = BTreeSet::new();
    let mut pending = replay;
    loop {
        for id in inspect(&pending.request) {
            assert!((1..=2305).contains(&id), "unexpected fixture ID");
            assert!(all.insert(id), "duplicate in accepted pass");
        }
        pending.accepted.send(true).expect("accept page");
        if all.len() == 2305 {
            break;
        }
        pending = receive(&mut receive_pages, &mut child).await;
    }
    assert_eq!(all, (1..=2305).collect());
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        receive_pages.try_recv().is_err(),
        "unexpected additional fixture page"
    );
    child.kill().await.expect("stop fixture process");
    let _ = child.wait().await.expect("observe stop");
    let _ = stop.send(());
    server.await.expect("capture exit");
    otel_arrow_dfe_telemetry::otel_info!("postgresql.integration.complete", rows = 2305u64);
}
