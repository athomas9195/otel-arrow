# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

"""Opt-in real PostgreSQL/TLS/OTLP test. Requires Docker and a built df_engine."""

import argparse
import copy
import http.server
import json
import os
from pathlib import Path
import secrets
import struct
import subprocess
import tempfile
import threading
import time


def docker(*args, input_text=None):
    result = subprocess.run(
        ["docker", *args], input=input_text, text=True, capture_output=True, check=False
    )
    if result.returncode:
        raise RuntimeError(f"Docker operation failed: {result.stderr[:2000]}")
    return result.stdout.strip()


def fields(data):
    offset = 0

    def varint():
        nonlocal offset
        value = shift = 0
        while offset < len(data):
            byte = data[offset]
            offset += 1
            value |= (byte & 127) << shift
            if byte < 128:
                return value
            shift += 7
            assert shift < 70
        raise AssertionError("truncated protobuf")

    while offset < len(data):
        tag = varint()
        field, wire = tag >> 3, tag & 7
        if wire == 0:
            value = varint()
        elif wire in (1, 2, 5):
            length = varint() if wire == 2 else (8 if wire == 1 else 4)
            value = data[offset:offset + length]
            assert len(value) == length
            offset += length
        else:
            raise AssertionError(f"unexpected protobuf wire type {wire}")
        yield field, value


def nested(data, field):
    return [value for key, value in fields(data) if key == field]


def ids_from_request(data):
    ids = []
    for resource in nested(data, 1):
        for scope in nested(resource, 2):
            for record in nested(scope, 2):
                body = nested(nested(record, 5)[0], 6)[0]
                columns = {
                    nested(entry, 1)[0].decode(): dict(fields(nested(entry, 2)[0]))
                    for entry in nested(body, 1)
                }
                ident = columns["event_id"][3]
                assert columns["payload"][1].decode() == f"row-{ident}"
                assert columns["amount"][1].decode() == "12345678901234567890.001200"
                assert columns["raw"][7] == bytes([0, 255, 128])
                assert json.loads(columns["document"][1]) == {"id": ident}
                assert columns["event_ts"][1].decode().endswith("Z")
                assert columns["flag"][2] == 1
                assert struct.unpack("<d", columns["score"][4])[0] == 0.125
                assert columns["token"][1].decode() == "00000000-0000-0000-0000-000000000042"
                assert columns["day"][1].decode() == "2026-01-01"
                assert columns["local_ts"][1].decode() == "2026-01-01T02:03:04.123456Z"
                assert columns["duration"][1].decode() == "2 mons 3 days 4000000 microseconds"
                assert columns["optional"] == {}
                ids.append(ident)
    return ids


class Sink(http.server.BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_POST(self):
        try:
            assert self.path == "/v1/logs"
            ids = ids_from_request(self.rfile.read(int(self.headers["Content-Length"])))
            state = self.server.state
            assert 0 < len(ids) <= 25
            state["requests"].append(ids)
            status = state["status"]
            if status != 200:
                time.sleep(0.25)
                assert checkpoint(state["directory"]) == state["before"], "checkpoint advanced before acceptance"
            if status == 503:
                state["status"] = 200
            if status == 200:
                state["accepted"].extend(ids)
            self.send_response(status)
            self.send_header("Content-Type", "application/x-protobuf")
            self.send_header("Content-Length", "0")
            self.end_headers()
        except Exception as error:
            self.server.state["errors"].append(repr(error))
            self.send_error(500)


def checkpoint(directory):
    states = [json.loads(p.read_text())["payload"] for p in directory.rglob("*.json")]
    return max(states, key=lambda value: value["revision"]) if states else None


def run_pipeline(binary, root, config, expected, status=200, terminal=False, error_text=None):
    directory = Path(config["groups"]["default"]["pipelines"]["main"]["nodes"]["pg"]["config"]["checkpoint"]["directory"])
    before = checkpoint(directory)
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Sink)
    server.state = {
        "requests": [], "accepted": [], "errors": [], "status": status,
        "before": before, "directory": directory,
    }
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    local = copy.deepcopy(config)
    local["groups"]["default"]["pipelines"]["main"]["nodes"]["out"]["config"]["endpoint"] = f"http://127.0.0.1:{server.server_port}"
    path = root / "pipeline.json"
    path.write_text(json.dumps(local))
    process = None
    log_path = root / "engine.log"
    try:
        with log_path.open("w") as log:
            process = subprocess.Popen(
                [str(binary), "--config", str(path), "--http-admin-bind", "127.0.0.1:0"],
                stdout=log, stderr=subprocess.STDOUT, cwd=root,
            )
            if terminal:
                assert process.wait(timeout=20) != 0, "expected an explicit failure"
                assert checkpoint(directory) == before, "failure advanced progress"
                if status == 400:
                    assert len(server.state["requests"]) == 1, "permanent NACK replayed"
                else:
                    assert not server.state["requests"], "invalid source emitted data"
                if error_text:
                    assert error_text in log_path.read_text(encoding="utf-8"), "wrong failure category"
            else:
                deadline = time.monotonic() + (3 if not expected else 30)
                while time.monotonic() < deadline:
                    assert process.poll() is None, log_path.read_text(encoding="utf-8")
                    assert not server.state["errors"], server.state["errors"]
                    saved = checkpoint(directory)
                    if expected and server.state["accepted"] == expected and saved and saved["cursor"]["tie_breaker"] == expected[-1]:
                        break
                    time.sleep(0.05)
                assert server.state["accepted"] == expected, server.state
                if not expected:
                    assert checkpoint(directory) == before
                if status == 503:
                    assert server.state["requests"][0] == server.state["requests"][1]
                # Terminate only our child. On Windows this intentionally exercises crash restart.
                process.terminate()
                process.wait(timeout=15)
        assert not server.state["errors"], server.state["errors"]
        print(json.dumps({"accepted": len(expected), "requests": len(server.state["requests"]),
                          "terminal": terminal, "status": status}), flush=True)
        return server.state
    except Exception:
        print(log_path.read_text(encoding="utf-8").encode("ascii", "backslashreplace").decode("ascii"), flush=True)
        raise
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait()
        server.shutdown()
        server.server_close()


# Scenario: A real PostgreSQL 15 source is collected over verified TLS into an OTLP endpoint.
# Guarantees: Ties/page bounds, typed values, ACK checkpointing, NACK replay, restart, terminal
# rejection, hostname/password failures, nullable cursors, timeouts and row-byte limits are exercised.
def test_postgresql_pipeline(binary, image):
    name = "otel-pg-e2e-" + secrets.token_hex(5)
    created = False
    with tempfile.TemporaryDirectory(prefix="otel-pg-e2e-") as tmp:
        root = Path(tmp)
        admin = secrets.token_hex(24)
        password = secrets.token_hex(24)
        (root / "admin").write_text(admin)
        (root / "username").write_text("collector")
        (root / "password").write_text(password)
        for item in ("admin", "username", "password"):
            os.chmod(root / item, 0o600)
        setup = """
set -eu
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 -subj /CN=OTel-test-CA -keyout /tmp/ca.key -out /tmp/ca.crt 2>/dev/null
openssl req -newkey rsa:2048 -nodes -subj /CN=localhost -keyout /tmp/server.key -out /tmp/server.csr 2>/dev/null
printf 'subjectAltName=DNS:localhost\\nbasicConstraints=CA:FALSE\\nextendedKeyUsage=serverAuth\\n' > /tmp/cert.ext
openssl x509 -req -in /tmp/server.csr -CA /tmp/ca.crt -CAkey /tmp/ca.key -CAcreateserial -days 1 -sha256 -extfile /tmp/cert.ext -out /tmp/server.crt 2>/dev/null
chown postgres:postgres /tmp/server.key /tmp/server.crt
chmod 600 /tmp/server.key
exec docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/tmp/server.crt -c ssl_key_file=/tmp/server.key
"""
        try:
            docker("run", "-d", "--name", name, "--publish", "127.0.0.1::5432",
                   "--mount", f"type=bind,source={root},target=/run/e2e,readonly",
                   "-e", "POSTGRES_PASSWORD_FILE=/run/e2e/admin", "-e", "POSTGRES_DB=events",
                   "--entrypoint", "sh", image, "-c", setup)
            created = True
            for _ in range(120):
                probe = subprocess.run(["docker", "exec", name, "pg_isready", "-U", "postgres", "-d", "events"],
                                       capture_output=True, check=False)
                if probe.returncode == 0:
                    break
                time.sleep(0.5)
            else:
                raise RuntimeError("PostgreSQL did not become ready")
            port = int(docker("port", name, "5432/tcp").rsplit(":", 1)[1])
            docker("cp", f"{name}:/tmp/ca.crt", str(root / "ca.pem"))

            def sql(text):
                docker("exec", "-i", name, "psql", "-U", "postgres", "-d", "events",
                       "-v", "ON_ERROR_STOP=1", input_text=text)

            sql(f"""
CREATE ROLE collector LOGIN PASSWORD '{password}';
CREATE TABLE events (
 event_ts timestamptz NOT NULL, event_id bigint NOT NULL, payload text NOT NULL,
 amount numeric(30,6), raw bytea, document jsonb,
 flag boolean DEFAULT true, score double precision DEFAULT 0.125,
 token uuid DEFAULT '00000000-0000-0000-0000-000000000042',
 day date DEFAULT '2026-01-01', local_ts timestamp DEFAULT '2026-01-01 02:03:04.123456',
 duration interval DEFAULT '2 months 3 days 4 seconds', optional text);
CREATE INDEX ON events(event_ts,event_id);
INSERT INTO events(event_ts,event_id,payload,amount,raw,document)
SELECT '2026-01-01T00:00:00Z'::timestamptz + (i/40)*interval '1 second',
 i, 'row-'||i, 12345678901234567890.001200, decode('00ff80','hex'), jsonb_build_object('id',i)
 FROM generate_series(1,65) i;
GRANT USAGE ON SCHEMA public TO collector;
GRANT SELECT ON events TO collector;
""")
            receiver = {
                "source_id": "e2e", "connection": {"host": "localhost", "port": port, "database": "events",
                                                    "tls": {"ca_file": str(root / "ca.pem")}},
                "authentication": {"username_file": str(root / "username"), "password_file": str(root / "password")},
                "query": {"statement": "SELECT event_ts, event_id, payload, amount, raw, document, "
                          "flag, score, token, day, local_ts, duration, optional FROM events "
                          "WHERE (event_ts > $1 OR (event_ts = $1 AND event_id > $2)) ORDER BY event_ts, event_id",
                          "fetch_size_rows": 7, "max_rows_per_poll": 25},
                "watermark": {"mode": "composite", "timestamp": {"column": "event_ts", "bind": "last_timestamp",
                              "initial": "2026-01-01T00:00:00Z", "timezone": "UTC"},
                              "tie_breaker": {"column": "event_id", "bind": "last_id", "initial": 0}},
                "checkpoint": {"directory": str(root / "checkpoints"), "on_nack": "rewind",
                               "nack_backoff": "100ms", "max_consecutive_failures": 3},
            }
            config = {"version": "otel_dataflow/v1", "engine": {},
                      "policies": {"resources": {"core_allocation": {"type": "core_count", "count": 1}},
                                   "runtime_recovery": {"enabled": False}},
                      "groups": {"default": {"pipelines": {"main": {"nodes": {
                          "pg": {"type": "urn:otel:receiver:postgresql", "config": receiver},
                          "out": {"type": "exporter:otlp_http", "config": {
                              "endpoint": "http://127.0.0.1:1", "client_pool_size": 1, "http": {}}}},
                          "connections": [{"from": "pg", "to": "out"}]}}}}}
            run_pipeline(binary, root, config, list(range(1, 66)), status=503)
            run_pipeline(binary, root, config, [])
            sql("""INSERT INTO events(event_ts,event_id,payload,amount,raw,document)
                SELECT '2026-01-01T00:00:02Z',i,'row-'||i,
                12345678901234567890.001200,decode('00ff80','hex'),jsonb_build_object('id',i)
                FROM generate_series(66,70) i;""")
            run_pipeline(binary, root, config, [], status=400, terminal=True, error_text="permanently rejected")
            run_pipeline(binary, root, config, list(range(66, 71)))
            state_directory = receiver["checkpoint"]["directory"]
            receiver["checkpoint"]["directory"] = str(root / "byte-prefix-state")
            receiver["query"]["max_batch_bytes"] = 6000
            bounded = run_pipeline(binary, root, config, list(range(1, 71)))
            assert len(bounded["requests"]) > 3, "byte bound must split pages before row limit"
            receiver["query"].pop("max_batch_bytes")
            sql("""CREATE TABLE details AS SELECT event_id,payload FROM events;
                GRANT SELECT ON details TO collector;""")
            original = receiver["query"]["statement"]
            receiver["query"]["statement"] = (
                "SELECT e.event_ts, e.event_id, d.payload, e.amount, e.raw, e.document, "
                "e.flag, e.score, e.token, e.day, e.local_ts, e.duration, e.optional "
                "FROM events e INNER JOIN details d ON d.event_id = e.event_id "
                "WHERE (e.event_ts > $1 OR (e.event_ts = $1 AND e.event_id > $2)) "
                "AND d.event_id > 0 ORDER BY e.event_ts, e.event_id")
            receiver["checkpoint"]["directory"] = str(root / "join-state")
            run_pipeline(binary, root, config, list(range(1, 71)))
            receiver["query"]["statement"] = original
            receiver["checkpoint"]["directory"] = str(root / "wrong-host-state")
            receiver["connection"]["host"] = "127.0.0.1"
            run_pipeline(binary, root, config, [], terminal=True, error_text="server verification")
            receiver["connection"]["host"] = "localhost"
            receiver["checkpoint"]["directory"] = state_directory
            docker("exec", name, "openssl", "req", "-x509", "-newkey", "rsa:2048",
                   "-nodes", "-sha256", "-days", "1", "-subj", "/CN=Untrusted-test-CA",
                   "-keyout", "/tmp/untrusted.key", "-out", "/tmp/untrusted.crt")
            docker("cp", f"{name}:/tmp/untrusted.crt", str(root / "untrusted.pem"))
            receiver["connection"]["tls"]["ca_file"] = str(root / "untrusted.pem")
            run_pipeline(binary, root, config, [], terminal=True, error_text="server verification")
            receiver["connection"]["tls"]["ca_file"] = str(root / "ca.pem")
            (root / "password").write_text("wrong-password")
            run_pipeline(binary, root, config, [], terminal=True, error_text="authentication")
            (root / "password").write_text(password)
            # Each failure below uses an isolated checkpoint identity.
            receiver["checkpoint"]["directory"] = str(root / "timeout-state")
            original = receiver["query"]["statement"]
            receiver["query"]["statement"] = original.replace("amount, raw, document", "amount, raw, document, pg_sleep(2)::text AS slow")
            receiver["query"]["timeout"] = "1s"
            run_pipeline(binary, root, config, [], terminal=True, error_text="query.timeout")
            receiver["query"]["statement"] = original
            receiver["query"]["timeout"] = "30s"
            receiver["query"]["max_batch_bytes"] = 64
            run_pipeline(binary, root, config, [], terminal=True, error_text="max_batch_bytes")
            receiver["query"].pop("max_batch_bytes")
            receiver["query"]["statement"] = original.replace("amount, raw, document", "amount, raw, document, ARRAY[1] AS unsupported")
            run_pipeline(binary, root, config, [], terminal=True, error_text="unsupported type")
            receiver["query"]["statement"] = original
            sql("ALTER TABLE events ALTER COLUMN event_ts DROP NOT NULL;")
            run_pipeline(binary, root, config, [], terminal=True, error_text="non-null")
            print("PASS: PostgreSQL TLS, delivery, replay, checkpoint/restart and failure-path E2E", flush=True)
        finally:
            if created:
                docker("rm", "-f", "-v", name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--image", default="postgres:15-bookworm")
    args = parser.parse_args()
    test_postgresql_pipeline(args.binary.resolve(strict=True), args.image)
