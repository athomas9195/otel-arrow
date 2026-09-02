# Oracle receiver single-poller benchmark

This benchmark measures the Oracle composite-watermark receiver with one source,
one receiver node, one engine core, and one page in flight. Oracle runs on a
separate VM so database and receiver CPU usage do not contend.

The receiver sends directly to the in-process performance exporter. This
isolates polling, Oracle value normalization, OTLP encoding, ACK handling, and
checkpoint persistence from network-export and backend limits.

## Topology

| Host | Workload | Default size |
| --- | --- | --- |
| `oracle-vm-1` | Oracle Enterprise and preloaded source rows | `Standard_E8bds_v5` |
| `oracle-vm-2` | One-core `df_engine` and test orchestrator | `Standard_E8bds_v5` |

The separately supplied `oracle-benchmark-vms.arm.json` template creates both
VMs in the same virtual network. Oracle port 1521 is reachable only inside that
virtual network; SSH is restricted to the supplied source CIDR.

## 1. Deploy the VMs

Oracle Enterprise images require accepting Oracle's license and authenticating
to `container-registry.oracle.com` before pulling the image.

```shell
az deployment group create \
  --resource-group <resource-group> \
  --template-file <path-to-oracle-benchmark-vms.arm.json> \
  --parameters \
    sshPublicKey="$(cat ~/.ssh/id_ed25519.pub)" \
    allowedSshSource="<your-public-ip>/32"
```

Record the two public IPs from the deployment output. The benchmark itself uses
Azure private DNS name `oracle-vm-1` between the VMs.

## 2. Start and prepare Oracle on VM 1

```shell
sudo mkdir -p /data/oracle
sudo chown -R 54321:54321 /data/oracle

docker login container-registry.oracle.com
docker run -d \
  --name oracle-ee \
  --hostname oracle-ee \
  --restart unless-stopped \
  --memory=48g \
  --cpus=8 \
  --shm-size=8g \
  -p 1521:1521 \
  -p 5500:5500 \
  -v /data/oracle:/opt/oracle/oradata \
  -e ORACLE_PWD='<system-password>' \
  container-registry.oracle.com/database/enterprise:latest
```

Wait until `docker logs oracle-ee` reports that the database is ready. Copy
`prepare-oracle.sh` to VM 1, then preload ten million deterministic rows:

```shell
export ORACLE_SYS_PASSWORD='<system-password>'
export ORACLE_RECEIVER_PASSWORD='<receiver-password>'
export ORACLE_BENCHMARK_ROWS=10000000
export ORACLE_COLLISION_SIZE=100
sh ./prepare-oracle.sh
```

The script creates an indexed `OTAP_BENCH.OTAP_ORACLE_EVENTS` table and a
read-only `OTAP_RECEIVER` principal. Use at least enough rows to keep the poller
busy for the 10-second warmup and 60-second observation window.

## 3. Build the benchmark image on VM 2

Install Git, Python 3, pip, and Docker, then clone the branch containing the
Oracle receiver and this suite. From the repository root:

```shell
docker build \
  --target oracle-benchmark \
  -f rust/otap-dataflow/Dockerfile.oracle-demo \
  -t df_engine_oracle_benchmark:latest .
```

The Oracle Instant Client is downloaded while building. Review Oracle's license
before redistributing the resulting image.

## 4. Create receiver credential files on VM 2

From `tools/pipeline_perf_test`:

```shell
mkdir -p test_suites/integration/configs/oracle/secrets
printf '%s' 'OTAP_RECEIVER' \
  >test_suites/integration/configs/oracle/secrets/username
printf '%s' '<receiver-password>' \
  >test_suites/integration/configs/oracle/secrets/password
chmod 600 test_suites/integration/configs/oracle/secrets/*
```

The files and generated checkpoint/configuration files are ignored by Git.

## 5. Run the benchmark

Still from `tools/pipeline_perf_test`:

```shell
python3 -m venv .venv
. .venv/bin/activate
pip install -r orchestrator/requirements.txt

python ./orchestrator/run_orchestrator.py \
  --debug \
  --docker.no-build \
  --config ./test_suites/integration/oracle/single-poller.yaml
```

Each run removes only the local benchmark checkpoint revisions, so it starts at
the initial watermark without reloading Oracle. The suite pins the container to
CPU 0 and starts `df_engine` with `--core-id-range 0-0`.

Results are printed and written under
`results/oracle_single_poller`. The report includes rows and encoded bytes per
second, rows per poll and batch, ACK/checkpoint counts, failure/replay counters,
CPU cores consumed, and memory bytes. Treat a run as a capacity result only when
`backlog_sustained` is `true` and `empty_polls` is zero; otherwise preload more
rows and repeat it.

For comparable runs, keep the VM size, Oracle image, row count, payload width,
poll bounds, warmup, and observation duration fixed. Run the scenario at least
three times after the first image/database warmup and report the median.
