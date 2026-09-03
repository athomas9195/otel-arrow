# Oracle receiver single-poller benchmark

This benchmark measures the Oracle composite-watermark receiver with one source,
one receiver node, one engine core, and one page in flight. Oracle runs on a
separate VM so database and receiver CPU usage do not contend.

The receiver sends directly to the in-process performance exporter. This
isolates Oracle polling, value normalization, OTLP encoding, ACK handling, and
checkpoint persistence from network-export and backend limits.

## Benchmark topology

| Azure resource | Hostname | Workload | Private IP | Default size |
| --- | --- | --- | --- | --- |
| `oracle-bench-vm-1` | `oracle-vm-1` | Oracle Enterprise | `10.50.1.4` | `Standard_E8bds_v5` |
| `oracle-bench-vm-2` | `oracle-vm-2` | One-core `df_engine` and orchestrator | `10.50.1.5` | `Standard_E8bds_v5` |

The checked-in
[`oracle-benchmark-vms.arm.json`](oracle-benchmark-vms.arm.json) template
creates both VMs, a virtual network, NAT gateway, SSH public IPs, network
security group, and one 512 GiB Premium SSD v2 data disk per VM.

The template uses static private IPs so `single-poller.yaml` works without local
edits. If the address parameters are overridden, update
`ORACLE_CONNECT_STRING` in that suite.

## Current temporary environment

As of 2026-09-02, a working environment exists in:

```text
Resource group: oracle-benchmark-rg
Region: westus3
Zone: 1
Oracle VM: oracle-bench-vm-1
Receiver VM: oracle-bench-vm-2
```

Query its current public addresses instead of copying them into documentation:

```shell
az vm list-ip-addresses \
  --resource-group oracle-benchmark-rg \
  --query "[].{vm:virtualMachine.name,private:virtualMachine.network.privateIpAddresses[0],public:virtualMachine.network.publicIpAddresses[0].ipAddress}" \
  --output table
```

Current state:

- Both VMs are running and billable.
- Oracle Enterprise is running as container `oracle-ee` on VM 1.
- `OTAP_BENCH.OTAP_ORACLE_EVENTS` contains 10,000,000 indexed rows.
- The read-only principal is `OTAP_RECEIVER`.
- The benchmark image is built on VM 2.
- The first successful benchmark completed without delivery failures.
- The original cloud-init status on these existing VMs remains `error` because
  Azure Linux 4 renamed one Docker package. Docker was repaired manually. New
  deployments should use the corrected checked-in template rather than the
  original supplied file.
- The SSH rule was temporarily widened to `70.37.27.0/24` because a VPN rotated
  among addresses in that range. Narrow it when stable access is available.

Do not expect another tester to have SSH access automatically. Add their public
key to `/home/azureuser/.ssh/authorized_keys` or give them Azure-authorized
access through an approved mechanism. Never share a private SSH key.

## Prerequisites

- An Azure subscription with quota for two `Standard_E8bds_v5` VMs.
- Azure CLI authenticated to the target subscription.
- An Ed25519 SSH key.
- An Oracle account with:
  - the Enterprise Database container terms accepted;
  - an Oracle Container Registry secret key.

Oracle SSO/MFA passwords do not authenticate Docker CLI pulls. Use the registry
secret key as the Docker password.

## 1. Deploy Azure resources

Create an SSH key if necessary:

```powershell
New-Item -ItemType Directory -Force "$HOME\.ssh"
ssh-keygen -t ed25519 -C "<name>" -f "$HOME\.ssh\id_ed25519"
```

Deploy from PowerShell:

```powershell
$resourceGroup = "oracle-benchmark-rg"
$location = "westus3"
$publicIp = (Invoke-RestMethod https://api.ipify.org).Trim()
$sshKey = (Get-Content "$HOME\.ssh\id_ed25519.pub" -Raw).Trim()

az group create `
  --name $resourceGroup `
  --location $location

az deployment group create `
  --resource-group $resourceGroup `
  --template-file ".\oracle-benchmark-vms.arm.json" `
  --parameters `
    sshPublicKey="$sshKey" `
    allowedSshSource="$publicIp/32"
```

The template intentionally:

- uses Azure Linux 4 because Azure Linux 3 is unavailable in `westus3`;
- omits Trusted Launch because this Azure Linux image reports
  `SecurityType: None`;
- installs `moby-engine`, `docker-cli`, `docker-compose`, and `docker-buildx`;
- mounts the data disk at `/data`;
- places Docker and containerd storage under `/data`, avoiding the small OS
  disk;
- configures SELinux labels for both container storage roots;
- adds `azureuser` to the Docker group.

The template passes Azure ARM preflight validation. The Azure Linux 4 package
names and host setup commands were verified on the current VMs, but the complete
corrected cloud-init sequence has not yet been exercised in a newly created
resource group. On the first clean deployment, verify:

```shell
cloud-init status --wait
docker info --format '{{.DockerRootDir}}'
grep '^root = ' /etc/containerd/config.toml
findmnt /data
```

Expected storage roots are `/data/docker` and `/data/containerd`. Record that
clean-deployment result here before treating cloud-init as fully validated.

After first SSH login, reconnect once so Docker group membership applies.

If a VPN changes the observed source address, update
`AllowSshFromApprovedSource` to the narrowest stable CIDR. Do not leave SSH open
to `Any`.

## 2. Start Oracle on VM 1

Connect using the VM 1 public IP from the deployment output:

```shell
ssh -i ~/.ssh/id_ed25519 azureuser@<vm-1-public-ip>
```

Generate passwords that satisfy Oracle and the preparation script:

```shell
openssl rand -hex 24 >~/oracle-system-password
openssl rand -hex 24 >~/oracle-receiver-password
chmod 600 ~/oracle-system-password ~/oracle-receiver-password

export ORACLE_SYS_PASSWORD="$(cat ~/oracle-system-password)"
export ORACLE_RECEIVER_PASSWORD="$(cat ~/oracle-receiver-password)"
```

Do not print, commit, or paste these values into logs or chat.

Sign in to Oracle Container Registry using the Oracle account email and registry
secret key:

```shell
docker login container-registry.oracle.com
docker pull container-registry.oracle.com/database/enterprise:latest
```

Start Oracle:

```shell
sudo mkdir -p /data/oracle
sudo chown -R 54321:54321 /data/oracle

docker run -d \
  --name oracle-ee \
  --hostname oracle-ee \
  --restart unless-stopped \
  --memory=48g \
  --cpus=8 \
  --shm-size=8g \
  -p 1521:1521 \
  -p 5500:5500 \
  -v /data/oracle:/opt/oracle/oradata:Z \
  -e ORACLE_PWD="$ORACLE_SYS_PASSWORD" \
  container-registry.oracle.com/database/enterprise:latest
```

Wait until the logs contain `DATABASE IS READY TO USE!`:

```shell
docker logs -f oracle-ee
```

## 3. Prepare the Oracle backlog

Copy or download `prepare-oracle.sh` onto VM 1, then run:

```shell
export ORACLE_SYS_PASSWORD="$(cat ~/oracle-system-password)"
export ORACLE_RECEIVER_PASSWORD="$(cat ~/oracle-receiver-password)"
export ORACLE_BENCHMARK_ROWS=10000000
export ORACLE_COLLISION_SIZE=100

chmod +x ./prepare-oracle.sh
./prepare-oracle.sh
```

The script recreates:

- owner `OTAP_BENCH`;
- read-only principal `OTAP_RECEIVER`;
- table `OTAP_BENCH.OTAP_ORACLE_EVENTS`;
- ten million deterministic 200-byte payloads;
- composite index `(EVENT_TS, EVENT_ID)`;
- optimizer statistics.

Successful completion prints:

```text
Prepared 10000000 indexed Oracle rows for the OTAP_RECEIVER principal.
```

Running the script again drops and recreates only the benchmark users and data.

## 4. Transfer only the receiver password

The receiver VM needs `oracle-receiver-password`, but not the Oracle SYSTEM
password. Relay it through a trusted workstation or another approved secret
transfer mechanism:

```powershell
$temporarySecret = "$env:TEMP\oracle-receiver-password"

scp -i "$HOME\.ssh\id_ed25519" `
  azureuser@<vm-1-public-ip>:/home/azureuser/oracle-receiver-password `
  $temporarySecret

scp -i "$HOME\.ssh\id_ed25519" `
  $temporarySecret `
  azureuser@<vm-2-public-ip>:/home/azureuser/oracle-receiver-password

Remove-Item $temporarySecret
```

## 5. Prepare VM 2

Connect to VM 2 and verify private connectivity:

```shell
ssh -i ~/.ssh/id_ed25519 azureuser@<vm-2-public-ip>

timeout 5 bash -c '</dev/tcp/10.50.1.4/1521' &&
  echo "Oracle is reachable"
```

Clone the benchmark branch:

```shell
sudo dnf install -y git python3 python3-pip

git clone --single-branch \
  --branch juanjosalco-oracle-single-poller-load-test \
  https://github.com/athomas9195/otel-arrow.git

cd ~/otel-arrow
```

Build the Oracle-enabled receiver image:

```shell
docker build \
  --target oracle-benchmark \
  -f rust/otap-dataflow/Dockerfile.oracle-demo \
  -t df_engine_oracle_benchmark:latest .
```

Prepare credentials, checkpoint storage, permissions, and SELinux labels:

```shell
chmod +x \
  tools/pipeline_perf_test/test_suites/integration/oracle/prepare-runner.sh

tools/pipeline_perf_test/test_suites/integration/oracle/prepare-runner.sh \
  ~/oracle-receiver-password
```

The runner script prevents the `Permission denied` failure previously seen when
the container tried to read `/app/config.yaml` from an SELinux-protected home
directory.

## 6. Run the benchmark

```shell
cd ~/otel-arrow/tools/pipeline_perf_test

python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
pip install -r orchestrator/requirements.txt

python ./orchestrator/run_orchestrator.py \
  --debug \
  --docker.no-build \
  --config ./test_suites/integration/oracle/single-poller.yaml
```

Each run:

1. verifies mounted credentials;
2. removes only benchmark checkpoint revisions;
3. renders the receiver configuration;
4. starts one receiver node on engine core 0 and Docker CPU 0;
5. warms up for 10 seconds;
6. measures for 60 seconds;
7. drains and stops the engine;
8. writes a console and JSON report.

Results are stored under:

```text
results/oracle_single_poller
```

Treat a run as a valid capacity measurement only when:

```text
backlog_sustained = true
empty_polls = 0
query_failures = 0
negative_acknowledgements = 0
replays = 0
checkpoint_failures = 0
```

Run the scenario at least four times, discard the first cache-warming result,
and report the median of the remaining three.

## First successful baseline

The first successful run used commit
`e822a84b2646e2ea6608d675b820683dd6db3916`, 10,000 rows per poll, a 10-second
warmup, and a 60-second observation.

| Measurement | Result |
| --- | ---: |
| Rows sent | 330,000 |
| Rows per second | 5,499.9 |
| Encoded bytes per second | 2,479,864 |
| Polls and batches | 33 |
| Rows per poll and batch | 10,000 |
| Average encoded row | approximately 451 bytes |
| Average container CPU | 0.161 cores |
| Peak container CPU | 0.181 cores |
| Average container memory | 48.4 MiB |
| Peak container memory | 53.9 MiB |

All 33 batches were acknowledged and checkpointed. There were no empty polls,
query failures, NACKs, replays, stale feedback, or checkpoint failures.

Low receiver CPU indicates that this run was not CPU-bound. Oracle execution,
native row fetching, network waits, or checkpoint latency are more likely
constraints. Collect Oracle VM CPU, disk, and network metrics before attributing
the limit to one specific layer.

## 64 KiB byte-throughput profile

The narrow profile and its `prepare-oracle.sh` setup are prerequisites. The
separate `single-poller-wide.yaml` profile tests whether the unchanged receiver
can approach 200 MB/s using 16 `VARCHAR2(4000)` payload columns, or 64,000 source
payload bytes per row.

On VM 1, download or copy `prepare-oracle-wide.sh`, then create the wide table:

```shell
export ORACLE_SYS_PASSWORD="$(cat ~/oracle-system-password)"
export ORACLE_WIDE_BENCHMARK_ROWS=2500000
export ORACLE_WIDE_BATCH_ROWS=10000
export ORACLE_COLLISION_SIZE=100

chmod +x ./prepare-oracle-wide.sh
./prepare-oracle-wide.sh
```

The default load is approximately 160 GB before Oracle row and index overhead.
It is committed in 10,000-row batches and can take substantial time. Confirm
adequate space before starting:

```shell
df -h /data
docker exec oracle-ee df -h /opt/oracle/oradata
```

The default 2.5 million rows provide comfortable headroom for a 200 MB/s
five-minute observation. Use at least four million rows when testing a receiver
already expected to exceed 400 MB/s, subject to available disk space.

On VM 2, no receiver image rebuild is needed. Run:

```shell
cd ~/otel-arrow/tools/pipeline_perf_test
. .venv/bin/activate

python ./orchestrator/run_orchestrator.py \
  --debug \
  --docker.no-build \
  --config ./test_suites/integration/oracle/single-poller-wide.yaml
```

The profile uses 3,000-row pages, 256 MiB byte limits, a 30-second warmup, and a
five-minute observation. Results are written to
`results/oracle_single_poller_wide`.

Use `encoded_bytes_per_second` to evaluate the target:

```text
200 MB/s = 200,000,000 encoded bytes per second
400 MB/s = 400,000,000 encoded bytes per second
```

The run is valid only when `backlog_sustained` is `true`, `empty_polls` is zero,
and every failure, NACK, and replay counter is zero. This measures encoded OTLP
bytes produced by the receiver, not bytes transmitted to a remote backend.

## Troubleshooting lessons

| Symptom | Cause and correction |
| --- | --- |
| Azure Linux 3 image not found | Use the checked-in Azure Linux 4 template. |
| Trusted Launch unsupported | The corrected template omits `securityProfile`. |
| `cloud-init status: error` installing `moby-cli` | The corrected template installs `docker-cli` and the Azure Linux 4 package set. |
| Oracle image pull denied after successful login | Accept Enterprise repository terms and use the registry secret key, not the MFA password. |
| `no space left on device` pulling Oracle | Docker was using the small OS disk; the corrected template stores Docker and containerd under `/data`. |
| `rust/experimental: not found` during image build | Fixed in commit `e822a84b2`; the Dockerfile now copies `rust/contrib`. |
| `/app/config.yaml: Permission denied` | Run `prepare-runner.sh` to apply persistent SELinux labels. |
| SSH port 22 denied | Update the NSG rule to the current narrow source CIDR; account for VPN address rotation. |

## Cleanup is required

The VMs, managed disks, public IPs, and NAT gateway continue generating Azure
charges while the resource group exists. After all benchmark runs and result
downloads are complete, delete the entire benchmark resource group:

```shell
az group delete \
  --name oracle-benchmark-rg \
  --yes
```

Before deleting it, copy reports from VM 2 and confirm no additional testing is
scheduled. Resource-group deletion permanently removes the Oracle database,
benchmark rows, VM-local password files, images, and checkpoints.
