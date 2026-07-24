# Developer Handoff — mongodb-flasharray-backup

You're inheriting this project. This doc is the **lab-setup + requirements** brief: what the tooling does, the
environment it needs (cluster / VMs / storage / Ops Manager / your workstation), and how the pieces fit. Once
your lab matches the requirements here, follow **[GETTING-STARTED.md](GETTING-STARTED.md)** for the click-by-click
install/configure/first-snapshot, and **[docs/AGENT-TESTING.md](docs/AGENT-TESTING.md)** to drive the full test
and certification suites with a Claude agent (the same way the project has been validated so far).

> The GitHub repo is **public** — never commit secrets. All credentials live in `.env` (git-ignored). Hostnames
> like `aen-mongo-01` are lab names, safe to keep; API keys / passwords / tokens are not.

---

## 1. What this is

A Python toolkit that performs **crash-consistent snapshot backup and point-in-time recovery (PITR)** of
**MongoDB 8.0** — both **sharded clusters** and **standalone replica sets** — using **Pure Storage FlashArray +
Fusion** under **Ops Manager (OM) third-party backup**.

The flow: OM opens a `$backupCursor` on the chosen member (the **primary**) of each replica set; while it's held,
a **FlashArray protection-group snapshot** is taken across all arrays; restore overwrites each volume in place
(sub-second copy-on-write) and WiredTiger crash-recovers. PITR adds a continuous **oplog tailer** (via OM's Oplog
Snapshot API) plus a **replay** step. The deep design is in **[docs/how-it-works.md](docs/how-it-works.md)**.

**Scope (what's certified vs. out of scope):** in-place **self-restore** (source == target) for sharded and RS,
full snapshots only (FlashArray snapshots are always full — no incremental chain). Cross-cluster RS→RS restore
is implemented but not live-validated; sharded-to-different and incremental are out of scope. See
**[tests-docs/Certification-Summary.md](tests-docs/Certification-Summary.md)**.

---

## 2. Reference lab architecture (what it was validated on)

Adapt the names to your environment — nothing here is hard-coded except the *shape* requirements in §3–§6.

| Component | Validated lab |
|---|---|
| **Sharded cluster** `aen-cluster` | Dedicated config server `aen-mongo-config-00` (`config`/`aen-shard_0`, :27019) + **3 data shards** `aen-shard_1/2/3` on `aen-mongo-01/02/03` (:27021/:27022/:27023); `mongos` on `aen-mongo-01:27017`. 10 mongod processes total. |
| **Replica set** `aen-rs-00` | 3 members `aen-mongo-05/06/07` (:27017). |
| **Spare node** | `aen-mongo-04` — no cluster role; used as the "extra node" for add-node / failover / gap tests. |
| **Storage** | Each node's `/data/mongo` is **one FlashArray volume** (direct pRDM). Arrays in one **Fusion fleet**: e.g. `sn1-x90r2-f06-27`, `sn1-x90r2-f06-33`, `sn1-x90r2-f07-27`, `sn1-c60-e12-16`. |
| **Protection groups** | One PG per cluster, named `<cluster>-pg`: `aen-cluster-pg`, `aen-rs-00-pg`. Each holds every node's data volume. |
| **Ops Manager** | OM 8.0, one project (group). Both clusters registered for **third-party backup** (`ACTIVE`). |
| **Control machine** | A workstation/VM with Python 3.11+, the OpenSSH client, and SSH key access to every node. This is where the console commands run. |

Both deployments live in **one `.env`** (shared infra + `<NAME>__` per-deployment overrides), selected at runtime
with `--deployment <name>`.

---

## 3. Cluster / VM (node) requirements

Every MongoDB node VM must satisfy:

- [ ] **OS:** Linux with `systemd` (validated on RHEL/Rocky 9). `python3` present (needed on nodes for oplog
      decode) and **`libsnappy.so`** installed (comes in as a dependency of the OM automation agent RPM; verify
      `rpm -q snappy`).
- [ ] **MongoDB 8.0**, managed by the **Ops Manager automation agent** (`mongodb-mms-automation-agent`).
- [ ] **`/data/mongo` on a FlashArray volume.** A **single direct pRDM** is the validated layout. LVM-over-multipath
      is supported in code but **not yet live-validated** — if you use it, see the hard requirement below.
- [ ] **HARD REQUIREMENT — one array per node.** All volumes backing a single node must be on the **same**
      FlashArray. FA snapshots are crash-consistent only *per array*; a node whose data spans two arrays cannot be
      snapshotted consistently. Single-volume nodes satisfy this automatically; for LVM, keep every PV of the VG on
      one array.
- [ ] **`mongosh` present** at a known path (all nodes use the same image → same path). The lab path is
      `/var/lib/mongodb-mms-automation/mongosh-linux-x86_64-<ver>/bin/mongosh`. This becomes `MONGOSH_PATH`.
- [ ] **SSH user (`SSH_USER`) in the `mongod` group** on **every** node — including any node added later — so the
      PITR tailer can `scp` the agent-written `640 mongod:mongod` `.oplogs` files. `ssh <SSH_USER>@<node> id`
      must list `mongod`; if not, `sudo usermod -aG mongod <SSH_USER>`.
- [ ] **Firewall:** each mongod/mongos port reachable cluster-wide (`27017`, and shard ports `27021–27023`,
      config `27019`). A member added on a new port needs that port opened
      (`firewall-cmd --add-port=<p>/tcp --permanent && firewall-cmd --reload`).
- [ ] **Passwordless `sudo` for `SSH_USER`** — **only `restore` needs it** (it stops agents, unmounts
      `/data/mongo`, rescans the LUN, remounts). Snapshot / PITR / init-pg use no sudo. Grant blanket
      `NOPASSWD: ALL` or the scoped `Cmnd_Alias` in
      [GETTING-STARTED.md → Sudo access](GETTING-STARTED.md#sudo-access-on-the-cluster-nodes-restore-only).

---

## 4. Storage (FlashArray + Fusion) requirements

- [ ] Every array holding a node's data volume is enrolled in **one Fusion fleet** (the tool discovers fleet
      members and routes tag/object calls through the gateway).
- [ ] **One protection group per cluster**, named `<cluster>-pg`, containing every node's data volume. Created /
      maintained by **`initialize-protection-groups`** (re-run it after any topology change — it also refreshes the
      copyable `mongo:` volume-map tags the hot path reads).
- [ ] **Credentials:** a FlashArray **directory account** (`FA_USERNAME`/`FA_PASSWORD`) that authorizes on **every**
      fleet member (preferred), or a single-array API token (`FA_APITOKEN`). Plus the **gateway endpoint**
      (`FA_ENDPOINT`) and `FA_API_VERSION`.

---

## 5. Ops Manager requirements

- [ ] **Ops Manager 8.0**, cluster(s) monitored + automated.
- [ ] **Third-party backup enabled** and each cluster **`ACTIVE`**. For a **replica set**, register via the
      third-party `…/clusters/{id}/manage` endpoint — **no OM snapshot store needed**; the managed-backup path
      (`backupConfigs statusName=STARTED`) is wrong here and 409s "no available Snapshot Store". See
      [docs/third-party-backup-reference.md](docs/third-party-backup-reference.md).
- [ ] **An OM API key** (public/private) with role **`GLOBAL_BACKUP_ADMIN`**, and the control machine's IP on the
      key's access list. These become `OM_PUBLIC_KEY` / `OM_PRIVATE_KEY`.
- [ ] Know your **group (project) id** and each **cluster id** → `OM_GROUP_ID`, `OM_CLUSTER_ID` (per deployment),
      plus `OM_BASE_URL` / `OM_API_VERSION`.

> **OM is the fragile dependency.** Topology changes (shard add/remove, config-server conversion) can *wedge*
> third-party backup, and the app server can silently stop. Recovery procedures (force-unmanage → `mongodb-mms`
> restart → `manage`; skip-forward oplog re-baseline) are in the runbook —
> [tests-docs/Certification-Summary.md](tests-docs/Certification-Summary.md) → "Operational runbook" — and
> [docs/LESSONS.md](docs/LESSONS.md).

---

## 6. Control machine (where you run the tools) requirements

- [ ] **Python 3.11+** and the **OpenSSH client** (`ssh`/`scp`).
- [ ] **SSH key-based auth** to every node as `SSH_USER` (no password prompts): `ssh <SSH_USER>@<node> true`.
- [ ] Network reach to **Ops Manager** (`OM_BASE_URL`) and the **FlashArray gateway** (`FA_ENDPOINT`).
- [ ] Clone + install (editable) into a venv — see [GETTING-STARTED.md → Install](GETTING-STARTED.md#1-install).

---

## 7. Command inventory

Installed as console scripts (`pip install -e .`). All accept `--deployment <name>` on a multi-deployment `.env`.

| Command | Purpose | Sudo on nodes? |
|---|---|---|
| `initialize-protection-groups` | Create/maintain the FA PG + write `mongo:` volume-map tags. Run after any topology change. | no |
| `new-mongo-snapshot` | Take a crash-consistent snapshot (opens `$backupCursor` on the primary; **stops the balancer** for sharded). | no |
| `restore-mongo-snapshot` | In-place self-restore (CoW overwrite → WT recovery → cluster re-forms). | **yes** |
| `restore-mongo-snapshot-to-target` | RS → *different* RS restore (seed + initial-sync). Cert 1.A.1.b; not live-validated. | yes (more) |
| `start-oplog-tailer` / `stop-oplog-tailer` | Continuous oplog capture for PITR / stop + write the T2 mark. | no |
| `invoke-oplog-replay` | Replay captured oplog forward to a target timestamp (0 = all). | no |
| `remove-old-artifacts` | Retention cleanup of old snapshots. | no |
| `initialize-test-data` / `start-insert-load` | Seed / continuously insert test data (`testdb.loadtest`/`payload`). | no |
| `run-all-tests` | Scripted Test 1–3 driver. | yes (restore) |

---

## 8. Documentation map

| Doc | What it's for |
|---|---|
| [README.md](README.md) | Feature overview, quick start, command/flag reference. |
| **HANDOFF.md** (this) | Lab requirements + architecture for standing it up fresh. |
| [GETTING-STARTED.md](GETTING-STARTED.md) | Step-by-step install → configure → first snapshot/restore/PITR, sudo scoping. |
| [docs/AGENT-TESTING.md](docs/AGENT-TESTING.md) | Drive the full test + certification suites with a Claude agent. |
| [docs/how-it-works.md](docs/how-it-works.md) | Deep design: backup cursor internals, whole-cluster revert, PITR, balancer, failure modes. |
| [docs/third-party-backup-reference.md](docs/third-party-backup-reference.md) | Enabling/operating OM third-party backup. |
| [docs/LESSONS.md](docs/LESSONS.md) | Hard-won operational lessons (recurring footguns). |
| [tests-docs/Certification-Summary.md](tests-docs/Certification-Summary.md) | Cert verdict, results table, **operational runbook** (field gotchas). |
| [tests-docs/Test-CertificationChecklist.md](tests-docs/Test-CertificationChecklist.md) | MongoDB 3rd-party-backup checklist mapped item-by-item. |
| [tests-docs/Test-SnapshotRestore.md](tests-docs/Test-SnapshotRestore.md) | The runnable test procedures (Tests 1–8), with verified results. |
| [demo/](demo/) | Standalone demo scripts (e.g. `rs-restore-demo.sh`). |

---

## 9. Handoff acceptance checklist

You're up and running when all of these pass:

- [ ] `new-mongo-snapshot --help` prints usage (install OK).
- [ ] `.env` filled; `ssh <SSH_USER>@<each-node> sudo -n true` succeeds; `id` lists `mongod`.
- [ ] OM reachable and both clusters report third-party backup **`ACTIVE`**; FA gateway reachable.
- [ ] `initialize-protection-groups --deployment <name>` succeeds and the PG lists every node's volume.
- [ ] A **snapshot → mutate → restore** cycle passes with **drift 0** and the sentinel gone
      (Test 4 sharded / Test 6 RS — or just run `demo/rs-restore-demo.sh`).
- [ ] A **PITR cycle** (`start-oplog-tailer` → snapshot → writes → drain → stop → restore → replay) reaches
      **`unrecoveredTail=0`** (Test 5 sharded / Test 7 RS).

For the automated way to prove all of the above — including the full certification matrix — hand
[docs/AGENT-TESTING.md](docs/AGENT-TESTING.md) to a Claude agent.
