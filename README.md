# MongoDB Snapshot & PITR — Pure Storage FlashArray + Fusion + Ops Manager (Python)

Crash-consistent snapshot backup and point-in-time recovery of a MongoDB 8.0 Enterprise **sharded cluster or
standalone replica set** using Pure Storage FlashArray, **Pure Storage Fusion**, and **MongoDB Ops Manager 8.0**.
(Python implementation of [`nocentino/mongodb-flasharray-backup`](https://github.com/nocentino/mongodb-flasharray-backup).)

A single `.env` can describe **multiple deployments** (e.g. a sharded cluster *and* a standalone replica set);
pick one per run with `--deployment <name>`. See [Configure](#configure).

Each MongoDB node's data volume lives on a FlashArray, and the arrays are enrolled in a Pure Storage Fusion
fleet. The toolkit talks to the arrays over the **FlashArray REST API**, connecting **directly to each fleet
member** (`fa_rest.py`) and authenticating fleet-wide with a directory account (`FA_USERNAME`/`FA_PASSWORD`) so
every array is reachable. `new-mongo-snapshot` uses the Ops Manager Third-Party Backup API to open a
`$backupCursor` on one snapshotable secondary per replica set — each shard's RS for a sharded cluster, or the
single RS for a standalone replica set (pinning the WiredTiger checkpoint and freezing journal cleanup);
while the cursor is open, a FlashArray protection-group snapshot is taken on every array in a coordinated
crash-consistent sweep — no `fsyncLock`, no write stall. `restore-mongo-snapshot` stops agents, unmounts,
overwrites each volume in-place from the snapshot (a sub-second CoW pointer swap), remounts, and restarts
agents — WiredTiger handles crash recovery automatically. For PITR, `start-oplog-tailer` continuously captures
oplog `.oplogs` segments via the Ops Manager Oplog Snapshot API, and `invoke-oplog-replay` replays them to a
target timestamp after a restore. See [docs/how-it-works.md](docs/how-it-works.md) for the recoverability deep
dive.

---

## Quick start

New here? The full step-by-step walkthrough is in **[GETTING-STARTED.md](GETTING-STARTED.md)**. The short
version, once the [prerequisites](GETTING-STARTED.md#0-before-you-begin--prerequisites) are in place:

```bash
git clone https://github.com/ScottyNVME/mongodb-flasharray-backup-python.git
cd mongodb-flasharray-backup-python
python3 -m venv .venv && source .venv/bin/activate && pip install -e .

cp .env.example .env                      # fill in FlashArray + Ops Manager + SSH details (see the guide)
initialize-protection-groups --what-if    # sanity-check connectivity...
initialize-protection-groups              # ...then apply (one-time)

new-mongo-snapshot                         # prints a tag, e.g. om-20260512-143022
restore-mongo-snapshot --snapshot-tag om-20260512-143022 --force
```

Append `--deployment <name>` to target a non-default deployment. For point-in-time recovery, retention, and
troubleshooting, follow **[GETTING-STARTED.md](GETTING-STARTED.md)**.

## Install

Requires **Python 3.11+** and the system OpenSSH client (`ssh`/`scp`) configured for key-based auth to every
cluster node.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

This installs the dependencies (`python-dotenv`, `requests`, `typer`) and the eleven console scripts listed below.

> **SSH multiplexing.** All remote commands shell out to the system `ssh`/`scp` with ControlMaster options
> (`ControlPath=/tmp/ssh-mux-%C`, `ControlPersist=60s`) to avoid saturating `sshd`'s `MaxStartups`. No Python
> SSH library is used.

## Configure

```bash
cp .env.example .env
# Edit .env and fill in the values described below.
```

FlashArray authentication (`fa_rest.py`) supports two modes:

- **`FA_USERNAME` + `FA_PASSWORD`** — directory login; authorizes on **every** fleet member. Required for
  fleet-wide snapshot/restore. *(Preferred.)*
- **`FA_APITOKEN`** — a FlashArray API token; only valid on the array that issued it (single-array use, falls
  back to this when no password is set).

Keys: `FA_ENDPOINT`, `FA_USERNAME`, `FA_API_VERSION`, `FA_PROTECTION_GROUP`, `FA_CLUSTER_NAME`, plus
`FA_PASSWORD` and/or `FA_APITOKEN`; `OM_BASE_URL`, `OM_API_VERSION`, `OM_GROUP_ID`, `OM_CLUSTER_ID`,
`OM_PUBLIC_KEY`, `OM_PRIVATE_KEY`; `MONGOSH_PATH`, `MONGOS_HOST`, `MONGOS_PORT`, `SSH_USER`,
`MONGO_TOOLS_BASE`; and optional `CLUSTER_NODES` (comma-separated fallback used only when Ops Manager is
unreachable).

The `.env` is located via `$MONGO_FA_BACKUP_ENV`, else python-dotenv's search from the current directory, else
`./.env`. A missing file or required key raises immediately.

### Multiple deployments in one `.env` (sharded + replica set)

A **deployment** is one MongoDB cluster backed by one FlashArray protection group. Shared infrastructure
(FA/OM credentials, `SSH_USER`, tool paths) stays in the flat keys; the **deployment-specific** keys —
`TOPOLOGY`, `OM_CLUSTER_ID`, `FA_PROTECTION_GROUP`, `FA_CLUSTER_NAME`, `MONGOS_HOST`/`MONGOS_PORT`,
`CLUSTER_NODES` — can be overridden per deployment with a **`<NAME>__` prefix** (NAME upper-cased,
hyphens → underscores) and selected with **`--deployment <name>`** on any command. With no `--deployment`, the
flat keys are used as-is (backward compatible with a single-deployment `.env`).

- **`TOPOLOGY`** is `sharded` (topology discovered/validated via `mongos`/`listShards`) or `replicaset` (a
  standalone replica set — no `mongos`; set `MONGOS_HOST` to **any RS member**, where document counts and the
  restore-stabilization probe run).
- Protection groups follow a `<cluster-name>-pg` convention (e.g. `aen-cluster-pg`, `aen-rs-00-pg`).

```bash
new-mongo-snapshot                          # default deployment (flat keys)
new-mongo-snapshot --deployment aen-rs-00   # the AEN_RS_00__* deployment
```

See [.env.example](.env.example) for a worked two-deployment example.

## Prerequisites

- Python 3.11+ and the OpenSSH client; SSH key auth from this machine to every `SSH_USER@<node>` (passwordless `sudo`).
- All FlashArrays enrolled in the same Pure Storage Fusion fleet.
- Ops Manager API user with role `GLOBAL_BACKUP_ADMIN` and a public/private API key (HTTP Digest auth).
- Ops Manager [third-party backup](https://www.mongodb.com/docs/ops-manager/current/core/third-party-backup/)
  enabled and the cluster registered (state = `ACTIVE`). For a **replica-set** deployment, register it via the
  third-party `…/clusters/{id}/manage` endpoint — **no OM snapshot store is required** (the FlashArray holds the
  snapshots). The standard `backupConfigs statusName=STARTED` path is OM-managed backup and will 409
  `Could not find available Snapshot Store`.
- Protection groups initialized on all FlashArrays (`initialize-protection-groups`, run per deployment).
- A data volume per node mounted at `/data/mongo`. A single direct volume (pRDM) is the **live-validated**
  layout; LVM-over-multipath (a VG spanning several FlashArray volumes) is **supported** by discovery, tagging
  and restore but is not yet validated on live hardware.

---

## Commands

Run `<command> --help` for full option details. The deployment-aware commands — `new-mongo-snapshot`,
`restore-mongo-snapshot`, `restore-mongo-snapshot-to-target`, `initialize-protection-groups`,
`start-oplog-tailer`, `stop-oplog-tailer`, and `invoke-oplog-replay` — accept **`--deployment <name>`** to select
a deployment from the `.env` (omit for the default/flat deployment).

| Command | Purpose |
|---|---|
| `initialize-protection-groups` | Create the PG on every fleet array and add data volumes. `--what-if`, `--prune`, `--force`. |
| `new-mongo-snapshot` | Take a crash-consistent FlashArray PG snapshot across all nodes via the backup-cursor window. `--snapshot-tag`, `--baseline-database`, `--baseline-collections`. |
| `restore-mongo-snapshot` | Destructive in-place restore from a snapshot tag. Verifies baseline counts; **sharded restores also verify per-shard data distribution** (each shard's RS counted directly, sums account for the mongos aggregate). `--snapshot-tag` (required), `--force`, `--verify-database`, `--skip-verification`. |
| `restore-mongo-snapshot-to-target` | Cross-cluster restore of a replica-set snapshot to a **different** replica set (seed + initial-sync; seed's volume must be on the same array as the source snapshot). `--snapshot-tag` (required), `--target-nodes`, `--target-rs-name`, `--target-seed`, `--target-member-port`, `--deployment`, `--force`. |
| `remove-old-artifacts` | Retention cleanup of old FA snapshots + local oplog/log dirs. `--older-than-days` (required, 1–365), `--what-if`. |
| `start-oplog-tailer` | Continuously capture oplog `.oplogs` segments for PITR. `--snapshot-tag`, `--interval-sec`, `--timeout-minutes`, `--poll-interval-sec`, `--abort-on-gap`. |
| `stop-oplog-tailer` | Stop the tailer (`.stop` sentinel) and capture the T2 mark. `--snapshot-tag`, `--wait-sec`, `--baseline-database`, `--baseline-collections`. |
| `invoke-oplog-replay` | Replay oplog segments to a target timestamp after a restore. Refuses if oplog gap markers are present (`--allow-gaps` to override). `--snapshot-tag`, `--target-timestamp`, `--verify-database`, `--t2-mark-path`, `--skip-verification`, `--allow-gaps`. |
| `initialize-test-data` | Seed `testdb.loadtest`/`payload` (hashed `_id` sharding). `--loadtest-docs`, `--payload-docs`, `--batch-size`, `--force`. |
| `start-insert-load` | Continuous insert load generator. `--max-docs`, `--batch-size`. |
| `run-all-tests` | 3-phase e2e suite (restore / restore-under-load / PITR). `--start-at-test`, `--stop-after-test`. |

## Backup & recovery workflows

**One-time setup**
```bash
initialize-protection-groups --what-if      # preview
initialize-protection-groups
```

**Take a snapshot**
```bash
new-mongo-snapshot
# Note the tag printed at the end, e.g. om-20260512-143022
```

**Restore (crash-consistent, to the snapshot point)**
```bash
restore-mongo-snapshot --snapshot-tag om-20260512-143022
```

**Point-in-time recovery (snapshot + oplog replay)**
```bash
# 1. Start the tailer alongside your workload, using the snapshot tag you will take
start-oplog-tailer --snapshot-tag om-20260512-143022 &
# 2. Take the snapshot (same tag)
new-mongo-snapshot --snapshot-tag om-20260512-143022
# ... time passes, writes accumulate ...
# 3. Stop the tailer (records the T2 mark)
stop-oplog-tailer --snapshot-tag om-20260512-143022
# 4. Restore the snapshot, then replay oplog to a target Unix timestamp (0 = replay all)
restore-mongo-snapshot --snapshot-tag om-20260512-143022 --force
invoke-oplog-replay --snapshot-tag om-20260512-143022 --target-timestamp 1747058400
```

## Snapshot metadata

Metadata is stored on the FlashArray PG-snapshot **tags**, with JSON string values: `mongo:volumes`,
`mongo:preSnap`, `mongo:postSnap`, `mongo:t1ts` — these are the source of truth for restore and PITR. PITR
stream state lives in JSON files under `~/mongo-oplog-stream/<tag>/` (`state.json`, `t2-mark.json`,
`gap-*.json`) with `.stop`/`.started`/`.stopped` sentinels. Logs are teed to console and to
`~/mongo-{snapshot,restore,oplogtailer,oplogreplay}-logs/`.

## Development & tests

```bash
pip install -e '.[dev]'
pytest -q          # unit tests for config.py + decode_oplogs round-trip
```

The `tests/` directory holds Python unit tests (Digest hashing, locking, parallel runner, `.env` loader,
SCSI-serial selection, oplog decode). The end-to-end orchestration is installed as the `initialize-test-data`,
`start-insert-load`, and `run-all-tests` commands. Manual end-to-end test procedures live in
[tests-docs/](tests-docs/).

**Certification status:** the MongoDB third-party backup certification testing is summarized in
[tests-docs/Certification-Summary.md](tests-docs/Certification-Summary.md) (shareable overview), with the
full item-by-item mapping in [tests-docs/Test-CertificationChecklist.md](tests-docs/Test-CertificationChecklist.md)
and the latest live run in [tests-docs/Test-Certification-Results-2026-07-22.md](tests-docs/Test-Certification-Results-2026-07-22.md).

## Implementation notes

- **FlashArray access** is a direct REST client (`fa_rest.py`): it connects to each fleet member directly, and
  `context_names=[<array>]` selects which array to connect to. A directory login (`FA_USERNAME`/`FA_PASSWORD`)
  authorizes fleet-wide; a configured `FA_APITOKEN` is array-local. Responses are unwrapped centrally — an API
  error becomes an empty result (for best-effort reads) or raises (for required operations).
- **Ops Manager auth** uses a manual RFC 2617 Digest flow over `requests`.
- **Topology and storage mappings are discovered at runtime** — cluster nodes from Ops Manager (with a
  `CLUSTER_NODES` fallback that `initialize-protection-groups` keeps current). The **node→array→volume map is
  precomputed and stored as copyable FlashArray volume tags** by `initialize-protection-groups` (re-run it
  after a topology change); snapshot/restore then read the tags (one `GET /volumes/tags` per array, **no
  per-node SSH**), verify each volume's serial, and fall back to live SCSI/LVM discovery only for an
  untagged/stale node. Handles single-volume **and** multi-volume (LVM-over-multipath) mounts. **Volume
  moves between arrays** are handled by serial — the owning array is derived, never hard-coded: a moved
  volume resolves directly if its tags travelled, self-heals via SSH rediscovery if a tag is lost/stale, and
  a `mongo:pvcount` guard refuses an incomplete multi-volume set — so a move yields a correct result or a
  loud abort, never a silent partial snapshot/restore (see [docs/how-it-works.md](docs/how-it-works.md)).
- **Topology-agnostic by design.** Snapshot node-selection iterates the Ops Manager third-party cluster
  detail's `replicaSets` (not `listShards`), so it works for sharded clusters and standalone replica sets
  alike. The only `mongos`/`listShards` call sites — the restore-stabilization wait and the PIT oplog anchors —
  branch on `TOPOLOGY` (`replicaset` verifies the single RS's writable primary directly); the sharded paths are
  unchanged.
- **Remote execution** shells out to system `ssh`/`scp`; logging is teed to console and file.
