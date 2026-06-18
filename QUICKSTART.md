# Quick Start

A step-by-step guide to set up **mongodb-flasharray-backup**, take a crash-consistent snapshot of a
MongoDB **sharded cluster or standalone replica set** on Pure Storage FlashArray, and restore it. New to the
project? Start here, then see [README.md](README.md) for full command reference and
[docs/how-it-works.md](docs/how-it-works.md) for the recoverability deep dive.

> **The 30-second mental model:** Ops Manager opens a `$backupCursor` on one snapshotable secondary per replica
> set (each shard, or the single RS — pinning a consistent point), and while it's held, the tool takes a
> FlashArray protection-group snapshot on every array at once. Restore overwrites each node's volume from that
> snapshot in place. No `fsyncLock`, no write stall.

---

## 0. Before you begin — prerequisites

You need all of these in place first (one-time, usually by an admin):

- [ ] **A MongoDB 8.0 sharded cluster *or* standalone replica set** managed by **Ops Manager 8.0**, with
      `/data/mongo` on a **FlashArray** volume (single direct pRDM is the validated layout; LVM-over-multipath
      is supported but not yet live-validated) — every array enrolled in the same **Pure Storage Fusion fleet**.
- [ ] **Ops Manager third-party backup enabled** and the cluster **registered / `ACTIVE`**
      (see [docs/third-party-backup-reference.md](docs/third-party-backup-reference.md), Steps 1–7). *For a
      replica set, register it via the third-party `…/clusters/{id}/manage` endpoint — no OM snapshot store is
      needed; the standard `backupConfigs statusName=STARTED` path 409s `Could not find available Snapshot Store`.*
- [ ] **SSH key-based auth** from the machine you run this on to **every cluster node** as one user
      (`SSH_USER`), with **passwordless `sudo`**. Test: `ssh <SSH_USER>@<node> sudo -n true`.
- [ ] **An Ops Manager API key** (public/private) with role **`GLOBAL_BACKUP_ADMIN`**, and your IP on its
      access list.
- [ ] **FlashArray credentials** — a directory account (`FA_USERNAME`/`FA_PASSWORD`) that authorizes on
      **every** fleet member (preferred), or a single-array API token (`FA_APITOKEN`).
- [ ] **Python 3.11+** and the OpenSSH client (`ssh`/`scp`) on the machine you run from.

---

## 1. Install

```bash
git clone <this-repo> && cd mongodb-flasharray-backup
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

This installs the dependencies and the console commands (`new-mongo-snapshot`, `restore-mongo-snapshot`, …).
Verify: `new-mongo-snapshot --help` should print usage.

---

## 2. Configure `.env`

```bash
cp .env.example .env
# then edit .env
```

Fill in (the tool finds `.env` via `$MONGO_FA_BACKUP_ENV`, else the current directory):

| Key | What it is / where to get it |
|---|---|
| `FA_ENDPOINT` | The **gateway** FlashArray FQDN that can reach all fleet members (e.g. `sn1-x90r2-f06-27.example.com`). |
| `FA_USERNAME` + `FA_PASSWORD` | Directory login that authorizes fleet-wide *(preferred)*. **Or** set `FA_APITOKEN` for single-array use. |
| `FA_API_VERSION` | FlashArray REST API version (e.g. `2.51`). |
| `TOPOLOGY` | `sharded` (default) or `replicaset` — see the multi-deployment note below. |
| `FA_PROTECTION_GROUP` | Protection group name; convention `<cluster-name>-pg` (e.g. `aen-cluster-pg`). |
| `FA_CLUSTER_NAME` | Logical name for your cluster (used in tags/labels). |
| `OM_BASE_URL` | Ops Manager URL, e.g. `http://opsmgr.example.com:8080`. |
| `OM_API_VERSION` | OM public API version (e.g. `v1.0`). |
| `OM_GROUP_ID` | OM **Project** ID — Project Settings, or the `/groups/<id>/` segment in the OM URL. |
| `OM_CLUSTER_ID` | The OM **cluster** ID (sharded cluster *or* replica set) — from `GET /groups/{groupId}/clusters`. |
| `OM_PUBLIC_KEY` / `OM_PRIVATE_KEY` | The OM API key pair from step 0. |
| `MONGOSH_PATH` | Path to `mongosh` on the cluster nodes (the OM-managed agent ships one, e.g. `/var/lib/mongodb-mms-automation/mongosh-*/bin/mongosh`). |
| `MONGOS_HOST` / `MONGOS_PORT` | **Sharded:** a `mongos` router (e.g. `aen-mongo-01` / `27017`). **Replica set:** any RS member (counts/probes run there). |
| `SSH_USER` | The SSH user with passwordless sudo on every node. |
| `MONGO_TOOLS_BASE` | Path to the MongoDB Database Tools (`mongodump`/`mongorestore`/decoder) on the nodes. |
| `CLUSTER_NODES` *(optional)* | Comma-separated node hostnames — a fallback used only if Ops Manager is unreachable. `initialize-protection-groups` refreshes it from OM on each run. |

> Keep `.env` out of git (it holds secrets) — it's already in `.gitignore`.

**Multiple deployments (sharded + replica set) in one `.env`.** Shared infra stays in the flat keys above;
deployment-specific keys can be overridden per deployment with a `<NAME>__` prefix and selected with
`--deployment <name>` on any command (omit for the default/flat deployment). Set `TOPOLOGY=replicaset` for a
standalone replica set (no `mongos`; point `MONGOS_HOST` at any RS member). Example:

```
# default (flat) deployment = sharded
TOPOLOGY=sharded
FA_PROTECTION_GROUP=aen-cluster-pg
OM_CLUSTER_ID=<sharded-cluster-id>
MONGOS_HOST=aen-mongo-01
# second deployment, used with: --deployment aen-rs-00
AEN_RS_00__TOPOLOGY=replicaset
AEN_RS_00__FA_PROTECTION_GROUP=aen-rs-00-pg
AEN_RS_00__OM_CLUSTER_ID=<rs-cluster-id>
AEN_RS_00__MONGOS_HOST=aen-mongo-05.fsa.lab
```
See [.env.example](.env.example) for the full template.

---

## 3. Sanity-check connectivity (recommended)

```bash
# FlashArray + Ops Manager + node discovery all exercised by the PG preview:
initialize-protection-groups --what-if
```

A healthy run prints: the gateway it connected to, the fleet arrays found, the cluster nodes discovered
from Ops Manager, and each node's `→ <array> / <volume>` mapping. If any of those fail, fix
connectivity/credentials before continuing.

---

## 4. One-time: initialize protection groups

Creates the protection group on **every** fleet array that hosts a cluster data volume and adds those volumes.

```bash
initialize-protection-groups --what-if    # preview — shows exactly what it will create/add
initialize-protection-groups              # apply
```

Expected: `Protection group '<FA_PROTECTION_GROUP>' is ready on all arrays.` Re-run any time the topology
changes (a node or array added/removed); it's idempotent. Use `--prune` to drop volumes no longer in the cluster.

> It also **records the node→volume map as FlashArray volume tags** (`mongo:node`, `mongo:serial`, …) so
> `new-mongo-snapshot`/`restore-mongo-snapshot` resolve each node's volume(s) by reading tags (no per-node
> SSH) — much faster at scale. **This is why you re-run it after a topology change:** to refresh the tags.

---

## 5. Take a snapshot

```bash
new-mongo-snapshot
```

What happens: pre-flight health checks → selects one snapshotable secondary per shard → opens the OM
`$backupCursor` → takes a coordinated FlashArray PG snapshot on every array → closes the cursor. **Let it run
to completion** (don't interrupt it). At the end it prints the **snapshot tag**, e.g.:

```
Snapshot tag: om-20260512-143022
```

**Write that tag down** — it's how you restore. (Metadata lives on the FA snapshot's tags: `mongo:volumes`,
`mongo:preSnap`, `mongo:postSnap`, `mongo:t1ts`.) Tip: take a tag of your own with
`new-mongo-snapshot --snapshot-tag om-YYYYMMDD-HHMMSS` if you want to choose it (must match that pattern).

---

## 6. Restore a snapshot

> **Destructive:** this overwrites the cluster's data volumes in place and restarts mongod on every node.
> Restore to the snapshot's crash-consistent point. Confirm you have the right tag.

```bash
restore-mongo-snapshot --snapshot-tag om-20260512-143022 --force
```

What happens: STEP 0 validates the snapshot is restorable (present on every array, every member, size match)
→ stops agents/mongod → unmounts `/data/mongo` → overwrites each FA volume from the snapshot (sub-second CoW
swap) → remounts → restarts agents (WiredTiger crash-recovers) → waits for the cluster to stabilize → verifies
document counts. For a **sharded** cluster it additionally connects to each shard's RS and confirms the shards
physically hold the data (per-shard totals account for the mongos aggregate); a replica set verifies directly.

Success looks like: `mongos up, N shards registered, N primaries reachable`, then
`Baseline OK : testdb.loadtest = … (drift=0)`, the per-shard distribution, and `Restore Complete`.

Useful flags: `--verify-database <db>` to count a specific DB, `--skip-verification` to skip the count check.

---

## 7. (Optional) Point-in-time recovery (PITR)

Snapshot = the crash-consistent point. To recover to a *later* moment, also capture the oplog and replay it:

```bash
# 1. Start the tailer alongside your workload, using the tag you will snapshot with
start-oplog-tailer --snapshot-tag om-20260512-143022 &
# 2. Take the snapshot with the SAME tag
new-mongo-snapshot --snapshot-tag om-20260512-143022
# ... writes accumulate ...
# 3. Stop the tailer (records the T2 mark). Let it drain ~2-3 min first —
#    OM oplog segments lag live writes, so the recoverable point trails "now" by a couple minutes.
stop-oplog-tailer --snapshot-tag om-20260512-143022
# 4. Restore the snapshot, then replay oplog to a target Unix timestamp (0 = replay everything captured)
restore-mongo-snapshot --snapshot-tag om-20260512-143022 --force
invoke-oplog-replay --snapshot-tag om-20260512-143022 --target-timestamp 0
```

`invoke-oplog-replay` refuses if it sees oplog gap markers (`gap-*.json`); add `--allow-gaps` to override
(PIT coverage in a gap window is unrecoverable). Stream state lives under `~/mongo-oplog-stream/<tag>/`.

---

## 8. Retention cleanup

```bash
remove-old-artifacts --older-than-days 30 --what-if   # preview
remove-old-artifacts --older-than-days 30             # delete FA snapshots + local oplog/log dirs older than N days
```

---

## Troubleshooting (quick hits)

- **Snapshot stuck in `PENDING`** → OM can't open the `$backupCursor` on a selected node. Check that node's
  automation agent is actually running (`ssh <node> systemctl is-active mongodb-mms-automation-agent`). OM can
  be slow to fail a stuck job (tens of seconds to minutes) — let it run; don't kill it mid-flight.
- **`initialize-protection-groups` can't find a node's volume** → that node's data volume lives on a fleet
  array with no PG yet; re-run `initialize-protection-groups` (it creates the PG there).
- **Anything FlashArray returns empty/`403`** → check `FA_USERNAME`/`FA_PASSWORD` (directory login authorizes
  fleet-wide; a single-array `FA_APITOKEN` only works on its own array).
- **Logs:** every run tees to `~/mongo-{snapshot,restore,oplogtailer,oplogreplay}-logs/`.
- **After adding/removing a node or shard** → re-run `initialize-protection-groups` so the PG matches the
  current topology before your next snapshot.
