# Getting Started

A complete, self-service walkthrough: from a fresh clone to taking a crash-consistent snapshot of a MongoDB
**sharded cluster or standalone replica set** on Pure Storage FlashArray — and restoring it. Follow it top to
bottom; you shouldn't need to ask anyone. Budget ~30 minutes the first time (most of it is filling in `.env`).

New here, want the 60-second version? See the **Quick start** in [README.md](README.md#quick-start). Want to
understand *why* it's crash-consistent? See [docs/how-it-works.md](docs/how-it-works.md).

> **The 30-second mental model:** Ops Manager opens a `$backupCursor` on one snapshotable secondary per replica
> set (each shard, or the single RS — pinning a consistent point), and while it's held, the tool takes a
> FlashArray protection-group snapshot on every array at once. Restore overwrites each node's volume from that
> snapshot in place; WiredTiger crash-recovers on restart. No `fsyncLock`, no write stall.

---

## 0. Before you begin — prerequisites

These are one-time and usually provisioned by an admin. Confirm each before continuing:

- [ ] **A MongoDB 8.0 sharded cluster *or* standalone replica set** managed by **Ops Manager 8.0**, with
      `/data/mongo` on a **FlashArray** volume (a single direct pRDM is the validated layout; LVM-over-multipath
      is supported but not yet live-validated) — every array enrolled in the same **Pure Storage Fusion fleet**.
- [ ] **Ops Manager third-party backup enabled** and the cluster **registered / `ACTIVE`**
      (see [docs/third-party-backup-reference.md](docs/third-party-backup-reference.md), Steps 1–7). *For a
      replica set, register it via the third-party `…/clusters/{id}/manage` endpoint — no OM snapshot store is
      needed; the standard `backupConfigs statusName=STARTED` path 409s `Could not find available Snapshot Store`.*
- [ ] **SSH key-based auth** from the machine you run this on to **every cluster node** as one user
      (`SSH_USER`), with **passwordless `sudo`** — needed only by **restore** (snapshot/PITR/init-pg use no
      sudo). You can grant blanket `NOPASSWD: ALL` or scope it to the exact commands restore runs; see
      [Sudo access on the cluster nodes](#sudo-access-on-the-cluster-nodes-restore-only). Test:
      `ssh <SSH_USER>@<node> sudo -n true`.
- [ ] **An Ops Manager API key** (public/private) with role **`GLOBAL_BACKUP_ADMIN`**, and your IP on its
      access list.
- [ ] **FlashArray credentials** — a directory account (`FA_USERNAME`/`FA_PASSWORD`) that authorizes on
      **every** fleet member (preferred), or a single-array API token (`FA_APITOKEN`).
- [ ] **Python 3.11+** and the OpenSSH client (`ssh`/`scp`) on the machine you run from.

Also make sure the SSH user is in the **`mongod` group** on every node (so it can read agent-written oplog
files for PITR): `ssh <SSH_USER>@<node> id` should list `mongod`. If not: `sudo usermod -aG mongod <SSH_USER>`.

---

## Sudo access on the cluster nodes (restore only)

Only **`restore-mongo-snapshot`** touches the OS as root — it stops the agents, unmounts `/data/mongo`,
rescans the LUN, and remounts. **`new-mongo-snapshot`, `start`/`stop-oplog-tailer`, `invoke-oplog-replay`, and
`initialize-protection-groups` use no sudo at all** — they run as `SSH_USER` (the tailer only needs the
`mongod` group, above, to read oplog files).

The prerequisites assume `SSH_USER` has passwordless `sudo`. If you'd rather not grant blanket
`NOPASSWD: ALL`, scope it to exactly what restore invokes over SSH on each node:

```text
systemctl stop  mongodb-mms-automation-agent      # STEP 1  quiesce OM's agent
systemctl start mongodb-mms-automation-agent      # STEP 6  hand control back
pkill -TERM -x mongod                             # STEP 2  stop mongod/mongos (and -KILL to escalate)
pkill -TERM -x mongos
pkill -KILL -x mongod
pkill -KILL -x mongos
umount /data/mongo                                # STEP 3  unmount before the volume overwrite
lsof  +f -- /data/mongo                           #   (only on an unmount failure — diagnostic)
fuser -mv /data/mongo                             #   (only on an unmount failure — diagnostic)
tee /sys/block/<disk>/device/rescan               # STEP 5  rescan the re-pointed LUN
blockdev --rereadpt /dev/<disk>                   #   (partprobe is the fallback)
partprobe /dev/<disk>
udevadm settle --timeout=15
pvscan --cache                                    #   LVM: no-op on a single pRDM
vgchange -ay                                      #   LVM: no-op on a single pRDM
blkid -s TYPE -o value /dev/<part>                #   detect fs type for the RO integrity check
xfs_repair -n /dev/<part>                         #   RO integrity check (xfs) ...
e2fsck   -n -f /dev/<part>                         #   ... or ext2/3/4
mount /data/mongo                                 # STEP 5  remount from fstab
```

### Example scoped `sudoers`

Drop this on **every** node as `/etc/sudoers.d/mongo-backup` (owner `root`, mode `0440`; always validate with
`visudo -cf /etc/sudoers.d/mongo-backup` before trusting it). Replace `backup` with your `SSH_USER`, and fix
the binary paths for your distro — find them with `command -v systemctl pkill umount mount blockdev partprobe udevadm pvscan vgchange blkid xfs_repair e2fsck lsof fuser tee`:

```sudoers
# Passwordless sudo for the mongodb-flasharray-backup SSH user, scoped to what restore runs.
Cmnd_Alias MONGO_RESTORE = \
    /usr/bin/systemctl stop mongodb-mms-automation-agent, \
    /usr/bin/systemctl start mongodb-mms-automation-agent, \
    /usr/bin/pkill -TERM -x mongod, /usr/bin/pkill -KILL -x mongod, \
    /usr/bin/pkill -TERM -x mongos, /usr/bin/pkill -KILL -x mongos, \
    /usr/bin/umount /data/mongo, /usr/bin/mount /data/mongo, \
    /usr/bin/lsof +f -- /data/mongo, /usr/bin/fuser -mv /data/mongo, \
    /usr/bin/tee /sys/block/*/device/rescan, \
    /usr/sbin/blockdev --rereadpt /dev/*, /usr/sbin/partprobe /dev/*, \
    /usr/bin/udevadm settle --timeout=15, \
    /usr/sbin/pvscan --cache, /usr/sbin/vgchange -ay, \
    /usr/sbin/blkid -s TYPE -o value /dev/*, \
    /usr/sbin/xfs_repair -n /dev/*, /usr/sbin/e2fsck -n -f /dev/*

backup ALL=(root) NOPASSWD: MONGO_RESTORE
```

> **How the matching works:** `sudo` matches the resolved command path **and its arguments**. The fixed
> commands must match exactly (they're constant in the tool), and the `/dev/*` / `/sys/block/*` wildcards cover
> the per-node device names. Verify with `sudo -ln` as `SSH_USER` — the `MONGO_RESTORE` entries should list.
> If your `xfs_repair`/`e2fsck`/`blockdev` live in `/sbin` rather than `/usr/sbin`, adjust (or symlink) — a
> path mismatch makes that one command fall through to a password prompt and stall the restore.

> **Cross-cluster restore (`restore-mongo-snapshot-to-target`, cert 1.A.1.b) needs more.** In addition to the
> above it runs `find`, `mkdir -p`, `chown`, `grep`, `awk`, `tee`, and `sudo -u <mongod-user> mongod …` (an
> offline `local.system.replset` rewrite). If you use that command, grant those too — or just use blanket
> `NOPASSWD: ALL` on the throwaway target host.

---

## 1. Install

```bash
git clone https://github.com/ScottyNVME/mongodb-flasharray-backup-python.git
cd mongodb-flasharray-backup-python
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

Fill in (the tool finds `.env` via `$MONGO_FA_BACKUP_ENV`, else python-dotenv's search from the current dir):

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
deployment-specific keys can be overridden per deployment with a `<NAME>__` prefix (NAME upper-cased, hyphens →
underscores) and selected with `--deployment <name>` on any command (omit for the default/flat deployment). Set
`TOPOLOGY=replicaset` for a standalone replica set (no `mongos`; point `MONGOS_HOST` at any RS member). Example:

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
AEN_RS_00__MONGOS_HOST=aen-mongo-05.example.com
```
See [.env.example](.env.example) for the full template. Append `--deployment <name>` to **every** command below
when working a non-default deployment.

---

## 3. Sanity-check connectivity (recommended)

```bash
initialize-protection-groups --what-if
```

A healthy run prints: the gateway it connected to, the fleet arrays found, the cluster nodes discovered from
Ops Manager, and each node's `→ <array> / <volume>` mapping. If any of those fail, fix
connectivity/credentials before continuing — this one command exercises FlashArray, Ops Manager, and per-node
SSH all at once.

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
`mongo:preSnap`, `mongo:postSnap`, `mongo:t1ts`.) Tip: choose your own tag with
`new-mongo-snapshot --snapshot-tag om-YYYYMMDD-HHMMSS` (must match that pattern).

---

## 6. Restore a snapshot

> **Destructive:** this overwrites the cluster's data volumes in place and restarts mongod on every node.
> Restore lands at the snapshot's crash-consistent point. Double-check you have the right tag.

```bash
restore-mongo-snapshot --snapshot-tag om-20260512-143022 --force
```

What happens: STEP 0 validates the snapshot is restorable (present on every array, every member, size match)
→ stops agents/mongod → unmounts `/data/mongo` → overwrites each FA volume from the snapshot (sub-second CoW
swap) → remounts → restarts agents (WiredTiger crash-recovers) → waits for the cluster to stabilize → verifies
document counts. For a **sharded** cluster it also connects to each shard's RS and confirms the shards
physically hold the data (per-shard totals account for the mongos aggregate); a replica set verifies directly.

Useful flags: `--verify-database <db>` to count a specific DB, `--skip-verification` to skip the count check.

### Verify it worked

Success looks like this at the end of the run:

```
… mongos up, N shards registered, N primaries reachable   (sharded)
… replica set up, primary elected: <host>                 (replica set)
Baseline OK : testdb.loadtest = 39600 in [39600, 39600] (drift=0)
  aen-shard_1: testdb.loadtest=26339 …                    (per-shard, sharded only)
=== Restore Complete ===
```

`drift=0` means the restored counts match the snapshot exactly. Every run is also teed to
`~/mongo-restore-logs/`. To double-check by hand, connect with `mongosh` and compare a known count to what it
was at snapshot time.

---

## 7. (Optional) Point-in-time recovery (PITR)

A snapshot recovers to *its* crash-consistent point. To recover to a **later** moment, also capture the oplog
and replay it. Run each step in its **own terminal** — don't pipe a long-running tailer into a foreground
command (they deadlock on the shared pipe).

```bash
# 1. Start the tailer alongside your workload, using the tag you will snapshot with
start-oplog-tailer --snapshot-tag om-20260512-143022 &
# 2. Take the snapshot with the SAME tag  (this is T1)
new-mongo-snapshot --snapshot-tag om-20260512-143022
# ... writes accumulate ...
# 3. Stop the tailer (records the T2 mark). Let it drain ~2-3 min first —
#    OM oplog segments lag live writes, so the recoverable point trails "now" by a couple minutes.
stop-oplog-tailer --snapshot-tag om-20260512-143022
# 4. Restore the snapshot (back to T1), then replay oplog forward
restore-mongo-snapshot --snapshot-tag om-20260512-143022 --force
invoke-oplog-replay --snapshot-tag om-20260512-143022 --target-timestamp 0   # 0 = replay everything captured
```

Replay to a specific point by passing a Unix timestamp instead of `0`. `invoke-oplog-replay` refuses if it sees
oplog gap markers (`gap-*.json`); add `--allow-gaps` to override (coverage inside a gap is unrecoverable).
Success prints `unrecoveredTail=0`. Stream state lives under `~/mongo-oplog-stream/<tag>/`.

---

## 8. Retention cleanup

```bash
remove-old-artifacts --older-than-days 30 --what-if   # preview
remove-old-artifacts --older-than-days 30             # delete FA snapshots + local oplog/log dirs older than N days
```

---

## Troubleshooting (quick hits)

- **Snapshot stuck in `PENDING`** → OM can't open the `$backupCursor` on a selected node. Check that node's
  automation agent is running (`ssh <node> systemctl is-active mongodb-mms-automation-agent`). OM can be slow
  to fail a stuck job — let it run; don't kill it mid-flight.
- **PITR replay recovers nothing / a stale point** → the OM oplog cursor is stale (e.g. after OM downtime).
  Re-baseline it forward (create one oplog snapshot spanning `[stale cursor → now]` and `/finish` without
  copying), then re-run the PITR cycle. The tailer must run *continuously* to keep coverage current.
- **Tailer `scp` fails / oplog job keeps failing** → `SSH_USER` isn't in the `mongod` group on some node
  (often one added later). `sudo usermod -aG mongod <SSH_USER>` on that node.
- **A newly-added member sits `health=0` / unreachable** → its mongod port (e.g. `27017`) isn't open in the
  node's firewall. `sudo firewall-cmd --add-port=27017/tcp --permanent && sudo firewall-cmd --reload`.
- **`initialize-protection-groups` can't find a node's volume** → that node's volume is on a fleet array with
  no PG yet; re-run `initialize-protection-groups` (it creates the PG there).
- **Anything FlashArray returns empty/`403`** → check `FA_USERNAME`/`FA_PASSWORD` (a directory login authorizes
  fleet-wide; a single-array `FA_APITOKEN` only works on its own array).
- **After adding/removing a node or shard** → re-run `initialize-protection-groups` so the PG + volume tags
  match the current topology before your next snapshot.
- **Logs:** every run tees to `~/mongo-{snapshot,restore,oplogtailer,oplogreplay}-logs/`.

---

## What's next

- **Full command reference** (all flags, every command): [README.md](README.md#commands).
- **How it works** (why the snapshot is recoverable, node→volume discovery, PITR internals):
  [docs/how-it-works.md](docs/how-it-works.md).
- **Ops Manager third-party backup setup + API** (enabling backup, endpoints, gotchas):
  [docs/third-party-backup-reference.md](docs/third-party-backup-reference.md).
- **Certification status** (what's validated, and an operational runbook of field gotchas):
  [tests-docs/Certification-Summary.md](tests-docs/Certification-Summary.md).
