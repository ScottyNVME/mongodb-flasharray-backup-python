# How It Works — Deep Dive

This document covers the full technical picture: the Ops Manager Third-Party Backup API, the `$backupCursor` recoverability guarantee, PITR design, and operational reference material.

---

## Ops Manager's Two Roles

Ops Manager serves two distinct functions in this pipeline:

**1. Cluster lifecycle manager.** An automation agent runs on every node (`mongodb-mms-automation-agent`). In normal operation it enforces desired state — if `mongod` crashes, the agent restarts it. During a restore this is exactly the wrong behavior: an agent that is still running will race to restart `mongod` the instant the script kills it, making it impossible to safely unmount and overwrite the data volume. The restore script deliberately takes over for the destructive window: STEP 1 stops the agents via `systemctl` over SSH, STEP 6 starts them again. When agents restart they see the newly-restored WiredTiger data as their new baseline and start `mongod` normally, allowing crash recovery to proceed without any Ops Manager API involvement on the critical path.

**2. Third-party backup coordinator.** Ops Manager exposes a partner API (`/api/public/v1.0/backup/third_party/...`) designed to let an external storage system own the snapshot. It manages only the cursor lifecycle:

```
POST /snapshot        → creates a job, returns snapshotId
POST /snapshot/start  → opens MongoDB's $backupCursor on selected nodes
                        state: PENDING → READY
[FlashArray PG snapshot taken here]
POST /snapshot/finish → closes $backupCursor
                        state: READY → FINISHED
```

Ops Manager holds no data and owns no storage. It is solely the coordinator for the cursor open/close window.

---

## Third-Party Backup API Reference

### Authentication

All calls use **HTTP Digest authentication**. The API user must have the `GLOBAL_BACKUP_ADMIN` role. Credentials are loaded from `.env` — see `.env.example`.

### Base Path

> The third-party backup API appends to the standard public API prefix, not an alternate root.

| API | Base path |
|---|---|
| Public API | `http://<om-host>:8080/api/public/v1.0` |
| Third-party backup API | `http://<om-host>:8080/api/public/v1.0/backup/third_party` |

All endpoints below are relative to `.../backup/third_party`.

### Discovery Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/group/settings` | Returns `brs.thirdparty.baseOplogFilePath` |
| `GET` | `/group/{groupId}/clusters` | Lists all clusters and their state |
| `GET` | `/group/{groupId}/clusters/{clusterId}` | Lists replica sets, nodes, `snapshotable` flags, oplog paths |

### Management Endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/group/{groupId}/clusters/{clusterId}/manage` | Enable third-party management for a cluster |
| `POST` | `/group/{groupId}/clusters/{clusterId}/unmanage` | Disable third-party management |

### Snapshot Endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/group/{groupId}/clusters/{clusterId}/snapshot` | Create snapshot job; returns `snapshotId` |
| `POST` | `/group/{groupId}/clusters/{clusterId}/snapshot/{id}/start` | Start timeout timer; opens `$backupCursor` |
| `GET` | `/group/{groupId}/clusters/{clusterId}/snapshot/{id}` | Poll state; also resets timeout timer (heartbeat) |
| `POST` | `/group/{groupId}/clusters/{clusterId}/snapshot/{id}/finish` | Signal files copied; moves to FINISHING |
| `POST` | `/group/{groupId}/clusters/{clusterId}/snapshot/{id}/fail` | Abort snapshot; moves to FAILING → FAILED |
| `POST` | `/group/{groupId}/clusters/{clusterId}/snapshot/{id}/unfreeze` | Close cursor on a single node (not used for volume snapshots) |
| `GET` | `/group/{groupId}/clusters/{clusterId}/snapshot/{id}/{nodeId}/fileList` | List files to copy (not used for volume snapshots) |

### Oplog Snapshot Endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/group/{groupId}/clusters/{clusterId}/preferredOplogNodes` | Set preferred nodes for oplog tailing; send empty list to disable |
| `POST` | `/group/{groupId}/clusters/{clusterId}/oplogSnapshot` | Create oplog snapshot job; returns `oplogSnapshotId` |
| `POST` | `/group/{groupId}/clusters/{clusterId}/oplogSnapshot/{id}/start` | Start timeout timer; state: PENDING → READY |
| `GET` | `/group/{groupId}/clusters/{clusterId}/oplogSnapshot/{id}` | Poll state; READY response includes `ranges[].end`, `.previousEnd`, `.nodes[].logFiles`; each GET resets timer |
| `POST` | `/group/{groupId}/clusters/{clusterId}/oplogSnapshot/{id}/finish` | Signal files copied; OM deletes `.oplogs` files asynchronously |
| `POST` | `/group/{groupId}/clusters/{clusterId}/oplogSnapshot/{id}/fail` | Abort oplog snapshot |

### Quick Reference — `curl` Connectivity Test

```bash
OM="http://10.21.229.11:8080/api/public/v1.0/backup/third_party"
AUTH='ygsksiuq:82029aaa-82df-45fa-a557-478cf7771961'
GID="69fa468cc577f94d3af37277"
CID="69fa4f72c577f94d3af39282"

# Verify API key and base settings
curl --user "$AUTH" --digest -H "Accept: application/json" "$OM/group/settings"

# List clusters in project
curl --user "$AUTH" --digest -H "Accept: application/json" "$OM/group/$GID/clusters"

# Get cluster detail (nodes, snapshotable flags)
curl --user "$AUTH" --digest -H "Accept: application/json" "$OM/group/$GID/clusters/$CID"

# Register cluster for third-party backup (idempotent)
curl --user "$AUTH" --digest -H "Accept: application/json" -H "Content-Type: application/json" \
  -X POST "$OM/group/$GID/clusters/$CID/manage"
```

---

## Snapshot State Machine

```
INITIAL → PENDING → READY → [take FlashArray snapshot] → FINISHING → FINISHED
                                                                    ↘ FAILING → FAILED
```

| State | Meaning |
|---|---|
| `INITIAL` | Snapshot job created |
| `PENDING` | Agents opening `$backupCursor`; MongoDB I/O continues normally |
| `READY` | Backup cursor open; WiredTiger checkpoint pinned — **take FlashArray snapshot now** |
| `FINISHING` | POST /finish received; closing backup cursor |
| `FINISHED` | Cursor closed; `snapshotMetadata` populated in GET response |
| `FAILING` | Intermediate error state |
| `FAILED` | Non-recoverable error; restart the process |

---

## Snapshot Workflow

### Step-by-step (volume snapshot — FlashArray)

Because FlashArray takes volume-level snapshots, the `fileList` / `fileDiffs` steps are **skipped** — all files on the volume are captured atomically by the array.

```
1. GET  /group/{groupId}/clusters/{clusterId}
         → identify snapshotable nodes from the cluster detail's replicaSets
           (one per shard + config RS for a sharded cluster; the single RS for a replica set)
         → select: hidden secondary > secondary > primary; prefer most recent opTime

2. POST /group/{groupId}/clusters/{clusterId}/snapshot
         body: { "nodeIds": ["aen-mongo-01:27020", "aen-mongo-01:27021", ...],
                 "timeoutMinutes": 150,
                 "incrementalMetadata": { "incremental": true,
                   "rsIdsToSrcBackupNames": [       ← omit for first/full snapshot
                     { "rsId": "aen-shard_0", "srcBackupName": "<thisBackupName from last snapshot>" },
                     ...
                   ]
                 }
               }
         response: { "snapshotId": "..." }

3. POST /group/{groupId}/clusters/{clusterId}/snapshot/{snapshotId}/start

4. GET  /group/{groupId}/clusters/{clusterId}/snapshot/{snapshotId}
         → poll until state = "READY"
         → each GET resets timeout timer (heartbeat)

5. *** TAKE FLASHARRAY PG SNAPSHOT HERE ***

6. Capture per-shard oplog anchors (cursor still open) → written to sidecar for PITR

7. POST /group/{groupId}/clusters/{clusterId}/snapshot/{snapshotId}/finish

8. GET  /group/{groupId}/clusters/{clusterId}/snapshot/{snapshotId}
         → poll until state = "FINISHED"
         → response now contains "snapshotMetadata" — store this
```

### Node selection rules (priority order)

1. `snapshotable: true` — mandatory
2. Previously snapshotted nodes — enables incremental
3. Most recent `opTime` — aligns shard timestamps
4. Hidden secondary > secondary > primary — minimizes production impact

### Topology requirements (sharded *and* replica set)

Node selection is **topology-agnostic**: it iterates the cluster detail's `replicaSets` array (the third-party
backup view), **not** `listShards`. The same code path covers both deployment types:

- **Sharded cluster** — `nodeIds` includes **exactly one snapshotable node from each shard's RS AND the config
  RS**. For `aen-cluster` (dedicated config): one node from each of `aen-shard_0` (config), `aen-shard_1`,
  `aen-shard_2`, `aen-shard_3`.
- **Standalone replica set** (`TOPOLOGY=replicaset`, selected with `--deployment <name>`) — the cluster detail
  returns a single `replicaSet`; `nodeIds` is one snapshotable member of it.
- Node ID format: `"hostname:port"` — e.g. `"aen-mongo-01:27021"` (sharded) or `"aen-mongo-06.fsa.lab:27017"` (RS).

`TOPOLOGY` only affects the two `mongos`/`listShards` call sites elsewhere — the restore-stabilization wait
(replica-set mode verifies the single RS has a writable primary instead of `listShards`) and the PIT oplog
anchors. Snapshot node-selection above is identical for both, and the sharded paths are byte-for-byte unchanged.

### Full vs. incremental snapshots

| Type | When | How |
|---|---|---|
| Full | First snapshot ever; periodic thereafter | Omit `rsIdsToSrcBackupNames` from the request body |
| Incremental | Every subsequent snapshot | `incremental: true` + `rsIdsToSrcBackupNames` using `thisBackupName` from the previous snapshot's `snapshotMetadata` |

> After a full snapshot is started, no prior snapshot can be used as a source for incremental — even if the full fails. Always start the incremental chain from a completed full.

---

## I/O Behavior During Snapshot

MongoDB I/O is **never blocked or frozen** at any point. This is the key advantage over `fsyncLock`.

| Phase | MongoDB I/O | What is happening |
|---|---|---|
| `PENDING` | ✅ Normal | Agents establishing the `$backupCursor` on each selected node |
| `READY` | ✅ Normal | WiredTiger pins the current checkpoint; new writes continue via CoW into new allocation blocks |
| FA snapshot | ✅ Normal | FlashArray snapshot is a pointer redirect at the storage layer — microseconds; applications see no pause |
| `FINISHING` | ✅ Normal | Ops Manager closes the backup cursor; WiredTiger checkpoint pin released |

Crash consistency is guaranteed because `$backupCursor` ensures the pinned checkpoint is internally consistent on each node. The FlashArray PG snapshot is taken per-array via `-ContextName`; the checkpoint is pinned on all nodes before any array is snapshotted and remains pinned until `/finish` is called after all arrays are done.

> **Hard requirement — one node's volumes must all be on one array.** A FlashArray PG snapshot is atomic
> **only within a single array**; the per-array snapshots in a run are *separate* point-in-time captures that
> fire independently (a few milliseconds apart). That's fine because each node's data lives entirely on one
> array, so that node's image is captured atomically. If a single node's data volumes were split **across two
> arrays** (e.g. an LVM VG with PVs on different arrays), the two arrays' snapshots would not be mutually
> atomic — the VG/filesystem image would be torn between two non-simultaneous point-in-time captures and may
> be unrecoverable. So **every volume backing a given node must reside on the same FlashArray.** (Different
> nodes on different arrays is expected and fine; the constraint is per-node.)

---

## Why `$backupCursor` Makes Every Snapshot Recoverable

WiredTiger continuously checkpoints data to disk and maintains a write-ahead journal. Under normal operation, once a new stable checkpoint is written it recycles old journal files. The risk: if a volume is snapshotted while journal cleanup is in flight, the image on disk may contain a checkpoint that references journal entries that no longer exist — producing an unrecoverable volume.

`$backupCursor` prevents this. When Ops Manager calls `POST /snapshot/start`:

- WiredTiger **freezes journal file cleanup** — no journal files are removed while the cursor is open
- A new stable checkpoint is flushed to disk
- The WiredTiger data directory is now in a state where the checkpoint plus the intact journal can replay forward to a fully consistent database

**Writes continue during the snapshot window.** This is not `fsyncLock`. The FlashArray PG snapshot captures whatever happens to be on the physical blocks at the instant it fires — a *crash-consistent* image, not write-quiesced. WiredTiger was designed with crash recovery as a first-class guarantee: on startup it detects whether the last checkpoint is consistent and, if not, replays the journal forward from the last good checkpoint. This is precisely what happens in STEP 6 of the restore:

```
STEP 4: FA volume overwritten from snapshot (metadata-only CoW flip, sub-second)
STEP 5: Linux rescans the block device, remounts /data/mongo
STEP 6: automation agent starts mongod → WiredTiger crash recovery runs automatically
STEP 7: poll until all shard primaries elect
```

Every FA snapshot taken inside the `$backupCursor` window is therefore guaranteed recoverable by WiredTiger on restart.

### Whole-cluster revert and the oplog-window requirement

The restore overwrites **every** member's volume from its own snapshot and brings the whole replica set / cluster back at once. Because the members were captured at slightly different oplog positions (only the `$backupCursor` node is frozen; the others are replicating during the window), they reconcile to a common point via **normal MongoDB replication/rollback** after restart — exactly as a member that was briefly offline would.

**Operational requirement (confirmed with MongoDB):** this reconciliation only succeeds while the elected primary still holds **enough oplog to span the gap between the snapshot point and the revert point**. If the oplog window is shorter than that spread, a lagging member finds no common point and hits `OplogStartMissing` (→ resync/rollback failure). So size the oplog comfortably larger than the replication lag present at snapshot time (and than any snapshot→restore delta you intend to land on). Backing up from a low-lag, low-traffic secondary — the default node-selection preference — keeps this spread small.

---

## How the Oplog Tailer Extends This to Full PITR

The crash-consistent snapshot gives you a single point in time, **T1**. The continuous oplog tailer closes the gap between T1 and the actual recovery target **T2**:

```
T1 (snapshot)              Failure event              T2 (recovery target)
      │                          │                            │
      ▼                          ▼                            ▼
──────●──────────────────────────●────────────────────────────●──▶
      │                                                       │
      └── oplog tailer captures every op as BSON segments ───┘
          ~/mongo-oplog-stream/<tag>/<shardId>/segments/
```

The key invariant is where the per-shard oplog anchor is captured: **after the FlashArray PG snapshot commits, but before `$backupCursor` closes**. Because the cursor is still open at anchor read time, any oplog entry with `ts ≤ anchor` is guaranteed to be physically present on the snapshotted volume. The tailer then captures `ts > anchor` continuously. There is no window in which an operation can exist in neither the snapshot nor the tailer's output.

Each tailer segment uses disjoint bounds (`{ ts: { $gt: lastTs, $lte: readTs } }`) so segments contain no duplicates and no gaps. Segments are named `oplog-NNNNNNNN-<t>-<i>.bson` so lexical sort equals capture order. `invoke-oplog-replay` applies them per-shard via `mongorestore --oplogReplay`; `--oplogLimit` trims the final segment to an exact `TargetTimestamp` for sub-segment precision.

---

## Backup Data Location

Ops Manager **does not store any backup data**. The third-party backup API only coordinates the `$backupCursor` lifecycle. All recoverable backup data lives on the **FlashArray volume snapshots**. Snapshot names follow the pattern `<pg-name>.<tag>.<volume-name>`, e.g. `aen-mongodb-pg.om-20260505-201951.aen-mongo-01-data`.

The Ops Manager UI (**Backup → [cluster] → Snapshots**) shows completed snapshot jobs with metadata, but no files or storage consumption on the Ops Manager side.

---

## `snapshotMetadata` — What to Store

When state = `FINISHED`, the GET snapshot response includes `snapshotMetadata`. `new-mongo-snapshot` reads it to extract the snapshot oplog timestamp (`snapshotTimestamp.time`) and stores that as the `mongo:t1ts` tag on the FlashArray snapshot — the anchor PITR replay starts from. Snapshots here are always full (no `srcBackupName` chaining), and the restore validates its target at the storage layer (see Node-to-Volume Discovery, Step 5), so the remaining fields are informational. The full `snapshotMetadata` shape:

| Field | Purpose |
|---|---|
| `snapshotId` | Identifies this snapshot for restore requests |
| `snapshotTimestamp.time` | Unix epoch of snapshot |
| `restoreTimestamp` | Use for PIT restore targeting |
| `rsSnapshotsMetadata[].rsId` | Replica set ID |
| `rsSnapshotsMetadata[].thisBackupName` | **Use as `srcBackupName` for the next incremental snapshot** (null for full) |
| `rsSnapshotsMetadata[].mongodbVersion` | Version consistency check |
| `rsSnapshotsMetadata[].fcv` | Feature compatibility version |
| `rsSnapshotsMetadata[].storageEngine` | Must match restore target |
| `rsSnapshotsMetadata[].incremental` | Whether this was an incremental snapshot |

---

## Snapshot Naming

Tags `ClusterName`, `BackupTimestamp`, and `BackupType` are written inline at creation time and replicate to all arrays. Snapshot tags are formatted `om-YYYYMMDD-HHmmss`. Each PG snapshot produces:

- `aen-mongodb-pg.<tag>` — the PG snapshot object on each array
- `aen-mongodb-pg.<tag>.<volume>` — the per-volume member used by the restore script

---

## Node-to-Volume Discovery

**Fast path (default): FlashArray volume tags.** Resolving the storage path per node on *every* snapshot/restore by SSH is slow at scale. `initialize-protection-groups` therefore precomputes the map once and writes it onto each FA volume as **copyable `mongo:` tags** — `deployment`, `node`, `mountpoint`, `serial`, `vg`, `pvindex`, `pvcount`. On the hot path, `resolve_node_volume_map` reads those tags (one `GET /volumes/tags` per array, **no SSH**), cross-checks each volume's serial against the array, and falls back to live discovery only for an untagged or stale node. **Re-run `initialize-protection-groups` after any topology change** (node/shard add or remove) to refresh the tags. (With no tags present the resolver degrades to full live discovery, so it stays correct out of the box.)

### What happens when a volume moves between arrays

The map is keyed on the **SCSI serial**, and the array a volume lives on is *derived* (which array's `GET /volumes/tags` returned it), never hard-coded — so an array-to-array move resolves correctly through one of three paths, and the failure modes are all **loud, never silent corruption**:

| Situation after the move | What the resolver does |
|---|---|
| Tags travelled with the volume; new array is in the PG set | Read from the new array → `ShortName` is the new array → serial matches → **used directly.** Self-correcting. |
| Tag lost, or serial changed, or a stale tag left on the old array | Serial cross-check fails (or the node is untagged) → **falls back to live SSH discovery** for that node, which re-finds the volume by serial on whichever PG array now owns it. |
| One PV of a multi-volume (LVM) node lost its tag | `mongo:pvcount` says the node should have *N* volumes but fewer resolved → **flagged incomplete → full SSH rediscovery** (without this guard the missing PV would be silently dropped, producing a partial backup). |
| Volume moved to an array with **no protection group** | SSH discovery searches only PG arrays, finds nothing, and **raises** — restore/snapshot abort before any change. Fix: re-run `initialize-protection-groups` so the destination array joins the PG, then retry. |

Restore adds defence-in-depth on top of resolution: it confirms the resolved volume **count** equals the snapshot's recorded `mongo:volumes` count, that each per-volume member snapshot **exists** on the resolved array, and that **sizes match** — all *before* the destructive overwrite. So a moved volume yields either a correct restore or a clean abort, never a wrong-volume overwrite.

**Discovery chain (runs at tag time, and as the fallback).** `discover_node_volumes` walks each node's data mount to *all* its backing FA volumes, returning `node -> [volumes]` — one entry for a single pRDM, several for an **LVM/multipath** mount whose VG spans multiple PVs. For a direct device it is the five steps below; for LVM it walks the inverse device tree (`lsblk -s`: mount → LV → VG → PVs → multipath) and reads each volume's serial from the SERIAL column (24-hex) or the NAA WWN (`0x624a9370<serial>`), de-duplicating multiple paths to the same volume. The original single-device chain (`resolve_node_to_array_volume_map`) has five steps:

### Step 1 — Find the mounted partition (`findmnt`)

```bash
findmnt -no SOURCE /data/mongo
# → /dev/sdb1
```

`findmnt` queries the kernel mount table to find which block device partition is currently mounted at `/data/mongo`. This is the authoritative answer: no assumptions about device names.

### Step 2 — Walk up to the parent disk (`lsblk PKNAME`)

```bash
lsblk -no PKNAME /dev/sdb1
# → sdb
```

`lsblk` maps the partition back to its parent block device. The parent disk is what the FlashArray presents as a pRDM; the partition is just an XFS slice of it.

### Step 3 — Read the SCSI serial (`lsblk SERIAL`)

```bash
lsblk -no SERIAL /dev/sdb
# → 1071bf0a0a224a050019bf3b
```

Every Pure Storage volume has a SCSI page-80 serial embedded in the block device — a 24-character NAA hex string that encodes the volume's WWN. This serial is stable across reboots and remounts and is guaranteed globally unique within the array fleet. It is the single durable identifier linking the Linux block device to a specific FlashArray volume.

> **Fallback (mid-restore, volume not mounted):** if `findmnt` finds nothing at `/data/mongo`, the command instead scans all disks for any serial matching the FA format (`^[0-9a-fA-F]{20,}$`). Since each node has exactly one FA data volume, this is unambiguous.

### Step 4 — Resolve serial to FlashArray volume (`GET /volumes?filter=serial=…`)

```python
fa.get_volumes(context_names=["sn1-x90r2-f07-27"], filter="serial='1071BF0A0A224A050019BF3B'")
# → aen-mongo-01-data
```

The client connects directly to each fleet array in turn (`context_names=[<array>]`). The FA REST API matches the serial (stored uppercase) against its volume table. The first array that returns a volume wins; all other arrays are skipped. This is how the script identifies both which array owns the volume **and** what the volume is named — both required for the PG snapshot and volume overwrite operations.

### Step 5 — Verify protection group membership (`GET /protection-groups/volumes`)

```python
fa.get_protection_groups_volumes(context_names=[array], group_names=["aen-mongodb-pg"])
```

With the array and volume name known, the pre-flight check confirms the volume is a member of the protection group. If any data volume is missing from the PG, the script throws before any snapshot is attempted — preventing a partial snapshot that would be unrestorable.

### What the full chain produces

This is the output logged by the script and confirmed by Test 1:

```
aen-mongo-01 serial: 1071bf0a0a224a050019bf3b
  -> sn1-x90r2-f07-27 / aen-mongo-01-data     ← array / volume
     PG member verified on sn1-x90r2-f07-27   ← PG membership confirmed

aen-mongo-02 serial: 81f096d1c1642a6902f39580
  -> sn1-x90r2-f06-27 / aen-mongo-02-data

aen-mongo-03 serial: ac5fc11f8b3b49a0031bcae1
  -> sn1-x90r2-f06-33 / aen-mongo-03-data
```

Because the mapping is derived entirely from kernel and storage APIs at runtime, it automatically reflects any infrastructure change — a node rebooted onto a different device path, a volume migrated to a different array, or a new node added to the cluster — with no script edits required.

---

## Gotchas

| Issue | Cause | Resolution |
|---|---|---|
| All `/backup/third_party/` calls return HTTP 404 | `mms.featureFlag.backup.thirdPartyManaged` is stored in the AppDB but API routes are only registered at startup | Restart Ops Manager: `sudo systemctl restart mongodb-mms` and wait ~2–3 min for JVM warmup |
| `/manage` returns `THIRD_PARTY_CLUSTER_ALREADY_MANAGED` | Cluster was already registered | Not an error — cluster is active |
| Backup agents stuck on `standby` in Deployment → Servers | No snapshot job assigned yet; agents activate when a snapshot is in progress | Expected with third-party backup |
| `THIRD_PARTY_DISCOVERY_ERROR` (snapshot pre-flight or snapshot-detail GET) | OM's backup coordinator can't reconcile its shard map after a topology change (shard add/remove, **dedicated-config-server migration**) | A UI re-enable is usually **not** enough — use the **force-unmanage recovery**: stop any orphaned old mongods + `DELETE` their stale OM monitoring hosts → delete the active `backupjobs` config docs (`clusters`/`jobs`/`thirdparty.jobs`, history kept) → `systemctl restart mongodb-mms` → `POST …/manage`. Also add any new node's data volume to the PG (`initialize-protection-groups`). See `tests-docs/Test-CertificationChecklist.md` and issues #1/#2. |
| Snapshot job stuck in `PENDING` | OM can't open the `$backupCursor` (selected node's agent down, or backup wedged by a topology change) | **Do NOT `/fail` a PENDING third-party snapshot** — OM turns it into a globally-blocking `FAILING` wedge. Instead `kill -9` the snapshot client (skips the `finally`→`/fail`), confirm the agent is running on the selected node (`systemctl is-active mongodb-mms-automation-agent`), and if it's a topology-change wedge, run the force-unmanage recovery (row above). |
| `Could not find available Snapshot Store` when enabling RS backup | Used the OM-**managed** path (`PATCH backupConfigs statusName=STARTED`) for a third-party replica set | Enable third-party backup via `POST …/clusters/{id}/manage` (with a `syncSource` = an RS secondary). The FlashArray holds the snapshots, so **no OM snapshot store is needed**. |

---

## Recoverability Properties — Summary

| Property | Mechanism |
|---|---|
| Snapshot is always crash-recoverable | `$backupCursor` freezes journal cleanup; WiredTiger replays journal on restart |
| No gap between snapshot and oplog stream | OM Oplog Snapshot API `previousEnd` field detects any continuity break between jobs; gap markers written immediately |
| No duplicates or gaps in oplog stream | OM agent produces contiguous, non-overlapping `.oplogs` files; `previousEnd` verified against `state.json` `lastEnd` each cycle |
| Deterministic replay order | Segment filenames encode epoch timestamps (`<startTs>_<endTs>.oplogs`); lexical sort = chronological order |
| Replay correctness | `mongorestore --oplogReplay --oplogFile <file>` routes to shard primary via full RS connection string |
| Post-restore verification | `preSnap ≤ postRestore ≤ T2_mark` range assertion; throws on violation |
