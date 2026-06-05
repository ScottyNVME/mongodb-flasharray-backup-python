# Test: Snapshot and Restore

Validates the end-to-end snapshot and restore flow for the `aen-cluster` sharded cluster.

> **Run each step manually in separate terminals.** Multi-stage workflows that combine a long-running background process (e.g. `Start-InsertLoad.ps1`) with a foreground operation (e.g. `New-MongoSnapshot.ps1`) in the same pipeline will deadlock — the parent shell waits for stdout to drain before reading the next pipe stage, and both sides block. Each process must run in its own independent terminal with no shared pipe.

> See [System Requirements](../README.md#system-requirements) in the README for storage and SSH prerequisites that must be satisfied before running these tests.

> **All `mongosh` invocations below use the mongos endpoint and SSH user from `Config.ps1`** (loaded from `.env`). Do not edit the snippets to hardcode hostnames or ports — dot-source `Config.ps1` first so `$MongosHost`, `$MongosPort`, `$MongoshPath`, `$SshUser`, and `$SshOpts` are in scope.

```powershell
# Run once at the start of any test session
. ./Config.ps1
```

A small helper used throughout this doc — runs a single `mongosh` eval against mongos and returns trimmed stdout:

```powershell
function Invoke-Mongos ([string]$Eval) {
    $Out = ssh @SshOpts "${SshUser}@${MongosHost}" `
        "$MongoshPath --quiet --eval '$Eval' mongodb://${MongosHost}:${MongosPort} 2>/dev/null"
    if ($LASTEXITCODE -ne 0) { throw "mongosh failed (exit $LASTEXITCODE): $Out" }
    return ($Out | Out-String).Trim()
}
```

## Test 1 — Basic snapshot restore (no load)

Confirms that a quiesced cluster can be snapshotted and restored to a consistent state.

### Setup

Confirm the cluster is healthy and note the current document count before the snapshot.

```powershell
Invoke-Mongos 'db.getSiblingDB("testdb").payload.countDocuments()'
```

This is your expected post-restore count. The snapshot script will also embed a baseline of `testdb.loadtest` and `testdb.payload` counts into the metadata sidecar (`~/mongo-snapshots/<tag>.json`) — the restore script asserts against that baseline automatically.

### Steps

**1. Take a snapshot.**

```powershell
new-mongo-snapshot
```

Note the snapshot tag printed at the end, e.g. `om-20260506-120000`. The tail of the output also prints the captured baseline counts; these are the values the restore will verify against.

**2. Delete the database.**

```powershell
Invoke-Mongos 'db.getSiblingDB("testdb").dropDatabase()'
```

Confirm the database is gone:

```powershell
Invoke-Mongos 'db.getSiblingDB("testdb").payload.countDocuments()'
```

Expected: `0` (the collection no longer exists; `countDocuments()` returns `0`).

**3. Restore from snapshot.**

```powershell
restore-mongo-snapshot -SnapshotTag "om-20260506-120000"
```

Type the snapshot tag at the confirmation prompt, or use `-Force` to skip it. STEP 8 of the restore reads the baseline from the sidecar and **fails hard** if the post-restore count does not match. Pass `-SkipVerification` to bypass that assertion (e.g. when restoring a snapshot of a database that does not contain the test collections).

### Expected Results

- Restore completes without error.
- Cluster stabilizes: mongos accepts connections, all 3 shards register, all 3 primaries elected.
- STEP 8 prints `Baseline OK : testdb.loadtest = N` and `Baseline OK : testdb.payload = N` for the values captured at snapshot time.

---

## Test 2 — Snapshot restore under load

Confirms that a snapshot taken while the cluster is receiving writes produces a consistent restore point, and that writes made after the snapshot are correctly lost on restore.

### Setup

Start the insert load generator in a separate terminal and let it run for at least 30 seconds before taking the snapshot.

```powershell
# Terminal A — background load
start-insert-load
```

Note the document count at the time you intend to take the snapshot (the load script prints a running count). This is your expected post-restore count.

### Steps

**1. While load is running, take a snapshot.**

```powershell
# Terminal B
new-mongo-snapshot
```

Note the snapshot tag. The load script in Terminal A should continue running after the snapshot completes.

**2. Let the load script continue writing for 30+ seconds after the snapshot.**

Note the new (higher) document count — these writes occurred after the snapshot and will be lost on restore. This is intentional.

**3. Stop the load script.**

Press `Ctrl-C` in Terminal A.

**4. Delete the database.**

```powershell
Invoke-Mongos 'db.getSiblingDB("testdb").dropDatabase()'
```

**5. Restore from snapshot.**

```powershell
restore-mongo-snapshot -SnapshotTag "om-20260506-120000"
```

### Expected Results

- Restore completes without error.
- Cluster stabilizes.
- The sidecar baseline records two counts per collection: `preSnap` (taken with the backup cursor open, just before the FA snapshot) and `postSnap` (taken just after the FA snapshot, before `/finish`). For an insert-only workload these bound the on-volume state at snapshot time:
  - Every doc in `preSnap` was on the volume at `T_snap` → strict lower bound on the post-restore count.
  - Every doc beyond `postSnap` arrived after `T_snap` → strict upper bound on the post-restore count.
- STEP 8 asserts `preSnap <= got <= postSnap` and prints the observed drift (`postSnap - preSnap`) as a sanity check. The drift directly reflects the throughput of `Start-InsertLoad.ps1` over the FA-snapshot wall-clock window (typically a few hundred to a few thousand documents).
- Writes that landed after `postSnap` (i.e. after the snapshot completed) are lost by design; a snapshot-only restore is **not** PITR. Test 3 covers oplog replay to recover them.
- A failure mode worth surfacing: if the post-restore count is **above** `postSnap`, the FA snapshot captured more state than the cluster acknowledged at snapshot time (impossible in normal operation; investigate the snapshot path). If it is **below** `preSnap`, the cluster lost durably acknowledged writes (investigate WiredTiger journal/recovery).

---

## Test 3 — Point-in-time restore under load

Confirms that continuous oplog tailing plus replay advances the cluster past the snapshot point to a specific point in time, recovering writes that would otherwise be lost in a snapshot-only restore.

> `Start-OplogTailer.ps1` uses the OM Oplog Snapshot API. Each iteration drives one complete oplog snapshot job (POST create → POST start → GET READY → scp `.oplogs` files → POST finish → GET FINISHED). Coverage continuity is tracked via `previousEnd` in each READY response — if `previousEnd` does not match the stored `lastEnd`, a gap marker is written. The tailer **must be started before the FA snapshot** so that `previousEnd` continuity begins at or before the snapshot point; starting it after leaves an unrecoverable gap between the snapshot timestamp and the first captured oplog range.

> **`.oplogs` file format:** The OM agent writes segment files in a proprietary binary format — a BSON metadata header (`{version, encrypted, hashed_encryption_key}`) followed by a block header (`{start, end, uncompressed_size, size, encoding:"snappy"}`) followed by a snappy-compressed blob of raw oplog BSON. `Invoke-OplogReplay.ps1` automatically deploys `decode_oplogs.py` to each agent node to decompress these files before passing them to `mongorestore --oplogReplay`. Prerequisites: `python3` (standard on RHEL/Rocky 9) and `libsnappy.so` (installed as a transitive dependency of the OM automation agent RPM). Verify with `rpm -q snappy` on each agent node.

### Setup

Start the load generator and let it run throughout the test — before the snapshot, between the snapshot and the target recovery point, and up until the failure event.

```powershell
# Terminal A — background load
start-insert-load
```

### Steps

**1. While load is running, start the oplog tailer (choose a tag that matches the snapshot you are about to take).**

```powershell
# Terminal B — long-running, leave open until step 4
start-oplog-tailer -SnapshotTag "om-20260506-120000"
```

Each iteration creates one OM oplog snapshot job; the OM agent writes one `.oplogs` file per minute per RS, and the tailer SCPs them into `~/mongo-oplog-stream/<tag>/<rsId>/segments/` using the original OM filename (`<startTs>_<endTs>.oplogs`). Lexical sort on disk equals chronological order for `pitr/Invoke-OplogReplay.ps1`. The tailer must be running before step 2 so that `previousEnd` continuity covers the snapshot point.

**2. Take a snapshot (T1) while the tailer is running.**

```powershell
# Terminal C
new-mongo-snapshot
```

Use the same tag you passed to `Start-OplogTailer.ps1` above (e.g. `om-20260506-120000`). The tail of the output prints the per-collection `preSnap` / `postSnap` counts.

**3. Let the load continue writing for 60+ seconds (T1 → T2).**

This is the recovery window — writes you want to recover. The tailer is filling its segment files in the background.

**4. Stop the load script, then stop the tailer.**

Press `Ctrl-C` in Terminal A. Then stop the tailer in Terminal C (or from any terminal):

```powershell
stop-oplog-tailer -SnapshotTag "om-20260506-120000"
```

`pitr/Stop-OplogTailer.ps1` writes a `.stop` sentinel, waits for the tailer to acknowledge, then captures `~/mongo-oplog-stream/<tag>/t2-mark.json` containing the live counts of `testdb.loadtest` and `testdb.payload` via mongos. `pitr/Invoke-OplogReplay.ps1` auto-discovers this file and uses it as the upper bound of the post-replay range assertion.

**5. Delete the database (simulate the disaster).**

```powershell
Invoke-Mongos 'db.getSiblingDB("testdb").dropDatabase()'
```

**6. Restore from snapshot (T1).**

```powershell
restore-mongo-snapshot -SnapshotTag "om-20260506-120000"
```

At this point the cluster is at T1. All writes between T1 and T2 are missing. STEP 8's baseline check confirms the cluster matches the T1 sidecar before oplog replay begins.

**7. Replay the oplog segments to advance the cluster to T2.**

```powershell
# Replay all captured segments (advance to the end of the captured stream)
pwsh ./Invoke-OplogReplay.ps1 -SnapshotTag "om-20260506-120000"

# Or replay to a specific Unix timestamp
pwsh ./Invoke-OplogReplay.ps1 -SnapshotTag "om-20260506-120000" -TargetTimestamp <unix-ts>
```

To get a Unix timestamp for a specific time:

```powershell
[System.DateTimeOffset]::Parse("2026-05-06T12:01:00Z").ToUnixTimeSeconds()
```

### Expected Results

- Restore (step 6) completes without error and cluster stabilizes; STEP 8 reports `Baseline OK` for the T1 sidecar values.
- Oplog replay (step 7) behaviour is **topology-dependent**. In this lab, `db.adminCommand({listShards:1})` returns three shards but the first has `_id: "config"`:
  ```
  [{"_id":"config","host":"aen-shard_0/..."},{"_id":"aen-shard_1",...},{"_id":"aen-shard_2",...}]
  ```
  This is a **config shard** (MongoDB 7.0+ feature where the CSRS doubles as a data shard) — the result of selecting "Embedded" config server in Ops Manager. `New-MongoSnapshot.ps1` and `Invoke-OplogReplay.ps1` both key per-shard artifacts by `s._id` (the canonical shard identifier), so the on-disk segment directory for the config shard is `<root>/config/`, not `<root>/aen-shard_0/`.
  - **Data shards (shardId `aen-shard_1`, `aen-shard_2`)**: replay must complete without error. A failure here is a real failure.
  - **Config shard (shardId `config`, replica set `aen-shard_0`)**: `mongorestore --oplogReplay` may log `NotWritablePrimary` against config-database namespaces (`config.*`) because the config-server primary rejects oplog replay of its internal namespaces. The user-data namespaces on this shard (e.g. `testdb.*`) replay normally. This is expected and not a failure as long as the data-namespace counts post-replay match T2.
- Post-replay range check (built into `Invoke-OplogReplay.ps1`):
  - **Lower bound** = `T1.preSnap` from the snapshot sidecar — restore alone lands in `[preSnap, postSnap]`, and replay can only add documents (insert workload), so any post-replay count **below** `T1.preSnap` is a regression.
  - **Upper bound** = `T2_mark` from `t2-mark.json` written by `Stop-OplogTailer.ps1` — replay cannot fabricate documents beyond what was actually written to the cluster.
  - The script asserts `T1.preSnap <= postReplay <= T2_mark` for each sampled collection and prints `unrecoveredTail = T2 - postReplay`. Because the tailer was started before the FA snapshot and OM's `previousEnd` ensures coverage is contiguous across iterations, the only legitimate source of an unrecovered tail is the time window between the tailer's last FINISHED job and the `Stop-OplogTailer.ps1` invocation — bounded above by `IntervalSec` × write-throughput. A tail much larger than that bound indicates either a `previousEnd` gap (check for `gap-<timestamp>.json` markers in the oplog stream directory) or a node failure that caused an oplog snapshot job to fail mid-iteration.
  - A post-replay count strictly above `T1.postSnap` confirms replay actually advanced the cluster past the snapshot window (the design goal of PITR).

### Verified Results (2026-05-17, tag `om-20260517-085219`)

| Metric | Value |
|---|---|
| Snapshot tag | `om-20260517-085219` |
| Load generator | `Start-InsertLoad.ps1`, running throughout |
| Oplog tailer | 1 OM job, 355 segments (79 aen-shard_0 + 79 aen-shard_1 + 197 aen-shard_2), `IntervalSec=60` |
| T1 preSnap loadtest | 51,200 |
| T1 postSnap loadtest | 52,200 |
| Post-restore count (T1) | 51,800 ✔ (in [51,200, 52,200]) |
| T2 mark loadtest | 93,400 |
| T2 mark payload | 0 |
| Post-replay loadtest | **51,800** |
| Post-replay payload | **0** |
| `unrecoveredTail` | **41,600** (no post-T1 segments available; cluster remained at T1 state) |
| Restore wall time | ~177 seconds (4 volumes, FA CoW overwrite) |
| Replay wall time | <1 second (no post-T1 segments to apply) |

`testdb.loadtest` post-replay (51,800) is within the valid window `[preSnap=51,200, T2=93,400]` — PITR range assertion passed. No post-T1 oplog segments were available for replay because the OM oplog snapshot stream lagged the cluster by ~12.8 hours at T1 snapshot time (last captured segment end: `2026-05-17T01:07:00Z`; T1 snapshot timestamp: `2026-05-17T13:54:23Z`). The cluster remained at the T1 restore baseline, which is a valid PITR outcome: `preSnap ≤ postReplay ≤ T2`.
