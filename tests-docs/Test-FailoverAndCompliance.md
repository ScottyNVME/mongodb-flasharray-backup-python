# Test: Failover, Compliance, and Edge-Case Scenarios

Covers the remaining test matrix items from the MongoDB Third-Party Backup Testing Checklist
that are relevant to this implementation but not already covered by `Test-SnapshotRestore.md`
(which covers Tests 1–3: basic restore, restore under load, and PIT restore).

> Tests 1–3 in `Test-SnapshotRestore.md` map to:
> - Self Restore Sharded Cluster with Embedded Config (Tests 1 & 2)
> - PIT Self Restore Sharded Cluster with Embedded Config (Test 3)

```powershell
# Run once at the start of any test session
. ./Config.ps1
```

```powershell
function Invoke-Mongos ([string]$Eval) {
    $Out = ssh @SshOpts "${SshUser}@${MongosHost}" `
        "$MongoshPath --quiet --eval '$Eval' mongodb://${MongosHost}:${MongosPort} 2>/dev/null"
    if ($LASTEXITCODE -ne 0) { throw "mongosh failed (exit $LASTEXITCODE): $Out" }
    return ($Out | Out-String).Trim()
}
```

---

## Test 4 — Node down before taking a snapshot

Confirms the snapshot pre-flight correctly detects and rejects a degraded cluster before opening
a backup cursor, rather than producing an incomplete snapshot.

### Setup

Identify a secondary node to simulate as down. The cluster will still have a quorum and a
primary in each replica set.

```powershell
# Confirm current cluster topology
Invoke-Mongos 'db.adminCommand({listShards:1}).shards'
```

### Steps

**1. Stop the automation agent on one secondary node (e.g. aen-mongo-03).**

```powershell
ssh @SshOpts "${SshUser}@aen-mongo-03" 'sudo systemctl stop mongodb-mms-automation-agent'
```

Wait ~30 seconds for OM to mark the node as unreachable.

**2. Attempt a snapshot.**

```powershell
new-mongo-snapshot
```

**3. Restart the agent to restore the cluster.**

```powershell
ssh @SshOpts "${SshUser}@aen-mongo-03" 'sudo systemctl start mongodb-mms-automation-agent'
```

### Expected Results

- `New-MongoSnapshot.ps1` should either:
  - Complete successfully using the remaining snapshotable nodes (the down node is not
    selected because OM marks it as non-snapshotable when the agent is offline), **or**
  - Abort at pre-flight if no snapshotable secondary is available for any replica set and
    fall back to the primary — which is acceptable.
- The snapshot must **not** silently succeed with missing shard coverage.
- After restarting the agent in step 3, a clean snapshot should succeed.

---

## Test 5 — Node down before taking an oplog snapshot

Confirms that when the preferred oplog tailing node is unavailable, the oplog snapshot job
either fails cleanly or (after tailer restart with a new node selected) resumes correctly.

### Steps

**1. Start the oplog tailer on a known tag.**

```powershell
# Terminal A
start-oplog-tailer -SnapshotTag "om-20260516-failover"
```

Note which node was selected as the preferred tailing node per shard (printed in pre-flight).

**2. Stop the automation agent on the preferred tailing node.**

```powershell
# Use the node printed by the tailer in step 1
ssh @SshOpts "${SshUser}@aen-mongo-02" 'sudo systemctl stop mongodb-mms-automation-agent'
```

**3. Observe the tailer behavior.**

The in-flight oplog snapshot job will get stuck in `PENDING` (the agent on the selected node
is not responding). After `TimeoutMinutes`, the job will fail.

**4. Stop the tailer.**

```powershell
stop-oplog-tailer -SnapshotTag "om-20260516-failover"
```

**5. Restart the agent on the failed node.**

```powershell
ssh @SshOpts "${SshUser}@aen-mongo-02" 'sudo systemctl start mongodb-mms-automation-agent'
```

**6. Restart the tailer — it will re-select a healthy preferred node.**

```powershell
# Terminal B — new run; tailer re-runs preferredOplogNodes selection
start-oplog-tailer -SnapshotTag "om-20260516-failover"
```

### Expected Results

- Step 3: The stuck job transitions to `FAILED` after the timeout; the tailer catches this,
  calls `/fail`, and logs the error. A `gap-<timestamp>.json` marker is written if `previousEnd`
  continuity is broken.
- Step 6: The restarted tailer selects a new preferred node (OM updates `snapshotable` flags
  after agent reconnects) and resumes normally.
- Verify the gap marker is present before using the oplog stream for a PIT restore:
  ```powershell
  Get-ChildItem ~/mongo-oplog-stream/om-20260516-failover/gap-*.json
  ```

---

## Test 6 — Node fails during a snapshot

Confirms the snapshot cleanup path — specifically that `/fail` is called and the backup cursor
is released when a failure occurs after `/start` but before `/finish`.

### Steps

**1. Start a snapshot.**

```powershell
new-mongo-snapshot
```

**2. Immediately after the `STEP 3: Creating snapshot job` output appears (before READY is
reached), kill the script with Ctrl-C.**

This simulates a mid-flight failure after the backup cursor is open.

### Expected Results

- The `finally` block prints `Calling /fail to release backup cursor on <snapshotId>`.
- OM transitions the job to `FAILING → FAILED`.
- Verify via the OM API:
  ```powershell
  . ./Config.ps1
  $Detail = Invoke-OmApi -Path "group/$GroupId/clusters/$ClusterId"
  Invoke-OmApi -Path "group/$GroupId/clusters/$ClusterId/snapshot/$($Detail.snapshotId)"
  | Select-Object -ExpandProperty state
  ```
  Expected: `FAILED`
- A new snapshot can be started immediately after — no lingering cursor.

---

## Test 7 — Node fails during a restore

Confirms the parallel volume overwrite in STEP 4 does not silently continue past a per-node
failure, and that the cluster is left in a known (aborted) state.

### Steps

**1. Take a fresh snapshot to restore from.**

```powershell
new-mongo-snapshot
```

Note the tag.

**2. Begin a restore with `-Force` and inject a failure by stopping the agent on one node
mid-restore.**

Open a second terminal and watch the restore output. When `STEP 4: Overwriting FlashArray
volumes` begins, immediately run:

```powershell
ssh @SshOpts "${SshUser}@aen-mongo-03" 'sudo systemctl stop mongodb-mms-automation-agent'
```

The restore is done at the storage layer (FA volume overwrite); agent state does not affect
STEP 4 directly. To test a genuine STEP 4 failure, use a bad `-SnapshotTag` that does not
exist on one array:

```powershell
restore-mongo-snapshot -SnapshotTag "om-99991231-999999" -Force
```

### Expected Results

- For a non-existent snapshot tag: the pre-flight check at STEP 0 (`Verify the target snapshot
  exists on all context arrays`) fails before any destructive action is taken.
- For a real mid-STEP-4 failure: `Invoke-ParallelOrThrow` throws after all parallel results
  are collected; the script aborts and prints which node failed. No subsequent steps run.
- The cluster is in a partially overwritten state — do not attempt to start agents without
  completing the restore. Re-run with a valid tag to complete.

---

## Test 8 — PIT restore after changing preferred oplog node

Confirms that PIT coverage is maintained across a preferred-node change: all oplog segments
before and after the change are contiguous and `Invoke-OplogReplay.ps1` replays them correctly.

### Steps

**1. Start the load generator.**

```powershell
# Terminal A
start-insert-load
```

**2. Start the oplog tailer on Node A (initial preferred node).**

```powershell
# Terminal B
start-oplog-tailer -SnapshotTag "om-20260516-nodechange"
```

Note the preferred nodes selected (printed in pre-flight).

**3. Take a base snapshot.**

```powershell
# Terminal C
new-mongo-snapshot
```

**4. Let the tailer run for ~60 seconds to capture a few oplog segments.**

**5. Stop the tailer and restart it — simulating a node change by bouncing the tailer.**

```powershell
stop-oplog-tailer -SnapshotTag "om-20260516-nodechange"
```

If you want to force a different preferred node, temporarily bring down the current preferred
secondary before restarting:

```powershell
ssh @SshOpts "${SshUser}@aen-mongo-02" 'sudo systemctl stop mongodb-mms-automation-agent'
start-oplog-tailer -SnapshotTag "om-20260516-nodechange"
ssh @SshOpts "${SshUser}@aen-mongo-02" 'sudo systemctl start mongodb-mms-automation-agent'
```

**6. Let the tailer run for another ~60 seconds.**

**7. Stop the load, then stop the tailer.**

```powershell
# Ctrl-C in Terminal A, then:
stop-oplog-tailer -SnapshotTag "om-20260516-nodechange"
```

**8. Restore and replay.**

```powershell
restore-mongo-snapshot -SnapshotTag "om-20260516-nodechange"
invoke-oplog-replay    -SnapshotTag "om-20260516-nodechange"
```

### Expected Results

- No `gap-*.json` markers in the oplog stream directory (the tailer's `previousEnd` chain
  remained contiguous across the node change):
  ```powershell
  Get-ChildItem ~/mongo-oplog-stream/om-20260516-nodechange/gap-*.json
  ```
  Expected: no files.
- Post-replay range assertion passes: `T1.preSnap <= postReplay <= T2_mark`.
- A post-replay count strictly above `T1.postSnap` confirms oplog segments from both the
  pre- and post-node-change periods were applied.

---

## Test 9 — Verification: oplog tailer node selection on 3-node cluster

Confirms that `Start-OplogTailer.ps1` correctly selects one preferred node per replica set
from the three available nodes (`aen-mongo-01`, `aen-mongo-02`, `aen-mongo-03`).

### Steps

**1. Verify all three nodes appear in the OM cluster detail.**

```powershell
. ./Config.ps1
$Detail = Invoke-OmApi -Path "group/$GroupId/clusters/$ClusterId"
$Detail.replicaSets.nodes | Select-Object hostname, memberState, snapshotable, hidden
```

Expected: all three nodes visible, `snapshotable: true`, `hidden: false`.

**2. Start the tailer.**

```powershell
start-oplog-tailer -SnapshotTag "om-20260516-newnode"
```

**3. Confirm one preferred node is selected per replica set.**

The pre-flight output lists one chosen node per RS from among `aen-mongo-01`,
`aen-mongo-02`, and `aen-mongo-03`.

### Expected Results

- `preferredOplogNodes` is registered with OM with one entry per replica set.
- Confirm via the OM API:
  ```powershell
  $Detail = Invoke-OmApi -Path "group/$GroupId/clusters/$ClusterId"
  $Detail.preferredOplogNodes
  ```

---

## Test 10 — Verification: valid restore target endpoint called before restore

Confirms `Restore-MongoSnapshot.ps1` calls `POST /clusters` with `snapshotMetadata` and rejects
an incompatible target before any destructive action.

### Steps

**1. Take a snapshot and note the tag.**

```powershell
new-mongo-snapshot
```

**2. Verify `snapshotMetadata` is present in the sidecar.**

```powershell
$Sidecar = Get-Content ~/mongo-snapshots/<tag>.json | ConvertFrom-Json
$Sidecar.snapshotMetadata | ConvertTo-Json -Depth 3
```

Expected: non-null object containing `clusterId`, `rsSnapshotsMetadata`, `snapshotTimestamp`.

**3. Manually call the endpoint to confirm it accepts the sidecar metadata.**

```powershell
. ./Config.ps1
$Sidecar = Get-Content ~/mongo-snapshots/<tag>.json | ConvertFrom-Json
$ValidTargets = Invoke-OmApi -Method POST -Path "group/$GroupId/clusters" -Body $Sidecar.snapshotMetadata
$ValidTargets.clusters | Select-Object clusterId, clusterName
```

Expected: the current cluster ID appears in the list.

**4. Run the restore (dry run — confirm the validation passes in STEP 0 output).**

```powershell
restore-mongo-snapshot -SnapshotTag "<tag>"
```

Look for `Restore target validated: cluster <clusterId> is compatible.` in the STEP 0 output
before the destructive confirmation prompt.

### Expected Results

- STEP 0 prints the validation success line before asking for confirmation.
- If you cancel at the prompt (`Ctrl-C` or wrong tag), no volumes are touched.

---

## Test 11 — Verification: oplog gap check before PIT restore

Confirms that gap markers are detectable before initiating a PIT restore, and that
`Invoke-OplogReplay.ps1` behavior is understood when gaps are present.

### Steps

**1. Artificially create a gap by starting the tailer, letting it run one iteration, then
manually deleting the `state.json` to lose the `lastEnd` reference.**

```powershell
# Terminal A — start tailer
start-oplog-tailer -SnapshotTag "om-20260516-gaptest"
# Wait for one job to complete (watch for "Job 1: FINISHED" in output)
# Ctrl-C the tailer, then:
Remove-Item ~/mongo-oplog-stream/om-20260516-gaptest/state.json
# Restart the tailer — it will start a new job with no stored lastEnd
start-oplog-tailer -SnapshotTag "om-20260516-gaptest"
```

Without `state.json`, the tailer has no `lastEnd` to compare against `previousEnd` — no gap
marker is written on the second run. A more realistic gap simulation:

```powershell
# Overwrite state.json with a fake lastEnd far in the past
$FakeState = @{ snapshotTag='om-20260516-gaptest'; totalJobs=1; lastJobId='fake'; lastEnd=@{time=1000000000;inc=1}; updatedUtc=(Get-Date -Format 'o') }
$FakeState | ConvertTo-Json | Set-Content ~/mongo-oplog-stream/om-20260516-gaptest/state.json
# Next tailer iteration will detect mismatch and write gap marker
```

**2. Check for gap markers before any PIT replay.**

```powershell
$GapFiles = Get-ChildItem ~/mongo-oplog-stream/om-20260516-gaptest/gap-*.json -ErrorAction SilentlyContinue
if ($GapFiles) {
    Write-Host "GAP DETECTED — PIT restore unreliable in this window:" -ForegroundColor Red
    $GapFiles | ForEach-Object { Get-Content $_ | ConvertFrom-Json | Select-Object detectedUtc, storedLastEnd, omPreviousEnd }
} else {
    Write-Host "No gaps detected — PIT restore is safe." -ForegroundColor Green
}
```

### Expected Results

- Gap marker written as `gap-<timestamp>.json` containing `storedLastEnd` and `omPreviousEnd`.
- `Invoke-OplogReplay.ps1` does not check for gap markers automatically — the operator must
  run the check above before initiating replay against a stream with known gaps.
- **Action item**: add gap-marker pre-check to `Invoke-OplogReplay.ps1` (see `tasks/todo.md`).

---

## Test 12 — Verification: fail requests sent on bad state

Confirms the `/fail` cleanup path fires on both snapshot and oplog snapshot failures.

### Snapshot `/fail` path (covered partially by Test 6)

Add a deliberate failure by setting an unreachable endpoint in `.env` (`OM_BASE_URL`) after
`/start` succeeds but before `/finish`. The `finally` block must call `/fail`.

Check the OM snapshot state via the API after the script exits:

```powershell
. ./Config.ps1
$Detail = Invoke-OmApi -Path "group/$GroupId/clusters/$ClusterId"
(Invoke-OmApi -Path "group/$GroupId/clusters/$ClusterId/snapshot/$($Detail.snapshotId)").state
```

Expected: `FAILED`

### Oplog snapshot `/fail` path

Interrupt `Start-OplogTailer.ps1` mid-job (after `POST /start`, before `POST /finish`) with
`Ctrl-C`. The catch block in the tailer loop must call `/fail` on the in-flight job:

```powershell
. ./Config.ps1
$Detail = Invoke-OmApi -Path "group/$GroupId/clusters/$ClusterId"
(Invoke-OmApi -Path "group/$GroupId/clusters/$ClusterId/oplogSnapshot/$($Detail.oplogSnapshotId)").state
```

Expected: `FAILED`
