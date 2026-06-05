# Test: Failover, Compliance, and Edge-Case Scenarios

Covers the remaining test matrix items from the MongoDB Third-Party Backup Testing Checklist
that are relevant to this implementation but not already covered by `Test-SnapshotRestore.md`
(which covers Tests 1–3: basic restore, restore under load, and PIT restore).

> Tests 1–3 in `Test-SnapshotRestore.md` map to:
> - Self Restore Sharded Cluster with Embedded Config (Tests 1 & 2)
> - PIT Self Restore Sharded Cluster with Embedded Config (Test 3)

```bash
# Run once at the start of any test session.
source .venv/bin/activate              # puts the console scripts (new-mongo-snapshot, …) on PATH
set -a && . ./.env && set +a           # export SSH_*/MONGOS_*/… for the shell snippets below
SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=no)
```

```bash
mongos() {
    ssh "${SSH_OPTS[@]}" "${SSH_USER}@${MONGOS_HOST}" \
        "${MONGOSH_PATH} --quiet --eval '$1' mongodb://${MONGOS_HOST}:${MONGOS_PORT} 2>/dev/null"
}
```

> **Manual Ops Manager API calls** below use the project's own `config.invoke_om_api` helper (the same
> Digest-auth client every command uses), driven from a short `python3` heredoc. This replaces the
> PowerShell `Invoke-OmApi` cmdlet and reads `OM_*` settings from `.env` automatically.

---

## Test 4 — Node down before taking a snapshot

Confirms the snapshot pre-flight correctly detects and rejects a degraded cluster before opening
a backup cursor, rather than producing an incomplete snapshot.

### Setup

Identify a secondary node to simulate as down. The cluster will still have a quorum and a
primary in each replica set.

```bash
# Confirm current cluster topology
mongos 'db.adminCommand({listShards:1}).shards'
```

### Steps

**1. Stop the automation agent on one secondary node (e.g. aen-mongo-03).**

```bash
ssh "${SSH_OPTS[@]}" "${SSH_USER}@aen-mongo-03" 'sudo systemctl stop mongodb-mms-automation-agent'
```

Wait ~30 seconds for OM to mark the node as unreachable.

**2. Attempt a snapshot.**

```bash
new-mongo-snapshot
```

**3. Restart the agent to restore the cluster.**

```bash
ssh "${SSH_OPTS[@]}" "${SSH_USER}@aen-mongo-03" 'sudo systemctl start mongodb-mms-automation-agent'
```

### Expected Results

`new-mongo-snapshot` STEP 0 includes an explicit node-health gate (it rejects the run if any node
reports `memberState = DOWN`). Depending on how quickly OM reflects the stopped agent, one of:

- **OM has marked the node DOWN** → STEP 0 aborts at the health gate with
  `Cannot take snapshot while N node(s) are DOWN` **before** any OM job is created. Cleanest outcome.
- **OM still reports the node UP/snapshotable** → STEP 1 selects a *different* snapshotable secondary
  for each replica set and the snapshot proceeds; or, if the down node was dispatched a cursor-open,
  OM fails the job and the `finally` block calls `/fail` (see `Test-FailoverAndCompliance-Results.md`).
- It is also acceptable to fall back to the primary if no snapshotable secondary is available for a
  replica set.

In every case the snapshot must **not** silently succeed with missing shard coverage. After restarting
the agent in step 3, a clean snapshot should succeed.

---

## Test 5 — Node down before taking an oplog snapshot

Confirms that when the preferred oplog tailing node is unavailable, the oplog snapshot job
either fails cleanly or (after tailer restart with a new node selected) resumes correctly.

### Steps

**1. Start the oplog tailer on a known tag.**

```bash
# Terminal A
start-oplog-tailer --snapshot-tag "om-20260516-failover"
```

Note which node was selected as the preferred tailing node per shard (printed in pre-flight).

**2. Stop the automation agent on the preferred tailing node.**

```bash
# Use the node printed by the tailer in step 1
ssh "${SSH_OPTS[@]}" "${SSH_USER}@aen-mongo-02" 'sudo systemctl stop mongodb-mms-automation-agent'
```

**3. Observe the tailer behavior.**

The in-flight oplog snapshot job will get stuck in `PENDING` (the agent on the selected node
is not responding). After `--timeout-minutes`, the job will fail.

**4. Stop the tailer.**

```bash
stop-oplog-tailer --snapshot-tag "om-20260516-failover"
```

**5. Restart the agent on the failed node.**

```bash
ssh "${SSH_OPTS[@]}" "${SSH_USER}@aen-mongo-02" 'sudo systemctl start mongodb-mms-automation-agent'
```

**6. Restart the tailer — it will re-select a healthy preferred node.**

```bash
# Terminal B — new run; tailer re-runs preferredOplogNodes selection
start-oplog-tailer --snapshot-tag "om-20260516-failover"
```

### Expected Results

- Step 3: The stuck job transitions to `FAILED` after the timeout; the tailer catches this,
  calls `/fail`, and logs the error. A `gap-<timestamp>.json` marker is written if `previousEnd`
  continuity is broken.
- Step 6: The restarted tailer selects a new preferred node (OM updates `snapshotable` flags
  after agent reconnects) and resumes normally.
- Verify the gap marker is present before using the oplog stream for a PIT restore:
  ```bash
  ls ~/mongo-oplog-stream/om-20260516-failover/gap-*.json 2>/dev/null
  ```

---

## Test 6 — Node fails during a snapshot

Confirms the snapshot cleanup path — specifically that `/fail` is called and the backup cursor
is released when a failure occurs after `/start` but before `/finish`.

### Steps

**1. Start a snapshot.**

```bash
new-mongo-snapshot
```

**2. Immediately after the `STEP 3: Creating snapshot job` output appears (before READY is
reached), kill the command with Ctrl-C.**

This simulates a mid-flight failure after the backup cursor is open.

### Expected Results

- The `finally` block prints `Calling /fail to release backup cursor on <snapshotId>`.
- OM transitions the job to `FAILING → FAILED`.
- Verify via the OM API:
  ```bash
  python3 - <<'PY'
  from mongodb_flasharray_backup import config
  cfg = config.load_config()
  detail = config.invoke_om_api(path=f"group/{cfg.GroupId}/clusters/{cfg.ClusterId}")
  snap_id = detail.get("snapshotId")
  print(config.invoke_om_api(path=f"group/{cfg.GroupId}/clusters/{cfg.ClusterId}/snapshot/{snap_id}").get("state"))
  PY
  ```
  Expected: `FAILED`
- A new snapshot can be started immediately after — no lingering cursor.

---

## Test 7 — Node fails during a restore

Confirms the volume overwrite in STEP 4 does not silently continue past a per-volume
failure, and that the cluster is left in a known (aborted) state.

### Steps

**1. Take a fresh snapshot to restore from.**

```bash
new-mongo-snapshot
```

Note the tag.

**2. Begin a restore with `--force` and inject a failure.**

The restore is done at the storage layer (FA volume overwrite); automation-agent state does not
affect STEP 4 directly. To exercise a genuine pre-destruction abort, use a `--snapshot-tag` that
does not exist on the arrays:

```bash
restore-mongo-snapshot --snapshot-tag "om-99991231-999999" --force
```

### Expected Results

- For a non-existent snapshot tag: the STEP 0 pre-flight check (`Verify the target snapshot
  exists on all context arrays`) fails — `Snapshot '<tag>' found on 0 of N expected arrays` — and the
  command aborts **before any destructive action** is taken.
- For a real mid-STEP-4 failure: STEP 4 overwrites each volume **sequentially** with a 3-attempt retry
  per volume. Any volume that fails all retries is collected into `restore_failures`; after the loop the
  command raises `FlashArray volume restore failed on N volume(s)` and **no subsequent steps run**
  (rescan/remount, agent restart). (This differs from the parallel steps 1–3/5–6, which use
  `invoke_parallel_or_throw`; the volume overwrite itself is sequential.)
- The cluster is in a partially overwritten state — do not attempt to start agents without
  completing the restore. Re-run with a valid tag to complete.

---

## Test 8 — PIT restore after changing preferred oplog node

Confirms that PIT coverage is maintained across a preferred-node change: all oplog segments
before and after the change are contiguous and `invoke-oplog-replay` replays them correctly.

### Steps

**1. Start the load generator.**

```bash
# Terminal A
start-insert-load
```

**2. Start the oplog tailer on Node A (initial preferred node).**

```bash
# Terminal B
start-oplog-tailer --snapshot-tag "om-20260516-nodechange"
```

Note the preferred nodes selected (printed in pre-flight).

**3. Take a base snapshot.**

```bash
# Terminal C
new-mongo-snapshot
```

**4. Let the tailer run for ~60 seconds to capture a few oplog segments.**

**5. Stop the tailer and restart it — simulating a node change by bouncing the tailer.**

```bash
stop-oplog-tailer --snapshot-tag "om-20260516-nodechange"
```

If you want to force a different preferred node, temporarily bring down the current preferred
secondary before restarting:

```bash
ssh "${SSH_OPTS[@]}" "${SSH_USER}@aen-mongo-02" 'sudo systemctl stop mongodb-mms-automation-agent'
start-oplog-tailer --snapshot-tag "om-20260516-nodechange"
ssh "${SSH_OPTS[@]}" "${SSH_USER}@aen-mongo-02" 'sudo systemctl start mongodb-mms-automation-agent'
```

**6. Let the tailer run for another ~60 seconds.**

**7. Stop the load, then stop the tailer.**

```bash
# Ctrl-C in Terminal A, then:
stop-oplog-tailer --snapshot-tag "om-20260516-nodechange"
```

**8. Restore and replay.**

```bash
restore-mongo-snapshot --snapshot-tag "om-20260516-nodechange"
invoke-oplog-replay    --snapshot-tag "om-20260516-nodechange"
```

### Expected Results

- No `gap-*.json` markers in the oplog stream directory (the tailer's `previousEnd` chain
  remained contiguous across the node change):
  ```bash
  ls ~/mongo-oplog-stream/om-20260516-nodechange/gap-*.json 2>/dev/null
  ```
  Expected: no files.
- Post-replay range assertion passes: `preSnap <= postReplay <= T2_mark`.
- A post-replay count strictly above the `mongo:postSnap` tag confirms oplog segments from both the
  pre- and post-node-change periods were applied.

---

## Test 9 — Verification: oplog tailer node selection on 3-node cluster

Confirms that `start-oplog-tailer` correctly selects one preferred node per replica set
from the three available nodes (`aen-mongo-01`, `aen-mongo-02`, `aen-mongo-03`).

### Steps

**1. Verify all three nodes appear in the OM cluster detail.**

```bash
python3 - <<'PY'
from mongodb_flasharray_backup import config
cfg = config.load_config()
detail = config.invoke_om_api(path=f"group/{cfg.GroupId}/clusters/{cfg.ClusterId}")
for rs in detail.get("replicaSets", []):
    for n in rs.get("nodes", []):
        print(n.get("hostname"), n.get("memberState"), "snapshotable=", n.get("snapshotable"), "hidden=", n.get("hidden"))
PY
```

Expected: all three nodes visible, `snapshotable: true`, `hidden: false`.

**2. Start the tailer.**

```bash
start-oplog-tailer --snapshot-tag "om-20260516-newnode"
```

**3. Confirm one preferred node is selected per replica set.**

The pre-flight output lists one chosen node per RS from among `aen-mongo-01`,
`aen-mongo-02`, and `aen-mongo-03`.

### Expected Results

- `preferredOplogNodes` is registered with OM with one entry per replica set.
- Confirm via the OM API:
  ```bash
  python3 - <<'PY'
  from mongodb_flasharray_backup import config
  cfg = config.load_config()
  detail = config.invoke_om_api(path=f"group/{cfg.GroupId}/clusters/{cfg.ClusterId}")
  print(detail.get("preferredOplogNodes"))
  PY
  ```

---

## Test 10 — Verification: restore validates a complete, compatible target before any destructive action

Confirms `restore-mongo-snapshot` refuses to touch a single volume unless the target snapshot is
present and consistent on every relevant array.

> **Implementation note (differs from the PowerShell original):** the PowerShell version validated the
> restore target by reading a `~/mongo-snapshots/<tag>.json` sidecar and calling `POST /clusters` with
> its `snapshotMetadata`. The Python port has **no sidecar and no `POST /clusters` step**. Instead, all
> restore metadata lives in the FlashArray snapshot **tags**, and STEP 0 validates at the storage layer:
> the snapshot must exist on every context array, every node's member snapshot must exist, and each
> live volume's size must match its snapshot — otherwise the command aborts before any overwrite.

### Steps

**1. Take a snapshot and note the tag.**

```bash
new-mongo-snapshot
```

**2. Inspect the metadata tags the restore will consume** (replace `<tag>`):

```bash
python3 - <<'PY'
from mongodb_flasharray_backup import config
cfg = config.load_config()
fa = config.connect_fa()
ctx = config.resolve_fa_context_names(fa, cfg.ProtectionGroupName)
tags = config.get_fa_snapshot_tags(fa, ctx, f"{cfg.ProtectionGroupName}.<tag>")
for k, v in tags.items():
    print(f"{k} = {v}")
PY
```

Expected: `mongo:volumes` (the data volumes captured), `mongo:preSnap` / `mongo:postSnap`
(restore-side baseline), and `mongo:t1ts` (the PITR oplog anchor).

**3. Run the restore and watch STEP 0 validation (cancel at the prompt — no volumes are touched).**

```bash
restore-mongo-snapshot --snapshot-tag "<tag>"
```

Look in the STEP 0 output for the validation gates before the destructive confirmation prompt:

- `Snapshot found: <pg>.<tag> on <array>` for **every** context array, then `All N snapshots confirmed.`
- `Member verified: <pg>.<tag>.<vol> (size <bytes>)` for **every** node volume.
- `All node volume members confirmed in snapshot.`

### Expected Results

- STEP 0 prints the snapshot-found / member-verified / size-match lines for all arrays and volumes
  **before** asking for confirmation.
- If the snapshot is missing on any array, a member snapshot is missing, or a live volume's size no
  longer matches its snapshot, STEP 0 aborts before any volume is overwritten.
- If you cancel at the prompt (`Ctrl-C` or wrong tag), no volumes are touched.

---

## Test 11 — Verification: oplog gap check before PIT restore

Confirms that gap markers are detectable before initiating a PIT restore, and that
`invoke-oplog-replay` behavior is understood when gaps are present.

### Steps

**1. Artificially create a gap.** The tailer compares each job's `previousEnd` against the `lastEnd`
stored in `state.json`; a mismatch writes a `gap-<timestamp>.json` marker. The most reliable way to
force one is to overwrite `state.json` with a `lastEnd` far in the past, then run another iteration:

```bash
# Terminal A — start the tailer, wait for one job to FINISH, then Ctrl-C it
start-oplog-tailer --snapshot-tag "om-20260516-gaptest"

# Overwrite state.json with a fake lastEnd far in the past (schema matches the tailer's writer)
cat > ~/mongo-oplog-stream/om-20260516-gaptest/state.json <<'JSON'
{
  "snapshotTag": "om-20260516-gaptest",
  "totalJobs": 1,
  "lastJobId": "fake",
  "lastEnd": { "time": 1000000000, "inc": 1 },
  "updatedUtc": "2026-01-01T00:00:00Z"
}
JSON

# Next tailer iteration will detect the previousEnd/lastEnd mismatch and write a gap marker
start-oplog-tailer --snapshot-tag "om-20260516-gaptest"
```

> Note: simply deleting `state.json` does **not** create a gap — with no stored `lastEnd` there is
> nothing to compare against, so the next run starts a fresh chain with no marker.

**2. Check for gap markers before any PIT replay.**

```bash
if ls ~/mongo-oplog-stream/om-20260516-gaptest/gap-*.json >/dev/null 2>&1; then
    echo "GAP DETECTED — PIT restore unreliable in this window:"
    cat ~/mongo-oplog-stream/om-20260516-gaptest/gap-*.json
else
    echo "No gaps detected — PIT restore is safe."
fi
```

### Expected Results

- Gap marker written as `gap-<timestamp>.json` containing `detectedUtc`, `storedLastEnd`,
  `omPreviousEnd`, and `jobId`.
- `invoke-oplog-replay` does not check for gap markers automatically — the operator must
  run the check above before initiating replay against a stream with known gaps. (Run the tailer
  with `--abort-on-gap` to make the tailer itself stop the moment a gap is detected.)
- **Action item**: add a gap-marker pre-check to `invoke-oplog-replay` (see `tasks/todo.md`).

---

## Test 12 — Verification: fail requests sent on bad state

Confirms the `/fail` cleanup path fires on both snapshot and oplog snapshot failures.

### Snapshot `/fail` path (covered partially by Test 6)

Add a deliberate failure by setting an unreachable endpoint in `.env` (`OM_BASE_URL`) after
`/start` succeeds but before `/finish`. The `finally` block must call `/fail`.

Check the OM snapshot state via the API after the command exits:

```bash
python3 - <<'PY'
from mongodb_flasharray_backup import config
cfg = config.load_config()
detail = config.invoke_om_api(path=f"group/{cfg.GroupId}/clusters/{cfg.ClusterId}")
print(config.invoke_om_api(path=f"group/{cfg.GroupId}/clusters/{cfg.ClusterId}/snapshot/{detail.get('snapshotId')}").get("state"))
PY
```

Expected: `FAILED`

### Oplog snapshot `/fail` path

Interrupt `start-oplog-tailer` mid-job (after `POST /start`, before `POST /finish`) with
`Ctrl-C`. The catch block in the tailer loop must call `/fail` on the in-flight job:

```bash
python3 - <<'PY'
from mongodb_flasharray_backup import config
cfg = config.load_config()
detail = config.invoke_om_api(path=f"group/{cfg.GroupId}/clusters/{cfg.ClusterId}")
print(config.invoke_om_api(path=f"group/{cfg.GroupId}/clusters/{cfg.ClusterId}/oplogSnapshot/{detail.get('oplogSnapshotId')}").get("state"))
PY
```

Expected: `FAILED`
