# Test Results: Failover, Compliance, and Edge-Case Scenarios

Run date: 2026-05-16  
Cluster: `aen-cluster` — 3 shards (`config`/`aen-shard_0`, `aen-shard_1`, `aen-shard_2`), 3 nodes each (`aen-mongo-01`–`03`)  
OM version: 8.0.23 | MongoDB: 8.0.21-ent

---

## Test 4 — Node down before taking a snapshot

**Result: PARTIAL PASS**

### What was done

1. Confirmed cluster topology: 3 shards, 3 nodes each, all healthy.
2. Stopped the automation agent on `aen-mongo-02` (secondary on all shards):
   ```
   sudo systemctl stop mongodb-mms-automation-agent
   ```
3. Waited 35 seconds for OM to detect the failure.
4. Ran `new-mongo-snapshot` — observed the full output.
5. Restarted the agent on `aen-mongo-02` and confirmed cluster stability.
6. Queried the OM API for the snapshot job to confirm OM recorded the failure.

### Observed behavior

| Phase | Result |
|---|---|
| STEP 0 pre-flight | Passed — `aen-mongo-02` still listed as `snapshotable: true` in OM after 35s |
| STEP 1 node selection | Selected `aen-mongo-02:27020` for `aen-shard_0` (OM hadn't yet flipped `snapshotable=false`) |
| STEP 3–4 | Job `6a086df557c4d953749d2ccb` created, `/start` called, job stuck `PENDING` |
| OM failure detection | OM transitioned job to `FAILED` (agent not responding to cursor-open request) |
| `wait_om_snapshot_state` | Detected `FAILED` state, threw exception |
| `finally` block | `/fail` called → `Backup cursor released` printed → script exited with error |
| FA snapshot | **Never fired** — job never reached `READY` |

### OM API confirmation

Queried `GET .../snapshot/6a086df557c4d953749d2ccb` after the test:

```json
{
  "state": "FAILED",
  "nodes": [
    { "id": "aen-mongo-02:27020", "rsId": "aen-shard_0", "snapshotable": true, "lastAgentPing": "2026-05-16T13:22:25Z" },
    { "id": "aen-mongo-01:27021", "rsId": "aen-shard_1", "snapshotable": true },
    { "id": "aen-mongo-01:27022", "rsId": "aen-shard_2", "snapshotable": true }
  ]
}
```

`snapshotable: true` on `aen-mongo-02` after the test confirms the agent was back up and OM re-registered it. `lastAgentPing` during the test would have been stale — the job got stuck because OM dispatched the cursor-open to a non-responding agent, then failed the job itself.

### Pass/fail breakdown

- ✅ Cleanup path correct — `/fail` called, cursor released, no leaked job
- ✅ FA snapshot never fired — no inconsistent storage snapshot produced
- ✅ Cluster returned to full health after agent restart
- ⚠️ Script did not proactively skip the down node — OM takes >35s to flip `snapshotable=false`. The script selected `aen-mongo-02` and relied on OM to detect the stuck job rather than detecting agent-down at pre-flight. This is the current design; it relies on OM as the failure detector rather than independent agent-health checking.

### Cluster state after test

All 3 members returned to correct SECONDARY/PRIMARY roles on all 3 shards. Verified via `rs.status()`.

---

## Test 5 — Node down before taking an oplog snapshot

**Result: PARTIAL PASS**

### What was done

1. Confirmed cluster topology: 3 shards, 3 nodes each, all healthy.
2. Stopped the automation agent on `aen-mongo-02` (secondary on all shards):
   ```
   sudo systemctl stop mongodb-mms-automation-agent
   ```
3. Immediately (without waiting for OM to detect the failure) ran `start-oplog-tailer`:
   ```
   start-oplog-tailer --snapshot-tag "om-20260516-090000"
   ```
4. Observed tailer pre-flight and loop behavior until the script exited.
5. Queried the OM API for the oplog snapshot job to confirm its final state.

### Observed behavior

| Phase | Result |
|---|---|
| Pre-flight warning | `group/settings` returned 401 → WARNING printed, tailer continued |
| Node selection | `aen-shard_0` → `aen-mongo-02:27020` [SECONDARY] — down node selected |
| `preferredOplogNodes` set | OM told to tail `aen-mongo-01:27022`, `aen-mongo-02:27020`, `aen-mongo-01:27021` |
| Job creation | Job `6a08709b57c4d953749d45b9` created at `08:26:51` |
| OM auto-failure | **Never occurred** — OM did not transition job from `PENDING` to `FAILED` |
| Poll duration | Job polled every 5s from `08:26:57` to script timeout — continuously `PENDING` |
| 30-minute deadline | Script deadline fired at ~`08:56:51`; exception thrown: `Oplog snapshot … timed out waiting for READY` |
| Catch block | Printed error, called `POST /oplogSnapshot/{id}/fail` |
| `/fail` result | OM did not honor `/fail` on a `PENDING` oplog job — job remained `PENDING` |
| Script exit | Exited with code `1` |
| Oplog data captured | **None** — no segments copied, no `state.json` written |

### OM API confirmation

Queried `GET .../oplogSnapshot/6a08709b57c4d953749d45b9` after the test:

```json
{
  "ranges": [],
  "state": "PENDING"
}
```

Job remained `PENDING` throughout the test and after script exit. OM never transitioned it to `FAILED` or `FINISHED`.

### Critical findings

1. **OM does not auto-fail stuck oplog snapshot jobs.** Unlike block snapshot jobs (Test 4, where OM flipped state to `FAILED` in ~35s after the agent stopped responding), oplog snapshot jobs that are dispatched to an unreachable agent remain `PENDING` indefinitely. No automatic failure detection was observed during the 30-minute test window.

2. **`POST /fail` does not work on PENDING oplog jobs.** Calling the `/fail` endpoint on a job that has never left `PENDING` state had no effect — OM kept the job `PENDING`. This leaves a leaked job in OM.

3. **30-minute oplog coverage gap.** From job creation at `08:26:51` to script timeout at `~08:56:51`, no oplog data was captured. Any PIT restore targeting a window that spans this interval would be unrecoverable.

4. **Behavior differs significantly from Test 4 (block snapshot).** Block snapshot failure was detected and resolved within ~35s by OM's own agent heartbeat mechanism. Oplog snapshot failures require the script's own timeout (default 30 minutes) as the only safety valve.

### Pass/fail breakdown

- ✅ Script's timeout safety valve fired correctly — loop did not hang forever
- ✅ Catch block executed — `/fail` was attempted, error was logged
- ✅ No FlashArray snapshot was taken (tailer never reached the FA snapshot phase)
- ✅ Cluster mongod processes unaffected — only the OM agent was stopped
- ⚠️ OM did not auto-detect or auto-fail the stuck oplog job (unlike block snapshots)
- ⚠️ `/fail` on a PENDING oplog job is a no-op — job leaked in OM
- ⚠️ 30-minute oplog gap created — PIT restore coverage lost for that window
- ⚠️ Script does not check agent health before setting preferred oplog nodes — same root cause as Test 4

### Recommended improvement

Add an agent-reachability pre-flight check in `start-oplog-tailer` before setting `preferredOplogNodes`. If any preferred node's agent is not responding (queryable via `GET group/{id}/agents/MONITORING` or direct TCP probe), skip that node and select the next available secondary. This would prevent dispatching oplog jobs to unreachable agents and avoid the 30-minute coverage gap.

### Cluster state after test

`aen-mongo-02` automation agent is **still stopped** — must be restarted before Test 6. All mongod processes remained running throughout; replica set membership was unaffected.

---

## Tests 6–11 — live validation (2026-06-05, hybrid gateway-routed client)

Run against `aen-cluster` with `FA_ENDPOINT=sn1-x90r2-f06-27` (object/read ops routed through the Fusion
gateway via `context_names`; tag ops direct per array). All snapshots/restores used the validated hybrid
client. (Contrary to the 2026-05-16 Test 5 end-state above, all automation agents are healthy — every node
reports `snapshotable=true`.)

| Test | Result | Evidence |
|---|---|---|
| 6 — snapshot mid-flight interrupt → `/fail` | ✅ PASS | `SIGINT` after `/start` (PENDING) → `Calling /fail to release backup cursor on 6a23788c…` → `Backup cursor released`; OM job → `FAILING`/`FAILED`. *(Required resetting the SIGINT disposition to default before exec — background jobs inherit `SIGINT=ignore`; a harness detail, not a product issue.)* |
| 7 — restore aborts on bad/incomplete target | ✅ PASS | `restore --snapshot-tag om-99991231-999999 --force` → `Snapshot '…' found on 0 of 3 expected arrays — aborting before any changes`. STEP 1 never reached → no destruction. *(Only the bad-tag pre-flight abort is meaningfully injectable; a literal node failure doesn't affect the storage-layer STEP 4 overwrite.)* |
| 8 — PITR continuity across tailer bounce | ✅ PASS | Load → tailer → T1 → **bounce tailer** → continue → restore → replay. **0 gap markers** (continuity preserved); all 3 shards incl. `config/` replayed; post-restore 2,996,846 → **post-replay 3,021,646** ∈ `[preSnap 2,996,246, T2 3,089,246]`, `unrecoveredTail=67,600`. *(The bounce reselected the same preferred nodes — deterministic selection — so this proves restart-continuity; the optional forced-node-change variant was not run.)* |
| 9 — oplog tailer node selection (3-node) | ✅ PASS | All 9 members `snapshotable=true, hidden=false`; `preferredOplogNodes` = one node per RS (e.g. `aen-mongo-01:27022`, `aen-mongo-02:27020`, `aen-mongo-01:27021`). |
| 10 — restore validates target before destruction | ✅ PASS | Valid tag → STEP 0 prints `Snapshot found … on` all 3 arrays / `All 3 snapshots confirmed` / `Member verified … (size …)` ×3 / `All node volume members confirmed`; declined at the prompt → STEP 1 never reached → no destruction. |
| 11 — oplog gap detection | ✅ PASS | Forced via a far-past `state.json` `lastEnd` → `gap-*.json` written with `detectedUtc`/`storedLastEnd`/`omPreviousEnd`/`jobId`; tailer logged `Oplog gap: stored lastEnd=(1000000000:1) but OM previousEnd=(…)`. |

**Test 12 — not run (needs rewrite).** Its snapshot `/fail` injection (edit `OM_BASE_URL` in `.env` mid-run)
does not work in the Python port — `config.load_config()` reads `.env` once at process start, so a mid-run edit
has no effect (use Test 6's interrupt instead). Its oplog `/fail` expectation (`FAILED`) also contradicts the
Test 5 finding that OM leaves stuck `PENDING` oplog jobs with `/fail` a no-op. The `/fail` paths themselves are
covered by Tests 5 (oplog) and 6 (snapshot).

---

