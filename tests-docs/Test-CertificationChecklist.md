# MongoDB 3rd-Party Backup — Certification Checklist (mapped to this implementation)

This maps the MongoDB *3rd-Party Backup Testing Checklist* to **mongodb-flasharray-backup** (Pure Storage
FlashArray + Fusion + Ops Manager third-party backup). Each item is marked with its applicability to this
implementation, how to run it, and current status. Detailed step-by-step procedures live in
[Test-SnapshotRestore.md](Test-SnapshotRestore.md) (Tests 1–3) and
[Test-FailoverAndCompliance.md](Test-FailoverAndCompliance.md) (failover / verification); recorded results live
alongside in the `*-Results.md` files.

## Test Summary

Outcome: ✅ pass · ⚠️ partial / design-difference · 🔲 in progress / blocked · ❌ out of scope. Detailed
per-item status and rationale follow in the sections below.

| Test | Outcome | What occurred |
|---|---|---|
| **2.A.e** Self-restore, embedded config (full snapshot) | ✅ Pass | Test 1 (no load): exact recovery, **drift 0**. Test 2 (under load): post-restore count within `[preSnap, postSnap]`, crash-consistency confirmed at ~334 docs/s (2026-06-05). |
| **2.B.e** PIT self-restore, embedded config | ✅ Pass (full forward recovery) | Restore to T1 exact; replay-all advanced past T1 across all 3 shards incl. the `config` shard. With a drain wait, **`unrecoveredTail=0`** (tag `om-20260608-165149`). |
| **2.B.c** PIT after preferred-node change | ✅ Pass (continuity) | Restart-continuity + a forced node-change (stopped an agent → tailer reselected a different node); **0 gap markers** across the change. Forced-change + forward-advance not yet combined in one pass. |
| **3.a** Node down before snapshot / oplog snapshot | ✅ Pass | Agent-reachability pre-check (`ssh systemctl is-active`) routes around a down agent or fails loud instead of dispatching to it (Tests 4 & 5). |
| **3.b** Node fails during a snapshot | ✅ Pass | Mid-flight interrupt triggers `finally`→`/fail`, releasing the backup cursor (Test 6). |
| **3.c** Node fails during a restore | ✅ Pass | STEP 0 pre-flight aborts before destructive steps; sequential overwrite aborts on a per-volume failure (Test 7). |
| **4.a** Preferred oplog node (+ when a node is added) | ✅ Pass | Selection validated; **add-node reselection confirmed live** — the snapshot opened shard_2's backup cursor on the newly-added `aen-mongo-04:27022`. |
| **4.b** Valid restore-target endpoint before restore | ⚠️ Design difference | Validates at the **storage layer** (snapshot present on every array, per-member, size match) rather than OM's `POST /clusters` endpoint (which 400'd and is redundant for in-place self-restore). |
| **4.c** No oplog gaps before PIT restore | ✅ Pass | `invoke-oplog-replay` refuses on `gap-*.json` markers (`--allow-gaps` overrides). Capture-lag (~2–3 min) documented as expected, not a gap. |
| **4.d** Status polling during snapshot/restore | ✅ Pass | Polls `snapshot`/`oplogSnapshot`/cluster state every run. |
| **4.e** Fail requests on bad state | ✅ Pass (snapshot) / ⚠️ oplog | Snapshot `/fail` fires on interrupt/failure; OM treats `/fail` as a no-op on a stuck `PENDING` *oplog* job (pre-check avoids creating that state). |
| **Add node** — aen-mongo-04 as 4th member of all 3 shards | ✅ Pass | Added via OM `automationConfig` API (v8→v9), clean initial sync. `initialize-protection-groups` auto-created the PG on a **new fleet array** (`sn1-c60-e12-16`) for -04's volume. Snapshot+restore (`om-20260609-075555`): 4 volumes / 4 arrays, **drift 0**. |
| **2.A.c** Add a shard, then restore | ✅ Pass | Added `aen-shard_3` (single-member on -04:27023) via API. Two gotchas surfaced & cleared: (1) the new port had to be opened in the host **firewall** (mongos couldn't reach 27023 → `addShard` hung), (2) a newly-added shard has **no backup job** yet → first full snapshot `500 JOB_NOT_FOUND` until OM's backup daemon caught up. Snapshot+restore (`om-20260609-085646`): 4 shards, **drift 0**. |
| **2.A.d** Remove a shard, then restore | ✅ Pass | Snapshot+restore on the post-removal 3-shard topology: `om-20260610-152019` → restore → mongos up, 3 shards, 3 primaries, **drift 0** (loadtest 39600 / payload 20000). Backup was first recovered (see below) — `removeShard`'s `topologyAbort` had wedged it pre-`STARTED`. | `removeShard` drained the shard to 0 chunks; removed from `automationConfig` → cluster healthy 3-shard, `listShards`=3. Post-removal backup wedged: the `removeShard` fired a **`topologyAbort`** (`backupjobs.clusters.lastAbort`) that cancelled the in-flight backup and left it **stuck below `STARTED`** — shards `synced:false`, so `WTCheckpointResource` returns **"no running members"** → `$backupCursor` never opens → snapshots stall `PENDING`. Catch-22: can't `unmanage` (`stopCluster` needs `STARTED`) or `manage` (`startCluster` needs `INACTIVE`); API + UI disable/enable both hit the `STARTED` wall. **RECOVERED 2026-06-10 (no MongoDB code change):** cleaned stale refs → **force-unmanage** (backed up + deleted the active `backupjobs.clusters`/`jobs`/`thirdparty.jobs` docs, kept history) → **`mongodb-mms` restart** (the blocking lifecycle state is *in-memory*, not persisted) → **`manage`** (fresh rebuild, topology validated, `ACTIVE`) → snapshot `om-20260610-145351` (`6a29b201`) **READY→FINISHED**, FA snaps on 4 arrays. Backup is functional again; see issue #1 (closed). **Next: exercise the restore + verify drift 0.** Cluster data + automation healthy throughout. |
| **2.A.g / 2.A.h** Config conversion (embedded ↔ dedicated config server) | 🔲 In progress | **config-00 provisioned**: created 1 TB FA volume `aen-mongo-config-00-data` on sn1-c60-e12-16 → connected to host group `vc01-Workload-Cluster-1` → attached as a physical RDM via PowerCLI → `xfs` mounted at `/data/mongo`. The conversion itself (Option A: add config-00 to the config-server RS, then `transition{To,From}DedicatedConfigServer` + snapshot/restore each direction) is now **unblocked** — backup was recovered (see 2.A.d) and snapshots work again. |

**Cross-cutting findings (this effort):**
- **Oplog capture lag (~2–3 min):** the recoverable PIT point trails the live oplog until segments are captured; PIT tests must drain past the target before relying on it (not a defect — replay applies 100% of captured segments, 0 gaps).
- **New shard/member on a new port** must have that port opened in the node firewall, and a newly-added shard needs its **backup job initialized** (transient `JOB_NOT_FOUND`) before the first full snapshot.
- **Shard removal can wedge OM third-party backup — and how to recover it (significant finding):** `removeShard` fires a **`topologyAbort`** that cancels the in-flight backup and leaves it **stuck below the `STARTED` lifecycle state** (shards `synced:false`, `workingOn:false`). The agents then get **"no running members for group"** from `WTCheckpointResource`, so the `$backupCursor` is never opened and every snapshot stalls `PENDING`. It's a catch-22: `unmanage` needs `STARTED`, `manage` needs `INACTIVE`, and the OM UI disable/enable hits the same wall (`THIRD_PARTY_UNMANAGE_BACKUP_REQUEST_FAILED` / `not in STARTED state`). Diagnosed from OM's own logs (`mms0.log`/`daemon.log`). **Recovery (verified, no MongoDB code change):** (1) clean stale node refs in `backupjobs.thirdparty.jobs`/`jobs` + clear `backupjobs.clusters.lastAbort`; (2) **force-unmanage** — back up then delete the active config docs (`backupjobs.clusters`, per-shard `backupjobs.jobs`, `backupjobs.thirdparty.jobs`), keeping history; (3) **restart `mongodb-mms`** — the blocking lifecycle state is *in-memory*, not in any appdb collection, so the restart is what makes `manage` work; (4) `POST /manage` rebuilds fresh + validates topology (`ACTIVE`); (5) snapshot → `READY→FINISHED`. Cluster data + automation healthy throughout; only OM's backup metadata is involved. **2.A.d backup is recovered; 2.A.d restore + 2.A.g/h config conversion are now unblocked.**
- **Tool hardening:** `new-mongo-snapshot` STEP 0 now tolerates a failed pre-check GET of the prior snapshot (stale after a topology change) instead of crashing.

## Status legend
- ✅ **Validated** — exercised live against `aen-cluster` (see referenced doc / results).
- 🟡 **Supported, not yet tested** — handled by the implementation; not exercised here.
- ⚙️ **Supported with operator procedure** — works, but requires the noted setup step.
- ⚠️ **Gap / differs** — applicable cert item the implementation handles differently or only partially.
- ❌ **Not applicable** — out of scope for this implementation (rationale given).

## Scope of this implementation (read first — it determines applicability)
- **Sharded clusters only.** The tooling discovers topology via `mongos`/`listShards` and the Ops Manager cluster
  API. A **standalone replica set** (no `mongos`) is **out of scope** → all *Replica Set Tests* are ❌.
- **In-place self-restore only.** `restore-mongo-snapshot` overwrites the *source* cluster's own FlashArray
  volumes from their snapshots (a CoW pointer swap), then WiredTiger recovers. **Restoring to a *different*
  cluster/replica set is not implemented** → "Restore … to different …" items are ❌.
- **Full snapshots only.** FlashArray protection-group snapshots are always full (CoW); `new-mongo-snapshot`
  takes full snapshots and stores no incremental chain (`srcBackupName` is never used). **All *Incremental Backup
  Tests* are ❌** (not a concept for storage-snapshot backup).
- **Topology:** validated on a 3-shard cluster with an **embedded config shard** (`config`/`aen-shard_0`,
  `aen-shard_1`, `aen-shard_2`), one FlashArray data volume per node in a Fusion fleet, driven by the hybrid
  gateway-routed FA client.

## Data-insertion & verification methodology (per the checklist)
- **Non-PIT:** insert **data set A** → `new-mongo-snapshot` → insert **data set B** → restore → verify **A and
  only A** (B is correctly lost). Realized here via `start-insert-load` (bulk) and/or `mongos` (marker docs); the
  snapshot embeds `mongo:preSnap`/`mongo:postSnap` count baselines into the FA snapshot tags and STEP 8 of the
  restore asserts the post-restore count falls in `[preSnap, postSnap]` (the consistency window).
- **PIT:** `start-oplog-tailer` → insert **A** → `new-mongo-snapshot` (T1) → insert **B** → (tailer keeps
  capturing) → insert **C** → `stop-oplog-tailer` (writes the T2 mark) → restore to T1 → `invoke-oplog-replay`
  to a target timestamp **after B** → verify **A+B and only A+B** (C beyond the target is excluded).
  `invoke-oplog-replay` asserts `preSnap ≤ postReplay ≤ T2`.

```bash
# Run once per session.
source .venv/bin/activate
set -a && . ./.env && set +a
SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=no)
mongos() { ssh "${SSH_OPTS[@]}" "${SSH_USER}@${MONGOS_HOST}" "${MONGOSH_PATH} --quiet --eval '$1' mongodb://${MONGOS_HOST}:${MONGOS_PORT} 2>/dev/null"; }
```

---

## 1. Replica Set Tests — ❌ Not applicable
This implementation targets **sharded** clusters (requires `mongos`/`listShards`). A standalone replica set is out
of scope, so 1.A/1.B (self / different-RS / arbiter, full & incremental, non-PIT & PIT) are all **❌ N/A**. (A
single-shard sharded cluster would be covered by the Sharded tests below, not these.)

---

## 2. Sharded Cluster Tests

### 2.A Full Snapshot Restore — Non-Incremental

| # | Test | Applicability | How / Status |
|---|---|---|---|
| a | Self Restore Sharded Cluster | ✅ Supported | In-place self-restore. Validated on the embedded-config cluster (see 2.A.e). A dedicated-config cluster would run identically. |
| b | Restore Sharded Cluster *(to different)* | ❌ N/A | In-place self-restore only — no cross-cluster restore. |
| c | Add a shard, then restore | ⚙️ Supported w/ procedure | Add the shard, then **`initialize-protection-groups`** so the new node's data volume is added to the PG (the snapshot's STEP 0 gate hard-fails if any discovered data volume isn't a PG member). Topology is re-discovered every run (`get_cluster_nodes` + SCSI-serial map), so the new shard is picked up automatically. **🟡 not yet tested.** |
| d | Remove a shard, then restore | ⚙️ Supported w/ procedure | After removing the shard, optionally `initialize-protection-groups --prune` to drop the orphaned volume. Runtime discovery reflects the smaller topology; restore targets only the discovered (current) volumes via the `mongo:volumes` tag. **🟡 not yet tested.** |
| e | **Self Restore Sharded Cluster with Embedded Config** | ✅ **Validated** | This is `aen-cluster`. **Test 1** (no load) and **Test 2** (under load) — see [Test-SnapshotRestore.md](Test-SnapshotRestore.md); 2026-06-05 results: Test 1 drift 0, Test 2 post-restore within `[preSnap, postSnap]`. |
| f | Restore Sharded Cluster w/ Embedded Config *(to different)* | ❌ N/A | In-place self-restore only. |
| g | Convert Dedicated→Embedded config, then restore | ⚙️ Supported w/ procedure | Discovery keys per-shard artifacts by canonical shard id (`config` for the embedded config shard); after the conversion, re-run `initialize-protection-groups` and snapshot. **🟡 not yet tested.** |
| h | Convert Embedded→Dedicated config, then restore | ⚙️ Supported w/ procedure | Same as (g) in reverse. **🟡 not yet tested.** |

### 2.A Full Snapshot Restore — Incremental — ❌ Not applicable
2.A.2.a/b (incremental, with/without embedded config): **❌ N/A** — FlashArray snapshots are always full;
`new-mongo-snapshot` takes full snapshots only.

### 2.B PIT Restore — Non-Incremental

| # | Test | Applicability | How / Status |
|---|---|---|---|
| a | PIT Self Restore Sharded Cluster | ✅ Supported | In-place PIT self-restore. Validated on embedded config (see 2.B.e); dedicated config identical. |
| b | PIT Restore Sharded Cluster *(to different)* | ❌ N/A | In-place self-restore only. |
| c | PIT Restore After Changing Preferred Node | ✅ **Validated** | Both variants run live: **restart-continuity** (Test 8, 06-05 — 0 gaps + forward replay across all shards) **and** a **forced node-change** (06-08 — stopping `aen-mongo-01`'s agent before the bounce made the agent-health pre-check reselect `aen-shard_2` from `aen-mongo-01`→`aen-mongo-02`, with **0 gap markers** across the change). On the forced run forward *advance* was a no-op because the oplog stream had gone ~2 days stale (captured segments pre-dated T1; range assertion held at the T1 baseline). The two halves are each independently proven on a current stream: **continuity across a forced node-change** (this forced run, 0 gaps) and **forward advance with full recovery** (2.B.e, tag `om-20260608-165149`, `unrecoveredTail==0`); the only combination not yet run *together* is forced-node-change + forward-advance in a single pass. **Operational notes:** (1) the tailer must run *continuously* — a stale stream yields no-op replays (reinforces 4.c); a stale stream can be re-baselined to ~now by a *skip-forward* (create one oplog snapshot spanning `[stale cursor → now]`, `/finish` it without copying — the cursor advances and OM discards the skipped window). (2) OM `.oplog` segments lag live writes by ~2–3 min, so a PIT recovery point trails live by that lag until drained — see 4.c note. |
| d | PIT Restore to Sharded Cluster with Arbiter | 🟡 Supported, not tested | Arbiters hold no data and are never `snapshotable`, so node selection skips them; data-bearing members snapshot/replay normally. Not yet exercised with an arbiter present. |
| e | **PIT Self Restore Sharded Cluster with Embedded Config** | ✅ **Validated (full recovery)** | This is `aen-cluster`. **Test 3** — true forward PITR demonstrated (2026-06-05, tag `om-20260605-195104`: post-replay advanced past T1 across all 3 shards incl. `config`). Re-confirmed end-to-end on a current stream (2026-06-08, tag `om-20260608-165149`): A=20000 → restore lands exactly at T1 (20000) → replay-all recovers to the **full captured T2 (39600, `unrecoveredTail==0`)** across all 3 shards, **0 gap markers**. See [Test-SnapshotRestore.md](Test-SnapshotRestore.md). |
| f | PIT Restore Sharded Cluster w/ Embedded Config *(to different)* | ❌ N/A | In-place self-restore only. |

### 2.B PIT Restore — Incremental — ❌ Not applicable
2.B.2.a/b: **❌ N/A** — full snapshots only.

---

## 3. Failover Tests

| # | Test | Status |
|---|---|---|
| a | Detect & handle a node down before taking a snapshot / oplog snapshot | ✅ **SOLVED & validated** — **Tests 4 (snapshot) & 5 (oplog tailer)**. Both `new-mongo-snapshot` STEP 1 and `start-oplog-tailer` now run an **agent-reachability pre-check** (`ssh systemctl is-active mongodb-mms-automation-agent`) and route around a down agent (or fail loud) instead of dispatching to it. Validated 2026-06-06 — see the *agent-reachability fix* section in [Test-FailoverAndCompliance-Results.md](Test-FailoverAndCompliance-Results.md). |
| b | Detect & handle a node fails during a snapshot | ✅ **Validated** — **Test 6**: interrupting mid-flight triggers the `finally`→`/fail`, releasing the backup cursor (OM job → `FAILING`/`FAILED`). |
| c | Detect & handle a node fails during a restore | ✅ **Validated** — **Test 7**: STEP 0 pre-flight aborts before any destructive action on a missing/incomplete target; STEP 4 overwrites sequentially and aborts (no subsequent steps) on a per-volume failure. |

---

## 4. Verification Tests

| # | Test | Status |
|---|---|---|
| a | Set a preferred oplog-tailing node (incl. when a new node is added) | ✅ Validated (selection) / 🟡 add-node — **Test 9**: `start-oplog-tailer` registers one `preferredOplogNodes` entry per RS, re-selecting on every run (so a newly-added node is considered next run). The explicit *add-a-node-then-confirm-reselection* hasn't been run. |
| b | Call the valid restore-target endpoint before restore | ⚠️ **Design difference (not the OM endpoint)** — validates the restore target at the **storage layer** in STEP 0 (snapshot present on every context array, every member snapshot present, live-volume size matches the snapshot — **Test 10**), which directly verifies the snapshot is restorable. It does **not** call OM's `POST …/clusters` valid-target endpoint; a 2026-06-08 probe of that endpoint returned **`400`** on the documented path/body, and its cross-cluster compatibility checks (shard count, version, FCV, encryption) are largely redundant for this tool's **in-place self-restore** (target == source). The full `snapshotMetadata` *is* available from the FINISHED job if the literal OM call is later required. |
| c | Validate no oplog gaps before a PIT restore | ✅ **Closed (2026-06-08)** — gaps are detected/recorded (`gap-*.json`, **Test 11**), and **`invoke-oplog-replay` now refuses to run when any gap marker is present** — it errors out *before* touching the cluster (validated); `--allow-gaps` overrides with a warning. `start-oplog-tailer --abort-on-gap` also halts the tailer the moment a gap appears. **Capture-lag note (2026-06-08):** OM `.oplog` segments become available ~2–3 min after the writes (60s windows + agent/OM processing), so the recoverable point trails the live oplog by that lag. This is not a gap and not a defect — `invoke-oplog-replay` applies 100% of captured segments in order and reports `unrecoveredTail` (in-range = expected). A PIT recovery to a specific target must ensure the tailer's `lastEnd` has passed that target (drain the lag) before relying on it; continuous production tailing satisfies this automatically. |
| d | Call status endpoints during Snapshots and Restores | ✅ **Validated** — `new-mongo-snapshot` polls `GET …/snapshot/{id}` to READY/FINISHED (`wait_om_snapshot_state`); the oplog tailer polls `GET …/oplogSnapshot/{id}`; restore polls cluster state until stable. Exercised in every snapshot/restore/PITR run this session. |
| e | Send fail requests when a Snapshot or Restore is in a bad state | ✅ Validated (snapshot) / ⚠️ oplog OM-limited — snapshot `/fail` fires on interrupt/failure (**Test 6**) and on OM-detected failure (**Test 4** results). The oplog tailer attempts `/fail` too, **but OM treats `/fail` as a no-op on a stuck `PENDING` oplog job** (**Test 5** finding — an OM-side limitation); the agent-reachability pre-check now avoids creating that stuck state in the first place. |

---

## Summary for certification

| Area | Verdict |
|---|---|
| Sharded **self-restore**, full snapshot, embedded config (2.A.e) | ✅ Validated |
| Sharded **PIT self-restore**, embedded config (2.B.e) | ✅ Validated (full forward recovery, `unrecoveredTail==0`, current stream) |
| Forced preferred-node change (2.B.c) | ✅ Validated (0 gaps across change); forced-change + forward-advance in one pass not yet combined |
| Restore under load / consistency window | ✅ Validated |
| Failover: node-down pre-flight (3.a), fail-during-snapshot (3.b), fail-during-restore (3.c) | ✅ Validated (3.a solved this session) |
| Verification: status polling (4.d), fail-on-bad-state (4.e snapshot) | ✅ Validated |
| Add/remove shard, config conversion, arbiter | 🟡 Supported, not yet exercised |
| Valid-restore-target endpoint (4.b) | ⚠️ Storage-layer validation instead of the OM endpoint |
| Auto gap-check before replay (4.c) | ✅ Enforced — replay refuses on gap markers (`--allow-gaps` overrides) |
| Replica-set tests; restore-to-different; all incremental | ❌ Out of scope (sharded, in-place self-restore, full-only) |

**Cert-readiness read:** the in-scope, applicable scenarios that have been exercised all pass — including full
forward PIT recovery on a current stream (2.B.e, `unrecoveredTail==0`) and node-change continuity (2.B.c, 0 gaps);
the remaining applicable items (add/remove shard, config conversion, arbiter, and the forced-node-change +
forward-advance *combined* pass) are supported by design but not yet run, and two verification items (4.b OM
valid-target endpoint, 4.c auto gap-check) are **conscious design differences** that should be confirmed against
the certifier's exact requirements before submission.
