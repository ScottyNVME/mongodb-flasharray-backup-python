# MongoDB 3rd-Party Backup — Certification Checklist (mapped to this implementation)

This maps the MongoDB *3rd-Party Backup Testing Checklist* to **mongodb-flasharray-backup** (Pure Storage
FlashArray + Fusion + Ops Manager third-party backup). Each item is marked with its applicability to this
implementation, how to run it, and current status. **For a shareable, high-level overview, start with
[Certification-Summary.md](Certification-Summary.md).** Detailed step-by-step procedures live in
[Test-SnapshotRestore.md](Test-SnapshotRestore.md) (Tests 1–3) and
[Test-FailoverAndCompliance.md](Test-FailoverAndCompliance.md) (failover / verification); recorded results live
alongside in the `*-Results.md` files.

> **Re-validation run 2026-07-22** (build `5c29659`, post Path-A revert): the four core in-scope tests all
> re-passed live — **1.A.1.a** + **2.A.a** (self-restore, drift 0 / sentinel-gone) and **1.B.1.a** + **2.B.e**
> (PIT, `unrecoveredTail=0`). PIT required a **skip-forward re-baseline** of the OM oplog cursor (stale from
> the ~5-week OM outage; the stream itself was continuous), and the sharded config server needed
> `packer∈mongod` for oplog `scp`. Details in
> [Test-Certification-Results-2026-07-22.md](Test-Certification-Results-2026-07-22.md).

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
| **4.a** Preferred oplog node (+ when a node is added) | ✅ Pass (selection) / 🟡 add-node | Tailer registers one `preferredOplogNodes` entry per RS, reselected every run; snapshot node-selection already adapts to a new member (opened a cursor on the newly-added `aen-mongo-04:27022`). The explicit *add-node → confirm oplog-preferred reselection* hasn't been run as its own test. |
| **4.b** Valid restore-target endpoint before restore | ⚠️ Design difference | Validates at the **storage layer** (snapshot present on every array, per-member, size match) rather than OM's `POST /clusters` endpoint (which 400'd and is redundant for in-place self-restore). |
| **4.c** No oplog gaps before PIT restore | ✅ Pass | `invoke-oplog-replay` refuses on `gap-*.json` markers (`--allow-gaps` overrides). Capture-lag (~2–3 min) documented as expected, not a gap. |
| **4.d** Status polling during snapshot/restore | ✅ Pass | Polls `snapshot`/`oplogSnapshot`/cluster state every run. |
| **4.e** Fail requests on bad state | ✅ Pass (snapshot) / ⚠️ oplog | Snapshot `/fail` fires on interrupt/failure; OM treats `/fail` as a no-op on a stuck `PENDING` *oplog* job (pre-check avoids creating that state). |
| **Add node** — aen-mongo-04 as 4th member of all 3 shards | ✅ Pass | Added via OM `automationConfig` API (v8→v9), clean initial sync. `initialize-protection-groups` auto-created the PG on a **new fleet array** (`sn1-c60-e12-16`) for -04's volume. Snapshot+restore (`om-20260609-075555`): 4 volumes / 4 arrays, **drift 0**. |
| **2.A.c** Add a shard, then restore | ✅ Pass | Added `aen-shard_3` (single-member on -04:27023) via API. Two gotchas surfaced & cleared: (1) the new port had to be opened in the host **firewall** (mongos couldn't reach 27023 → `addShard` hung), (2) a newly-added shard has **no backup job** yet → first full snapshot `500 JOB_NOT_FOUND` until OM's backup daemon caught up. Snapshot+restore (`om-20260609-085646`): 4 shards, **drift 0**. |
| **2.A.d** Remove a shard, then restore | ✅ Pass | Snapshot+restore on the post-removal 3-shard topology: `om-20260610-152019` → restore → mongos up, 3 shards, 3 primaries, **drift 0** (loadtest 39600 / payload 20000). Backup was first recovered (see below) — `removeShard`'s `topologyAbort` had wedged it pre-`STARTED`. | `removeShard` drained the shard to 0 chunks; removed from `automationConfig` → cluster healthy 3-shard, `listShards`=3. Post-removal backup wedged: the `removeShard` fired a **`topologyAbort`** (`backupjobs.clusters.lastAbort`) that cancelled the in-flight backup and left it **stuck below `STARTED`** — shards `synced:false`, so `WTCheckpointResource` returns **"no running members"** → `$backupCursor` never opens → snapshots stall `PENDING`. Catch-22: can't `unmanage` (`stopCluster` needs `STARTED`) or `manage` (`startCluster` needs `INACTIVE`); API + UI disable/enable both hit the `STARTED` wall. **RECOVERED 2026-06-10 (no MongoDB code change):** cleaned stale refs → **force-unmanage** (backed up + deleted the active `backupjobs.clusters`/`jobs`/`thirdparty.jobs` docs, kept history) → **`mongodb-mms` restart** (the blocking lifecycle state is *in-memory*, not persisted) → **`manage`** (fresh rebuild, topology validated, `ACTIVE`) → snapshot `om-20260610-145351` (`6a29b201`) **READY→FINISHED**, FA snaps on 4 arrays. Backup is functional again; see issue #1 (closed). Restore was subsequently exercised on the 3-shard topology → **drift 0** (`om-20260610-152019`, per the outcome above). Cluster data + automation healthy throughout. |
| **2.A.g / 2.A.h** Config conversion (embedded ↔ dedicated config server) | ✅ Pass (both directions) | Done **in-place** on the existing config RS (no new node needed; `transitionToDedicatedConfigServer` / `transitionFromDedicatedConfigServer` change a *logical* state, not hardware — config-00 left staged). **2.A.h embedded→dedicated:** `transitionToDedicatedConfigServer` drained the config shard's chunks to shard_1/shard_2 + `movePrimary testdb→aen-shard_1` (the command pauses for an explicit `movePrimary` of each DB whose primary is the config shard) → `completed`, config shard at **0 user chunks**; backup stayed `ACTIVE`; snapshot `om-20260610-155312` → restore → **drift 0**. **2.A.g dedicated→embedded:** `transitionFromDedicatedConfigServer` → config shard data-bearing again; this re-embed **wedged backup** (cursor "waiting for other shards/configs to load", `lastAbort=null` — milder than the shard-removal wedge), **cleared by a single `mongodb-mms` restart** (no force-unmanage needed); snapshot `om-20260610-161057` → restore → **drift 0**. No data lost across either conversion (39600/20000 throughout). |
| **1.A.1.a** RS self-restore — **standalone replica set** | ✅ Pass | **`aen-rs-00`** (multi-deployment, `--deployment aen-rs-00`): snapshot `om-20260611-124009` → **mutate** (`testdb.loadtest/payload`→0/0 + add `testdb.sentinel`) → restore → **5000/5000, `sentinel` GONE, drift 0** — a true point-in-time rollback (not a no-op). Topology-agnostic node selection + the new RS STEP 7 branch (verify writable primary, no `listShards`). |
| **1.B.1.a** RS **PIT** self-restore | ✅ Pass | `aen-rs-00`, tag `om-20260611-150821`: T1=8000 → insert B (+3000)=11000 → **24 oplog segments** captured → restore→**8000 (T1, drift 0)** → replay-all→**11000 (T2, `unrecoveredTail=0`)**. New `replicaset` branches in `start-oplog-tailer`/`invoke-oplog-replay` (no `listShards`); required `packer`∈`mongod` group on the RS nodes for oplog `scp`. |
| **Restructure** → dedicated config server, then backup recovery | ✅ Pass | `aen-cluster` migrated to a **dedicated config server** (`aen-mongo-config-00`) + 3 data shards (`aen-shard_1/2/3`). The topology change wedged OM third-party backup (`THIRD_PARTY_DISCOVERY_ERROR`; API `unmanage` accepted but stuck `TERMINATING` through 2 restarts); recovered via **orphan cleanup → force-unmanage → `mongodb-mms` restart → `manage`**, plus adding config-00's data volume to `aen-cluster-pg` → snapshot `om-20260611-093257` **FINISHED**. Filed as **issue #2** for MongoDB. |

**Cross-cutting findings (this effort):**
- **Oplog capture lag (~2–3 min):** the recoverable PIT point trails the live oplog until segments are captured; PIT tests must drain past the target before relying on it (not a defect — replay applies 100% of captured segments, 0 gaps).
- **New shard/member on a new port** must have that port opened in the node firewall, and a newly-added shard needs its **backup job initialized** (transient `JOB_NOT_FOUND`) before the first full snapshot.
- **Shard removal can wedge OM third-party backup — and how to recover it (significant finding):** `removeShard` fires a **`topologyAbort`** that cancels the in-flight backup and leaves it **stuck below the `STARTED` lifecycle state** (shards `synced:false`, `workingOn:false`). The agents then get **"no running members for group"** from `WTCheckpointResource`, so the `$backupCursor` is never opened and every snapshot stalls `PENDING`. It's a catch-22: `unmanage` needs `STARTED`, `manage` needs `INACTIVE`, and the OM UI disable/enable hits the same wall (`THIRD_PARTY_UNMANAGE_BACKUP_REQUEST_FAILED` / `not in STARTED state`). Diagnosed from OM's own logs (`mms0.log`/`daemon.log`). **Recovery (verified, no MongoDB code change):** (1) clean stale node refs in `backupjobs.thirdparty.jobs`/`jobs` + clear `backupjobs.clusters.lastAbort`; (2) **force-unmanage** — back up then delete the active config docs (`backupjobs.clusters`, per-shard `backupjobs.jobs`, `backupjobs.thirdparty.jobs`), keeping history; (3) **restart `mongodb-mms`** — the blocking lifecycle state is *in-memory*, not in any appdb collection, so the restart is what makes `manage` work; (4) `POST /manage` rebuilds fresh + validates topology (`ACTIVE`); (5) snapshot → `READY→FINISHED`. Cluster data + automation healthy throughout; only OM's backup metadata is involved. **2.A.d backup is recovered; 2.A.d restore + 2.A.g/h config conversion are now unblocked.**
- **Tool hardening:** `new-mongo-snapshot` STEP 0 now tolerates a failed pre-check GET of the prior snapshot (stale after a topology change) instead of crashing.
- **Multi-deployment support (2026-06-11):** the tool now backs up **both** a sharded cluster and a standalone replica set from one `.env` (`--deployment`, `TOPOLOGY=sharded|replicaset`). Node selection was already OM-`replicaSets`-driven (not `listShards`); added `replicaset` branches to the two remaining `listShards` callers — restore STEP 7 (verify the single RS's writable primary) and the snapshot PIT anchors — leaving the certified sharded paths byte-for-byte unchanged (13/13 unit tests pass). `initialize-protection-groups` now also writes the OM-discovered node list back into the deployment's `CLUSTER_NODES` so the OM-unreachable fallback stays current; the FA client gained `patch_protection_groups` (PG rename). The PG was renamed `aen-mongodb-pg`→`aen-cluster-pg` (Purity cascaded all 81 snapshots).
- **Enabling third-party backup on an RS:** use `POST backup/third_party/.../clusters/{id}/manage` (FA controls snapshots → **no OM snapshot store needed**). The standard managed-backup path (`PATCH backupConfigs statusName=STARTED`) is wrong for third-party and 409s `Could not find available Snapshot Store`.
- **Dedicated-config-server migration also wedges backup (issue #2, the discovery-wedge variant):** after migrating to a dedicated config server + re-adding a shard, the third-party backup wedged with `THIRD_PARTY_DISCOVERY_ERROR` ("inconsistent data from Monitoring & Automation"). New nuances vs the shard-removal wedge: (1) leftover **old config-RS mongods** (`:27020`) must be stopped and their **stale OM monitoring host entries deleted** (`DELETE …/hosts/{id}`) — this made API `unmanage` *accepted* (previously rejected), though it then stuck in `TERMINATING` through two `mongodb-mms` restarts, so the **appdb force-unmanage was still required**; (2) the **dedicated config server's data volume** (on a different array) must be added to the backup PG via `initialize-protection-groups` or the snapshot fails node→volume mapping. Recovery then succeeded end-to-end (`om-20260611-093257` FINISHED). `snapshotSchedule` returning HTTP 500 is a pre-existing OM quirk for third-party configs, **not** a wedge signal.
- **Restore-fidelity proof method:** a same-count drift check (`got ∈ [preSnap, postSnap]`) passes even for a no-op restore. The RS self-restore (1.A.1.a) was therefore proven by forcing **divergence first** (delete all docs → 0/0, add a `sentinel` collection), then confirming the restore reverts to the snapshot point-in-time (5000/5000, **sentinel gone**). Recommend applying this mutate-then-restore method to the sharded self-restore rows too.
- **Per-shard restore verification (added + validated live 2026-06-11):** to satisfy the checklist's *"verify the shards contain the correct data"* explicitly (not just via the mongos-routed total), `restore-mongo-snapshot` STEP 8 now — for `sharded` topology — enumerates shards via `listShards`, connects to each shard's RS directly, counts the verify collections per-shard, prints the distribution, and asserts the per-shard totals account for the mongos aggregate (`sum ≥ routed total`; a shard short of data **fails** the restore, orphaned docs inflate-only and are noted). Replica-set deployments skip it (the single RS *is* the aggregate). **Exercised live** on the 4-shard `aen-cluster` restore (`om-20260611-153131`): reported `aen-shard_1=26339` + `aen-shard_2=13261` = 39600 (= aggregate), with `aen-shard_3`/`config` empty noted-not-failed.
- **Tag-based node→volume mapping + volume-move safety (2026-06-12 / 2026-06-17):** runtime SSH+SCSI discovery of each node's FA volume on *every* snapshot/restore is slow at scale, so `initialize-protection-groups` now precomputes the map and stores it on each volume as copyable `mongo:` tags (`deployment`/`node`/`mountpoint`/`serial`/`vg`/`pvindex`/`pvcount`); the hot path reads tags (one `GET /volumes/tags` per array, **no SSH**), cross-checks each serial, and falls back to live discovery for any untagged/stale node. **Re-run `initialize-protection-groups` after a topology change** to refresh the tags. **Volume moves between arrays are safe by construction** — the owning array is *derived* (not hard-coded), so a moved volume resolves directly (tags travelled), self-heals via SSH rediscovery (tag lost/stale/serial changed), is caught by the `mongo:pvcount` completeness guard (a multi-volume/LVM node missing a PV ⇒ rediscover instead of a silently partial backup), or fails loudly if the destination array has no PG; restore also re-checks volume **count vs `mongo:volumes`**, member-snapshot **existence**, and **size** before the overwrite. Resolve-path + both move fallbacks live-validated on `aen-rs-00`; **end-to-end snapshot+restore with the new tags still pending** (OM was unreachable 2026-06-17), and the **multi-volume/LVM path is unit-tested only — needs live validation on a real LVM cluster** (the lab is single-volume pRDM).
- **Replica-set PIT (1.B.1.a, validated 2026-06-11):** `start-oplog-tailer` node selection was already OM-`replicaSets`-driven; the only `listShards` users for PIT were `invoke-oplog-replay` (shard discovery + mongos routing-cache warm-up) — added a `replicaset` branch that builds the single-RS replay target from the OM cluster detail (its `replicaSet` id is both the tailer's segment-dir name and the `mongorestore --host` replSetName) and skips the warm-up. `--deployment` added to all three PITR commands. **Operational gotcha:** the tailer `scp`s the agent-written `.oplogs` segments off the node as `SSH_USER`; those files are `640 mongod:mongod`, so `SSH_USER` (packer) must be in the **`mongod` group** on every node (reference "Step 5"). This was set on the original sharded nodes but missing on the newer RS nodes — every `scp` failed silently (job created → `/fail` → retry) until `usermod -aG mongod packer` was applied to 05/06/07.
- **Restore model confirmed with MongoDB (2026-07-22):** the per-member snapshot + **whole-cluster revert** (all members overwritten from their own snapshots, reverted together) is a supported model. Members reconcile via normal replication/rollback on restart — valid **as long as the primary retains enough oplog to span the snapshot→revert-point gap** (otherwise a lagging member hits `OplogStartMissing`). Size the oplog above the replication lag at snapshot time. (The snapshot's backup cursor and the oplog tailer both target the **primary** now, so the pinned/consistent member is the freshest; secondaries trail it by their replication lag — keep that lag low to keep the reconciliation spread small.) See [how-it-works.md](../docs/how-it-works.md) → "Whole-cluster revert and the oplog-window requirement". (A single-source clone-to-all alternative — "Path A" — was prototyped and reverted once MongoDB confirmed this model.)

## Status legend
- ✅ **Validated** — exercised live against `aen-cluster` (see referenced doc / results).
- 🟡 **Supported, not yet tested** — handled by the implementation; not exercised here.
- ⚙️ **Supported with operator procedure** — works, but requires the noted setup step.
- ⚠️ **Gap / differs** — applicable cert item the implementation handles differently or only partially.
- ❌ **Not applicable** — out of scope for this implementation (rationale given).

## Scope of this implementation (read first — it determines applicability)
- **Sharded *and* standalone replica-set clusters (multi-deployment).** As of 2026-06-11 the tooling is
  configured per *deployment* in a **single `.env`** (shared infra + `<NAME>__` key overrides), selected with
  `--deployment <name>` and a `TOPOLOGY` of `sharded` or `replicaset`. Topology discovery is **Ops-Manager-driven**
  (the third-party cluster detail's `replicaSets`), so snapshot node-selection is topology-agnostic; the only
  `mongos`/`listShards` uses (restore-stabilization, PIT anchors) now branch on `TOPOLOGY` — `replicaset` verifies
  the single RS's writable primary directly. → **Replica Set Tests are now in scope** (see §1). The proven
  sharded `listShards` paths are unchanged; the unit suite still passes.
- **In-place self-restore (primary, certified path) + a new cross-cluster RS-to-RS restore (pending
  validation).** `restore-mongo-snapshot` overwrites the *source* cluster's own FlashArray volumes (CoW pointer
  swap → WiredTiger recovery) — the validated path for all self-restore items. A separate command
  **`restore-mongo-snapshot-to-target`** (`restore_to_target.py`) restores an RS snapshot to a *different* RS via
  seed + initial-sync (same arrays, different volumes) — cert **1.A.1.b**, unit-tested, **not yet live-validated**;
  the certified `restore.py` is untouched. Other to-different targets (sharded-to-different 2.A.1.b/f,
  different-OM/project) remain ❌.
- **Full snapshots only.** FlashArray protection-group snapshots are always full (CoW); `new-mongo-snapshot`
  takes full snapshots and stores no incremental chain (`srcBackupName` is never used). **All *Incremental Backup
  Tests* are ❌** (not a concept for storage-snapshot backup).
- **Topology:** validated on (a) **`aen-cluster`** — restructured to a **dedicated config server**
  (`aen-shard_0` on `aen-mongo-config-00`) + **3 data shards** (`aen-shard_1/2/3` on aen-mongo-01/02/03); and
  (b) **`aen-rs-00`** — a **standalone 3-member replica set** (aen-mongo-05/06/07). One FlashArray data volume per
  node in a Fusion fleet, driven by the hybrid gateway-routed FA client. Backup protection groups follow a
  `<cluster-name>-pg` convention (`aen-cluster-pg`, `aen-rs-00-pg`).

## Data-insertion & verification methodology (per the checklist)
- **Non-PIT:** insert **data set A** → `new-mongo-snapshot` → insert **data set B** → restore → verify **A and
  only A** (B is correctly lost). Realized here via `start-insert-load` (bulk) and/or `mongos` (marker docs); the
  snapshot embeds `mongo:preSnap`/`mongo:postSnap` count baselines into the FA snapshot tags and STEP 8 of the
  restore asserts the post-restore count falls in `[preSnap, postSnap]` (the consistency window). **For sharded
  clusters, STEP 8 also verifies the shards themselves hold the data** (the checklist's *"verify the shards
  contain the correct data"*): it enumerates shards via `listShards`, connects to each shard's RS directly,
  counts the verify collections per-shard, prints the distribution, and asserts the per-shard totals account for
  the mongos aggregate (`sum ≥ routed total` — a shard *short* of data fails the restore; a direct shard count
  can include orphaned docs, which only inflate the sum and are noted, not failed). The strongest **"only A"**
  proof is the *mutate-then-restore* method: between snapshot and restore, delete/alter the data and add a
  sentinel collection, then confirm the restore reverts to the snapshot point-in-time and the sentinel is gone
  (done for the RS self-restore, 1.A.1.a; recommended for the sharded rows).
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

## 1. Replica Set Tests — ✅ In scope (multi-deployment, 2026-06-11)
Validated against **`aen-rs-00`** — a standalone 3-member replica set (aen-mongo-05/06/07). OM third-party backup
was enabled via the `backup/third_party/.../manage` endpoint (**no OM snapshot store required** — the FlashArray
holds the snapshots; the standard `backupConfigs statusName=STARTED` path is wrong here and 409s "no available
Snapshot Store"). Run any command with `--deployment aen-rs-00`.

| # | Official checklist item | Applicability | How / Status |
|---|---|---|---|
| 1.A.1.a | **Restore Replica Set to self** (full snapshot) | ✅ **Validated** | Snapshot `aen-rs-00-pg.om-20260611-124009` → **true point-in-time fidelity proof**: baseline `testdb.loadtest/payload=5000` → **mutate** (`deleteMany`→0/0 + add `testdb.sentinel`) → restore → post-restore **5000/5000 and `testdb.sentinel` GONE** (B reverted, A and only A present, **drift 0**). STEP 1 selected an RS secondary (no `listShards`); STEP 7 used the `replicaset` branch ("primary elected: aen-mongo-06:27017"); STEP 8 drift 0. |
| 1.A.1.b | Restore Replica Set to **different** Replica Set | 🟡 **Implemented, pending live validation** | NEW command **`restore-mongo-snapshot-to-target`** (`restore_to_target.py`) — seed + initial-sync, same-arrays/different-volumes, separate `--target-*` flags; the certified self-restore `restore.py` is untouched. **12 unit tests pass; not yet run live.** Main risk: OM reconcile on the destination (agent accepting the single-member on-disk `local.system.replset` rewrite + growing the RS to its automationConfig members). See `docs/TODO-restore-to-different-rs.md`. |
| 1.A.2.a | **Incremental** Restore RS to different RS | ❌ N/A | FlashArray snapshots are always full; no incremental chain. |
| 1.B.1.a | **PIT Restore Replica Set to self** | ✅ **Validated** | `aen-rs-00`, tag `om-20260611-150821`: A=8000 at T1 → insert B (+3000) → tailer captured **24 oplog segments** → restore lands exactly at T1 (**8000, drift 0**) → replay-all recovers to T2 (**11000, `unrecoveredTail=0`**). Uses the new `replicaset` branches in `start-oplog-tailer` + `invoke-oplog-replay` (no `listShards`). Setup note: `packer` must be in the `mongod` group on the RS nodes so the tailer can `scp` the agent-written `.oplogs` segments (reference "Step 5"; was missing on the newer RS nodes). |
| 1.B.1.b | PIT Restore RS to **different** RS | ❌ N/A | In-place self-restore only. |
| 1.B.1.c | PIT Restore RS to **different** RS **with Arbiter** | ❌ N/A | To-different is out of scope. *(Arbiter handling itself is supported: arbiters hold no data and are never `snapshotable`, so node selection skips them — a self-restore of an RS that contains an arbiter works fine; only the to-different target makes this item N/A.)* |
| 1.B.2.a | **Incremental** PIT Restore RS to different RS | ❌ N/A | Full snapshots only. |

---

## 2. Sharded Cluster Tests

### 2.A Full Snapshot Restore — Non-Incremental

| # | Test | Applicability | How / Status |
|---|---|---|---|
| a | Self Restore Sharded Cluster | ✅ **Validated** | In-place self-restore, validated on **both** topologies: embedded-config (2.A.e) and **dedicated-config 4-shard `aen-cluster`** (2026-06-11, tag `om-20260611-153131`): snapshot (baseline 39600/20000) → delete all + add `sentinel` → restore → **drift 0, `sentinel` gone**, and the **per-shard STEP 8** confirmed each shard holds its own data — `aen-shard_1=26339`, `aen-shard_2=13261` (sum 39600 = mongos aggregate); `aen-shard_3`/`config` empty (data was sharded before `aen-shard_3` was added — correctly noted, not failed). |
| b | Restore Sharded Cluster *(to different)* | ❌ N/A | In-place self-restore only — no cross-cluster restore. |
| c | Add a shard, then restore | ✅ **Validated** | Added `aen-shard_3` via the OM API, then `initialize-protection-groups` added the new node's volume to the PG (STEP 0 hard-fails if any discovered volume isn't a PG member; topology re-discovered every run). Two gotchas cleared: new-port firewall, and transient `JOB_NOT_FOUND` until the new shard's backup job initialized. Snapshot+restore `om-20260609-085646`: 4 shards, **drift 0**. |
| d | Remove a shard, then restore | ✅ **Validated** | `removeShard` → 3-shard topology; `initialize-protection-groups --prune` drops the orphaned volume; restore targets only the current volumes via the `mongo:volumes` tag. The removal wedged OM backup (`topologyAbort`) — recovered via force-unmanage → `mongodb-mms` restart → `manage` (cross-cutting findings; issue #1). Snapshot+restore `om-20260610-152019`: 3 shards, 3 primaries, **drift 0**. |
| e | **Self Restore Sharded Cluster with Embedded Config** | ✅ **Validated** | This is `aen-cluster`. **Test 1** (no load) and **Test 2** (under load) — see [Test-SnapshotRestore.md](Test-SnapshotRestore.md); 2026-06-05 results: Test 1 drift 0, Test 2 post-restore within `[preSnap, postSnap]`. |
| f | Restore Sharded Cluster w/ Embedded Config *(to different)* | ❌ N/A | In-place self-restore only. |
| g | Convert **Dedicated→Embedded** config, then restore | ✅ **Validated** | `transitionFromDedicatedConfigServer` → config shard data-bearing again; mild backup wedge ("waiting for other shards/configs to load", `lastAbort=null`) **cleared by a single `mongodb-mms` restart** (no force-unmanage). Discovery keys per-shard artifacts by canonical shard id (`config`). Snapshot+restore `om-20260610-161057`: **drift 0**. |
| h | Convert **Embedded→Dedicated** config, then restore | ✅ **Validated** | `transitionToDedicatedConfigServer` (drains config-shard chunks + `movePrimary`); backup stayed `ACTIVE`. Snapshot+restore `om-20260610-155312`: **drift 0**. *(`aen-cluster` was later migrated to a fully **dedicated** config server on `aen-mongo-config-00` — see the Restructure row in the Test Summary.)* |

### 2.A Full Snapshot Restore — Incremental — ❌ Not applicable
2.A.2.a/b (incremental, with/without embedded config): **❌ N/A** — FlashArray snapshots are always full;
`new-mongo-snapshot` takes full snapshots only.

### 2.B PIT Restore — Non-Incremental

| # | Test | Applicability | How / Status |
|---|---|---|---|
| a | PIT Self Restore Sharded Cluster | ✅ Supported | In-place PIT self-restore. Validated on embedded config (see 2.B.e); dedicated config identical. |
| b | PIT Restore Sharded Cluster *(to different)* | ❌ N/A | In-place self-restore only. |
| c | PIT Restore After Changing Preferred Node | ✅ **Validated** | Both variants run live: **restart-continuity** (Test 8, 06-05 — 0 gaps + forward replay across all shards) **and** a **forced node-change** (06-08 — stopping `aen-mongo-01`'s agent before the bounce made the agent-health pre-check reselect `aen-shard_2` from `aen-mongo-01`→`aen-mongo-02`, with **0 gap markers** across the change). On the forced run forward *advance* was a no-op because the oplog stream had gone ~2 days stale (captured segments pre-dated T1; range assertion held at the T1 baseline). The two halves are each independently proven on a current stream: **continuity across a forced node-change** (this forced run, 0 gaps) and **forward advance with full recovery** (2.B.e, tag `om-20260608-165149`, `unrecoveredTail==0`); the only combination not yet run *together* is forced-node-change + forward-advance in a single pass. **Operational notes:** (1) the tailer must run *continuously* — a stale stream yields no-op replays (reinforces 4.c); a stale stream can be re-baselined to ~now by a *skip-forward* (create one oplog snapshot spanning `[stale cursor → now]`, `/finish` it without copying — the cursor advances and OM discards the skipped window). (2) OM `.oplog` segments lag live writes by ~2–3 min, so a PIT recovery point trails live by that lag until drained — see 4.c note. |
| d | PIT Restore to Sharded Cluster with Arbiter | 🟡 Supported; **blocked on an OM prerequisite** (2026-07-22) | Arbiters hold no data and are never `snapshotable`, so node selection skips them by design. Attempted live (add `aen-mongo-04` as an arbiter to `aen-rs-00`): OM **rejects adding an arbiter** (FCV ≥ 5.0) until a cluster-wide **DefaultWriteConcern** is configured in OM's `clusterWideConfigurations` — cluster-side `setDefaultRWConcern` did **not** satisfy OM's automationConfig validation. A real operational prerequisite for the arbiter item; not completed (undocumented OM schema, not reverse-engineered on the shared prod project). See [Test-Certification-Results-2026-07-22.md](Test-Certification-Results-2026-07-22.md). |
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
| a | Set a preferred oplog-tailing node (incl. when a new node is added) | ✅ **Validated** — **Test 9** + a **live add-node on 2026-07-22**: added `aen-mongo-04` to `aen-rs-00`; on the next run the tailer re-registered `preferredOplogNodes` over the updated 4-node topology. Note: OM lags flagging a freshly-added member `snapshotable` (like a new shard's backup job — `JOB_NOT_FOUND`), so a just-added node is *considered* by the selector but becomes an eligible preferred-oplog node only after OM's backup re-scan. |
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
| **Replica-set self-restore (1.A.a)** | ✅ **Validated** on `aen-rs-00` — true point-in-time fidelity proof (mutate→restore→`sentinel` gone, drift 0) |
| **Replica-set PIT (1.B.1.a)** | ✅ **Validated** on `aen-rs-00` — restore→T1 (drift 0), replay-all→T2 (`unrecoveredTail=0`); via new `replicaset` branches in tailer/replay |
| **Dedicated config-server migration + backup recovery** | ✅ Validated — restructured `aen-cluster` to dedicated config-00; recovered the post-migration backup wedge (issue #2) → snapshot FINISHED |
| Add/remove shard, config conversion (in-place), arbiter | 🟡/✅ — add/remove shard & in-place config conversion validated earlier; arbiter not exercised |
| Valid-restore-target endpoint (4.b) | ⚠️ Storage-layer validation instead of the OM endpoint |
| Auto gap-check before replay (4.c) | ✅ Enforced — replay refuses on gap markers (`--allow-gaps` overrides) |
| Restore-to-different; all incremental | ❌ Out of scope (in-place self-restore, full-only) |

**Cert-readiness read:** the in-scope, applicable scenarios that have been exercised all pass — sharded
self-restore (2.A.e), full forward PIT recovery on a current stream (2.B.e, `unrecoveredTail==0`), node-change
continuity (2.B.c, 0 gaps), **replica-set self-restore** (1.A.1.a, fidelity proof on `aen-rs-00`), and
**replica-set PIT** (1.B.1.a — restore→T1 drift 0, replay-all→T2 `unrecoveredTail=0`). The tool is now
**multi-deployment** (sharded + standalone RS from one `.env`), and the OM third-party backup wedge that follows
topology changes (shard removal *and* dedicated-config-server migration) is recoverable by a documented operator
procedure (force-unmanage → `mongodb-mms` restart → `manage`; filed as issues #1/#2). Remaining applicable items:
arbiter (sharded PIT 2.B.1.d), the forced-node-change + forward-advance *combined* pass (2.B.1.c), and the
explicit add-node→oplog-reselection (4.a). Two verification items (4.b OM valid-target endpoint, 4.c auto
gap-check) are **conscious design differences** to confirm against the certifier's exact requirements before
submission.
