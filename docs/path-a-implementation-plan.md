# Path A Implementation Plan — single-source restore via a replicated secondary snapshot

**Status:** Planning (no code yet). Companion to [restore-consistency-remediation.md](restore-consistency-remediation.md).
**Goal:** Make in-place restore consistent-by-construction: every member of a replica set is restored from the **one OM-frozen secondary's snapshot**, pre-replicated at snapshot time to the arrays that host that RS's other members.

## Decisions locked (from review)

| Decision | Choice |
|---|---|
| Replication transport | **Async PG replication** (PG targets + array-connections) |
| Restore mechanism | **OM restore API** (`POST /restore` …) with **`volumeRestore: true`** |
| Replication fan-out | **Only that RS's member arrays** (not all cluster arrays) |
| Snapshot scope | **Unchanged** — still snapshot the full PG (all members); restore just never uses the non-secondary members' snapshots |
| Snapshot completion | **Block** until replication to all targets is confirmed; restore also verifies presence before touching data |
| Path B | Left as-is (`restore-mongo-snapshot-to-target`) |

## Model in one paragraph

Per replica set, OM freezes exactly one secondary (the `nodeIds[]` member). That member's volume snapshot is the **only** consistent image. At snapshot time we replicate *that one snapshot* to the arrays hosting the RS's other members (async PG replication, targets = sibling-member arrays). At restore, OM drives the lifecycle (`volumeRestore: true`, no `DeleteDbFiles`), and for every non-arbiter member we clone the (now-local) secondary snapshot onto that member's data volume, then signal `filesCopied`. All members come up from byte-identical data at one oplog endpoint → no divergence, no `OplogStartMissing`.

---

## Component changes

### A. `initialize-protection-groups`  (topology + replication wiring)
1. **Group nodes by replica set.** Pull RS membership from the OM cluster detail (`replicaSets[].nodes[]`); today init-pg treats nodes as a flat list. Persist the RS per node as a tag `mongo:rs`.
2. **Per-member replication PG.** For each member volume, create a PG (e.g. `<deployment>-<node>-repl`) whose sole member is that member's data volume and whose **replication targets = the arrays hosting the *other* members of the same RS**. Record the name as tag `mongo:replpg`. (Per-member, because the frozen member varies run-to-run — whichever one OM picks, its repl-PG already fans out to the right siblings.)
3. **Array-connections.** Ensure an async-replication connection exists between every pair of arrays that host members of the same RS; create if missing (or verify + fail with guidance if the fleet forbids auto-creating them). New FA calls: `get_array_connections` / `post_array_connections`.
4. Keep the existing full deployment PG (`aen-cluster-pg`) exactly as today.
5. `--what-if` must preview the repl-PGs, their targets, and any array-connections it would create.

### B. `new-mongo-snapshot`  (replicate the secondary's snapshot)
1. STEP 1 already selects the frozen secondary per RS (`NodeIds`). Map each frozen `host:port` → its data volume via `NodeVolumeMap` → its `mongo:replpg`.
2. Keep STEP 5 (full-PG snapshot) unchanged.
3. **New step:** after OM reaches READY (cursors open), for each RS take + replicate the **frozen member's repl-PG** snapshot on-demand with the run's tag: `post_protection_group_snapshots(source_names=[<frozen replpg>], replicate_now=True, ...)`. **Block** until each target array reports the replicated snapshot present (poll `get_protection_group_snapshots` on the target filtered by the source-prefixed name).
4. **New tags on the FA snapshot:** `mongo:sourceVolume` (the frozen secondary's volume per RS) and `mongo:sourceReplPg`, so restore knows the single source and where its replicas landed. Keep `mongo:volumes`, `mongo:preSnap`, `mongo:postSnap`, `mongo:t1ts`.
5. Do **not** call `/finish` until both the full-PG snapshot and the replication are confirmed.

### C. Restore — new OM-restore-API path (`volumeRestore: true`)
Replaces the out-of-band flow in `restore.py` (old flow retained but gated/deprecated until this passes — see Rollout).
1. **Preconditions (hard):** every non-arbiter node **and** all mongos routers up with agents running — explicit pre-check; abort if any down (OM waits indefinitely otherwise).
2. `POST /{group}/clusters/{cluster}/restore` with `{ snapshotsMetadata, nodes: [{id, restoreRole}], volumeRestore: true }`. **Arbiters are listed in `nodes[]` but receive no data.** Then `POST /restore/{id}/start`.
3. Poll `GET /restore/{id}` until OM signals the vendor to place files.
4. **Per non-arbiter node** (the vendor's placement = a volume restore): clone the frozen secondary's **local replicated** snapshot onto that node's data volume — `post_volumes(names=[node_vol], volume={"source": {"name": <replicated source snap>}}, overwrite=True, context_names=[node_array])` — mount, then `POST /restore/{id}/filesCopied` for that node.
5. Poll to `COMPLETED`; on any failure `POST /restore/{id}/fail`.
6. STEP 8 verification (baseline counts + per-shard distribution) unchanged.

### D. `fa_rest.py` — new methods
- `get_array_connections()` / `post_array_connections(...)` — inspect/establish replication links.
- Set PG replication targets — extend `patch_protection_groups` (`protection_group={"targets": [...]}`) or add `post_protection_groups_targets`.
- Add `replicate_now` (and `replicate`) params to `post_protection_group_snapshots`.
- Query replicated snapshots on a target array (source-prefixed names) to confirm replication completion.

### E. `config.py` — OM restore helpers
- `invoke_om_restore(...)`, `wait_om_restore_state(restore_id, target_state, ...)`, `om_restore_files_copied(restore_id, node_id)`, `om_restore_fail(restore_id)` — thin wrappers over the existing `invoke_om_api` / retry, mirroring `wait_om_snapshot_state`.

### F. Tag schema additions
`mongo:rs` (RS id per member volume, at init), `mongo:replpg` (per-member repl-PG name, at init), `mongo:sourceVolume` + `mongo:sourceReplPg` (frozen source per RS, at snapshot). `mongo:pvcount` and the existing map tags stay.

---

## Sequencing (each phase independently testable; commit per phase)

1. **FA replication primitives** — array-connections + PG targets + `replicate_now` in `fa_rest.py`; unit tests + a live round-trip replicating one PG snapshot between two lab arrays and reading it on the target.
2. **init-pg replication wiring** — RS grouping, per-member repl-PGs, targets, array-connections, `--what-if`; verify on `aen-rs-00` (3 members / 3 arrays) that each member's repl-PG targets the other two arrays.
3. **Snapshot replication step** — replicate the frozen member's repl-PG at snapshot time + block + new tags; verify the secondary's snapshot appears on both sibling arrays with the run tag.
4. **OM restore helpers** — `config.py` wrappers + `fa_rest` clone-from-replicated-source; unit tests.
5. **New restore path** — OM restore API + `volumeRestore: true` + per-node clone + `filesCopied`; behind a flag next to the existing flow.
6. **Cutover + retire the out-of-band restore** once phase 5 passes the reproduction test.

---

## Test & validation plan (must reproduce the failure first)

- **Reproduce the incident on the OLD model:** sustained write load + induced secondary lag during READY → confirm `OplogStartMissing`/rollback on restore. (This is the test that was missing.)
- **Prove Path A:** same load+lag → snapshot → replicate → OM restore w/ `volumeRestore:true` → all members converge from the single source, no `OplogStartMissing`, correct data, per-shard verification intact.
- **Replication completeness:** source snapshot present on every sibling array before restore starts; snapshot blocks until so.
- **Arbiter:** arbiter listed in `nodes[]`, no data written, RS healthy after.
- Add a load+lag restore scenario to `run-all-tests`; re-run all prior self-restore/PIT/per-shard scenarios on the new model.

---

## Risks / unknowns to confirm

1. **OM restore API `volumeRestore` field** — the reference doc (`POST /restore`, `/start`, `/filesCopied`, `/fail`, `nodes[].restoreRole`) does not show `volumeRestore` explicitly. Confirm exact field name/placement with MongoDB (open Q in the remediation doc). Placement of `snapshotsMetadata` for a volume vendor also TBD.
2. **FA async replication in a Fusion fleet** — do array-connections already exist between the lab arrays, or must init-pg create them? Replication bandwidth/latency drives snapshot wall-clock (blocking). Capacity for the replicated copies on sibling arrays.
3. **Replicated snapshot naming** — the source-array-prefixed name on the target that restore must reference for the clone source.
4. **Cloning the secondary's `local` (oplog/replset) to all members via `volumeRestore`** — confirm OM accepts this as the `filesCopied` provenance for a volume vendor (remediation doc open Qs #1/#2).
5. **PIT interaction** — `pitTimestamp` on the restore body + oplog replay still layer on after; out of scope for phase 1 but keep the seam.

## Rollout / backward compatibility

New restore path lands behind a flag; the current out-of-band restore stays available (marked **unsafe — low-lag only**) until the new path passes the reproduction test, then becomes the default and the old path is removed. `restore-mongo-snapshot-to-target` (Path B) is untouched. Prior cert results remain downgraded to "valid at low replication lag" (remediation doc §10) until re-validated here.
