# Restore Consistency Remediation Plan

**Status:** Draft for review (Pure ↔ MongoDB). No code changed yet.
**Owner:** (Pure) · **Trigger:** MongoDB architecture review, July 2026, citing the April 2026 customer integration incident.
**Scope:** The `new-mongo-snapshot` / `restore-mongo-snapshot` **in-place self-restore** path. PITR and cross-cluster restore are noted where affected.

---

## 1. Problem statement

MongoDB's review found that our integration model — **snapshot a volume for every data-bearing member of every replica set, and at restore reattach each member to its own snapshot** — is **not a documented third-party backup path**, and is unsound under write load.

The third-party backup contract, per replica set, is:

1. **Exactly one member is frozen.** Ops Manager opens the `$backupCursor` on the single member passed in `nodeIds[]` during the READY window. The other members are **not** frozen and get **no consistency guarantee**.
2. **Cross-shard consistency** is established by aligning optimes via `$backupCursorExtend` across **one cursor per shard** — alignment is at **RS granularity, not member granularity**.
3. **At restore**, the *single* snapshot's data is propagated to every non-arbiter member of the RS via one of **two documented paths** (A or B below).

Capturing an independent snapshot of every member and restoring each from its own snapshot introduces a consistency boundary the contract does not provide. Non-frozen members are mid-write at a **different oplog position** than the frozen node; the resulting snapshots contain **divergent oplog endpoints**. On restart this surfaces as `OplogStartMissing` → rollback → (in the OM SeedRestore flow) a `stableTimestamp=(0,0)` fassert. This is the April 2026 failure mode (~240 s of oplog spread across three members of one shard).

**This cannot be fixed by making the PG snapshot "more atomic."** OM only froze one member; the others were actively replicating when the snapshot fired. Perfect atomicity across non-frozen volumes just captures divergent state at the same instant — it produces divergent inconsistency faster, not a consistent source. The only way to legitimately snap multiple members of one RS is an **integration-owned, RS-wide freeze outside the OM protocol** — a large design change that Paths A and B avoid entirely.

---

## 2. As-built (what the code does today)

| Step | Behavior | Location |
|---|---|---|
| Init | `initialize-protection-groups` adds **every** discovered node's data volume to the PG | [init_protection_groups.py:96](../mongodb_flasharray_backup/init_protection_groups.py#L96) |
| Snapshot select | Picks **one** snapshotable secondary per RS → `nodeIds[]` (OM opens the cursor there; OM handles `$backupCursorExtend` across shards) | [snapshot.py:240](../mongodb_flasharray_backup/snapshot.py#L240), [snapshot.py:308](../mongodb_flasharray_backup/snapshot.py#L308) |
| Snapshot take | FA **PG snapshot across the whole PG** → captures a volume for **every** member | [snapshot.py:358](../mongodb_flasharray_backup/snapshot.py#L358) |
| Snapshot tags | Records `mongo:volumes` (**all** volumes), `mongo:preSnap`, `mongo:postSnap`, `mongo:t1ts`. **Does not record which member was frozen.** | [snapshot.py:494-606](../mongodb_flasharray_backup/snapshot.py#L494) |
| Restore | Overwrites **each member's** volume in-place **from its own** snapshot member (`<pg>.<tag>.<volume>`); restarts agents; WiredTiger crash-recovers; trusts RS self-healing | [restore.py:469](../mongodb_flasharray_backup/restore.py#L469) |

**Two facts that matter for remediation:**

- **The in-place restore never calls the OM restore API.** No `POST /restore`, no `/filesCopied`, no `volumeRestore`. It is a fully **out-of-band** storage restore (stop agents → unmount → FA overwrite → remount → restart → crash recovery). MongoDB's API-layer audit assumed the OM restore API is used — that assumption matches the **HLD**, not the shipped in-place code (see §3).
- **`restore-mongo-snapshot-to-target` is already Path B-shaped:** restore one snapshot to a single seed, rewrite its `local.system.replset` to a single-member config offline, let OM grow the RS and the wiped members initial-sync ([restore_to_target.py:8-23](../mongodb_flasharray_backup/restore_to_target.py#L8)). The "one-node + rebuild" primitive already exists in this repo.

---

## 3. HLD-vs-code gap (must be reconciled)

MongoDB reviewed an **HLD** describing an **OM-restore-API flow** (Mode 2: `POST /restore` + `volumeRestore: true` + per-node `/filesCopied`). The **shipped** `restore-mongo-snapshot` does **not** use the OM restore API at all. Consequences:

- The `volumeRestore: true` / `DeleteDbFiles` / Phase-2 `NamespaceNotFound` items describe the **HLD's OM-restore path**, not the current code path — those exact signatures won't appear in our out-of-band flow.
- **The root-cause consistency flaw is independent of the restore mechanism.** Divergent per-member snapshots are unsound whether volumes are reattached out-of-band or signaled via `/filesCopied`. The fix is required either way.
- **Decision required:** align on ONE restore mechanism (see §8.1) and make the HLD and the code describe the same thing.

---

## 4. Root-cause analysis

**Why it fails under load.** Members are at different oplog positions when the PG snapshot fires (async replication lag). Restoring each member from its own image reconstructs a set whose members disagree on the oplog. On restart, election picks the highest optime; a member with no common point in the primary's oplog hits `OplogStartMissing`, cannot roll back cleanly, and (in the OM SeedRestore flow) fasserts on `stableTimestamp=(0,0)`.

**Why our lab runs were green.** With little/no write load during the snapshot, replication lag ≈ 0, so member oplog endpoints were nearly identical and normal RS self-healing reconciled the tiny spread → drift 0. **Correctness was riding on low replication lag, which the contract does not guarantee.** The under-load cert runs did not reproduce the incident because healthy secondaries kept lag sub-second; the failure needs a busy or lagging secondary during the READY window.

**Implication:** every "✅ validated" in-place self-restore result (cert §1.A.1.a, §2.A.a/e, per-shard STEP 8, etc.) is **valid only for the low-lag case** and must be re-qualified after remediation (see §11).

---

## 5. Path A — snapshot one member, clone-to-all at restore  *(MongoDB-preferred)*

**Idea.** Snapshot **only the OM-frozen member's volume** per RS (one consistent source). At restore, use FlashArray's clone-from-snapshot primitive to create **N per-member copies from that single source**; each member mounts its own clone. All clones share the same oplog endpoint → **divergence eliminated by construction.**

**Snapshot-side changes (`snapshot.py`, `init_protection_groups.py`):**
- The FA snapshot source per RS must be **only** the `nodeIds[]` member's volume. Options:
  - **A1 (recommended):** keep the PG for scheduling/membership but, at snapshot time, take a **per-volume snapshot of just the frozen member's volume** per RS (not a whole-PG snapshot), or a PG scoped to the frozen volumes. OM already tells us the frozen node (`nodeIds`).
  - **A2:** maintain one PG **per RS** containing only that RS's members and snapshot per RS — still must select the frozen member as the clone source.
- **Record the frozen member per RS** in the snapshot tags — new tag, e.g. `mongo:sourceVolumes` = the one frozen volume per RS (today `mongo:volumes` lists all volumes and does not identify the frozen one). Restore reads this to know the single source.

**Restore-side changes (`restore.py` STEP 4):**
- For each RS, for **every** non-arbiter member volume, `post_volumes(names=[member_vol], volume={"source": {"name": <frozen-member snapshot>}}, overwrite=True, context_names=[member_array])` — i.e. clone the **one** source to **all** members. The FA primitive already exists ([fa_rest.py:325](../mongodb_flasharray_backup/fa_rest.py#L325)); this is a change of *source selection*, not a new capability.
- Cloning the frozen member's `local` (oplog, `system.replset`) to all members is exactly what a physical initial sync produces — members are meant to be identical copies, so this is correct, not a conflict.

**Pros:** fast (CoW clones), no HA gap, no initial-sync rebuild, matches the volume-vendor shape MongoDB documents. **Cons:** requires the snapshot-source rescope + a restore rewrite; must confirm FA cross-array clone semantics when a member's volume lives on a different array than the source snapshot.

---

## 6. Path B — one data member per RS + `rs.add()` rebuild  *(fallback)*

**Idea.** Restore only the frozen member per RS into a reduced topology, reach COMPLETE, then grow the RS back; MongoDB's standard initial sync populates the new members.

**Changes:** generalize the shipped `restore-mongo-snapshot-to-target` seed+initial-sync flow to the **self-restore** case (same RS name, same hosts): restore the frozen member's volume, bring the RS up single-member, let OM grow it and the wiped members initial-sync. Most of the machinery exists.

**Pros:** uses only documented primitives, no volume-clone dependency, lowest net-new code. **Cons:** temporary HA gap (single member per RS during the window), slower (full initial sync per added member), more moving parts on large data sets.

---

## 7. Cross-cutting requirements (apply to whichever path)

### 7.1 Restore mechanism: OM restore API vs out-of-band — **decide first**
- **Out-of-band (today):** simplest, no OM restore orchestration; but off the documented contract and unblessed by OM. If kept, `volumeRestore`/`filesCopied` are moot but the HLD must be rewritten to describe the out-of-band flow.
- **OM restore API (HLD Mode 2):** on the documented contract; then §7.2–7.4 are mandatory.

### 7.2 `volumeRestore: true` — hard requirement (only if using the OM restore API)
Pure is classified **volume-level-only** in MongoDB's Vendor Feature Matrix, so `POST /restore` **must** set `'volumeRestore': true`. Absent (default false), the agent runs `DeleteDbFiles` + `fileList.txt` pruning, wiping the oplog between Phase 1 and Phase 2 → fatal `NamespaceNotFound (Can't find local.oplog.rs)` in Phase 2 SeedRestore (the *initial* root cause in April 2026). Belongs in **Mode 2 preconditions**, not "Python dependencies."

### 7.3 Restore preconditions
All non-arbiter nodes **and** mongos routers must be up with MongoDB agents running before initiating the restore — OM waits **indefinitely** for a down node and will not auto-fail. Add an explicit pre-restore up-check (today we only wait for mongos *after* the overwrite, [restore.py:749](../mongodb_flasharray_backup/restore.py#L749)).

### 7.4 Arbiter handling
Arbiters must be listed in the restore request's `nodes[]` array but receive **no data**. Discovery/PG membership must classify and skip arbiters as data targets while still including them where the restore request requires.

---

## 8. Recommendation

1. **Adopt Path A** as the primary self-restore model (fast, no HA gap, documented volume-vendor shape); keep the Path-B machinery (already in `restore-to-target`) as the fallback for environments where cross-array clone is unavailable.
2. **Decide the restore mechanism (§7.1)** — recommend moving the restore onto the **OM restore API with `volumeRestore: true`** so we are on the documented contract and OM-blessed, unless there is a concrete reason the out-of-band flow must stay.
3. **Reconcile the HLD and the code** to describe the single chosen model in Scope + Operational Modes.

---

## 9. Test & validation plan (must actually reproduce the failure)

Prior green results don't count until we can **reproduce the incident and then show the fix prevents it.**
- **Reproduce:** drive sustained write load and induce secondary lag (e.g. throttle/pause a secondary) during the READY window on the **current** model; confirm `OplogStartMissing` / rollback / fassert on restore. This is the missing test that let the flaw ship.
- **Prove Path A:** same load + lag, snapshot one member, clone-to-all, restore → all members converge, no `OplogStartMissing`, correct + consistent data, per-shard verification intact.
- **Regression:** the existing self-restore, PIT, and per-shard cert scenarios, re-run on the new model.
- Add a load+lag restore scenario to `run-all-tests`.

---

## 10. Impact on prior certification results

The following are **downgraded to "valid at low replication lag only"** pending re-validation on the remediated model: cert §1.A.1.a (RS self-restore), §1.B.1.a (RS PIT self-restore), §2.A.a/§2.A.e (sharded self-restore), the per-shard STEP 8 checks, and every drift-0 result produced by the current per-member restore. `restore-to-target` (Path B) is unaffected in model but should adopt §7.2–7.4 if it moves to the OM restore API.

---

## 11. Open questions for MongoDB

1. For a **volume vendor on Path A**, is FA clone-from-snapshot at restore an accepted `filesCopied` provenance, or must the clone be reflected through a specific restore-API sequence?
2. Does Path A require `volumeRestore: true` (no `DeleteDbFiles`) the same way the seed flow does, given all members receive identical cloned files?
3. Confirm the `nodeIds[]` member is the **only** valid clone source, and that OM's `$backupCursorExtend` alignment across shards is sufficient for cross-shard consistency with Path A.
4. Arbiter entries in `nodes[]` for a restore that writes no data to them — exact expected request shape.
5. Is an out-of-band storage restore (no OM restore API) ever a supported shape for a volume vendor, or is the OM restore API mandatory for certification?
