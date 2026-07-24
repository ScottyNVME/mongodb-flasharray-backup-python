# Test: Snapshot and Restore

Validates the end-to-end snapshot and restore flow for both deployments: the `aen-cluster` sharded cluster
(Tests 1–5) and the `aen-rs-00` standalone replica set (Tests 6–7).

> **Replica-set deployments:** the same procedure applies to a standalone replica set — append
> `--deployment aen-rs-00` to every command and point `mongosh` at an RS member instead of `mongos`. Fully worked
> RS runs are in **Test 6** (self-restore, fidelity proof) and **Test 7** (PIT with A/B markers) below; Tests 1–3
> and 4–5 use the sharded `aen-cluster`.

> **Run each step manually in separate terminals.** Multi-stage workflows that combine a long-running background process (e.g. `start-insert-load`) with a foreground operation (e.g. `new-mongo-snapshot`) in the same pipeline will deadlock — the parent shell waits for stdout to drain before reading the next pipe stage, and both sides block. Each process must run in its own independent terminal with no shared pipe.

> See [System Requirements](../README.md#system-requirements) in the README for storage and SSH prerequisites that must be satisfied before running these tests.

> **All `mongosh` invocations below use the mongos endpoint and SSH user from `.env`** (the same file every command loads via `config.load_config()`). Do not edit the snippets to hardcode hostnames or ports — run the session-setup block first so `$SSH_USER`, `$MONGOS_HOST`, `$MONGOS_PORT`, `$MONGOSH_PATH`, and `$SSH_OPTS` are in your shell.

```bash
# Run once at the start of any test session.
source .venv/bin/activate              # puts the console scripts (new-mongo-snapshot, …) on PATH
set -a && . ./.env && set +a           # export SSH_*/MONGOS_*/… for the shell snippets below
SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=no)
```

A small helper used throughout this doc — runs a single `mongosh` eval against mongos and returns trimmed stdout:

```bash
mongos() {
    ssh "${SSH_OPTS[@]}" "${SSH_USER}@${MONGOS_HOST}" \
        "${MONGOSH_PATH} --quiet --eval '$1' mongodb://${MONGOS_HOST}:${MONGOS_PORT} 2>/dev/null"
}
```

## Test 1 — Basic snapshot restore (no load)

Confirms that a quiesced cluster can be snapshotted and restored to a consistent state.

### Setup

Confirm the cluster is healthy and note the current document count before the snapshot.

```bash
mongos 'db.getSiblingDB("testdb").payload.countDocuments()'
```

This is your expected post-restore count. The snapshot command also embeds a baseline of `testdb.loadtest` and `testdb.payload` counts into the FlashArray snapshot tags (`mongo:preSnap` / `mongo:postSnap`) — the restore command reads those tags and asserts against them automatically.

### Steps

**1. Take a snapshot.**

```bash
new-mongo-snapshot
```

Note the snapshot tag printed at the end, e.g. `om-20260506-120000`. The tail of the output also prints the captured baseline counts; these are the values the restore will verify against.

**2. Delete the database.**

```bash
mongos 'db.getSiblingDB("testdb").dropDatabase()'
```

Confirm the database is gone:

```bash
mongos 'db.getSiblingDB("testdb").payload.countDocuments()'
```

Expected: `0` (the collection no longer exists; `countDocuments()` returns `0`).

**3. Restore from snapshot.**

```bash
restore-mongo-snapshot --snapshot-tag "om-20260506-120000"
```

Type the snapshot tag at the confirmation prompt, or use `--force` to skip it. STEP 8 of the restore reads the baseline from the FA snapshot tags and **fails hard** if the post-restore count does not match. Pass `--skip-verification` to bypass that assertion (e.g. when restoring a snapshot of a database that does not contain the test collections).

### Expected Results

- Restore completes without error.
- Cluster stabilizes: mongos accepts connections, all 3 shards register, all 3 primaries elected.
- STEP 8 prints `Baseline OK : testdb.loadtest = N` and `Baseline OK : testdb.payload = N` for the values captured at snapshot time.

---

## Test 2 — Snapshot restore under load

Confirms that a snapshot taken while the cluster is receiving writes produces a consistent restore point, and that writes made after the snapshot are correctly lost on restore.

### Setup

Start the insert load generator in a separate terminal and let it run for at least 30 seconds before taking the snapshot.

```bash
# Terminal A — background load
start-insert-load
```

Note the document count at the time you intend to take the snapshot (the load command prints a running count). This is your expected post-restore count.

### Steps

**1. While load is running, take a snapshot.**

```bash
# Terminal B
new-mongo-snapshot
```

Note the snapshot tag. The load command in Terminal A should continue running after the snapshot completes.

**2. Let the load command continue writing for 30+ seconds after the snapshot.**

Note the new (higher) document count — these writes occurred after the snapshot and will be lost on restore. This is intentional.

**3. Stop the load command.**

Press `Ctrl-C` in Terminal A.

**4. Delete the database.**

```bash
mongos 'db.getSiblingDB("testdb").dropDatabase()'
```

**5. Restore from snapshot.**

```bash
restore-mongo-snapshot --snapshot-tag "om-20260506-120000"
```

### Expected Results

- Restore completes without error.
- Cluster stabilizes.
- The FA snapshot tags record two counts per collection: `mongo:preSnap` (taken with the backup cursor open, just before the FA snapshot) and `mongo:postSnap` (taken just after the FA snapshot, before `/finish`). For an insert-only workload these bound the on-volume state at snapshot time:
  - Every doc in `preSnap` was on the volume at `T_snap` → strict lower bound on the post-restore count.
  - Every doc beyond `postSnap` arrived after `T_snap` → strict upper bound on the post-restore count.
- STEP 8 asserts `preSnap <= got <= postSnap` and prints the observed drift (`postSnap - preSnap`) as a sanity check. The drift directly reflects the throughput of `start-insert-load` over the FA-snapshot wall-clock window (typically a few hundred to a few thousand documents).
- Writes that landed after `postSnap` (i.e. after the snapshot completed) are lost by design; a snapshot-only restore is **not** PITR. Test 3 covers oplog replay to recover them.
- A failure mode worth surfacing: if the post-restore count is **above** `postSnap`, the FA snapshot captured more state than the cluster acknowledged at snapshot time (impossible in normal operation; investigate the snapshot path). If it is **below** `preSnap`, the cluster lost durably acknowledged writes (investigate WiredTiger journal/recovery).

---

## Test 3 — Point-in-time restore under load

Confirms that continuous oplog tailing plus replay advances the cluster past the snapshot point to a specific point in time, recovering writes that would otherwise be lost in a snapshot-only restore.

> `start-oplog-tailer` uses the OM Oplog Snapshot API. Each iteration drives one complete oplog snapshot job (POST create → POST start → GET READY → scp `.oplogs` files → POST finish → GET FINISHED). Coverage continuity is tracked via `previousEnd` in each READY response — if `previousEnd` does not match the stored `lastEnd`, a gap marker is written. The tailer **must be started before the FA snapshot** so that `previousEnd` continuity begins at or before the snapshot point; starting it after leaves an unrecoverable gap between the snapshot timestamp and the first captured oplog range.

> **`.oplogs` file format:** The OM agent writes segment files in a proprietary binary format — a BSON metadata header (`{version, encrypted, hashed_encryption_key}`) followed by a block header (`{start, end, uncompressed_size, size, encoding:"snappy"}`) followed by a snappy-compressed blob of raw oplog BSON. `invoke-oplog-replay` automatically deploys `decode_oplogs.py` to each agent node to decompress these files before passing them to `mongorestore --oplogReplay`. Prerequisites: `python3` (standard on RHEL/Rocky 9) and `libsnappy.so` (installed as a transitive dependency of the OM automation agent RPM). Verify with `rpm -q snappy` on each agent node.

### Setup

Start the load generator and let it run throughout the test — before the snapshot, between the snapshot and the target recovery point, and up until the failure event.

```bash
# Terminal A — background load
start-insert-load
```

### Steps

**1. While load is running, start the oplog tailer (choose a tag that matches the snapshot you are about to take).**

```bash
# Terminal B — long-running, leave open until step 4
start-oplog-tailer --snapshot-tag "om-20260506-120000"
```

Each iteration creates one OM oplog snapshot job; the OM agent writes one `.oplogs` file per minute per RS, and the tailer SCPs them into `~/mongo-oplog-stream/<tag>/<rsId>/segments/` using the original OM filename (`<startTs>_<endTs>.oplogs`). Lexical sort on disk equals chronological order for `invoke-oplog-replay`. The tailer must be running before step 2 so that `previousEnd` continuity covers the snapshot point.

**2. Take a snapshot (T1) while the tailer is running.**

```bash
# Terminal C
new-mongo-snapshot
```

Use the same tag you passed to `start-oplog-tailer` above (e.g. `om-20260506-120000`). The tail of the output prints the per-collection `preSnap` / `postSnap` counts.

**3. Let the load continue writing for 60+ seconds (T1 → T2).**

This is the recovery window — writes you want to recover. The tailer is filling its segment files in the background.

**4. Stop the load command, then stop the tailer.**

Press `Ctrl-C` in Terminal A. Then stop the tailer in Terminal C (or from any terminal):

```bash
stop-oplog-tailer --snapshot-tag "om-20260506-120000"
```

`stop-oplog-tailer` writes a `.stop` sentinel, waits for the tailer to acknowledge, then captures `~/mongo-oplog-stream/<tag>/t2-mark.json` containing the live counts of `testdb.loadtest` and `testdb.payload` via mongos. `invoke-oplog-replay` auto-discovers this file and uses it as the upper bound of the post-replay range assertion.

**5. Delete the database (simulate the disaster).**

```bash
mongos 'db.getSiblingDB("testdb").dropDatabase()'
```

**6. Restore from snapshot (T1).**

```bash
restore-mongo-snapshot --snapshot-tag "om-20260506-120000"
```

At this point the cluster is at T1. All writes between T1 and T2 are missing. STEP 8's baseline check confirms the cluster matches the T1 baseline tags before oplog replay begins.

**7. Replay the oplog segments to advance the cluster to T2.**

```bash
# Replay all captured segments (advance to the end of the captured stream)
invoke-oplog-replay --snapshot-tag "om-20260506-120000"

# Or replay to a specific Unix timestamp
invoke-oplog-replay --snapshot-tag "om-20260506-120000" --target-timestamp <unix-ts>
```

To get a Unix timestamp for a specific time:

```bash
python3 -c "import datetime as d; print(int(d.datetime.fromisoformat('2026-05-06T12:01:00+00:00').timestamp()))"
```

### Expected Results

- Restore (step 6) completes without error and cluster stabilizes; STEP 8 reports `Baseline OK` for the T1 baseline-tag values.
- Oplog replay (step 7) behaviour is **topology-dependent**. In this lab, `db.adminCommand({listShards:1})` returns three shards but the first has `_id: "config"`:
  ```
  [{"_id":"config","host":"aen-shard_0/..."},{"_id":"aen-shard_1",...},{"_id":"aen-shard_2",...}]
  ```
  This is a **config shard** (MongoDB 7.0+ feature where the CSRS doubles as a data shard) — the result of selecting "Embedded" config server in Ops Manager. `new-mongo-snapshot` and `invoke-oplog-replay` both key per-shard artifacts by `s._id` (the canonical shard identifier), so the on-disk segment directory for the config shard is `<root>/config/`, not `<root>/aen-shard_0/`.
  - **Data shards (shardId `aen-shard_1`, `aen-shard_2`)**: replay must complete without error. A failure here is a real failure.
  - **Config shard (shardId `config`, replica set `aen-shard_0`)**: `mongorestore --oplogReplay` may log `NotWritablePrimary` against config-database namespaces (`config.*`) because the config-server primary rejects oplog replay of its internal namespaces. The user-data namespaces on this shard (e.g. `testdb.*`) replay normally. This is expected and not a failure as long as the data-namespace counts post-replay match T2.
- Post-replay range check (built into `invoke-oplog-replay`):
  - **Lower bound** = the `mongo:preSnap` tag from the snapshot — restore alone lands in `[preSnap, postSnap]`, and replay can only add documents (insert workload), so any post-replay count **below** `mongo:preSnap` is a regression.
  - **Upper bound** = `T2_mark` from `t2-mark.json` written by `stop-oplog-tailer` — replay cannot fabricate documents beyond what was actually written to the cluster.
  - The command asserts `preSnap <= postReplay <= T2_mark` for each sampled collection and prints `unrecoveredTail = T2 - postReplay`. Because the tailer was started before the FA snapshot and OM's `previousEnd` ensures coverage is contiguous across iterations, the only legitimate source of an unrecovered tail is the time window between the tailer's last FINISHED job and the `stop-oplog-tailer` invocation — bounded above by `--interval-sec` × write-throughput. A tail much larger than that bound indicates either a `previousEnd` gap (check for `gap-<timestamp>.json` markers in the oplog stream directory) or a node failure that caused an oplog snapshot job to fail mid-iteration.
  - A post-replay count strictly above the `mongo:postSnap` tag confirms replay actually advanced the cluster past the snapshot window (the design goal of PITR).

### Verified Results (2026-05-17, tag `om-20260517-085219`)

| Metric | Value |
|---|---|
| Snapshot tag | `om-20260517-085219` |
| Load generator | `start-insert-load`, running throughout |
| Oplog tailer | 1 OM job, 355 segments (79 aen-shard_0 + 79 aen-shard_1 + 197 aen-shard_2), `--interval-sec 60` |
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

---

## Verified Results (2026-06-05 — hybrid gateway-routed client, Tests 1–3)

Run via `run-all-tests` (all three phases) against `aen-cluster` with the **hybrid** client
(`FA_ENDPOINT=sn1-x90r2-f06-27`): object/read operations routed through the Fusion gateway via
`context_names`, tag operations direct per array. The suite drove 3 snapshots + 3 restores and exited `0`.

| Test | Result | Tag | Detail |
|---|---|---|---|
| 1 — basic restore (no load) | ✅ PASS | `om-20260605-183119` | exact recovery — `loadtest` 2,159,780 / `payload` 200,000, **drift 0** |
| 2 — restore under load | ✅ PASS | `om-20260605-183942` | post-restore `loadtest` 2,330,780 ∈ `[preSnap 2,330,180, postSnap 2,331,180]` (**drift 1000**); ~31k post-snapshot writes correctly lost (pre-drop was 2,361,980). Crash-consistency under ~334 docs/s confirmed. |
| 3 — PITR under load | ✅ PASS (forward recovery, all 3 shards) | `om-20260605-195104` | restored to T1 (`loadtest` 2,819,246), replayed post-T1 segments on all three shards **including the config shard** → post-replay **2,853,446** = **+34,200 docs past T1**, ∈ `[preSnap 2,818,646, T2=2,872,846]`, `unrecoveredTail=19,400` (≈ the inherent ~1-min stop-window). **True forward PITR.** |

**Test 3 — forward-PITR notes.** Getting a *true* forward replay took three steps. The first attempt
(`om-20260605-185104`) was a no-op: the OM oplog-snapshot stream was ~19 days stale (serving leftover `2026-05-17`
segments while T1 was `2026-06-05`), so `invoke-oplog-replay` correctly filtered every captured segment as pre-T1.
Running the tailer continuously brought the stream current — the OM agent was already producing current segments;
the cursor just needed to advance past the stale backlog. A re-run (`om-20260605-192735`) then showed real forward
recovery (+26,466 docs past T1) **but skipped the config shard** — surfacing a bug: the tailer wrote each shard's
segment dir keyed by *replica-set id*, so the embedded config shard's oplog landed under `aen-shard_0/` while
replay looked for it under the canonical *shard id* `config/`.

**Fixed and confirmed.** The tailer now keys segment dirs by shard id (mapped from mongos `listShards`, falling
back to rsId if mongos is unreachable) and replay tolerates either layout. The validated run above
(`om-20260605-195104`) writes the config shard under `config/`, replays it with no "skipping shard" warning, and
recovers across all three shards — leaving `unrecoveredTail=19,400`, which is now just the inherent stop-window
(≈ `--interval-sec` × write-throughput ≈ one minute at ~330 docs/s), not a dropped shard. Shrink it further with a
smaller `--interval-sec` or a brief quiesce before `stop-oplog-tailer`.

---

## Test 4 — Sharded self-restore, fidelity proof (mutate-then-restore)

The count-window check in Tests 1–2 (`preSnap ≤ got ≤ postSnap`) passes even for a *no-op* restore — if the
volumes were never actually reverted, the counts still match. This test proves a **true point-in-time revert** the
same way the replica-set cert item (1.A.1.a) does: between snapshot and restore, **mutate** the data and add a
**sentinel collection**, then confirm the restore both reverts the counts *and* makes the sentinel disappear. This
is the sharded analogue of the RS fidelity proof and is the recommended way to run the sharded self-restore rows
(2.A.a / 2.A.e).

### Setup

Run the session-setup block, then note the baseline (this is your expected post-restore count):

```bash
mongos 'var d=db.getSiblingDB("testdb"); print("loadtest="+d.loadtest.countDocuments()+" payload="+d.payload.countDocuments()+" sentinel="+d.sentinel.countDocuments())'
```

Expected: some `loadtest`/`payload` counts and `sentinel=0`.

### Steps

**1. Take a snapshot (T).**

```bash
new-mongo-snapshot --deployment aen-cluster --snapshot-tag "om-20260723-185500"
```

Snapshot tags must match `^om-\d{8}-\d{6}$` (no suffix). The tail prints the captured `preSnap`/`postSnap` counts.

**2. Mutate: delete the data and write a sentinel that did not exist at snapshot time.**

```bash
mongos 'var d=db.getSiblingDB("testdb"); d.loadtest.deleteMany({}); d.payload.deleteMany({}); d.sentinel.insertOne({s:"diverge",t:new Date()}); print("MUTATED loadtest="+d.loadtest.countDocuments()+" payload="+d.payload.countDocuments()+" sentinel="+d.sentinel.countDocuments())'
```

Expected: `loadtest=0 payload=0 sentinel=1` — the on-disk state now differs from the snapshot in both directions
(data removed *and* a new collection added).

**3. Restore from the snapshot.**

```bash
restore-mongo-snapshot --deployment aen-cluster --snapshot-tag "om-20260723-185500" --force
```

**4. Confirm the sentinel is gone.**

```bash
mongos 'print("sentinel="+db.getSiblingDB("testdb").sentinel.countDocuments())'
```

### Expected Results

- Restore completes; mongos up, all 4 shards registered, all primaries reachable.
- STEP 8 reports `Baseline OK` with **drift 0** for `testdb.loadtest` and `testdb.payload` (reverted to the
  snapshot counts), and the **per-shard** verification prints each shard's own counts and asserts their sum
  accounts for the mongos aggregate (`sum ≥ routed total`).
- Step 4 returns **`sentinel=0`** — the post-snapshot collection is gone, proving a real revert rather than a
  no-op (the sentinel would survive a no-op restore).

### Verified Results (2026-07-23, tag `om-20260723-185500`)

| Metric | Value |
|---|---|
| Baseline / post-restore | `loadtest` 49,600 / `payload` 20,000, **drift 0** |
| Mutation before restore | `loadtest` 0 / `payload` 0 / `sentinel` 1 |
| Per-shard (STEP 8) | `aen-shard_1` 33,055 + `aen-shard_2` 16,545 = 49,600 (= mongos aggregate); `config`/`aen-shard_3` empty, noted not failed |
| Sentinel after restore | **`sentinel=0`** (post-snapshot collection reverted away) |
| Restore wall time | ~113 s (4 volumes, FA CoW overwrite) |

---

## Test 5 — Sharded PIT self-restore with deterministic markers (A/B)

The sharded analogue of the RS PIT cert item (1.B.1.a). In addition to the count-window / `unrecoveredTail`
assertion (Test 3), this test carries two **deterministic marker documents** through the cycle: **A** inserted
*before* the snapshot and **B** inserted *after* it. A correct PIT restore lands at T1 with **A present, B absent**;
a correct replay to T2 recovers **B**. The markers remove all ambiguity from a live/idle load generator — even if
the bulk counts do not move, `B` reappearing after replay is proof the post-snapshot stream was captured and
applied. Markers live in an unsharded `pitrtest.marks` collection (routed to the primary shard).

> **Primary sourcing (2026-07-23):** both `start-oplog-tailer` and `new-mongo-snapshot` now target the **PRIMARY**
> of each shard/RS. The tailer prints `tailing on <host> [PRIMARY]` per shard and sets `preferredOplogNodes`
> accordingly. If OM returns HTTP 500 on `preferredOplogNodes`, an oplog snapshot is in progress — see the
> skip-forward note below and [../docs/LESSONS.md](../docs/LESSONS.md).

### Setup

Start the tailer **before** the snapshot so `previousEnd` continuity covers the snapshot point (same rule as
Test 3), and choose the tag you will use for the snapshot too:

```bash
# Terminal B — long-running, leave open until step 6
start-oplog-tailer --deployment aen-cluster --snapshot-tag "om-20260723-191000"
```

Confirm it selects a **PRIMARY** for every shard and begins capturing current segments. If the sharded oplog
cursor is **stale** (it drains a large backlog dated days ago, and the rapid `scp` may hit SSH `exit 255` →
`/fail` → re-drain with no progress), stop the tailer and **skip-forward re-baseline** first: create one oplog
snapshot spanning `[stale → now]`, `/start` it, then `/finish` it **without copying** — the cursor jumps to ~now
(a prior `FAILED` tailer job does not block the fresh create). Then restart the tailer; it captures current
segments immediately.

### Steps

**1. Insert marker A (pre-snapshot).**

```bash
mongos 'var m=db.getSiblingDB("pitrtest").marks; m.deleteMany({}); m.insertOne({m:"A",t:new Date()}); print("A="+m.countDocuments({m:"A"})+" B="+m.countDocuments({m:"B"}))'
```

**2. Take the snapshot (T1) with the same tag as the tailer.**

```bash
# Terminal C
new-mongo-snapshot --deployment aen-cluster --snapshot-tag "om-20260723-191000"
```

**3. Insert marker B plus a batch of post-T1 documents (the recovery window).**

```bash
mongos 'var m=db.getSiblingDB("pitrtest").marks; m.insertOne({m:"B",t:new Date()}); var d=db.getSiblingDB("testdb"); var b=[]; for(var i=0;i<2000;i++){b.push({postT1:1,i:i})}; d.loadtest.insertMany(b); print("B="+m.countDocuments({m:"B"})+" loadtest="+d.loadtest.countDocuments())'
```

**4. Drain, then stop the tailer.** Keep tailing until the captured `lastEnd` passes the wall-clock of step 3
(OM segments lag live writes by ~2–3 min — see Test 3's drain note), then:

```bash
stop-oplog-tailer --deployment aen-cluster --snapshot-tag "om-20260723-191000"
```

This writes `t2-mark.json` (the T2 upper bound).

**5. Restore from the snapshot (T1).**

```bash
restore-mongo-snapshot --deployment aen-cluster --snapshot-tag "om-20260723-191000" --force
```

Then confirm the markers are at the T1 state:

```bash
mongos 'var m=db.getSiblingDB("pitrtest").marks; print("A="+m.countDocuments({m:"A"})+" B="+m.countDocuments({m:"B"})+" loadtest="+db.getSiblingDB("testdb").loadtest.countDocuments())'
```

**6. Replay all captured segments to advance to T2.**

```bash
invoke-oplog-replay --deployment aen-cluster --snapshot-tag "om-20260723-191000"
```

Then confirm marker B is recovered:

```bash
mongos 'var m=db.getSiblingDB("pitrtest").marks; print("A="+m.countDocuments({m:"A"})+" B="+m.countDocuments({m:"B"})+" loadtest="+db.getSiblingDB("testdb").loadtest.countDocuments())'
```

> **Always pass `--deployment`.** On this multi-deployment install, omitting it makes `stop-oplog-tailer` and
> `invoke-oplog-replay` default to the sharded cluster's *sibling* deployment lookup and read the wrong T2 mark /
> target the wrong cluster (a real footgun — see [../docs/LESSONS.md](../docs/LESSONS.md)).

### Expected Results

- **After restore (step 5):** counts at the T1 baseline (**drift 0**, per-shard STEP 8 sums to the aggregate),
  and markers **`A=1 B=0`** — the cluster is exactly at the snapshot point, before B.
- **After replay (step 6):** `invoke-oplog-replay` asserts `preSnap ≤ postReplay ≤ T2` per collection and prints
  **`unrecoveredTail=0`** once the stream was drained; markers **`A=1 B=1`** — the post-snapshot write was
  recovered forward across all shards.
- Config-shard `NotWritablePrimary` warnings against `config.*` namespaces during replay are expected and not a
  failure (see Test 3); user-data namespaces (`testdb.*`, `pitrtest.*`) replay normally.

### Verified Results (2026-07-23, tag `om-20260723-191000`, primary-sourced)

| Metric | Value |
|---|---|
| Tailer node selection | **PRIMARY** per shard (`shard_2→aen-mongo-01`, `shard_3→aen-mongo-03`, `config→aen-mongo-config-00`, `shard_1→aen-mongo-02`) |
| Cursor re-baseline | skip-forward from stale → **70 s behind now** before the run |
| A (pre-snapshot) / B (post-snapshot) | inserted; T1 `loadtest` 49,600 → +2,000 → 51,600 |
| Post-restore (T1) | `loadtest` **49,600 (drift 0)**, markers **`A=1 B=0`** |
| Post-replay (T2) | `loadtest` **51,600 `unrecoveredTail=0`**, `payload` 20,000 `unrecoveredTail=0`, markers **`A=1 B=1`** |
| Gap markers | 0 |

---

## Test 6 — Replica-set self-restore, fidelity proof (mutate-then-restore)

The replica-set analogue of Test 4 (cert item **1.A.1.a**), run against the standalone 3-member RS `aen-rs-00`
(`aen-mongo-05/06/07`). Identical fidelity method — mutate + sentinel, then confirm the restore reverts both — but
there is **no mongos**: point `mongosh` at an RS member, and node selection / STEP 7 use the `replicaset` branch
(verify a writable primary, no `listShards`).

An RS helper (uses the RS member host from `.env`; run the session-setup block first):

```bash
rs() {
    ssh "${SSH_OPTS[@]}" "${SSH_USER}@${AEN_RS_00__MONGOS_HOST}" \
        "${MONGOSH_PATH} --quiet --eval '$1'"
}
```

### Setup

```bash
rs 'var d=db.getSiblingDB("testdb"); print("loadtest="+d.loadtest.countDocuments()+" payload="+d.payload.countDocuments()+" sentinel="+d.sentinel.countDocuments())'
```

### Steps

**1. Snapshot (T).** `new-mongo-snapshot --deployment aen-rs-00 --snapshot-tag "om-20260723-184500"`
— STEP 1 opens the `$backupCursor` on the **primary** (confirmable in OM's log: `checkpointingTarget = <primary>`).

**2. Mutate + sentinel.**

```bash
rs 'var d=db.getSiblingDB("testdb"); d.loadtest.deleteMany({}); d.payload.deleteMany({}); d.sentinel.insertOne({s:"diverge",t:new Date()}); print("MUTATED loadtest="+d.loadtest.countDocuments()+" payload="+d.payload.countDocuments()+" sentinel="+d.sentinel.countDocuments())'
```

**3. Restore.** `restore-mongo-snapshot --deployment aen-rs-00 --snapshot-tag "om-20260723-184500" --force`

**4. Confirm the sentinel is gone** (the restore may elect a different primary; the helper targets any member):

```bash
rs 'var d=db.getSiblingDB("testdb"); print("loadtest="+d.loadtest.countDocuments()+" payload="+d.payload.countDocuments()+" sentinel="+d.sentinel.countDocuments())'
```

### Expected Results

- Restore completes; STEP 7 reports `replica set up, primary elected: <host>` (no `listShards`); STEP 8 reports
  **drift 0** for both collections (RS deployments skip the per-shard STEP 8 — the single RS *is* the aggregate).
- Step 4 returns **`sentinel=0`** — true point-in-time revert, not a no-op.

### Verified Results (2026-07-23, tag `om-20260723-184500`)

| Metric | Value |
|---|---|
| Baseline / post-restore | `loadtest` 17,000 / `payload` 5,000, **drift 0** |
| Mutation before restore | `loadtest` 0 / `payload` 0 / `sentinel` 1 |
| Cursor node | primary `aen-mongo-06` (STEP 1) |
| Primary after restore | `aen-mongo-05` (re-elected on restart — normal) |
| Sentinel after restore | **`sentinel=0`** |
| Restore wall time | ~71 s (3 volumes, FA CoW overwrite) |

---

## Test 7 — Replica-set PIT self-restore with deterministic markers (A/B)

The replica-set analogue of Test 5 (cert item **1.B.1.a**), primary-sourced. Same A/B marker method; the tailer and
replay use the `replicaset` branches (single RS, no `listShards`). **Prerequisite:** `SSH_USER` (e.g. `packer`)
must be in the **`mongod` group** on every RS node so the tailer can `scp` the `640 mongod:mongod` `.oplogs`
segments (`sudo usermod -aG mongod <ssh_user>`).

### Setup

```bash
# Terminal B — long-running, leave open until step 6
start-oplog-tailer --deployment aen-rs-00 --snapshot-tag "om-20260723-183000"
```

Confirm it prints `tailing on <host> [PRIMARY]` and `Preferred oplog nodes set: <primary>`. Re-baseline first with
a skip-forward if the cursor is stale (same as Test 5).

### Steps

**1. Marker A (pre-snapshot).**

```bash
rs 'var m=db.getSiblingDB("pitrtest").marks; m.deleteMany({}); m.insertOne({m:"A",t:new Date()}); print("A="+m.countDocuments({m:"A"})+" B="+m.countDocuments({m:"B"}))'
```

**2. Snapshot (T1).** `new-mongo-snapshot --deployment aen-rs-00 --snapshot-tag "om-20260723-183000"`

**3. Marker B + a batch of post-T1 documents.**

```bash
rs 'var m=db.getSiblingDB("pitrtest").marks; m.insertOne({m:"B",t:new Date()}); var d=db.getSiblingDB("testdb"); var b=[]; for(var i=0;i<2000;i++){b.push({postT1:1,i:i})}; d.loadtest.insertMany(b); print("B="+m.countDocuments({m:"B"})+" loadtest="+d.loadtest.countDocuments())'
```

**4. Drain, then stop.** Wait until the tailer's captured `lastEnd` passes step 3's wall-clock, then
`stop-oplog-tailer --deployment aen-rs-00 --snapshot-tag "om-20260723-183000"`.

**5. Restore (T1)** — `restore-mongo-snapshot --deployment aen-rs-00 --snapshot-tag "om-20260723-183000" --force`
— then confirm `A=1 B=0`:

```bash
rs 'var m=db.getSiblingDB("pitrtest").marks; print("A="+m.countDocuments({m:"A"})+" B="+m.countDocuments({m:"B"})+" loadtest="+db.getSiblingDB("testdb").loadtest.countDocuments())'
```

**6. Replay to T2** — `invoke-oplog-replay --deployment aen-rs-00 --snapshot-tag "om-20260723-183000"` — then
confirm B is recovered (`A=1 B=1`) with the same `rs '…marks…'` eval.

> **Always pass `--deployment aen-rs-00`** on `stop-oplog-tailer` and `invoke-oplog-replay` — omitting it defaults
> to the sibling (sharded) deployment and reads the wrong T2 mark / targets the wrong cluster (see
> [../docs/LESSONS.md](../docs/LESSONS.md)).

### Expected Results

- **After restore:** `loadtest` at the T1 baseline (**drift 0**), markers **`A=1 B=0`**.
- **After replay:** `preSnap ≤ postReplay ≤ T2` with **`unrecoveredTail=0`** once drained, markers **`A=1 B=1`** —
  the post-snapshot write recovered from the **primary-sourced** oplog stream.

### Verified Results (2026-07-23, tag `om-20260723-183000`, primary-sourced)

| Metric | Value |
|---|---|
| Tailer + cursor node | **primary `aen-mongo-06`** (OM log: `checkpointingTarget` + `oplogTailTarget` = `aen-mongo-06:27017`) |
| Post-restore (T1) | markers **`A=1 B=0`**, `loadtest` at T1 (drift 0) |
| Post-replay (T2) | markers **`A=1 B=1`** — marker B recovered forward |
| Deterministic proof | B absent at T1 → present at T2 confirms the primary-sourced stream captured & replayed the post-snapshot write |

> The 2026-07-23 run's load generator was idle (only the manual marker B and batch were post-snapshot writes), so
> marker B is the headline proof. A prior RS PIT run with a live bulk delta (2026-06-11, tag `om-20260611-150821`:
> T1=8,000 → T2=11,000) achieved the count-based **`unrecoveredTail=0`** — together they cover both the
> deterministic and the throughput cases.

### Re-baseline + zero-tail forward PITR (2026-06-08, tag `om-20260608-165149`)

A clean re-run that drives `unrecoveredTail` to **0** and isolates the cause of the residual tail.

**Re-baseline first.** The OM oplog-snapshot stream had again gone stale (cursor ~66.8 h behind while the oplog
head was current). The stream is a continuous chain — each `oplogSnapshot` job spans `[previousEnd → head]`, the
create call takes no start param, and the cursor advances only on `/finish` — so it cannot "jump to now." Toggling
`preferredOplogNodes` (empty list then re-set) does **not** re-baseline (verified: cursor unchanged; node toggling
only picks *which* node tails). What works is a **skip-forward**: create one oplog snapshot spanning
`[stale cursor → now]` and `/finish` it **without copying** the backlog — OM's `/finish` sends an empty body and
never verifies the copy, so the cursor advances to `now` and OM async-deletes the uncopied files. This permanently
discards PIT coverage for the skipped window (here 11,991 segments, 2026-06-06→06-08), so use it only when that
backlog is intentionally dropped. Result: cursor 66.8 h-behind → 0.1 h-behind in seconds.

**Then the PIT test, with a drain.** drop `testdb` → load A (`loadtest`=20,000) → start tailer → T1 snapshot →
load B (60 s) → **drain** (keep tailing until the captured `lastEnd` passes the load-stop epoch) → stop → restore →
replay-all.

| Metric | Value |
|---|---|
| A (snapshot point) | 20,000 |
| B_total (live at ~T2) | 39,600 |
| post-restore (T1) | 20,000 — restore lands **exactly** at the snapshot |
| post-replay | **39,600 — full recovery, `unrecoveredTail=0`** |
| gap markers | 0 |

**Why the drain matters (the residual-tail cause).** OM `.oplog` segments become available ~2–3 min *after* the
writes (60 s windows + agent/OM processing). Verified here: the tailer's last job finished at wall-clock 16:45:20
but the newest available segment ended at 16:43:00 — a ~2m20s lag. A first pass that stopped the tailer immediately
after the load recovered only 36,000 of ~49,200 (`unrecoveredTail=13,200`); the drain pass — waiting until
`lastEnd` (epoch 1780952520) passed the load-stop time (1780952496) — recovered **all** of B. So the residual tail
is **capture lag, not a restore/replay defect**: `invoke-oplog-replay` applies 100% of captured segments in order,
0 gaps. Continuous production tailing already keeps the stream current; only a PIT *test harness* needs the explicit
drain wait before relying on a recovery target.
