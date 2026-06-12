# TODO — Restore Replica Set to a *different* Replica Set (follow-ups)

This tracks the deliberately-deferred work for cross-cluster restore. The first increment shipped as
`restore-mongo-snapshot-to-target` (module `mongodb_flasharray_backup/restore_to_target.py`,
certification item **1.A.1.b**), built for the simplest, most canonical, lowest-risk case. The items
below extend it.

## What shipped (the baseline)
- **RS reconcile = seed + initial-sync.** The snapshot is restored to ONE destination node (the *seed*);
  its `local.system.replset` is rewritten offline to a single-member config for the target RS name; the
  agent brings it up as primary; OM grows the RS to its member set and the other (wiped) members
  full-initial-sync from the seed.
- **Storage = same arrays, different volumes.** The seed's data volume must live on the *same* FlashArray
  that holds the source PG snapshot, so the volume overwrite resolves the snapshot member locally.
- **Full snapshot only** (no oplog / PITR).
- Addressing via explicit `--target-nodes` / `--target-rs-name` / `--target-seed` / `--target-member-port`
  flags; `--deployment` still selects the **source** (its PG, fleet arrays, and baseline tags).

## Follow-ups (in rough priority order)

### 1. Restore-all + force-reconfig (faster RS reconcile)
Instead of seeding one node and initial-syncing the rest, overwrite **every** destination member's volume
from the PG snapshot (mirrors `restore-mongo-snapshot`), offline-rewrite the RS identity on each, then
force one node primary (`rs.reconfig(..., {force:true})`). Avoids full initial sync (huge win on large
data) and preserves the instant-restore value prop. More moving parts: relies on the crash-consistent PG
snapshot, must reconcile slightly-divergent per-member oplog positions, and must avoid racing OM's
automationConfig. Add as a `--reconcile-mode {seed-initial-sync|restore-all}` flag (default the proven
seed mode).

### 2. Different arrays (replicated / copied snapshots)
Today the seed's volume and the source snapshot member must be on the same array. To support a destination
on *different* arrays, first replicate/copy the source PG snapshot to the destination arrays (async
replication target, pod, or `post_volume_snapshots` copy), then resolve the **replicated** (renamed)
snapshot name in the destination array's context before the overwrite. Add the replication step + name
resolution; surface clear errors when replication hasn't completed.

### 3. PITR to a different RS (1.B.1.b / 1.B.1.c)
Extend `start-oplog-tailer` / `invoke-oplog-replay` to apply oplogs onto the different RS after the
snapshot restore (and 1.B.1.c: handle a destination that includes an **arbiter** — arbiters hold no data,
are never snapshotable, and must be excluded from data steps but counted for RS quorum). **Gated on
RS-PIT-to-self (1.B.1.a)**, which still resolves shards via `listShards` and needs the `replicaset` branch
first.

### 4. DRY — share the per-node SSH steps with `restore.py`
`restore_to_target.py` intentionally **replicates** `restore.py`'s certified remote-shell steps (stop
agent, force-stop mongod, unmount, rescan+mount) verbatim rather than refactoring the only-live-validated
self-restore path. Once cross-cluster restore is itself certified, extract these into shared helpers in
`config.py` and have **both** modules call them, then re-run 1.A.1.a (self) and 1.A.1.b (to-different) to
confirm no regression.

## Known risks to watch (from live testing)
- **OM reconcile on the destination** — the agent must accept the rewritten single-member on-disk config
  and grow it to the automationConfig member set without wedging (cf. the topology/backup wedges noted in
  the project history). This is the main thing to validate live.
- **Offline standalone start specifics** — binary/dbPath/port/user are captured from the live process
  before stop; verify against the MongoDB "restore a replica set to new hosts" tutorial for any
  `minvalid` / `oplogTruncateAfterPoint` handling. Assumes no at-rest encryption keyFile on the data path.
