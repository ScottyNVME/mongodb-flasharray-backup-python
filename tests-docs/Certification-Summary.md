# MongoDB Third-Party Backup — Certification Summary

**Audience:** team / stakeholders. A high-level summary of where `mongodb-flasharray-backup` stands against
MongoDB's third-party backup certification. For the full item-by-item mapping see
[Test-CertificationChecklist.md](Test-CertificationChecklist.md); for the latest live run see
[Test-Certification-Results-2026-07-22.md](Test-Certification-Results-2026-07-22.md).

## What this is

A Python toolkit that performs **crash-consistent snapshot backup and point-in-time recovery (PITR)** of
MongoDB 8.0 (sharded clusters *and* standalone replica sets) using **Pure Storage FlashArray + Fusion** under
**Ops Manager third-party backup**. Ops Manager opens a `$backupCursor`; while it's held, a FlashArray
protection-group snapshot is taken across all arrays; restore overwrites each volume in place (sub-second CoW)
and WiredTiger crash-recovers. PITR adds a continuous oplog tailer + replay.

## Verdict

**The certification-critical paths are validated on live hardware:** self-restore and point-in-time recovery,
on both a sharded cluster and a standalone replica set, plus failover and verification behaviors. Remaining
items are edge-topology cases (arbiter, cross-cluster restore, a combined node-change+PIT pass) — each either
implemented-and-unit-tested, blocked on a documented operator prerequisite, or out of scope.

## Environment

| Deployment | Topology | Notes |
|---|---|---|
| `aen-cluster` (sharded) | dedicated config server + 3 shards (`aen-mongo-01/02/03`, `-config-00`) | 10 nodes, one OM project |
| `aen-rs-00` (replica set) | 3 members (`aen-mongo-05/06/07`) | multi-deployment from one `.env` |

Each node's `/data/mongo` is a FlashArray volume in a Fusion fleet.

## Results (in-scope, applicable items)

| Item | Scenario | Status |
|---|---|---|
| 1.A.1.a | Replica-set **self-restore** | ✅ Validated (drift 0, mutate→restore fidelity proof) |
| 1.B.1.a | Replica-set **PIT** self-restore | ✅ Validated (restore→T1, replay→T2, `unrecoveredTail=0`) |
| 2.A.a / 2.A.e | Sharded **self-restore** (dedicated + embedded config) | ✅ Validated (drift 0, per-shard distribution verified) |
| 2.B.e | Sharded **PIT** self-restore | ✅ Validated (full forward recovery, all shards, `unrecoveredTail=0`) |
| 2.A.c / 2.A.d | Add / remove a shard, then restore | ✅ Validated |
| 2.A.g / 2.A.h | Config-server conversion (embedded ↔ dedicated), then restore | ✅ Validated |
| 2.B.c | PIT after preferred-node change | ✅ Validated in halves (continuity across a node change; forward recovery) — **combined single pass pending** |
| 3.a / 3.b / 3.c | Failover: node down pre-snapshot / fails mid-snapshot / mid-restore | ✅ Validated |
| 4.a | Set preferred oplog node (incl. on node add) | ✅ Validated (live node-add 2026-07-22) |
| 4.c / 4.d / 4.e | Gap-check before replay / status polling / fail-on-bad-state | ✅ Validated |
| 1.A.1.b | Restore RS → a **different** RS | 🟡 Implemented (`restore-mongo-snapshot-to-target`), unit-tested, not yet live-validated |
| 2.B.1.d | PIT restore with an **arbiter** | ⚠️ Blocked on an OM prerequisite (see runbook) |
| 4.b | Valid-restore-target endpoint | ⚠️ Design difference — validated at the **storage layer** (snapshot present + size match), not OM's endpoint |
| Incremental (all); restore-to-**different** (sharded / different-OM) | — | ❌ Out of scope (FlashArray snapshots are always full; in-place self-restore) |

## Restore model (confirmed with MongoDB)

Per-member snapshot + **whole-cluster revert**: every member is restored from its own snapshot and the cluster
comes back together, reconciling via normal replication on restart. **Operational requirement:** the primary
must retain **enough oplog to span the snapshot→revert-point gap**, or a lagging member hits
`OplogStartMissing`. Back up from a low-lag secondary to keep that spread small.

## Operational runbook (field-tested gotchas)

Things that will bite an operator, and the fix for each:

1. **Ops Manager app server can silently stop** (systemd unit shows `active (exited)`, only the backup daemon
   running, nothing on `:8080`). → Restart `mongodb-mms` on the OM host; it takes ~90 s to bind `:8080`.
2. **Oplog cursor goes stale after OM downtime.** The oplog *stream* stays continuous (the backup daemon keeps
   the agent capturing), but the OM snapshot cursor doesn't advance, so the tailer would drain a huge backlog.
   → **Skip-forward re-baseline:** create one oplog snapshot spanning `[stale cursor → now]` and `/finish` it
   *without copying*; the cursor jumps to now. (Sharded also needs `preferredOplogNodes` set first.)
3. **`SSH_USER` (e.g. `packer`) must be in the `mongod` group on *every* node** — including nodes added later
   (this bit us on the config server). Otherwise the tailer can't `scp` the `640 mongod:mongod` oplog files and
   `/fails` the job every cycle. → `sudo usermod -aG mongod <ssh_user>`.
4. **A new RS member on a new port needs the host firewall opened** (e.g. `27017`), or peers can't reach it and
   it sits `health=0`. → `sudo firewall-cmd --add-port=27017/tcp --permanent && sudo firewall-cmd --reload`.
5. **Adding an arbiter** (FCV ≥ 5.0) requires a cluster-wide **DefaultWriteConcern** configured in Ops Manager
   first; the implicit default becomes ambiguous with an arbiter. (Cluster-side `setDefaultRWConcern` alone did
   not satisfy OM's automationConfig validation — it must be set in OM's config.)
6. **Topology changes can wedge OM third-party backup** (shard removal `topologyAbort`; dedicated-config
   migration `THIRD_PARTY_DISCOVERY_ERROR`). → Recover with **force-unmanage → `mongodb-mms` restart →
   `manage`** (filed as issues #1 / #2).
7. **Oplog capture lag ~2–3 min** — the recoverable PIT point trails live until segments are captured; drain
   past your target before relying on it. A newly-added member/shard is likewise not `snapshotable` until OM's
   backup subsystem re-scans (transient `JOB_NOT_FOUND`).
8. **Re-run `initialize-protection-groups` after any topology change** — it refreshes the FA protection-group
   membership *and* the `mongo:` volume-map tags that snapshot/restore read (no per-node SSH on the hot path).

## Pending / follow-ups

- **1.A.1.b** cross-cluster RS→RS restore — live-validate (needs a target RS whose seed volume is on an array
  that holds the source snapshot).
- **2.B.1.d arbiter** — configure OM's cluster-wide DefaultWriteConcern, then add the arbiter and run the PIT.
- **2.B.c** — run the forced node-change **and** forward-advance together in a single PIT pass.

## Where to look

- Item-by-item mapping + rationale: [Test-CertificationChecklist.md](Test-CertificationChecklist.md)
- Latest live run + evidence (tags, counts): [Test-Certification-Results-2026-07-22.md](Test-Certification-Results-2026-07-22.md)
- How it works (deep dive): [../docs/how-it-works.md](../docs/how-it-works.md)
- Third-party backup API reference: [../docs/third-party-backup-reference.md](../docs/third-party-backup-reference.md)
- Manual test procedures: [Test-SnapshotRestore.md](Test-SnapshotRestore.md)
