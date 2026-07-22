# Certification Test Run — 2026-07-22

Live cert-test pass against both deployments. Results feed back into
[Test-CertificationChecklist.md](Test-CertificationChecklist.md).

- **Build:** `main` @ `5c29659` (post Path-A revert; per-member snapshot + whole-cluster revert model).
- **Environment gate (passed):** OM `mongodb-mms` app server was **down** since ~Jun 17 (unit `active (exited)`, only the backup daemon running, nothing on :8080); restarted on `aen-mongo-00`, :8080 bound in ~90s. Verified reachable from the tooling (HTTP 303).
  - **Sharded** (`aen-cluster`, cluster `69fa4f72…`): 4 replica sets (config + 3 shards), **10/10 nodes healthy**, third-party **ACTIVE**.
  - **Replica set** (`aen-rs-00`, cluster `6a2a9456…`): 1 RS, **3/3 nodes healthy** (primary `aen-mongo-07`), third-party **ACTIVE**.

| Test | Deployment | Snapshot tag | Result |
|---|---|---|---|
| 1.A.1.a RS self-restore | `aen-rs-00` | `om-20260722-125520` | ✅ **PASS** |
| 2.A.a Sharded self-restore | `aen-cluster` | `om-20260722-131519` | ✅ **PASS** |
| 1.B.1.a RS PIT | `aen-rs-00` | — | ⚠️ **BLOCKED** (stale oplog stream) |
| 2.B.e Sharded PIT | `aen-cluster` | — | ⚠️ **BLOCKED** (stale oplog stream) |

---

## 1.A.1.a — Restore Replica Set to self (full snapshot) — ✅ PASS

Mutate-then-restore fidelity proof on `aen-rs-00`.

1. **Baseline:** `testdb.loadtest=11000`, `testdb.payload=5000`, collections `[loadtest, payload]`.
2. **Snapshot:** `new-mongo-snapshot --deployment aen-rs-00` → `aen-rs-00-pg.om-20260722-125520` on all 3 arrays (`FINISHED`, 230s). preSnap/postSnap both 11000/5000.
3. **Mutate (force divergence):** `deleteMany` on loadtest+payload → 0/0; inserted `testdb.sentinel` (1 doc). Collections `[loadtest, payload, sentinel]`.
4. **Restore:** `restore-mongo-snapshot --deployment aen-rs-00 --snapshot-tag om-20260722-125520 --force` → STEP 7 primary re-elected `aen-mongo-07`; STEP 8 `loadtest=11000 in [11000,11000] (drift=0)`, `payload=5000 (drift=0)`; 119s.
5. **Fidelity check:** post-restore collections `[loadtest, payload]` — **`sentinel` GONE**.

**Verdict:** true point-in-time revert (not a no-op) — B fully reverted, A and only A present, drift 0.

---

## 2.A.a — Self Restore Sharded Cluster — ✅ PASS

Mutate-then-restore on `aen-cluster` (dedicated config + 3 shards).

1. **Baseline:** `testdb.loadtest=39600`, `payload=20000`, shards `[aen-shard_1, aen-shard_2, config, aen-shard_3]`.
2. **Snapshot:** `new-mongo-snapshot` → `aen-cluster-pg.om-20260722-131519` on all 4 arrays (`FINISHED`, 387s).
3. **Mutate:** delete loadtest+payload → 0/0; inserted `testdb.sentinel`.
4. **Restore:** `restore-mongo-snapshot --snapshot-tag om-20260722-131519 --force` → STEP 7 `mongos up, 4 shards registered, 4 primaries reachable`; STEP 8 `loadtest=39600 (drift 0)`, `payload=20000 (drift 0)`; 110s.
5. **Per-shard STEP 8:** `aen-shard_1=26339/13323`, `aen-shard_2=13261/6677`, `config=0/0`, `aen-shard_3=0/0` — per-shard sums = mongos aggregate (39600/20000); config + aen-shard_3 empty (data sharded before shard_3 was added — noted, not failed).
6. **Fidelity check:** post-restore collections `[loadtest, payload]` — **`sentinel` GONE**.

**Verdict:** true point-in-time revert, drift 0, and the shards physically hold the correct distribution.

---

## 1.B.1.a / 2.B.e — PIT self-restore (RS + sharded) — ⚠️ BLOCKED (stale oplog stream)

Not a tool fault — an environmental consequence of the ~5-week OM outage:

- The OM oplog-snapshot cursor is stale at ~**2026-06-12**; on the RS tailing node the newest captured oplog date-dir is **2026-07-17** (nothing for 07-18 → today).
- A PIT test needs a captured, continuous oplog spanning T1→T2, both of which are **today (2026-07-22)** — that window was never captured, so a replay would be a no-op against the stale point.
- Confirmed live: started the tailer for `aen-rs-00`, took T1 (`om-20260722-131500`), inserted B (→14000), but the captured segments were all June (epoch `1781216xxx`); aborted the run.

**Recovery (documented):** skip-forward re-baseline the stream (create one oplog snapshot spanning `[stale cursor → now]`, `/finish` without copying → cursor advances to now, OM discards the gap), let fresh capture run to build a window, then run a fresh T1→B→restore→replay cycle per cluster. The underlying capability passed live in **June** (1.B.1.a: restore→T1 drift 0, replay→T2 `unrecoveredTail=0`).
