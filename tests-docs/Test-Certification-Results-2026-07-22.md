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
| 1.B.1.a RS PIT | `aen-rs-00` | `om-20260722-190600` | ✅ **PASS** (after oplog re-baseline) |
| 2.B.e Sharded PIT | `aen-cluster` | `om-20260722-200000` | ✅ **PASS** (after config-00 `packer∈mongod` fix) |

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

**Correction:** the stream was **not** gapped — the earlier "newest oplog 2026-07-17 / capture stopped" read was a `tail` truncation artifact. The node has **continuous** oplog through now (the backup daemon kept the agent capturing while the API was down); only the OM **cursor** was stale at ~Jun 12. Fix = **skip-forward re-baseline**: create one oplog snapshot spanning `[stale cursor → now]`, `/finish` it *without copying* → the cursor advances to now. Verified live on both clusters (one job spanned prevEnd=Jun12 → end=now, ~140s behind, then FINISHED). Sharded also required setting `preferredOplogNodes` first (`THIRD_PARTY_OPLOG_PREFERENCE_MISSING` otherwise).

---

## 1.B.1.a — PIT Restore Replica Set to self — ✅ PASS (post re-baseline)

Fresh cycle on `aen-rs-00` after the skip-forward re-baseline, tag `om-20260722-190600`.

1. Tailer capturing current oplog from the advanced cursor.
2. **T1 snapshot** (A = `loadtest=14000, payload=5000`).
3. **Insert B** (+3000) → T2 = `17000/5000`.
4. Drain tailer past B (captured `end` ≥ B), stop → **T2 mark 17000/5000**.
5. **Restore → T1:** `loadtest=14000 (drift 0)`, primary re-elected.
6. **Replay-all (`--target-timestamp 0`) → T2:** `loadtest=17000 in [14000,17000] (unrecoveredTail=0)`, `payload=5000`.

**Verdict:** restore lands exactly at T1, forward replay recovers to T2 with `unrecoveredTail=0`. ✅

---

## 2.B.e — PIT Self Restore Sharded Cluster — ⚠️ BLOCKED (config-00 oplog perms)

Re-baseline + T1 + B all succeeded (tag `om-20260722-191500`; T1=39600/20000, B→44600/20000), but the tailer could **not** capture the **config server's** oplog: every `scp` of `aen-shard_0` oplog from `aen-mongo-config-00` failed (`exit 1`), so the tailer `/failed` the whole oplog-snapshot job each cycle (config-shard coverage is required, so shard_2/shard_3 captures didn't commit either).

**Root cause:** `packer` is **not in the `mongod` group** on `aen-mongo-config-00` (groups: packer, wheel), so it can't read the `640 mongod:mongod` oplog files — the exact gotcha documented for the RS nodes, recurring on the config server added during the restructure.

**Fix applied (operator ran the classifier-gated `sudo` on config-00):**
```bash
ssh <you>@aen-mongo-config-00.fsa.lab 'sudo usermod -aG mongod packer'   # -> groups now include 990(mongod)
```

**Re-run after the fix → ✅ PASS** (fresh cycle, tag `om-20260722-200000`):
- All 4 shards (incl. `config` on config-00) captured cleanly — no `scp` errors; 140 segments over 12 OM jobs.
- **T1′** = `44600/20000`; **insert B′** (+5000) → **T2′** = `49600/20000`.
- **Restore → T1′:** `loadtest=44600 (drift 0)`; per-shard `aen-shard_1=29704/13323` + `aen-shard_2=14896/6677` = aggregate.
- **Replay-all → T2′:** `loadtest=49600 in [44600,49600] (unrecoveredTail=0)`, `payload=20000`.

**Verdict:** full forward PIT recovery across all shards, `unrecoveredTail=0`. ✅ The config-00 `packer∈mongod` gotcha (RS gotcha recurring on the restructure's config server) is worth adding to the runbook.
