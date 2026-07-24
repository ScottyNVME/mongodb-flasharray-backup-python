# Running the test & certification suites with a Claude agent

This project has been validated by driving the tests with a **Claude Code agent** (e.g. Claude Code in the
terminal or the IDE extension) that has shell access to the **control machine** described in
[../HANDOFF.md](../HANDOFF.md). This doc is the playbook: what to give the agent, ready-to-paste prompts, the
operational knowledge it needs to succeed, and how it should record results. It mirrors exactly how the suites
were run during certification.

> These operations are **destructive to `testdb`** on the target cluster (restore overwrites volumes; PIT tests
> `dropDatabase` / `deleteMany`). Run only against a **lab** deployment, and expect the agent to ask for (or be
> granted) permission before each destructive step.

---

## 1. What the agent needs

Point the agent at a checkout on the control machine with:

- The repo cloned, the venv created, and `pip install -e .` done (console scripts on `PATH`).
- A filled-in **`.env`** (both deployments).
- **Passwordless SSH** from the control machine to every node as `SSH_USER`, and `SSH_USER` in the `mongod`
  group on every node (for PITR `scp`).
- OM reachable + both clusters `ACTIVE`; FA gateway reachable.
- Permission to run destructive commands (`restore-mongo-snapshot --force`, `dropDatabase`, `deleteMany`,
  the balancer stop/restart, and — for prod-OM-write recovery paths — OM API writes). In Claude Code, the
  agent will hit permission/classifier prompts on these; approve the ones you intend.

Give it this doc plus [../HANDOFF.md](../HANDOFF.md) and
[../tests-docs/Test-SnapshotRestore.md](../tests-docs/Test-SnapshotRestore.md) as context.

---

## 2. Ready-to-paste prompts

These are the actual kinds of instructions used during certification. Paste one; the agent works autonomously and
reports pass/fail with evidence.

**Run the full documented test suite (Tests 1–8):**
```
Run all the tests in tests-docs/Test-SnapshotRestore.md end-to-end, both deployments. Do the environment
gate first (OM reachable + both clusters healthy). For each test report PASS/FAIL with the concrete evidence
(counts, drift, unrecoveredTail, sentinel/marker state). Record the results and commit.
```

**Run the certification matrix:**
```
Run all valid/in-scope certification tests from tests-docs/Test-CertificationChecklist.md. Document the
results in tests-docs/ and commit. Skip out-of-scope items (incremental, restore-to-different) but say why.
```

**One targeted flow (example — RS PITR):**
```
Live-verify a full PITR cycle on aen-rs-00: snapshot(T1) → insert a post-snapshot marker → restore→T1 →
replay→T2. Confirm the tailer and cursor target the primary, and that the marker is absent at T1 and present
at T2 with unrecoveredTail=0.
```

**Single self-restore fidelity check (fastest smoke test):**
```
Run demo/rs-restore-demo.sh against aen-rs-00 and report the result. (Non-PITR RS self-restore: mutate →
restore → sentinel gone, drift 0.)
```

The agent should use a **TODO list** to track multi-test runs, run destructive steps only after the
environment gate passes, and **stop + re-plan** if a step fails rather than pushing through.

---

## 3. The test inventory (what "all the tests" means)

From [../tests-docs/Test-SnapshotRestore.md](../tests-docs/Test-SnapshotRestore.md):

| # | Test | Deployment | Pass criteria |
|---|---|---|---|
| 1 | Basic restore (no load) | sharded | drop → restore → **drift 0** |
| 2 | Restore under load | sharded | post-restore count **∈ [preSnap, postSnap]**; post-snapshot writes lost |
| 3 | PITR under load | sharded | restore→T1 in window; replay→T2 **`unrecoveredTail=0`** |
| 4 | Self-restore fidelity | sharded | mutate → restore → **drift 0 + sentinel gone**; per-shard STEP 8 sums to aggregate |
| 5 | PIT with A/B markers | sharded | restore→T1 `A=1,B=0`; replay→T2 `A=1,B=1`, `unrecoveredTail=0` |
| 6 | RS self-restore fidelity | replica set | mutate → restore → **drift 0 + sentinel gone** |
| 7 | RS PIT with A/B markers | replica set | restore→T1 `A=1,B=0`; replay→T2 `A=1,B=1`, `unrecoveredTail=0` |
| 8 | Sharded snapshot quiesces balancer | sharded | balancer stopped for snapshot, **restored** to prior state |

The **certification** items ([../tests-docs/Test-CertificationChecklist.md](../tests-docs/Test-CertificationChecklist.md))
map onto these plus failover (3.a/b/c), verification (4.a–e), and edge-topology cases (add/remove shard, config
conversion, arbiter). The four core in-scope items are **1.A.1.a** (RS self-restore), **1.B.1.a** (RS PIT),
**2.A.a** (sharded self-restore), **2.B.e** (sharded PIT).

---

## 4. Operational knowledge the agent must have

These are the things that make a run succeed or fail — learned the hard way (see [LESSONS.md](LESSONS.md)). An
agent that knows them up front avoids the dead ends.

1. **Always pass `--deployment <name>`** on multi-deployment installs. Omitting it silently targets the *other*
   (default/sharded) deployment — the #1 footgun. It makes `stop-oplog-tailer`/`invoke-oplog-replay` read the
   wrong T2 mark or replay against the wrong cluster.
2. **Snapshot tags must match `^om-\d{8}-\d{6}$`** (e.g. `om-20260724-120000`) — no suffixes. The tailer and the
   snapshot must share the tag for replay to find both the FA snapshot and the oplog stream.
3. **Connect correctly:** sharded → `mongosh mongodb://<mongos-host>:27017` (a mongos, `isdbgrid`); RS → run
   `mongosh` on a member and find the writable primary (`db.hello().isWritablePrimary`). `mongosh` is not on
   `PATH` on nodes — use the full `MONGOSH_PATH` (`/var/lib/mongodb-mms-automation/mongosh-.../bin/mongosh`).
4. **Stale oplog cursor → skip-forward re-baseline.** If a PIT tailer starts draining a large backlog (segments
   dated hours/days ago) it can wedge on rapid `scp` (SSH `exit 255`) → `/fail` → re-drain with no progress.
   Fix: create one oplog snapshot spanning `[stale → now]`, `/start` it (from `INITIAL`!), then `/finish`
   **without copying** — the cursor jumps to ~now. A prior `FAILED` job does not block the fresh create.
5. **`preferredOplogNodes` HTTP 500 = "an oplog snapshot is in progress"**, not a bad node. Read the in-progress
   `oplogSnapshotId` from `GET .../clusters/{id}` and drive it to `FINISHED` (`/start` from `INITIAL` → poll
   `READY` → `/finish`), then retry.
6. **Drain past your target before stopping a PIT tailer.** OM `.oplogs` segments lag live writes by ~2–3 min;
   wait until the captured `lastEnd` passes the last write's wall-clock, or `unrecoveredTail` will be non-zero
   purely from capture lag (not a defect).
7. **Prove fidelity, don't just match counts.** A same-count check passes even for a no-op restore. Use the
   *mutate-then-restore* method (delete + a `sentinel` collection → confirm the sentinel is **gone**) and, for
   PIT, deterministic **A/B markers** in `pitrtest.marks` (A pre-snapshot, B post-snapshot).
8. **The load generator (`start-insert-load`) is fast** (~1–2 k docs/s) and targets the default (sharded)
   deployment. Consistency windows can span tens of thousands of docs — that's normal; the assertion is only
   that the restore lands *inside* the window.
9. **Sharded snapshots stop the balancer** automatically now (drains any in-flight migration, restores prior
   state). If it can't confirm the balancer stopped it fails loud — fix mongos reachability or pass
   `--skip-balancer-stop` (unsafe) to override.
10. **`SSH_USER` must be in `mongod`** on every node (incl. newly added ones) or the tailer `scp` fails silently.

---

## 5. How the agent should record results

Match what certification runs already do:

- Append a dated results section to **[../tests-docs/Test-Certification-Results-2026-07-22.md](../tests-docs/Test-Certification-Results-2026-07-22.md)**
  (or a new `Test-Certification-Results-<date>.md`) with a table of item → deployment → tag → result + the
  concrete evidence.
- Update the banner/status in **[../tests-docs/Test-CertificationChecklist.md](../tests-docs/Test-CertificationChecklist.md)**
  and the verdict in **[../tests-docs/Certification-Summary.md](../tests-docs/Certification-Summary.md)** if a
  status changed.
- Record any new footgun in **[LESSONS.md](LESSONS.md)**.
- Commit + push (this repo's convention is commit to `main`); end commit messages with the project's
  `Co-Authored-By:` trailer.
- Leave both clusters **healthy** and **no leftover** load/tailer processes; restore any state the tests
  changed (balancer, membership).

---

## 6. Interpreting the key signals

- **`drift = N`** on a restore — `postSnap - preSnap` at snapshot time (the consistency window width). `drift 0`
  means a quiesced/exact restore; a non-zero drift under load is fine as long as the post-restore count is in
  `[preSnap, postSnap]`.
- **`unrecoveredTail = 0`** on replay — the cluster reached the T2 mark; PITR recovered everything captured.
  A non-zero tail usually means the tailer wasn't drained (see §4.6), not a replay defect (replay applies 100%
  of captured segments in order).
- **`sentinel = 0`** post-restore / **marker `B` present** post-replay — the true fidelity proofs.
- Config-shard `NotWritablePrimary` warnings against `config.*` namespaces during replay are **expected**
  (the config server rejects oplog replay of its internal namespaces); user data (`testdb.*`) replays normally.
