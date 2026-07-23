# Lessons

Running log of mistakes and the rules adopted to prevent recurrence (per the agent operating rules).

## 2026-07-23 — Always pass `--deployment` on the PITR subcommands

**Mistake:** During the primary-sourced PITR verification on `aen-rs-00`, I ran `stop-oplog-tailer` and
`invoke-oplog-replay` with only `--snapshot-tag` (no `--deployment`). Both default to the first/sharded
deployment (`aen-cluster`), so the T2 mark and the first replay pass targeted the wrong cluster. The replay
found no matching `aen-rs-00` segments and silently no-op'd, and the T2 baseline (sharded counts) produced a
misleading `unrecoveredTail`. The actual RS recovery was correct (proven by a deterministic marker), but the
noise cost time.

**Rule:** On multi-deployment installs, **always pass `--deployment <name>` explicitly** to every subcommand
(`new-mongo-snapshot`, `restore-mongo-snapshot`, `start/stop-oplog-tailer`, `invoke-oplog-replay`,
`initialize-protection-groups`). Do not rely on the default. When a PITR result looks off (e.g. an unexpected
`unrecoveredTail`), first confirm the tool ran against the intended deployment before investigating data.

## 2026-07-23 — Clear an in-progress oplog snapshot before touching `preferredOplogNodes`

**Mistake:** The tailer failed with HTTP 500 on `POST preferredOplogNodes`. I first assumed OM rejects the
primary as an oplog node; it does not. OM's log showed `Cannot update preferred oplog nodes while an oplog
snapshot is in progress` — an orphaned in-progress oplog job (left by earlier OM instability) was the blocker,
for any node.

**Rule:** Treat a 500 on `preferredOplogNodes` as "a job is in progress," not "bad node." Read the
in-progress `oplogSnapshotId` from `GET .../clusters/{id}` and drive it to `FINISHED`
(`POST /start` **from `INITIAL`** → poll `READY` → `POST /finish`). `/start` must be issued from `INITIAL`;
the job does not advance on its own. Also: idempotent OM POSTs (like `preferredOplogNodes`) should use
`invoke_om_api_with_retry`, not `invoke_om_api`, so a transient 500 doesn't abort the whole flow.
