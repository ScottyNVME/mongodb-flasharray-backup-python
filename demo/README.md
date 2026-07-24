# Demo scripts

Runnable, self-contained demos of the backup/restore workflows. Each derives its connection details from
`.env` (via `config.load_config()` conventions) and uses the installed console scripts — run them from the
repo root with the virtualenv present.

> These demos are **destructive to the target's `testdb`** (a self-restore overwrites the cluster's own
> FlashArray volumes). Run only against a lab / test deployment.

## `rs-restore-demo.sh` — replica-set non-PITR restore (cert item 1.A.1.a)

Walks the certified replica-set self-restore end-to-end with a fidelity proof:

1. baseline the data
2. `new-mongo-snapshot` — opens `$backupCursor` on the primary, takes an FA protection-group snapshot
3. **mutate** — delete `testdb` data **and** insert a `sentinel` collection that did not exist at snapshot time
4. `restore-mongo-snapshot --force` — CoW volume overwrite → WiredTiger crash recovery → RS re-forms
5. **verify** — counts revert to the snapshot (drift 0) **and** the sentinel is gone

The sentinel is the point: a restore that merely matched on counts (a no-op) would leave it behind; a true
point-in-time revert removes it.

```bash
# default: deployment aen-rs-00, auto-generated tag, interactive confirmation
demo/rs-restore-demo.sh

# non-interactive, explicit deployment/tag
demo/rs-restore-demo.sh --deployment aen-rs-00 --tag om-20260724-120000 --yes
```

Flags: `--deployment <name>` (must be `TOPOLOGY=replicaset`), `--tag om-YYYYMMDD-HHMMSS`, `--yes` (skip the
destructive-action confirmation), `--help`.

Exit code `0` = PASS (drift 0 + sentinel gone), `1` = FAIL (sentinel survived → volumes not truly reverted).

For the full manual procedure and recorded results, see
[../tests-docs/Test-SnapshotRestore.md](../tests-docs/Test-SnapshotRestore.md) (Test 6).
