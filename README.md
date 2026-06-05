# MongoDB Snapshot & PITR — Pure Storage FlashArray + Fusion + Ops Manager (Python)

Python 3.11+ port of [`nocentino/mongodb-flasharray-backup`](https://github.com/nocentino/mongodb-flasharray-backup):
crash-consistent snapshot backup and point-in-time recovery of a MongoDB 8.0 Enterprise **sharded cluster**
using Pure Storage FlashArray, **Pure Storage Fusion**, and **MongoDB Ops Manager 8.0**. This is a faithful,
behavior-preserving 1:1 translation of the original PowerShell suite — same sequencing, same safety/locking,
same retry logic, same on-array metadata.

The FlashArrays — one per MongoDB node — are enrolled in a Pure Storage Fusion fleet. A single
`connect_fa()` call targets the Fusion gateway; all subsequent FlashArray operations are routed to the correct
array via `context_names=[...]` (the Python equivalent of the SDK's `-ContextName`), enabling fleet-wide
protection-group coordination. `new-mongo-snapshot` uses the Ops Manager Third-Party Backup API to open a
`$backupCursor` on one secondary per shard (pinning the WiredTiger checkpoint and freezing journal cleanup);
while the cursor is open, the Fusion-coordinated FlashArray protection-group snapshot captures every data
volume in a coordinated crash-consistent sweep — no `fsyncLock`, no write stall. `restore-mongo-snapshot`
stops agents, unmounts, overwrites each volume in-place from the snapshot (a sub-second CoW pointer swap),
remounts, and restarts agents — WiredTiger handles crash recovery automatically. For PITR,
`start-oplog-tailer` continuously captures oplog `.oplogs` segments via the Ops Manager Oplog Snapshot API,
and `invoke-oplog-replay` replays them to a target timestamp after a restore. See
[docs/how-it-works.md](docs/how-it-works.md) for the recoverability deep dive.

---

## Install

Requires **Python 3.11+** and the system OpenSSH client (`ssh`/`scp`) configured for key-based auth to every
cluster node.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -e .
```

This installs the dependencies (`python-dotenv`, `py-pure-client`, `requests`, `typer`) and the ten
console-scripts listed below.

> **Note on this folder name:** the project directory ends with a trailing space (`MongoDB Snaps Python `).
> Python's site machinery `rstrip()`s `.pth` lines, which breaks the *editable* (`pip install -e .`) path
> autoloader for a trailing-space path. Use a regular install (`pip install .`, which copies the package into
> site-packages and works from anywhere) here, or rename the folder to drop the trailing space if you want an
> editable install. A non-editable install of all ten console-scripts has been verified to work from any cwd.

> **SSH multiplexing is preserved 1:1.** All remote commands shell out to the system `ssh`/`scp` with the
> original ControlMaster options (`ControlPath=/tmp/ssh-mux-%C`, `ControlPersist=60s`) to avoid saturating
> `sshd`'s `MaxStartups` — exactly as the PowerShell version did. No Python SSH library is used.

## Configure

```bash
cp .env.example .env
# Edit .env — fill in FA_ENDPOINT, FA_USERNAME, FA_PASSWORD, OM_BASE_URL, OM_API_VERSION,
# OM_GROUP_ID, OM_CLUSTER_ID, OM_PUBLIC_KEY, OM_PRIVATE_KEY, MONGOSH_PATH, MONGOS_HOST,
# MONGOS_PORT, SSH_USER, MONGO_TOOLS_BASE, FA_PROTECTION_GROUP, FA_CLUSTER_NAME,
# CLUSTER_NODES (comma-separated fallback used only when Ops Manager is unreachable)
```

The `.env` is located via `$MONGO_FA_BACKUP_ENV`, else python-dotenv's search from the current directory, else
`./.env` (the Python analogue of the original `$PSScriptRoot/.env`). A missing file or required key raises
immediately — exactly like dot-sourcing `Config.ps1`.

## Prerequisites

- Python 3.11+ and the OpenSSH client; SSH key auth from this machine to every `SSH_USER@<node>` (passwordless `sudo`).
- All FlashArrays enrolled in the same Pure Storage Fusion fleet.
- Ops Manager API user with role `GLOBAL_BACKUP_ADMIN` and a public/private API key (HTTP Digest auth).
- Ops Manager [third-party backup](https://www.mongodb.com/docs/ops-manager/current/core/third-party-backup/)
  enabled and the cluster registered (state = `ACTIVE`).
- Protection groups initialized on all FlashArrays (`initialize-protection-groups`).
- A single data volume per node mounted at `/data/mongo` (no LVM).

---

## Commands

All command names mirror the original scripts 1:1. Run `<command> --help` for full option details. PowerShell
PascalCase parameters became kebab-case options (`-SnapshotTag` → `--snapshot-tag`, `-WhatIf` → `--what-if`).

| Command | Original script | Purpose |
|---|---|---|
| `initialize-protection-groups` | `Initialize-ProtectionGroups.ps1` | Create the PG on every fleet array and add data volumes. `--what-if`, `--prune`, `--force`. |
| `new-mongo-snapshot` | `New-MongoSnapshot.ps1` | Take a crash-consistent FlashArray PG snapshot across all nodes via the backup-cursor window. `--snapshot-tag`, `--baseline-database`, `--baseline-collections`. |
| `restore-mongo-snapshot` | `Restore-MongoSnapshot.ps1` | Destructive in-place restore from a snapshot tag. `--snapshot-tag` (required), `--force`, `--verify-database`, `--skip-verification`. |
| `remove-old-artifacts` | `Remove-OldArtifacts.ps1` | Retention cleanup of old FA snapshots + local oplog/log dirs. `--older-than-days` (required, 1–365), `--what-if`. |
| `start-oplog-tailer` | `pitr/Start-OplogTailer.ps1` | Continuously capture oplog `.oplogs` segments for PITR. `--snapshot-tag`, `--interval-sec`, `--timeout-minutes`, `--poll-interval-sec`, `--abort-on-gap`. |
| `stop-oplog-tailer` | `pitr/Stop-OplogTailer.ps1` | Stop the tailer (`.stop` sentinel) and capture the T2 mark. `--snapshot-tag`, `--wait-sec`, `--baseline-database`, `--baseline-collections`. |
| `invoke-oplog-replay` | `pitr/Invoke-OplogReplay.ps1` | Replay oplog segments to a target timestamp after a restore. `--snapshot-tag`, `--target-timestamp`, `--verify-database`, `--t2-mark-path`, `--skip-verification`. |
| `initialize-test-data` | `tests/Initialize-TestData.ps1` | Seed `testdb.loadtest`/`payload` (hashed `_id` sharding). `--loadtest-docs`, `--payload-docs`, `--batch-size`, `--force`. |
| `start-insert-load` | `tests/Start-InsertLoad.ps1` | Continuous insert load generator. `--max-docs`, `--batch-size`. |
| `run-all-tests` | `tests/Run-AllTests.ps1` | 3-phase e2e suite (restore / restore-under-load / PITR). `--start-at-test`, `--stop-after-test`. |

## Backup & recovery workflows

**One-time setup**
```bash
initialize-protection-groups --what-if      # preview
initialize-protection-groups
```

**Take a snapshot**
```bash
new-mongo-snapshot
# Note the tag printed at the end, e.g. om-20260512-143022
```

**Restore (crash-consistent, to the snapshot point)**
```bash
restore-mongo-snapshot --snapshot-tag om-20260512-143022
```

**Point-in-time recovery (snapshot + oplog replay)**
```bash
# 1. Start the tailer alongside your workload, using the snapshot tag you will take
start-oplog-tailer --snapshot-tag om-20260512-143022 &
# 2. Take the snapshot (same tag)
new-mongo-snapshot --snapshot-tag om-20260512-143022
# ... time passes, writes accumulate ...
# 3. Stop the tailer (records the T2 mark)
stop-oplog-tailer --snapshot-tag om-20260512-143022
# 4. Restore the snapshot, then replay oplog to a target Unix timestamp (0 = replay all)
restore-mongo-snapshot --snapshot-tag om-20260512-143022 --force
invoke-oplog-replay --snapshot-tag om-20260512-143022 --target-timestamp 1747058400
```

## Snapshot metadata

Metadata is stored on the FlashArray PG-snapshot **tags** (exactly as the original), with JSON string values:
`mongo:volumes`, `mongo:preSnap`, `mongo:postSnap`, `mongo:t1ts`. PITR state lives in JSON sidecars under
`~/mongo-oplog-stream/<tag>/` (`state.json`, `t2-mark.json`, `gap-*.json`) with `.stop`/`.started`/`.stopped`
sentinels. Logs are teed to console and to `~/mongo-{snapshot,restore,oplogtailer,oplogreplay}-logs/`.

## Development & tests

```bash
pip install -e '.[dev]'
pytest -q          # parity unit tests for config.py + decode_oplogs round-trip
```

The `tests/` directory holds Python parity unit tests (Digest hashing, locking, parallel runner, `.env`
loader, SCSI-serial selection, oplog decode). The faithful translations of the upstream `tests/*.ps1`
end-to-end orchestration scripts are installed as the `initialize-test-data`, `start-insert-load`, and
`run-all-tests` commands.

## Relationship to the original

This package is a 1:1 behavioral port. Notable Python-specific adaptations (all behavior-preserving):
- Remote execution shells out to system `ssh`/`scp` (keeps ControlMaster multiplexing).
- FlashArray operations use `py-pure-client`; `-ContextName @($x)` → `context_names=[x]`; responses are
  unwrapped centrally (`-ErrorAction SilentlyContinue` → return empty, `Stop` → raise).
- Ops Manager auth uses the same manual RFC 2617 Digest flow (`requests`).
- `Start-Transcript` → Python `logging` teed to console + file; `Write-Host -ForegroundColor` → `typer.secho`.
