# MongoDB Snapshot & PITR — Pure Storage FlashArray + Fusion + Ops Manager (Python)

Crash-consistent snapshot backup and point-in-time recovery of a MongoDB 8.0 Enterprise **sharded cluster**
using Pure Storage FlashArray, **Pure Storage Fusion**, and **MongoDB Ops Manager 8.0**. (Python implementation
of [`nocentino/mongodb-flasharray-backup`](https://github.com/nocentino/mongodb-flasharray-backup).)

Each MongoDB node's data volume lives on a FlashArray, and the arrays are enrolled in a Pure Storage Fusion
fleet. The toolkit talks to the arrays over the **FlashArray REST API**, connecting **directly to each fleet
member** (`fa_rest.py`) and authenticating fleet-wide with a directory account (`FA_USERNAME`/`FA_PASSWORD`) so
every array is reachable. `new-mongo-snapshot` uses the Ops Manager Third-Party Backup API to open a
`$backupCursor` on one secondary per shard (pinning the WiredTiger checkpoint and freezing journal cleanup);
while the cursor is open, a FlashArray protection-group snapshot is taken on every array in a coordinated
crash-consistent sweep — no `fsyncLock`, no write stall. `restore-mongo-snapshot` stops agents, unmounts,
overwrites each volume in-place from the snapshot (a sub-second CoW pointer swap), remounts, and restarts
agents — WiredTiger handles crash recovery automatically. For PITR, `start-oplog-tailer` continuously captures
oplog `.oplogs` segments via the Ops Manager Oplog Snapshot API, and `invoke-oplog-replay` replays them to a
target timestamp after a restore. See [docs/how-it-works.md](docs/how-it-works.md) for the recoverability deep
dive.

---

## Install

Requires **Python 3.11+** and the system OpenSSH client (`ssh`/`scp`) configured for key-based auth to every
cluster node.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

This installs the dependencies (`python-dotenv`, `requests`, `typer`) and the ten console scripts listed below.

> **SSH multiplexing.** All remote commands shell out to the system `ssh`/`scp` with ControlMaster options
> (`ControlPath=/tmp/ssh-mux-%C`, `ControlPersist=60s`) to avoid saturating `sshd`'s `MaxStartups`. No Python
> SSH library is used.

## Configure

```bash
cp .env.example .env
# Edit .env and fill in the values described below.
```

FlashArray authentication (`fa_rest.py`) supports two modes:

- **`FA_USERNAME` + `FA_PASSWORD`** — directory login; authorizes on **every** fleet member. Required for
  fleet-wide snapshot/restore. *(Preferred.)*
- **`FA_APITOKEN`** — a FlashArray API token; only valid on the array that issued it (single-array use, falls
  back to this when no password is set).

Keys: `FA_ENDPOINT`, `FA_USERNAME`, `FA_API_VERSION`, `FA_PROTECTION_GROUP`, `FA_CLUSTER_NAME`, plus
`FA_PASSWORD` and/or `FA_APITOKEN`; `OM_BASE_URL`, `OM_API_VERSION`, `OM_GROUP_ID`, `OM_CLUSTER_ID`,
`OM_PUBLIC_KEY`, `OM_PRIVATE_KEY`; `MONGOSH_PATH`, `MONGOS_HOST`, `MONGOS_PORT`, `SSH_USER`,
`MONGO_TOOLS_BASE`; and optional `CLUSTER_NODES` (comma-separated fallback used only when Ops Manager is
unreachable).

The `.env` is located via `$MONGO_FA_BACKUP_ENV`, else python-dotenv's search from the current directory, else
`./.env`. A missing file or required key raises immediately.

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

Run `<command> --help` for full option details.

| Command | Purpose |
|---|---|
| `initialize-protection-groups` | Create the PG on every fleet array and add data volumes. `--what-if`, `--prune`, `--force`. |
| `new-mongo-snapshot` | Take a crash-consistent FlashArray PG snapshot across all nodes via the backup-cursor window. `--snapshot-tag`, `--baseline-database`, `--baseline-collections`. |
| `restore-mongo-snapshot` | Destructive in-place restore from a snapshot tag. `--snapshot-tag` (required), `--force`, `--verify-database`, `--skip-verification`. |
| `remove-old-artifacts` | Retention cleanup of old FA snapshots + local oplog/log dirs. `--older-than-days` (required, 1–365), `--what-if`. |
| `start-oplog-tailer` | Continuously capture oplog `.oplogs` segments for PITR. `--snapshot-tag`, `--interval-sec`, `--timeout-minutes`, `--poll-interval-sec`, `--abort-on-gap`. |
| `stop-oplog-tailer` | Stop the tailer (`.stop` sentinel) and capture the T2 mark. `--snapshot-tag`, `--wait-sec`, `--baseline-database`, `--baseline-collections`. |
| `invoke-oplog-replay` | Replay oplog segments to a target timestamp after a restore. `--snapshot-tag`, `--target-timestamp`, `--verify-database`, `--t2-mark-path`, `--skip-verification`. |
| `initialize-test-data` | Seed `testdb.loadtest`/`payload` (hashed `_id` sharding). `--loadtest-docs`, `--payload-docs`, `--batch-size`, `--force`. |
| `start-insert-load` | Continuous insert load generator. `--max-docs`, `--batch-size`. |
| `run-all-tests` | 3-phase e2e suite (restore / restore-under-load / PITR). `--start-at-test`, `--stop-after-test`. |

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

Metadata is stored on the FlashArray PG-snapshot **tags**, with JSON string values: `mongo:volumes`,
`mongo:preSnap`, `mongo:postSnap`, `mongo:t1ts` — these are the source of truth for restore and PITR. PITR
stream state lives in JSON files under `~/mongo-oplog-stream/<tag>/` (`state.json`, `t2-mark.json`,
`gap-*.json`) with `.stop`/`.started`/`.stopped` sentinels. Logs are teed to console and to
`~/mongo-{snapshot,restore,oplogtailer,oplogreplay}-logs/`.

## Development & tests

```bash
pip install -e '.[dev]'
pytest -q          # unit tests for config.py + decode_oplogs round-trip
```

The `tests/` directory holds Python unit tests (Digest hashing, locking, parallel runner, `.env` loader,
SCSI-serial selection, oplog decode). The end-to-end orchestration is installed as the `initialize-test-data`,
`start-insert-load`, and `run-all-tests` commands. Manual end-to-end test procedures live in
[tests-docs/](tests-docs/).

## Implementation notes

- **FlashArray access** is a direct REST client (`fa_rest.py`): it connects to each fleet member directly, and
  `context_names=[<array>]` selects which array to connect to. A directory login (`FA_USERNAME`/`FA_PASSWORD`)
  authorizes fleet-wide; a configured `FA_APITOKEN` is array-local. Responses are unwrapped centrally — an API
  error becomes an empty result (for best-effort reads) or raises (for required operations).
- **Ops Manager auth** uses a manual RFC 2617 Digest flow over `requests`.
- **Topology and storage mappings are discovered at runtime** — cluster nodes from Ops Manager (with a
  `CLUSTER_NODES` fallback), and node→array→volume mappings from SCSI serial numbers — so the workflow adapts
  automatically as nodes or arrays are added or removed.
- **Remote execution** shells out to system `ssh`/`scp`; logging is teed to console and file.
