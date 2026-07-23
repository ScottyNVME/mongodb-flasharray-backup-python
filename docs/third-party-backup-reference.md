# MongoDB Third-Party Backup — Reference Documentation

> Source: MongoDB Ops Manager Third-Party Backup official documentation (6 documents, last updated Dec 2025).
> All API paths are relative to `$HOST:$PORT/api/public/v1.0/backup/third_party`.

---

## Table of Contents

1. [Preliminary Setup](#preliminary-setup)
2. [High Level Architecture](#high-level-architecture)
3. [Pipelines](#pipelines)
   - [Create Snapshot](#create-snapshot)
   - [Create Incremental Snapshot](#create-incremental-snapshot)
   - [Create Oplog Snapshot](#create-oplog-snapshot)
   - [Restore](#restore)
4. [FAQ](#faq)
5. [Testing Checklist](#testing-checklist)
6. [API Reference](#api-reference)
   - [Discovery API](#discovery-api)
   - [Management API](#management-api)
   - [Snapshot API](#snapshot-api)
   - [Restore API](#restore-api)
   - [Oplog Snapshot API](#oplog-snapshot-api)

---

## Preliminary Setup

### Step 0 — Synchronize time across machines

Implement Network Time Protocol (NTP) on all nodes.

### Step 1 — Set the oplog file path

Navigate to **Admin → General → Ops Manager Config** and add:

| Key | Value |
|---|---|
| `brs.thirdparty.baseOplogFilePath` | path to a directory accessible by the automation agent |

### Step 2a — Generate Global API Keys

1. Navigate to **Admin → General → API Keys**
2. Click **Create API Key**
3. Write a description and enable the **Global Backup Admin** permission
4. Click **Next**, copy and store the Public Key and Private Key securely
5. Click **Done**

### Step 2b — Generate Project Level API Keys

1. Navigate to **Access Manager → Project Access → API Keys**
2. Click **Create API Key**
3. Write a description and enable the **Project Backup Admin** permission
4. Click **Next**, copy and store the Public Key and Private Key securely
5. Click **Done**

### Step 3 — Enable Third-Party Backup

Navigate to **Admin → General → Ops Manager Config** and add:

| Key | Value | Scope |
|---|---|---|
| `mms.featureFlag.backup.thirdPartyManaged` | `controlled` | Project-level only |
| `mms.featureFlag.backup.thirdPartyManaged` | `enabled` | Globally for all projects |

For project-level: navigate to **Project Settings → Beta Features** and enable **Backup Third Party Managed**.

### Step 4 — Install MongoDB Agent on every server

Navigate to **Agents → Downloads & Settings**, select the operating system, and follow the instructions.
See: https://www.mongodb.com/docs/ops-manager/current/tutorial/nav/install-mongodb-agent/

### Step 5 — Configure ownership and permissions

Verify (e.g. via `stat`) that the agent on each node can read and write to the oplog directory configured in Step 1. Make the agent's owner and permissions match those of the oplog directory.

### Step 6 — Enable monitoring and backup on every server

1. Navigate to **Deployment → Servers**
2. For each server: click the meatball menu → **Activate Monitoring** and **Activate Backup**
3. Click **Review & Deploy** → **Confirm & Deploy**

### Step 7 — Mark the cluster as Managed

Navigate to **Continuous Backup**, hover over the status of the target cluster and click **Manage**. The status will change to **Third Party Managed**. This can also be done via the API (`POST .../clusters/{id}/manage`).

> **Replica sets:** `POST .../clusters/{id}/manage` is the correct enable path for a standalone replica set too — it needs **no OM snapshot store** (the FlashArray holds the snapshots). Supply a `syncSource` (an RS secondary) if OM requests one. Do **not** use the OM-*managed* path (`PATCH .../backupConfigs {statusName: STARTED}`) for third-party backup; it 409s `Could not find available Snapshot Store`.

### Step 8 — Integrate your third-party software

Use the API keys from Step 2 to integrate your third-party backup software via the API.

---

## High Level Architecture

### Create Snapshot

1. Third-party requests a snapshot from Ops Manager
2. Ops Manager notifies one agent per shard to create a snapshot
3. The agent instructs mongod to freeze data for a snapshot and adds a `fileList.txt` to the DB path; shards are aligned to ensure the freeze point is consistent across the cluster

### Copy Snapshot

1. Third-party calls the `fileList` endpoint to get a list of files
2. Third-party copies files from the MongoDB node to its storage location
3. *(Optional)* Third-party copies incremental blocks for an incremental snapshot
4. Third-party notifies Ops Manager that copy is complete

> **Volume snapshots** skip the fileList/copy steps — all files on the volume are captured atomically by the storage array.

### Create Oplog Snapshot

1. Third-party chooses oplog tailing nodes
2. Third-party requests an Oplog Snapshot from Ops Manager
3. Ops Manager notifies one agent per shard to create an Oplog Snapshot
4. The agent creates an Oplog Snapshot consisting of `.oplog` files at one-minute intervals

### Copy Oplog Snapshot

1. Third-party gets a list from OM of `.oplog` files to copy
2. Third-party copies the `.oplog` files from the MongoDB node to its storage location
3. Third-party notifies Ops Manager that copy is complete
4. Agents delete the copied oplog files from the oplog directory

### Restore

1. Third-party requests from Ops Manager a restore to a set of target nodes (optional: include a PIT timestamp)
2. Ops Manager notifies agents on all nodes to prepare for a restore
3. Third-party copies the snapshot files to the target primary and secondary (non-arbiter) nodes
4. *(PIT only)* Third-party copies oplog files to the target nodes in a **separate directory** (not where the agent writes to); this can be done any time before step 5 as long as all oplogs up to the PIT timestamp are copied
5. Third-party notifies Ops Manager that files have been copied
6. Third-party polls Ops Manager until the restore is complete

---

## Pipelines

### Create Snapshot

**Key rules:**

- Take a snapshot (full or incremental) whenever an oplog gap is detected
- A full snapshot should be taken at least once per week
- After a full snapshot is **started**, no prior snapshot can be used as a source — even if the full fails
- `incrementalMetadata.incremental` defaults to `true`

**How to choose target nodes** (from the `GET /{groupId}/clusters/{clusterId}` response):

| Priority | Rule |
|---|---|
| 1 | `snapshotable: true` — mandatory |
| 2 | Previously snapshotted nodes — enables incremental |
| 3 | Most recent `opTime` — aligns shard timestamps |
| 4 | **Primary** (highest-optime member); secondary fallback if the primary isn't snapshotable/reachable |
| 5 | Nodes in the region where you want the snapshot stored |

**Flow:**

```
POST /{groupId}/clusters/{clusterId}/snapshot
  → snapshotId returned

POST /{groupId}/clusters/{clusterId}/snapshot/{snapshotId}/start
  → starts timeout timer; opens $backupCursor; state: PENDING → READY

GET  /{groupId}/clusters/{clusterId}/snapshot/{snapshotId}
  → poll until state = READY (each GET resets timeout timer)

*** TAKE SNAPSHOT HERE (copy files or volume snapshot) ***

  [Volume snapshots: skip fileList/fileDiffs/unfreeze steps]
  [File-level: GET fileList, copy files (0 to fileSize bytes), POST unfreeze per node]

POST /{groupId}/clusters/{clusterId}/snapshot/{snapshotId}/finish
  → state: READY → FINISHING → FINISHED

GET  /{groupId}/clusters/{clusterId}/snapshot/{snapshotId}
  → poll until state = FINISHED
  → response now contains snapshotMetadata — store this for restore and next incremental
```

To abort: `POST .../snapshot/{id}/fail` → state: FAILING → FAILED.

---

### Create Incremental Snapshot

**Key rules:**

- Before an incremental snapshot, a prior full snapshot must exist from the same nodes
- MongoDB replication creates logical copies (not physical); using the same nodes minimizes data transfer
- After a full is started, no prior snapshot can be used as source — even if the full fails
- Take an incremental at least once per day
- Only the most recent successful snapshot can be used as a source (no cumulative incremental)

**Request body difference from full:**

```json
{
  "incrementalMetadata": {
    "incremental": true,
    "rsIdsToSrcBackupNames": [
      { "rsId": "<rsId>",  "srcBackupName": "<thisBackupName from previous snapshot>" },
      ...
    ]
  }
}
```

> To take a weekly full: omit `rsIdsToSrcBackupNames` (do NOT set `incremental: false`).

**`fileDiffs` for incremental (file-level only, skip for volume snapshots):**

After receiving the `finishedToken` from the last `fileList` request, POST to `/{nodeId}/fileDiffs`:

- `allBlocksChanged: true` → replace the entire file
- `allBlocksChanged: false` → use `diff` (Base64-encoded bitset): bit `1` = block needs replacing, `0` = block unchanged
- Block size is 16 MB (last block may be smaller)
- If `diff` is missing → copy the whole file

---

### Create Oplog Snapshot

**Key rules:**

- Take an oplog snapshot whenever a gap is found; take as frequently as possible
- Initialization order: **oplog snapshot first**, then full snapshot
- If a node runs out of disk space it stops tailing and gets stuck in `PENDING`
- There is no automatic failover: if a tailing node fails, the client must select a new one
- The oplog partition should be on a separate partition from the data directory (prevents crash if oplog fills disk)

**Flow:**

```
POST /{groupId}/clusters/{clusterId}/oplogSnapshot
  → oplogSnapshotId returned

POST /{groupId}/clusters/{clusterId}/oplogSnapshot/{id}/start
  → starts timeout timer; state: PENDING → READY

GET  /{groupId}/clusters/{clusterId}/oplogSnapshot/{id}
  → poll until state = READY
  → READY response includes: logFiles[], start/end epoch timestamps (PIT range)

*** COPY .oplog FILES HERE ***

POST /{groupId}/clusters/{clusterId}/oplogSnapshot/{id}/finish
  → OM deletes .oplog files asynchronously

GET  /{groupId}/clusters/{clusterId}/oplogSnapshot/{id}
  → poll until state = FINISHED
```

---

### Restore

**Prerequisites:**

- Restore destination cluster must match source: number of replica sets/shards, encryption settings + keys, `directoryPerDB`/`directoryForIndexes`, MongoDB version (equal or one greater with matching FCV)
- Restores delete all pre-existing data in the destination cluster

**Validate restore targets first:**

```
POST /{groupId}/clusters   (body: snapshotMetadata)
  → returns list of valid restore target clusters
```

**Flow:**

```
POST /{targetGroupId}/clusters/{targetClusterId}/restore
  body: { snapshotsMetadata, nodes (id + restoreRole), optional pitTimestamp }
  → restoreId returned

POST /{targetGroupId}/restore/{restoreId}/start
  → state: SHUTDOWN_IN_PROGRESS → COPY_FILES

GET  /{targetGroupId}/restore/{restoreId}
  → poll until state = COPY_FILES

*** COPY SNAPSHOT FILES TO EACH NON-ARBITER NODE ***
  [PIT only: also copy oplog files to a separate directory before this step]

POST /{targetGroupId}/restore/{restoreId}/filesCopied
  body: { id, dbPath, oplogPaths (PIT only) }
  → state: RECOVERY_IN_PROGRESS → COMPLETED

GET  /{targetGroupId}/restore/{restoreId}
  → poll until state = COMPLETED
```

**Restore states:**

| State | Meaning |
|---|---|
| `INITIAL` | Restore job created |
| `PENDING` | Preparing |
| `SHUTDOWN_IN_PROGRESS` | Agents shutting down mongod on target nodes |
| `COPY_FILES` | Ready for files to be copied to target nodes |
| `RECOVERY_IN_PROGRESS` | WiredTiger crash recovery running; mongod starting |
| `LIVE_RESTORE_IN_PROGRESS` | Live restore in progress |
| `COMPLETED` | Restore done |
| `FAILING` | Intermediate error state |
| `FAILED` | Non-recoverable error |
| `BROKEN` | Client must send a fail request to proceed |

> If a PIT restore gets stuck in `RECOVERY_IN_PROGRESS`, there is an oplog gap between the snapshot time and `pitTimestamp`. Verify no gaps exist before retrying.

To abort: `POST .../restore/{id}/fail` → deletes all data.

---

## FAQ

### General

**Q: What is Third-Party Backup?**
A set of APIs that orchestrate backups and restores. Ops Manager coordinates the cluster setup, backup cursor, and snapshot timing; the third party owns the storage and copy operations.

**Q: Can Third-Party Backup and Ops Manager Backup run together for the same cluster?**
No — they are mutually exclusive. However, a project can have both third-party managed and Ops Manager managed clusters simultaneously.

**Q: Is encrypted backup supported?**
Yes. The target node(s) must have the same encryption key as the node(s) from which the snapshot was taken.

**Q: How do distributed transactions work with snapshots?**
Backup cursors are opened across multiple shards (e.g. at T1, T2, T3). The optime is aligned across shards by extending the backup cursor to the largest value (e.g. T3). A transaction is either fully committed (before T3) or not at all (after T3). Uncommitted transactions are rolled back on recovery. If a PIT restore is performed and the transaction is in the oplogs, it will be reflected in the restored state.

**Q: How are volume snapshots different from regular snapshots?**
Volume snapshots capture the entire storage volume atomically. They should never be incremental — incremental snapshots track file-block changes, which provides no benefit for volume snapshots. A major advantage is restore efficiency: the restore is handled directly from the storage volume, eliminating the need to manually copy files.

**Q: Why should each source and target node have its own mountpoint for volume snapshots?**
Each node needs its own third-party vendor-managed mountpoint so it can be separately snapshotted and restored. This is achieved by each node having its own server (recommended) or its own mount point on a shared server.

**Q: Does each server need an oplog partition?**
Yes. The default is `/oplogs` (overridable via appsetting). It should be its own partition, isolated from the data directory, to prevent the mongod from crashing if the oplog fills the disk.

**Q: Is throttling required between snapshots and oplog snapshots?**
No. Oplog tailing and data backup can happen simultaneously for the same node.

---

### Snapshot API

**Q: How frequently should snapshots be taken?**
Incremental snapshots: at least once per day. Full snapshots: at least once per week. Also take a snapshot whenever an oplog gap is detected.

**Q: Can I use S2 as the source for S4 if I started (and failed) a full snapshot S3?**
No. After a full snapshot is started, no prior snapshot can be used as a source — even if the full fails.

**Q: Is `fileSize` the exact number of bytes to copy?**
Yes, for file-level snapshots — `fileSize` is the recommended max. The file can grow during the backup but bytes beyond `fileSize` would include unnecessary extra data. Volume snapshots are the exception.

**Q: What does the `diff` field in `fileDiffs` mean?**
`diff` is a Base64-encoded bitset. Each bit corresponds to a 16 MB block. `1` = block needs replacing, `0` = block unchanged. If `diff` is missing or `allBlocksChanged: true`, copy the whole file.

**Q: What does "frozen" mean for a node/file?**
Frozen files contain frozen sections of a B-tree (not all data). Only critical data is frozen. The hash of all data in a frozen file can change even while frozen.

**Q: Why use `unfreeze` when `finish` unfreezes all nodes anyway?**
`unfreeze` allows fine-grained cursor closure per node for performance. If 2 of 3 shards are done but the third needs 3 more hours, calling `unfreeze` on the completed shards lets WiredTiger resume optimal write patterns for those shards instead of writing everything to the top of the file.

**Q: What is the difference between `snapshotMetadata.restoreTimestamp` and `snapshotMetadata.snapshotTimestamp`?**
All DML operations up to and including `snapshotTimestamp` are in the snapshot. For MongoDB > 4.2, `restoreTimestamp` equals `snapshotTimestamp`.

**Q: Why is `snapshotMetadata` only returned in FINISHED state, not READY?**
This is a guardrail. If the snapshot fails between READY and FINISHED (e.g. a backup cursor is closed prematurely), metadata returned at READY could point to a corrupted snapshot. Waiting until FINISHED guarantees the metadata is valid.

---

### Oplog API

**Q: Can an Oplog Snapshot be started if there are no oplogs?**
Yes, but OM will wait 10 minutes for an oplog to be created. If no oplogs appear after 10 minutes, the Oplog Snapshot fails.

**Q: How do I disable oplog tailing?**
Send an empty list in a `POST .../preferredOplogNodes` request.

**Q: What happens if a node with an oplog tail fails?**
OM does not automatically start tailing another node. The client must select a new preferred node.

---

## Testing Checklist

> **Live status for *this* implementation is tracked in
> [../tests-docs/Test-CertificationChecklist.md](../tests-docs/Test-CertificationChecklist.md)** (the raw MongoDB
> checklist below, mapped to the tool with recorded results). Annotated here: ✅ validated · 🟡 supported, not yet
> run · ❌ out of scope for this tool (in-place self-restore + full snapshots only).

### Replica Set Testing

**Full Snapshot Restore — Non-Incremental**
- [x] Self Restore Replica Set — ✅ validated on `aen-rs-00` (drift 0, point-in-time fidelity proof)
- [ ] Restore Replica Set to different Replica Set — 🟡 implemented as `restore-mongo-snapshot-to-target` (cert 1.A.1.b; seed + initial-sync, same arrays); unit-tested, not yet live-validated
- [ ] Restore Replica Set to different Ops Manager — ❌

**PIT Restore — Non-Incremental**
- [x] PIT Restore Replica Set to self — ✅ validated on `aen-rs-00` (cert 1.B.1.a; oplog replay to a target ts, `unrecoveredTail=0`)
- [ ] Restore Replica Set to different Replica Set
- [ ] Restore Replica Set with Encryption at Rest to different Replica Set
- [ ] PIT Restore Replica Set to different Replica Set with Arbiter
- [ ] PIT Restore Replica Set to different Ops Manager

---

### Sharded Cluster Testing

**Full Snapshot Restore — Non-Incremental**
- [ ] Self Restore Sharded Cluster
- [ ] Restore Sharded Cluster to a different Cluster
- [ ] Restore Sharded Cluster to a different project
- [ ] Restore Sharded Cluster to a different Ops Manager
- [ ] Restore Sharded Cluster with Encryption at Rest to a different project
- [ ] Add A Shard then Restore Sharded Cluster
- [ ] Remove A Shard then Restore Sharded Cluster
- [ ] Self Restore Sharded Cluster with Embedded Config
- [ ] Restore Sharded Cluster with Embedded Config
- [ ] Convert Dedicated RS Sharded Cluster to Embedded Config Sharded Cluster — then Restore Sharded Cluster with Embedded Config
- [ ] Convert Embedded Config Sharded Cluster to Dedicated RS Sharded Cluster — then Restore Sharded Cluster

**PIT Restore — Non-Incremental**
- [ ] PIT Self Restore Sharded Cluster
- [ ] PIT Restore Sharded Cluster
- [ ] PIT Restore Sharded Cluster to a different cluster
- [ ] PIT Restore Sharded Cluster to a different project
- [ ] PIT Restore Sharded Cluster to a different Ops Manager
- [ ] PIT Restore Sharded Cluster with Encryption at Rest
- [ ] PIT Restore Sharded Cluster After Changing Preferred Node
- [ ] PIT Restore Sharded Cluster to Sharded Cluster with Arbiter
- [ ] PIT Self Restore Sharded Cluster with Embedded Config
- [ ] PIT Restore Sharded Cluster with Embedded Config

---

### Failover Tests
- [ ] Detect and handle a node down before taking a Snapshot
- [ ] Detect and handle a node down before taking an Oplog Snapshot
- [ ] Detect and handle a node fails during a Snapshot
- [ ] Detect and handle a node fails during a restore

### Verification Tests
- [ ] Do they set a preferred node for oplog tailing when a new node is added?
- [ ] Do they call the valid restore target endpoint before Restore?
- [ ] Do they validate there are no oplog gaps before a PIT restore?
- [ ] Are they calling status endpoints during Snapshots and Restores?
- [ ] Does the workflow send fail requests when a Snapshot or Restore is in a bad state?
- [ ] Do they take a Snapshot when an oplog gap is detected?

---

## API Reference

> All paths below are relative to `$HOST:$PORT/api/public/v1.0/backup/third_party`.
> All APIs use **HTTP Digest authentication**.
> See: https://docs.opsmanager.mongodb.com/current/core/api/

---

### Discovery API

#### `GET /group/settings`

Returns global settings including the oplog base path.

**Response:**
```json
{
  "settings": {
    "brs.thirdparty.baseOplogFilePath": "/path/defined/in/config"
  }
}
```

---

#### `GET /group/{groupId}/clusters`

Lists all clusters and their state.

**Response:**
```json
{
  "clusters": [
    {
      "clusterId": "67229aa09a2f69608904a547",
      "clusterName": "Sharded31",
      "groupId": "67227f947a6c4d2e6ab7fe0e",
      "jobId": "67229aa09a2f69608904a547",
      "state": "ACTIVE"
    }
  ]
}
```

---

#### `POST /group/{groupId}/clusters`

Pass `snapshotMetadata` to retrieve valid restore target clusters.

**Request body:** Same format as `snapshotMetadata` from the Get Snapshot Status response (see Snapshot API).

**Response:** List of valid target clusters in the same format as `GET /group/{groupId}/clusters`.

---

#### `GET /group/{groupId}/clusters/{clusterId}`

Returns detailed cluster information: replica sets, nodes, `snapshotable` flags, oplog paths, most recent snapshot/oplog/restore IDs.

**Response:**
```json
{
  "oplogSnapshotId": "6722a79e9a2f69608904e2dc",
  "preferredOplogNodes": ["hostname:port", "..."],
  "replicaSets": [
    {
      "id": "myShard31_1",
      "nodes": [
        {
          "dbPath": "/data/mongo",
          "hidden": false,
          "hostname": "aen-mongo-01",
          "id": "aen-mongo-01:27020",
          "lastAgentPing": "2024-11-04T20:30:40Z",
          "memberState": "PRIMARY",
          "mongodbVersion": "8.0.21",
          "opTime": "2024-11-04T20:29:16Z",
          "oplogPath": "/oplogs",
          "oplogStoreType": "thirdPartyOplogStore",
          "port": 27020,
          "priority": 1,
          "rsId": "aen-shard_1",
          "snapshotable": true,
          "tags": { "name": "firstNode" },
          "votes": 1
        }
      ],
      "oplogType": "thirdParty"
    }
  ],
  "restoreId": "67252033ca15e4137dbe0c75",
  "snapshotId": "6723dbf26be34e6a1e658606"
}
```

> Existing Measurements APIs support viewing disks, disk sizes, oplog generated/hr, etc.:
> - https://docs.cloudmanager.mongodb.com/reference/api/measurements/
> - https://docs.cloudmanager.mongodb.com/reference/api/measures/measurement-types/

---

### Management API

#### `POST /group/{groupId}/clusters/{clusterId}/manage`

Enable third-party management for a cluster. Body: empty. Idempotent — returns `THIRD_PARTY_CLUSTER_ALREADY_MANAGED` if already active (not an error).

**Response:**
```json
{ "errorCode": "NONE", "version": "1", "status": "OK" }
```

#### `POST /group/{groupId}/clusters/{clusterId}/unmanage`

Disable third-party management for a cluster. Body: empty.

**Response:**
```json
{ "errorCode": "NONE", "version": "1", "status": "OK" }
```

---

### Snapshot API

#### `POST /group/{groupId}/clusters/{clusterId}/snapshot`

Create a snapshot job.

**Request body:**
```json
{
  "timeoutMinutes": 150,
  "incrementalMetadata": {
    "incremental": true,
    "rsIdsToSrcBackupNames": [
      { "rsId": "aen-shard_0", "srcBackupName": "<thisBackupName from previous snapshot>" },
      { "rsId": "aen-shard_1", "srcBackupName": "<thisBackupName from previous snapshot>" },
      { "rsId": "aen-shard_2", "srcBackupName": "<thisBackupName from previous snapshot>" }
    ]
  },
  "thirdPartyMetadata": {},
  "nodeIds": ["aen-mongo-01:27020", "aen-mongo-01:27021", "aen-mongo-02:27022"]
}
```

> `nodeIds` must contain exactly one `hostname:port` for one node from each shard and the config replica set.
> For full snapshot: omit `rsIdsToSrcBackupNames` (do NOT set `incremental: false`).

**Response:**
```json
{ "snapshotId": "61f0447f8737383b4d6f34a0" }
```

---

#### `POST /group/{groupId}/clusters/{clusterId}/snapshot/{snapshotId}/start`

Start the timeout timer and open `$backupCursor` on selected nodes. Body: empty.

**Response:**
```json
{ "errorCode": "NONE", "version": "1", "status": "OK" }
```

---

#### `GET /group/{groupId}/clusters/{clusterId}/snapshot/{snapshotId}`

Poll snapshot state. Each request resets the timeout timer (heartbeat).

**Response (PENDING / READY / FINISHING / FAILED states):**
```json
{
  "nodes": [
    {
      "dbPath": "/data/mongo",
      "hidden": false,
      "hostname": "aen-mongo-01",
      "id": "aen-mongo-01:27020",
      "lastAgentPing": "2024-10-31T19:34:58Z",
      "memberState": "SECONDARY",
      "mongodbVersion": "8.0.21",
      "opTime": "2024-10-31T19:35:16Z",
      "oplogPath": "/oplogs",
      "oplogStoreType": "thirdPartyOplogStore",
      "port": 27020,
      "priority": 1,
      "rsId": "aen-shard_0",
      "snapshotable": true,
      "votes": 1
    }
  ],
  "state": "PENDING"
}
```

**State values:**

| State | Meaning |
|---|---|
| `INITIAL` | Snapshot job created |
| `PENDING` | Opening `$backupCursor`; preparing mongod for snapshot |
| `READY` | Ready — take snapshot now |
| `FINISHING` | Third-party signalled done; closing backup cursor |
| `FINISHED` | Cursor closed; `snapshotMetadata` populated |
| `FAILING` | Intermediate state before FAILED |
| `FAILED` | Non-recoverable error |

**Response (FINISHED state — `snapshotMetadata` included):**
```json
{
  "nodes": [ "..." ],
  "snapshotMetadata": {
    "clusterId": "67229aa09a2f69608904a547",
    "clusterName": "Sharded31",
    "groupId": "67227f947a6c4d2e6ab7fe0e",
    "restoreTimestamp": { "increment": 1, "time": 1730389124 },
    "rsSnapshotsMetadata": [
      {
        "defaultRWConcern": { "..." : "..." },
        "encryption": false,
        "fcv": "7.0",
        "incremental": false,
        "isConfigShard": false,
        "keyEncryptionUUID": null,
        "lastOplogApplied": { "increment": 1, "time": 1730389124 },
        "member": "CONFIG_SERVER",
        "mongodbVersion": "8.0.21",
        "oplogStoreType": "thirdPartyOplogStore",
        "rsId": "aen-shard_0",
        "shardId": "",
        "snapshotVersion": 1,
        "srcBackupName": null,
        "startupOptions": {},
        "storageEngine": "wiredTiger",
        "thisBackupName": null
      }
    ],
    "snapshotId": "6723a4399a2f69608905cec3",
    "snapshotTimestamp": { "increment": 1, "time": 1730389124 }
  },
  "state": "FINISHED"
}
```

> `thisBackupName` is non-null for incremental backups. Use it as `srcBackupName` in the next incremental snapshot request.

---

#### `GET /group/{groupId}/clusters/{clusterId}/snapshot/{snapshotId}/{nodeId}/fileList`

*(File-level snapshots only — skip for volume snapshots)*

Get list of files to copy. Paginated via `nextToken`.

**Query parameter:** `nextToken` (omit on first call; use value returned in response for subsequent calls)

**Response:**
```json
{
  "fileList": [
    {
      "fileName": "/data/mongo/journal/WiredTigerLog.0000000006",
      "fileSize": 104857600
    }
  ],
  "finishedToken": null,
  "nextToken": "<base64-token>"
}
```

> `finishedToken` is returned on the last page. Use it in the `fileDiffs` request for incremental snapshots.
> Copy exactly `fileSize` bytes from the start of each file (not the full on-disk size).

---

#### `POST /group/{groupId}/clusters/{clusterId}/snapshot/{snapshotId}/{nodeId}/fileDiffs`

*(Incremental file-level snapshots only — skip for volume snapshots)*

Get block-level diff for incremental copy.

**Request body:**
```json
{
  "finishedToken": "<finishedToken from last fileList response>",
  "fileNames": ["/file/path/1", "..."]
}
```

> Maximum 500 file names per request.

**Response:**
```json
{
  "blockSize": 16777216,
  "files": [
    {
      "fileName": "/file/path/1",
      "numberOfBlocks": 7,
      "allBlocksChanged": false,
      "diff": "QNSO=="
    }
  ]
}
```

> `diff` is a Base64-encoded bitset: `1` = block updated (replace), `0` = block unchanged (keep from previous snapshot). If `diff` is missing or `allBlocksChanged: true`, copy the whole file.

---

#### `POST /group/{groupId}/clusters/{clusterId}/snapshot/{snapshotId}/unfreeze`

*(File-level snapshots only — skip for volume snapshots)*

Close the backup cursor for specific nodes early (performance optimization).

**Request body:**
```json
{ "nodeIds": ["aen-mongo-01:27020", "aen-mongo-01:27021"] }
```

**Response:**
```json
{ "errorCode": "NONE", "version": "1", "status": "OK" }
```

---

#### `POST /group/{groupId}/clusters/{clusterId}/snapshot/{snapshotId}/finish`

Signal that all files have been copied. Moves state to FINISHING → FINISHED. Body: empty.

**Response:**
```json
{ "errorCode": "NONE", "version": "1", "status": "OK" }
```

---

#### `POST /group/{groupId}/clusters/{clusterId}/snapshot/{snapshotId}/fail`

Abort the snapshot. Immediately moves state to FAILING → FAILED. Body: empty.

**Response:**
```json
{ "errorCode": "NONE", "version": "1", "status": "OK" }
```

---

### Restore API

#### `POST /group/{targetGroupId}/clusters/{targetClusterId}/restore`

Create a restore job.

**Request body:**
```json
{
  "snapshotsMetadata": [{
    "clusterId": "67229aa09a2f69608904a547",
    "clusterName": "Sharded31",
    "groupId": "67227f947a6c4d2e6ab7fe0e",
    "restoreTimestamp": { "increment": 1, "time": 1730398353 },
    "rsSnapshotsMetadata": [ "..." ],
    "snapshotId": "6723c8216be34e6a1e650313",
    "snapshotTimestamp": { "increment": 1, "time": 1730398353 }
  }],
  "nodes": [
    {
      "id": "aen-mongo-01:27020",
      "restoreRole": "RESTORE"
    }
  ],
  "pitTimestamp": {
    "increment": 1,
    "time": 1643139932
  },
  "timeoutMinutes": 180
}
```

> `pitTimestamp` is optional (BSONTimestamp). Set `increment: 1` to restore to the first entry at the given second. Must be within the `start`–`end` range from the Oplog Snapshot.
> Include ARBITER nodes in `nodes` even though the snapshot won't be copied to them.
> `restoreRole` must be `RESTORE` or `LIVE_RESTORE`.

**Response:**
```json
{
  "clusterMapping": [
    {
      "nodes": [
        {
          "error": null,
          "hostname": "aen-mongo-01",
          "id": "aen-mongo-01:27020",
          "port": 27020,
          "rsId": "aen-shard_0",
          "state": "INITIAL",
          "stateUpdateTime": null
        }
      ],
      "rsId": "aen-shard_0",
      "targetRsId": "aen-shard_0"
    }
  ],
  "restoreId": "61f2cc013317fe07065c596f"
}
```

---

#### `POST /group/{targetGroupId}/restore/{restoreId}/start`

Start the restore. Moves state to SHUTDOWN_IN_PROGRESS. Body: empty.

**Response:**
```json
{ "errorCode": "NONE", "version": "1", "status": "OK" }
```

---

#### `GET /group/{targetGroupId}/restore/{restoreId}`

Poll restore state. Each request resets the timeout timer.

**Response:**
```json
{
  "nodes": [
    {
      "error": null,
      "hostname": "aen-mongo-01",
      "id": "aen-mongo-01:27020",
      "port": 27020,
      "rsId": "aen-shard_0",
      "state": "COMPLETED",
      "stateUpdateTime": 0,
      "oplogPaths": ["/path/to/oplogs/dir1"]
    }
  ],
  "state": "COMPLETED",
  "clusterMapping": [
    { "rsId": "aen-shard_0", "targetRsId": "aen-shard_0" }
  ]
}
```

---

#### `POST /group/{targetGroupId}/restore/{restoreId}/filesCopied`

Notify OM that files have been copied to a node.

**Request body:**
```json
{
  "id": "aen-mongo-01:27020",
  "dbPath": "/data/mongo/",
  "oplogPaths": ["/path/to/restore_oplogs"]
}
```

> `oplogPaths` is only set for PIT restores. The folder structure after each path must match the agent-generated structure:
> `<oplogPaths>/<source_shard_name>/<source_port>/<date>/<start>_<end>.oplogs`

**Response:**
```json
{ "errorCode": "NONE", "version": "1", "status": "OK" }
```

---

#### `POST /group/{targetGroupId}/restore/{restoreId}/fail`

Abort the restore. Deletes all data on target. Body: empty.

**Response:**
```json
{ "errorCode": "NONE", "version": "1", "status": "OK" }
```

---

### Oplog Snapshot API

#### `POST /group/{groupId}/clusters/{clusterId}/preferredOplogNodes`

Set preferred nodes for oplog tailing. Send an empty list to disable oplog tailing.

**Request body:**
```json
{
  "nodeIds": ["aen-mongo-01:27020", "..."]
}
```

**Response:**
```json
{ "errorCode": "NONE", "version": "1", "status": "OK" }
```

---

#### `POST /group/{groupId}/clusters/{clusterId}/oplogSnapshot`

Create an oplog snapshot job.

**Request body:**
```json
{
  "timeoutMinutes": 60,
  "nodeIds": ["aen-mongo-01:27020", "..."]
}
```

> `nodeIds` is optional. If omitted, uses existing preferred oplog nodes. If specified, overwrites the existing preference.

**Response:**
```json
{ "oplogSnapshotId": "61f0430a8737383b4d6f253ad" }
```

---

#### `POST /group/{groupId}/clusters/{clusterId}/oplogSnapshot/{oplogSnapshotId}/start`

Start the oplog snapshot timeout timer. Body: empty.

**Response:**
```json
{ "errorCode": "NONE", "version": "1", "status": "OK" }
```

---

#### `GET /group/{groupId}/clusters/{clusterId}/oplogSnapshot/{oplogSnapshotId}`

Poll oplog snapshot state. Each request resets the timeout timer.

**Response (PENDING):**
```json
{ "ranges": [], "state": "PENDING" }
```

**Response (READY — with oplog files):**
```json
{
  "ranges": [
    {
      "end":          { "time": 1730324580, "inc": 1 },
      "previousEnd":  { "time": 1730324460, "inc": 1 },
      "start":        { "time": 1730324520, "inc": 1 },
      "nodes": [
        {
          "id": "aen-mongo-01:27020",
          "logFiles": [
            "/oplogs/aen-shard_1/27020/2024-10-30/1730324520_1730324580.oplogs"
          ],
          "rsId": "aen-shard_1",
          "status": "READY"
        }
      ]
    }
  ],
  "state": "READY"
}
```

> `previousEnd` is useful for checking there are no gaps between this and the previous oplog snapshot.
> Choose a PIT restore timestamp within the `start`–`end` range.

**Response (FINISHED):**
```json
{ "ranges": [], "state": "FINISHED" }
```

**Response (FAILED):**
```json
{
  "message": "Potentially transient: There is no valid restorable ranges during the specified timeframe.",
  "ranges": [],
  "state": "FAILED"
}
```

---

#### `POST /group/{groupId}/clusters/{clusterId}/oplogSnapshot/{oplogSnapshotId}/finish`

Signal that all oplog files have been copied. OM deletes the `.oplog` files asynchronously. Body: empty.

**Response:**
```json
{ "errorCode": "NONE", "version": "1", "status": "OK" }
```

---

#### `POST /group/{groupId}/clusters/{clusterId}/oplogSnapshot/{oplogSnapshotId}/fail`

Abort the oplog snapshot. Body: empty.

**Response:**
```json
{ "errorCode": "NONE", "version": "1", "status": "OK" }
```
