###############################################################################################################################
# Multi-Array MongoDB Snapshot - Pure Storage FlashArray + MongoDB Ops Manager Third-Party Backup API
#
# Orchestrates a crash-consistent, multi-array MongoDB backup: it validates cluster health, opens the Ops
# Manager backup cursor, takes FlashArray protection group snapshots across the fleet, captures baselines and
# oplog anchors, then finishes the snapshot job. The flow is organized into the numbered STEP/region blocks below.
###############################################################################################################################

# region --- imports / module setup ---
from __future__ import annotations

import json
import logging
import math
import os
import re
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import typer

from . import config

# FlashArray access is via the direct-REST client in config.connect_fa() (see fa_rest.py).

# endregion


# Validates --snapshot-tag against the required om-YYYYMMDD-HHmmss pattern.
def _validate_snapshot_tag(value: str) -> str:
    if value:
        if not re.match(r"^om-\d{8}-\d{6}$", value):
            raise typer.BadParameter(
                "Cannot validate argument on parameter 'SnapshotTag'. "
                r"The argument \"" + value + r"\" does not match the \"^om-\d{8}-\d{6}$\" pattern."
            )
    return value


# Tees output to a transcript: a console StreamHandler plus an appending FileHandler on the same log file.
def _start_transcript(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("mongo-snapshot")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False
    fmt = logging.Formatter("%(message)s")
    fh = logging.FileHandler(log_path, mode="a")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def _stop_transcript(logger: Optional[logging.Logger]) -> None:
    if logger is None:
        return
    for h in list(logger.handlers):
        try:
            h.close()
        except Exception:
            pass
        logger.removeHandler(h)


# region --- worker (param block) ---
def _run(
    # --baseline-database (default 'testdb'): db whose collection counts to record as a baseline.
    baseline_database: str = typer.Option(
        "testdb",
        "--baseline-database",
        help="Database whose collection counts to record as a baseline for restore-side verification. "
        "Set to '' to skip baseline capture entirely.",
    ),
    # --baseline-collections (default ['loadtest', 'payload']): collections within --baseline-database to count.
    baseline_collections: list[str] = typer.Option(
        ["loadtest", "payload"],
        "--baseline-collections",
        help="Collections within --baseline-database to count.",
    ),
    # --snapshot-tag (default ''): optional override of the auto-generated snapshot tag (om-YYYYMMDD-HHmmss).
    snapshot_tag: str = typer.Option(
        "",
        "--snapshot-tag",
        callback=_validate_snapshot_tag,
        help="Optional: override the auto-generated snapshot tag (om-YYYYMMDD-HHmmss).",
    ),
) -> None:
    # Load .env FIRST (raises if missing) so all configuration is available before any work begins.
    config.load_config()
    cfg = config.CFG

    # region --- Configuration ---
    TimeoutMinutes = 150     # Max time for PENDING -> FINISHED
    PollIntervalSec = 3      # Seconds between GET polls
    # endregion

    # region --- STEP 0: Pre-flight ---
    config.write_host("\n=== STEP 0: Pre-flight ===", fg=config.YELLOW)

    # Concurrency lock with stale-PID detection.
    LockPath = str(Path.home() / ".mongo-snapshot.lock")
    config.new_script_lock(LockPath)

    # State carried across the nested try/finally blocks (Start, FaSnapshots, SnapshotId, etc.).
    Start = datetime.now()
    SnapshotId = None
    FaSnapshots: list = []
    transcript_logger: Optional[logging.Logger] = None

    try:
        # Verify that third-party backup is ACTIVE on this cluster before opening a backup cursor.
        ClusterDetail = config.invoke_om_api(path=f"group/{cfg.GroupId}/clusters/{cfg.ClusterId}")
        replica_sets = ClusterDetail.get("replicaSets") or []
        NonThirdParty = [rs for rs in replica_sets if rs.get("oplogType") != "thirdParty"]
        if len(replica_sets) == 0 or len(NonThirdParty) > 0:
            ids = ", ".join(str(rs.get("id")) for rs in NonThirdParty)
            raise RuntimeError(
                f"Third-party backup is not active on all replica sets for cluster '{cfg.ClusterId}'. "
                f"Non-conforming sets: {ids}. Enable third-party backup in Ops Manager before running snapshots."
            )
        config.write_host(
            f"  Third-party backup: ACTIVE ({len(replica_sets)} replica sets)", fg=config.GREEN
        )

        # Check that all MongoDB nodes are UP before attempting to open backup cursors.
        config.write_host("  Checking node health...", fg=config.CYAN)
        AllNodes = []
        for rs in replica_sets:
            AllNodes.extend(rs.get("nodes") or [])
        DownNodes = [n for n in AllNodes if n.get("memberState") == "DOWN"]
        if len(DownNodes) > 0:
            config.write_host("  ERROR: The following MongoDB nodes are DOWN:", fg=config.RED)
            for n in DownNodes:
                config.write_host(
                    f"    - {n.get('hostname')}:{n.get('port')} ({n.get('rsId')}) "
                    f"— memberState={n.get('memberState')}, opTime={n.get('opTime')}",
                    fg=config.RED,
                )
            raise RuntimeError(
                f"Cannot take snapshot while {len(DownNodes)} node(s) are DOWN. "
                "Ops Manager cannot open backup cursors on unreachable nodes. "
                "Fix the cluster health issue before retrying the snapshot."
            )
        config.write_host(f"  Node health: all {len(AllNodes)} nodes UP", fg=config.GREEN)

        # Check that no OM snapshot job is currently in progress.
        ActiveStates = ["PENDING", "READY", "FINISHING", "THIRD_PARTY_SNAPSHOT_IN_PROGRESS", "FAILING"]
        PrevSnapshotId = ClusterDetail.get("snapshotId")
        if PrevSnapshotId:
            try:
                ExistingJob = config.invoke_om_api(
                    path=f"group/{cfg.GroupId}/clusters/{cfg.ClusterId}/snapshot/{PrevSnapshotId}"
                )
            except Exception as e:  # noqa: BLE001
                # After a topology change (shard added/removed), OM can transiently report
                # THIRD_PARTY_DISCOVERY_ERROR or fail to return the prior snapshot whose recorded
                # topology no longer matches the current cluster. That prior job is not active, so
                # the pre-check must not block a new snapshot — proceed with a warning. (A genuinely
                # active job would still be rejected by OM at create time, so this is safe.)
                config.write_host(
                    f"  OM snapshot check: could not read prior snapshot '{PrevSnapshotId}' ({e}); "
                    "treating as no active job and proceeding (expected after a topology change).",
                    fg=config.YELLOW,
                )
                ExistingJob = None
            if ExistingJob and ExistingJob.get("state") in ActiveStates:
                raise RuntimeError(
                    f"OM snapshot job '{PrevSnapshotId}' is already in progress "
                    f"(state={ExistingJob.get('state')}). Wait for it to finish or call /fail to release "
                    "the cursor before starting a new snapshot."
                )
            if ExistingJob:
                config.write_host(
                    f"  OM snapshot check: no active job (last={PrevSnapshotId}, "
                    f"state={ExistingJob.get('state')})",
                    fg=config.GREEN,
                )
        else:
            config.write_host("  OM snapshot check: no previous snapshot job on record.", fg=config.GREEN)

        # Connect to the FA gateway once. All subsequent FlashArray calls reuse this session.
        FA = config.connect_fa()
        config.write_host(f"  Connected to gateway: {cfg.FaEndpoint}", fg=config.GREEN)

        # Discover which fleet arrays have the protection group.
        config.write_host("  Discovering FA context names from fleet...", fg=config.CYAN)
        FaContextNames = config.resolve_fa_context_names(FA, cfg.ProtectionGroupName)

        # Discover cluster nodes from Ops Manager (fallback: CLUSTER_NODES in .env).
        config.write_host("  Discovering cluster nodes...", fg=config.CYAN)
        ClusterNodes = config.get_cluster_nodes()

        # Discover which FA volume backs /data/mongo on each node via SCSI serial.
        config.write_host("  Discovering node-to-volume mappings via SCSI serial...", fg=config.CYAN)
        NodeVolumeMap = config.resolve_node_to_array_volume_map(
            FA, ClusterNodes, cfg.SshUser, config.SSH_OPTS, FaContextNames
        )

        # Verify every discovered data volume is actually a member of the protection group.
        config.write_host("  Verifying all data volumes are PG members...", fg=config.CYAN)
        for Node, entry in NodeVolumeMap.items():
            ShortName = entry["ShortName"]
            VolumeName = entry["VolumeName"]
            Member = None
            attempt = 1
            while attempt <= 3 and not Member:
                Member = config._fa(
                    FA.get_protection_groups_volumes(
                        context_names=[ShortName],
                        group_names=[cfg.ProtectionGroupName],
                        member_names=[VolumeName],
                    ),
                    allow_error=True,
                )
                if not Member and attempt < 3:
                    time.sleep(5)
                attempt += 1
            if not Member:
                raise RuntimeError(
                    f"Volume '{VolumeName}' (node {Node}, array {ShortName}) is NOT a member of PG "
                    f"'{cfg.ProtectionGroupName}'. Run initialize-protection-groups to add it before "
                    "taking snapshots."
                )
            config.write_host(f"    PG member verified: {VolumeName} on {ShortName}", fg=config.CYAN)
        config.write_host("  All data volumes confirmed as PG members.", fg=config.GREEN)
        # endregion

        # region --- STEP 1: Select snapshotable nodes (one secondary per shard) ---
        config.write_host(
            "\n=== STEP 1: Selecting snapshotable nodes (one secondary per shard) ===", fg=config.YELLOW
        )

        # ClusterDetail was already fetched in STEP 0 pre-flight.
        NodeIds: list[str] = []

        # Agent-reachability pre-check (mirrors start_oplog_tailer): don't select a node whose automation
        # agent is down to open the backup cursor on — OM's snapshotable flag lags a stopped agent by ~35s,
        # so the cursor-open would stall the job. Confirm the agent is running on each candidate's host
        # (SSH `systemctl is-active`); result cached per host.
        _agent_state: dict[str, str] = {}

        def _agent_active(node: dict) -> bool:
            host = (node.get("id") or "").split(":")[0]
            if host not in _agent_state:
                proc = subprocess.run(
                    ["ssh", *config.SSH_OPTS, f"{cfg.SshUser}@{host}",
                     "systemctl is-active mongodb-mms-automation-agent"],
                    capture_output=True,
                    text=True,
                )
                _agent_state[host] = (proc.stdout or "").strip() or "unreachable"
            st = _agent_state[host]
            if st != "active":
                config.write_host(
                    f"    skipping {node.get('id')}: automation agent is '{st}' "
                    f"(lastAgentPing={node.get('lastAgentPing')})",
                    fg=config.YELLOW,
                )
            return st == "active"

        for RS in replica_sets:
            # Snapshotable candidates whose automation agent is confirmed running.
            # Priority: hidden secondary > secondary > primary.
            Candidates = [
                n for n in (RS.get("nodes") or [])
                if n.get("snapshotable") is True and _agent_active(n)
            ]
            Chosen = next(
                (n for n in Candidates if n.get("memberState") == "SECONDARY" and n.get("hidden") is True),
                None,
            )
            if not Chosen:
                Chosen = next((n for n in Candidates if n.get("memberState") == "SECONDARY"), None)
            if not Chosen:
                Chosen = Candidates[0] if Candidates else None  # agent-reachable primary fallback
            if not Chosen:
                raise RuntimeError(
                    f"No snapshotable node with a reachable automation agent for replica set "
                    f"{RS.get('id')}. Refusing to open a backup cursor on a host whose agent is down "
                    "(the snapshot job would stall). Restart the agent or wait for a healthy secondary."
                )
            NodeIds.append(Chosen.get("id"))
            config.write_host(
                f"  {RS.get('id')} -> {Chosen.get('id')} [{Chosen.get('memberState')}] (agent active)", fg=config.CYAN
            )

        config.write_host(f"Selected {len(NodeIds)} nodes for snapshot.", fg=config.GREEN)
        # endregion

        # region --- STEP 2: Build snapshot request body ---
        config.write_host("\n=== STEP 2: Preparing snapshot request ===", fg=config.YELLOW)

        # Storage-based snapshots are always full; no incremental metadata needed.
        SnapshotBody = {
            "timeoutMinutes": TimeoutMinutes,
            "nodeIds": list(NodeIds),
        }
        config.write_host("  Full snapshot requested.", fg=config.CYAN)
        # endregion

        # region --- STEP 3: Create and start the snapshot job ---
        config.write_host("\n=== STEP 3: Creating snapshot job ===", fg=config.YELLOW)

        # Audit log: every snapshot run is captured to a transcript file.
        LogDir = Path.home() / "mongo-snapshot-logs"
        LogDir.mkdir(parents=True, exist_ok=True)
        LogPath = LogDir / f"snapshot-{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"
        transcript_logger = _start_transcript(LogPath)

        # Reset the duration timer here, after the transcript is open, so it measures the actual snapshot work.
        Start = datetime.now()

        CreateResponse = config.invoke_om_api(
            method="POST",
            path=f"group/{cfg.GroupId}/clusters/{cfg.ClusterId}/snapshot",
            body=SnapshotBody,
        )
        SnapshotId = CreateResponse.get("snapshotId")
        config.write_host(f"  Snapshot job created: {SnapshotId}", fg=config.GREEN)

        config.write_host("  Starting snapshot (opens $backupCursor on each node)...", fg=config.CYAN)
        config.invoke_om_api(
            method="POST",
            path=f"group/{cfg.GroupId}/clusters/{cfg.ClusterId}/snapshot/{SnapshotId}/start",
        )
        config.write_host("  Started.", fg=config.GREEN)

        # After /start succeeds, the cursor is open on the cluster.
        SnapshotCompleted = False
        # endregion

        # Inner try/catch/finally: from here, any uncaught error must call /fail.
        try:
            # region --- STEP 4: Poll until READY ---
            config.write_host("\n=== STEP 4: Waiting for state = READY ===", fg=config.YELLOW)

            config.wait_om_snapshot_state(
                SnapshotId, "READY", timeout_minutes=TimeoutMinutes, poll_interval_sec=PollIntervalSec
            )

            config.write_host(
                "  Backup cursor is open. Proceeding to take FlashArray snapshots.", fg=config.GREEN
            )
            # endregion

            # region --- STEP 5: Take FlashArray protection group snapshots on all arrays ---
            config.write_host(
                "\n=== STEP 5: Taking FlashArray protection group snapshots ===", fg=config.YELLOW
            )

            # Use the provided tag if set, otherwise auto-generate one as om-YYYYMMDD-HHmmss.
            SnapshotTag = snapshot_tag if snapshot_tag else ("om-" + datetime.now().strftime("%Y%m%d-%H%M%S"))

            def get_collection_counts(label: str):
                if not baseline_database or not baseline_collections or len(baseline_collections) == 0:
                    return None
                config.write_host(
                    f"  Capturing {label} counts for {baseline_database} via mongos "
                    f"{cfg.MongosHost}:{cfg.MongosPort} ...",
                    fg=config.CYAN,
                )
                # Retry transient SSH/mongosh failures (handled inside invoke_mongosh_js, max_attempts=5).
                try:
                    DbCounts: dict[str, int] = {}
                    for Coll in baseline_collections:
                        Eval = f'db.getSiblingDB("{baseline_database}").{Coll}.countDocuments()'
                        Raw = config.invoke_mongosh_js(
                            ssh_target=cfg.MongosHost,
                            uri=f"mongodb://{cfg.MongosHost}:{cfg.MongosPort}",
                            js=Eval,
                            max_attempts=5,
                            context=f"{label} {baseline_database}.{Coll}",
                        )
                        if not re.match(r"^\d+$", Raw.strip()):
                            raise RuntimeError(
                                f"Unparseable count for {baseline_database}.{Coll}: '{Raw}'"
                            )
                        DbCounts[Coll] = int(Raw.strip())
                        config.write_host(
                            f"    {label}  {baseline_database}.{Coll} = {DbCounts[Coll]}", fg=config.GREEN
                        )
                    return {baseline_database: DbCounts}
                except Exception as e:  # noqa: BLE001
                    config.write_host(
                        f"  WARNING: {label} baseline capture failed - tag will omit {label}. ({e})",
                        fg=config.YELLOW,
                    )
                    return None

            def get_shard_oplog_anchors():
                config.write_host(
                    "  Capturing per-shard oplog anchors (post-FA-snap, cursor still open)...",
                    fg=config.CYAN,
                )
                try:
                    # Key by shard _id (canonical mongos identifier returned by listShards).
                    ShardJson = config.invoke_mongosh_js(
                        ssh_target=cfg.MongosHost,
                        uri=f"mongodb://{cfg.MongosHost}:{cfg.MongosPort}",
                        js=config.LIST_SHARDS_JS,
                        context="listShards via mongos",
                    )
                    Shards = json.loads(ShardJson)

                    Anchors: dict = {}
                    for Sh in Shards:
                        ShardId = Sh.get("shardId")
                        AnyNode = Sh.get("host").split(":")[0]
                        AnyPort = Sh.get("host").split(":")[1]

                        # Find the RS primary.
                        PrimaryRaw = config.invoke_mongosh_js(
                            ssh_target=AnyNode,
                            uri=f"'mongodb://{AnyNode}:{AnyPort}/?directConnection=true'",
                            js="var h=db.adminCommand({hello:1}); print(JSON.stringify({primary:h.primary}));",
                            context=f"find primary for {ShardId} via {AnyNode}:{AnyPort}",
                        )
                        PrimaryInfo = json.loads(PrimaryRaw)
                        if not PrimaryInfo.get("primary"):
                            raise RuntimeError(
                                f"Could not determine primary for shard {ShardId} "
                                f"(hello.primary was empty on {AnyNode}:{AnyPort})"
                            )
                        Node = PrimaryInfo.get("primary").split(":")[0]
                        Port = PrimaryInfo.get("primary").split(":")[1]

                        TsJson = config.invoke_mongosh_js(
                            ssh_target=Node,
                            uri=f"'mongodb://{Node}:{Port}/?directConnection=true'",
                            js=config.OPLOG_TOP_JS,
                            context=f"oplog top read for {ShardId} on {Node}:{Port} (primary)",
                        )
                        Ts = json.loads(TsJson)
                        Anchors[ShardId] = {
                            "t": int(Ts.get("t")),
                            "i": int(Ts.get("i")),
                            "node": Node,
                            "port": int(Port),
                        }
                        AnchorIso = (
                            datetime.fromtimestamp(int(Ts.get("t")), tz=timezone.utc)
                            .isoformat()
                            .replace("+00:00", "Z")
                        )
                        config.write_host(
                            f"    {ShardId} anchor: t={Ts.get('t')} i={Ts.get('i')} on "
                            f"{Node}:{Port} (primary)  ({AnchorIso})",
                            fg=config.GREEN,
                        )
                    return Anchors
                except Exception as e:  # noqa: BLE001
                    config.write_host(
                        f"  WARNING: oplog anchor capture failed - PITR tailer will not have a "
                        f"snapshot-aligned start point. ({e})",
                        fg=config.YELLOW,
                    )
                    return None

            # preSnap baseline - taken with the backup cursor open but before the FA snap.
            PreSnapBaseline = get_collection_counts("preSnap")

            # Route each PG snapshot creation via the gateway using context_names to target the fleet member.
            FaSnapshots = []
            for CtxName in FaContextNames:
                Snap = None
                attempt = 1
                while attempt <= 3 and not Snap:
                    try:
                        # mongo:volumes tag value: sorted, comma-joined volume names from the node map.
                        volumes_value = ",".join(
                            sorted(v["VolumeName"] for v in NodeVolumeMap.values())
                        )
                        presnap_value = (
                            json.dumps(PreSnapBaseline, separators=(",", ":")) if PreSnapBaseline else "{}"
                        )
                        tag_keys = [
                            "ClusterName",
                            "BackupTimestamp",
                            "BackupType",
                            "mongo:volumes",
                            "mongo:preSnap",
                        ]
                        tag_values = [
                            cfg.ClusterName,
                            SnapshotTag,
                            "SNAPSHOT",
                            volumes_value,
                            presnap_value,
                        ]
                        tag_copyable = [True, True, True, True, True]
                        snap_resp = FA.post_protection_group_snapshots(
                            context_names=[CtxName],
                            source_names=[cfg.ProtectionGroupName],
                            protection_group_snapshot={
                                "suffix": SnapshotTag,
                                "tags": [
                                    {"key": k, "value": v, "copyable": c}
                                    for k, v, c in zip(tag_keys, tag_values, tag_copyable)
                                ],
                            },
                        )
                        snap_items = config._fa(snap_resp)
                        Snap = snap_items[0] if snap_items else None
                    except Exception as e:  # noqa: BLE001
                        msg = str(e)
                        if re.search("Name already in use", msg):
                            # A prior attempt timed out but the FA still created the snapshot.
                            config.write_host(
                                f"  WARNING: Snapshot already exists on {CtxName} (prior attempt timed out "
                                "but succeeded). Fetching existing...",
                                fg=config.YELLOW,
                            )
                            ExistingName = f"{cfg.ProtectionGroupName}.{SnapshotTag}"
                            existing_items = config._fa(
                                FA.get_protection_group_snapshots(
                                    context_names=[CtxName], names=[ExistingName]
                                )
                            )
                            Snap = existing_items[0] if existing_items else None
                        elif attempt < 3:
                            config.write_host(
                                f"  WARNING: PG snapshot attempt {attempt} failed on {CtxName}: {msg}",
                                fg=config.YELLOW,
                            )
                            config.write_host("  Retrying in 5s...", fg=config.YELLOW)
                            time.sleep(5)
                        else:
                            raise
                    attempt += 1
                FaSnapshots.append(Snap)
                config.write_host(f"  Created: {Snap.name} on {CtxName}", fg=config.GREEN)

            # Verify one snapshot was created per context array.
            if len(FaSnapshots) != len(FaContextNames):
                raise RuntimeError(
                    f"FlashArray PG snapshot count mismatch: expected {len(FaContextNames)}, "
                    f"got {len(FaSnapshots)}"
                )

            # postSnap baseline - taken just after the FA snap and before /finish.
            PostSnapBaseline = get_collection_counts("postSnap")
            # endregion

            # region --- STEP 6: Signal finish to Ops Manager (closes $backupCursor) ---
            config.write_host("\n=== STEP 6: Finishing snapshot (closing $backupCursor) ===", fg=config.YELLOW)
            config.invoke_om_api_with_retry(
                method="POST",
                path=f"group/{cfg.GroupId}/clusters/{cfg.ClusterId}/snapshot/{SnapshotId}/finish",
            )
            config.write_host("  Finish signal sent.", fg=config.GREEN)
            # endregion

            # region --- STEP 7: Poll until FINISHED and collect snapshotMetadata ---
            config.write_host("\n=== STEP 7: Waiting for state = FINISHED ===", fg=config.YELLOW)

            config.wait_om_snapshot_state(
                SnapshotId, "FINISHED", timeout_minutes=TimeoutMinutes, poll_interval_sec=PollIntervalSec
            )

            # Retrieve snapshotMetadata from the FINISHED response to extract the T1 oplog timestamp.
            FinishedStatus = config.invoke_om_api(
                path=f"group/{cfg.GroupId}/clusters/{cfg.ClusterId}/snapshot/{SnapshotId}"
            )
            OmSnapshotMetadata = FinishedStatus.get("snapshotMetadata")

            SnapshotCompleted = True
            # endregion

            # region --- STEP 7.5: Update PG snapshot tags with post-snapshot metadata ---
            config.write_host(
                "\n=== STEP 7.5: Updating snapshot tags with post-snapshot metadata ===", fg=config.YELLOW
            )

            TagKeys: list[str] = []
            TagValues: list[str] = []

            if PostSnapBaseline:
                TagKeys.append("mongo:postSnap")
                TagValues.append(json.dumps(PostSnapBaseline, separators=(",", ":")))
            if OmSnapshotMetadata and OmSnapshotMetadata.get("snapshotTimestamp") is not None:
                TagKeys.append("mongo:t1ts")
                TagValues.append(str(OmSnapshotMetadata.get("snapshotTimestamp", {}).get("time")))

            if len(TagKeys) > 0:
                SnapName = f"{cfg.ProtectionGroupName}.{SnapshotTag}"
                CopyableArr = [True for _ in TagKeys]

                # fa_rest connects DIRECTLY to each fleet member, so writing the tag batch is just a
                # per-array call routed by context_names — no separate gateway-vs-remote session handling.
                for CtxName in FaContextNames:
                    TagsOk = False
                    attempt = 1
                    while attempt <= 3 and not TagsOk:
                        try:
                            config._fa(
                                FA.put_protection_group_snapshots_tags_batch(
                                    context_names=[CtxName],
                                    resource_names=[SnapName],
                                    tag=[
                                        {"key": k, "value": v, "copyable": c}
                                        for k, v, c in zip(TagKeys, TagValues, CopyableArr)
                                    ],
                                )
                            )
                            config.write_host(
                                f"  Post-snapshot tags written to {CtxName}.",
                                fg=config.GREEN,
                            )
                            TagsOk = True
                        except Exception as e:  # noqa: BLE001
                            if attempt < 3:
                                config.write_host(
                                    f"  WARNING: Tag update attempt {attempt} on {CtxName}: {e}. "
                                    "Retrying in 5s...",
                                    fg=config.YELLOW,
                                )
                                time.sleep(5)
                            else:
                                config.write_host(
                                    f"  WARNING: Failed to update post-snapshot tags on {CtxName} "
                                    f"after 3 attempts: {e}",
                                    fg=config.YELLOW,
                                )
                        attempt += 1
            else:
                config.write_host(
                    "  No post-snapshot tags to write (postSnap baseline and t1ts both absent).",
                    fg=config.DARK_GRAY,
                )
            # endregion

        except Exception:
            # Log the error and re-raise so the finally block can release the backup cursor.
            import sys

            config.write_host(f"\n  ERROR: {sys.exc_info()[1]}", fg=config.RED)
            raise
        finally:
            # finally runs on success, on thrown exceptions, AND on Ctrl-C / terminations.
            if SnapshotId and not SnapshotCompleted:
                config.write_host(
                    f"  Calling /fail to release backup cursor on {SnapshotId} ...", fg=config.YELLOW
                )
                try:
                    config.invoke_om_api_with_retry(
                        method="POST",
                        path=f"group/{cfg.GroupId}/clusters/{cfg.ClusterId}/snapshot/{SnapshotId}/fail",
                    )
                    config.write_host("  Backup cursor released.", fg=config.GREEN)
                except Exception as e:  # noqa: BLE001
                    config.write_host(f"  /fail call also failed: {e}", fg=config.RED)
                    config.write_host(
                        f"  Backup cursor will time out automatically in {TimeoutMinutes} minutes.",
                        fg=config.YELLOW,
                    )
            try:
                _stop_transcript(transcript_logger)
            except Exception:
                pass

    finally:
        # Release the concurrency lock regardless of outcome.
        config.remove_script_lock(LockPath)

    # region --- Summary ---
    Duration = (datetime.now() - Start).total_seconds()

    config.write_host("\n=== Snapshot Complete ===", fg=config.GREEN)
    config.write_host(f"  Snapshot ID      : {SnapshotId}", fg="white")
    config.write_host(f"  Total duration   : {round(Duration, 1)} seconds", fg="white")
    config.write_host("\n  FlashArray snapshots:", fg="white")
    for Snap in FaSnapshots:
        config.write_host(f"    {Snap.name}", fg=config.CYAN)

    config.write_host(
        "\n  Tags written to FA snapshot (source of truth for restore and PITR).", fg="white"
    )
    # endregion


# endregion


# Flat console-script entry point.
def main():
    typer.run(_run)


if __name__ == "__main__":
    main()
