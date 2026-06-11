###############################################################################################################################
# Restore Mongo Snapshot - Pure Storage FlashArray + MongoDB Ops Manager Third-Party Backup API
#
# Restores a MongoDB 8.0 sharded cluster from a set of FlashArray volume snapshots taken by
# the snapshot workflow. The restore overwrites each node's data volume in place,
# preserving the existing pRDM/LUN mapping - no vSphere reconfiguration required.
#
#    A single connection targets the gateway array (sn1-x90r2-f06-27). Cluster nodes
#    and FlashArray context names and volume mappings are discovered at runtime from Ops Manager
#    and via SCSI serial numbers. Each volume overwrite is routed to the correct array by passing
#    that array's short name as the context name. The target snapshot is located by querying the
#    FlashArray tag catalog across all context arrays (ClusterName + BackupTimestamp), rather than
#    by constructing a name pattern.
#
# Cluster topology:
#    aen-mongo-01  sn1-x90r2-f07-27  aen-mongo-01-data
#    aen-mongo-02  sn1-x90r2-f06-27  aen-mongo-02-data  <- gateway
#    aen-mongo-03  sn1-x90r2-f06-33  aen-mongo-03-data
#
# Restore flow:
#    0. Pre-flight: acquire concurrency lock; discover cluster nodes (OM first, .env fallback);
#       verify SSH to all nodes; discover FA context names from fleet; discover node->volume map
#       via SCSI serial; verify all node volumes are present in the target snapshot members;
#       prompt for confirmation
#    1. Stop automation agents on all nodes -> gracefully stops mongod/mongos
#    2. Wait for mongod/mongos child processes to fully exit
#    3. Unmount /data/mongo on all nodes (idempotent - skips if already unmounted)
#    4. Overwrite each FlashArray volume from snapshot via gateway with per-array -ContextName (parallel)
#    5. Rescan block device + remount /data/mongo on all nodes
#    6. Start automation agents -> WiredTiger crash recovery runs
#    7. Wait for cluster to stabilize (mongos up, all shards registered)
#    8. Verify data
#
# Disclaimer:
#    This example script is provided AS-IS. Restore is destructive - any data written since the
#    snapshot will be lost. Always test in a non-production environment first.
###############################################################################################################################

from __future__ import annotations

# Configuration is loaded via `from . import config` + config.load_config() called
# as the FIRST statement of the worker (_run).

import logging
import os
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path

import typer

from . import config

# FlashArray access is via the direct-REST client (config.connect_fa / fa_rest.py).


app = typer.Typer(add_completion=False)


# Validates --snapshot-tag against the expected om-<date>-<time> format.
_SNAPSHOT_TAG_RE = re.compile(r"^om-\d{8}-\d{6}$")


def _validate_snapshot_tag(value: str) -> str:
    """typer callback validating the snapshot tag against '^om-\\d{8}-\\d{6}$'."""
    if value is None or not _SNAPSHOT_TAG_RE.match(value):
        raise typer.BadParameter(
            "Cannot validate argument on parameter 'SnapshotTag'. "
            "The argument \"{}\" does not match the \"^om-\\d{{8}}-\\d{{6}}$\" pattern.".format(value)
        )
    return value


def _replicaset_primary(member_host: str, member_port: int) -> str:
    """Return the replica set's current primary 'host:port' as seen from `member_host` via a
    directConnection hello(), or '' if none is elected/reachable yet. Used by the STEP 7 replica-set
    readiness probe instead of the sharded listShards path. Never raises (any failure -> '', i.e.
    "not ready yet")."""
    try:
        raw = config.invoke_mongosh_js(
            ssh_target=member_host,
            uri=f"'mongodb://{member_host}:{member_port}/?directConnection=true'",
            js='var h=db.adminCommand({hello:1}); print(h.primary || "");',
            context=f"replica-set hello via {member_host}:{member_port}",
        )
    except Exception:  # noqa: BLE001 - readiness probe; any error means the RS is not ready yet.
        return ""
    lines = [ln.strip() for ln in (raw or "").splitlines() if ln.strip()]
    return lines[-1] if lines else ""


def _run(
    snapshot_tag: str = typer.Option(
        ...,
        "--snapshot-tag",
        callback=_validate_snapshot_tag,
        help='e.g. "om-20260505-165440"',
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="skip the destructive-operation confirmation",
    ),
    verify_database: str = typer.Option(
        "testdb",
        "--verify-database",
        help="database to count docs in during STEP 8 verification",
    ),
    skip_verification: bool = typer.Option(
        False,
        "--skip-verification",
        help="skip STEP 8 entirely; otherwise STEP 8 is fail-hard on unparseable counts",
    ),
    deployment: str = typer.Option(
        None,
        "--deployment",
        help="Deployment name to restore (selects '<NAME>__' keys in .env). Omit to use the flat keys.",
    ),
) -> None:
    # Load configuration. Must be FIRST so running without
    # .env throws (but --help still works because typer short-circuits before this).
    config.load_config(deployment=deployment)

    # region --- Configuration ---
    wait_timeout_sec = 600   # Max seconds to wait for cluster to stabilize
    poll_interval_sec = 10   # Seconds between readiness polls
    proc_wait_sec = 30       # Max seconds to wait for mongod TERM before SIGKILL
    # endregion

    # Concurrency lock with stale-PID detection.
    lock_path = str(Path(os.path.expanduser("~")) / ".mongo-restore.lock")
    config.new_script_lock(lock_path)

    # Logging handle so the finally-block can detach handlers.
    transcript_handlers: list[logging.Handler] = []
    transcript_logger = logging.getLogger("restore_transcript")

    try:  # outer try - paired with finally that removes the lock

        # Audit log.
        log_dir = Path(os.path.expanduser("~")) / "mongo-restore-logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"restore-{snapshot_tag}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"
        # Configure a logger writing to BOTH the file and console.
        transcript_logger.setLevel(logging.INFO)
        transcript_logger.propagate = False
        _fh = logging.FileHandler(str(log_path), mode="a")
        _sh = logging.StreamHandler()
        _fmt = logging.Formatter("%(message)s")
        _fh.setFormatter(_fmt)
        _sh.setFormatter(_fmt)
        transcript_logger.addHandler(_fh)
        transcript_logger.addHandler(_sh)
        transcript_handlers = [_fh, _sh]

        try:  # inner try - paired with finally that detaches the log handlers

            start = datetime.now()

            # region --- STEP 0: Pre-flight ---
            config.write_host("\n=== STEP 0: Pre-flight ===", fg=config.YELLOW)

            # Discover cluster nodes from Ops Manager (fallback: CLUSTER_NODES in .env).
            config.write_host("  Discovering cluster nodes...", fg=config.CYAN)
            cluster_nodes = config.get_cluster_nodes()

            # Verify SSH connectivity to every node before stopping anything.
            for node in cluster_nodes:
                proc = subprocess.run(
                    ["ssh", *config.SSH_OPTS, f"{config.CFG.SshUser}@{node}", "true"],
                    capture_output=True,
                    text=True,
                )
                if proc.returncode != 0:
                    raise RuntimeError(
                        f"SSH connectivity check failed for {config.CFG.SshUser}@{node} "
                        f"(exit {proc.returncode}). Verify key auth and reachability."
                    )
                config.write_host(f"  SSH OK: {node}", fg=config.CYAN)

            # Connect to the gateway once.
            fa = config.connect_fa()
            config.write_host(f"  Connected to gateway: {config.CFG.FaEndpoint}", fg=config.GREEN)

            # Resolve FA context names from live fleet discovery.
            fa_context_names = config.resolve_fa_context_names(fa, config.CFG.ProtectionGroupName)
            snap_name = f"{config.CFG.ProtectionGroupName}.{snapshot_tag}"
            snap_tags = config.get_fa_snapshot_tags(fa, fa_context_names, snap_name)

            # mongo:volumes records the volume names that were part of this snapshot.
            if snap_tags.get("mongo:volumes"):
                expected_snap_count = len(snap_tags["mongo:volumes"].split(","))
                config.write_host(
                    f"  Snapshot volumes from tag ({expected_snap_count}): {snap_tags['mongo:volumes']}",
                    fg=config.GREEN,
                )
            else:
                expected_snap_count = len(fa_context_names)
                config.write_host(
                    f"  mongo:volumes tag absent - expected count falls back to fleet discovery "
                    f"({expected_snap_count} arrays)",
                    fg=config.YELLOW,
                )

            # Verify the target snapshot exists on all context arrays via the gateway.
            snap_check: list = []
            for ctx_name in fa_context_names:
                filter_str = f"name='{config.CFG.ProtectionGroupName}.{snapshot_tag}'"
                snap = None
                attempt = 0
                while not snap and attempt < 3:
                    attempt += 1
                    snap_items = config._fa(
                        fa.get_protection_group_snapshots(context_names=[ctx_name], filter=filter_str),
                        allow_error=True,
                    )
                    snap = snap_items if snap_items else None
                    if not snap and attempt < 3:
                        config.write_host(
                            f"  [{ctx_name}] attempt {attempt} returned empty - retrying in 5s...",
                            fg=config.YELLOW,
                        )
                        time.sleep(5)
                if snap:
                    snap_check.append(snap)
                    # Result may be multi-item; take the first for the name to report.
                    config.write_host(f"  Snapshot found: {snap[0].name} on {ctx_name}", fg=config.CYAN)
                else:
                    config.write_host(
                        f"  Snapshot '{snapshot_tag}' NOT found on {ctx_name} after {attempt} attempt(s)",
                        fg=config.RED,
                    )
            if len(snap_check) != expected_snap_count:
                raise RuntimeError(
                    f"Snapshot '{snapshot_tag}' found on {len(snap_check)} of {expected_snap_count} "
                    f"expected arrays - aborting before any changes are made."
                )
            config.write_host(f"  All {len(snap_check)} snapshots confirmed.", fg=config.GREEN)

            # Discover which FA volume backs /data/mongo on each node via SCSI serial.
            config.write_host("  Discovering node-to-volume mappings via SCSI serial...", fg=config.CYAN)
            node_volume_map = config.resolve_node_to_array_volume_map(
                fa,
                cluster_nodes,
                config.CFG.SshUser,
                config.SSH_OPTS,
                fa_context_names,
            )

            # Guard: every expected volume must have been discovered.
            if len(node_volume_map) != expected_snap_count:
                raise RuntimeError(
                    f"SCSI volume discovery found {len(node_volume_map)} of {expected_snap_count} "
                    "expected volumes - aborting before any changes are made. Verify all cluster nodes "
                    "are reachable and their data volumes are presented."
                )

            # Verify every discovered node volume is present + size matches.
            config.write_host(
                f"  Verifying all node volumes are present in snapshot '{snapshot_tag}'...",
                fg=config.CYAN,
            )
            for node, entry in node_volume_map.items():
                short_name = entry["ShortName"]
                volume_name = entry["VolumeName"]
                member_snap = f"{config.CFG.ProtectionGroupName}.{snapshot_tag}.{volume_name}"
                vol_snap = None
                # Retry up to 3 times for the member snapshot to appear.
                attempt = 1
                while attempt <= 3 and not vol_snap:
                    vs_items = config._fa(
                        fa.get_volume_snapshots(context_names=[short_name], names=[member_snap]),
                        allow_error=True,
                    )
                    vol_snap = vs_items[0] if vs_items else None
                    if not vol_snap and attempt < 3:
                        time.sleep(5)
                    attempt += 1
                if not vol_snap:
                    raise RuntimeError(
                        f"Snapshot '{snapshot_tag}' is missing member snapshot '{member_snap}' on "
                        f"{short_name} - aborting before any changes."
                    )
                live_vol = None
                # Retry up to 3 times to read the live volume, re-raising on the final failure.
                attempt = 1
                while attempt <= 3 and not live_vol:
                    try:
                        lv_items = config._fa(
                            fa.get_volumes(context_names=[short_name], names=[volume_name]),
                            allow_error=False,
                        )
                        live_vol = lv_items[0] if lv_items else None
                    except Exception:  # noqa: BLE001 - broad catch to drive the retry loop
                        if attempt < 3:
                            time.sleep(5)
                        else:
                            raise
                    attempt += 1
                if live_vol.provisioned != vol_snap.provisioned:
                    raise RuntimeError(
                        f"Volume size mismatch: live '{volume_name}' = {live_vol.provisioned} bytes; "
                        f"snapshot '{member_snap}' = {vol_snap.provisioned} bytes. Restoring would resize "
                        "the live volume - aborting before any changes."
                    )
                config.write_host(
                    f"    Member verified: {member_snap} (size {vol_snap.provisioned} bytes)",
                    fg=config.CYAN,
                )
            config.write_host("  All node volume members confirmed in snapshot.", fg=config.GREEN)

            # Discover the underlying block device backing /data/mongo on each node.
            node_device: dict[str, str] = {}
            for node in cluster_nodes:
                cmd = (
                    'p=$(findmnt -no SOURCE /data/mongo 2>/dev/null); if [ -n "$p" ]; then '
                    'lsblk -no PKNAME "$p" 2>/dev/null; else '
                    "lsblk -dno NAME,SERIAL 2>/dev/null | awk '$2 ~ /^[0-9a-fA-F]{20,}$/ {print $1; exit}'; fi"
                )
                proc = subprocess.run(
                    ["ssh", *config.SSH_OPTS, f"{config.CFG.SshUser}@{node}", cmd],
                    capture_output=True,
                    text=True,
                )
                disk = proc.stdout.strip()
                if not disk or not re.match(r"^[a-z0-9]+$", disk):
                    raise RuntimeError(
                        f"Could not derive parent block device for /data/mongo on {node} (got: '{disk}'). "
                        "Verify the Pure Storage pRDM is presented."
                    )
                node_device[node] = disk
                config.write_host(f"  Device on {node}: /dev/{disk}", fg=config.CYAN)

            # Destructive-operation confirmation.
            if not force:
                config.write_host(
                    "\n  WARNING: This will OVERWRITE the live data volumes on:", fg=config.RED
                )
                for node in cluster_nodes:
                    config.write_host(f"    - {node} (/data/mongo)", fg=config.RED)
                config.write_host(
                    f"  Any data written since snapshot {snapshot_tag} will be LOST.", fg=config.RED
                )
                confirm = input("\n  Type the snapshot tag to confirm restore").strip()
                if confirm != snapshot_tag:
                    raise RuntimeError("Confirmation did not match. Aborting.")
            # endregion

            # region --- STEP 1: Stop automation agents ---
            config.write_host("\n=== STEP 1: Stopping automation agents ===", fg=config.YELLOW)

            # Worker run in parallel across nodes.
            def _step1_stop_agent(node):
                user = config.CFG.SshUser
                opts = config.SSH_OPTS
                config.write_host(f"  Stopping agent on {node} ...", fg=config.CYAN)
                proc = subprocess.run(
                    ["ssh", *opts, f"{user}@{node}", "sudo systemctl stop mongodb-mms-automation-agent"],
                    capture_output=True,
                    text=True,
                )
                if proc.returncode != 0:
                    return {"Node": node, "Success": False, "Message": f"systemctl stop failed (exit {proc.returncode})"}
                config.write_host(f"  Stopped on {node}", fg=config.GREEN)
                return {"Node": node, "Success": True, "Message": "stopped"}

            config.invoke_parallel_or_throw(cluster_nodes, _step1_stop_agent, "Stop automation agents")
            # endregion

            # region --- STEP 2: Force-stop mongod/mongos ---
            config.write_host("\n=== STEP 2: Stopping mongod/mongos ===", fg=config.YELLOW)

            # Worker run in parallel across nodes.
            def _step2_stop_mongo(node):
                user = config.CFG.SshUser
                opts = config.SSH_OPTS
                max_wait = proc_wait_sec

                # Remote shell script: only max_wait is interpolated into $(seq 1 <Max>); the
                # remaining shell variables ($i etc.) are evaluated on the remote host.
                stop_cmd = (
                    "set -e\n"
                    "sudo pkill -TERM -x mongos 2>/dev/null || true\n"
                    "sudo pkill -TERM -x mongod 2>/dev/null || true\n"
                    f"for i in $(seq 1 {max_wait}); do\n"
                    "  if ! pgrep -x mongod >/dev/null && ! pgrep -x mongos >/dev/null; then\n"
                    '    echo "clean-stop"; exit 0\n'
                    "  fi\n"
                    "  sleep 1\n"
                    "done\n"
                    'echo "escalating to SIGKILL"\n'
                    "sudo pkill -KILL -x mongos 2>/dev/null || true\n"
                    "sudo pkill -KILL -x mongod 2>/dev/null || true\n"
                    "sleep 2\n"
                    "if pgrep -x mongod >/dev/null || pgrep -x mongos >/dev/null; then\n"
                    '  echo "still-running"; exit 1\n'
                    "fi\n"
                    'echo "forced-stop"; exit 0'
                )
                proc = subprocess.run(
                    ["ssh", *opts, f"{user}@{node}", stop_cmd],
                    capture_output=True,
                    text=True,
                )
                result = (proc.stdout + proc.stderr).strip()
                if proc.returncode != 0:
                    return {"Node": node, "Success": False, "Message": f"mongo processes still running: {result}"}
                config.write_host(f"  {node}: {result}", fg=config.GREEN)
                return {"Node": node, "Success": True, "Message": result}

            config.invoke_parallel_or_throw(cluster_nodes, _step2_stop_mongo, "Stop mongod/mongos")
            # endregion

            # region --- STEP 3: Unmount /data/mongo (idempotent) ---
            config.write_host("\n=== STEP 3: Unmounting /data/mongo ===", fg=config.YELLOW)

            # Worker run in parallel across nodes.
            def _step3_unmount(node):
                user = config.CFG.SshUser
                opts = config.SSH_OPTS
                # Remote shell script: a fixed command with no interpolation.
                cmd = (
                    "if mountpoint -q /data/mongo; then\n"
                    "  if sudo umount /data/mongo; then\n"
                    "    echo unmounted\n"
                    "  else\n"
                    '    echo "umount failed - holders:"\n'
                    "    sudo lsof +f -- /data/mongo 2>/dev/null || true\n"
                    "    sudo fuser -mv /data/mongo 2>&1 || true\n"
                    "    exit 32\n"
                    "  fi\n"
                    "else\n"
                    "  echo already-unmounted\n"
                    "fi"
                )
                proc = subprocess.run(
                    ["ssh", *opts, f"{user}@{node}", cmd],
                    capture_output=True,
                    text=True,
                )
                out = (proc.stdout + proc.stderr).strip()
                if proc.returncode != 0:
                    return {"Node": node, "Success": False, "Message": f"umount failed: {out}"}
                # Report only the first line of the output.
                first_line = out.split("\n")[0] if out else ""
                config.write_host(f"  {node}: {first_line}", fg=config.GREEN)
                return {"Node": node, "Success": True, "Message": out}

            config.invoke_parallel_or_throw(cluster_nodes, _step3_unmount, "Unmount /data/mongo")
            # endregion

            # region --- STEP 4: Overwrite FlashArray volumes from snapshots (sequential) ---
            config.write_host(
                f"\n=== STEP 4: Restoring FlashArray volumes from '{snapshot_tag}' ===",
                fg=config.YELLOW,
            )

            restore_failures: list[str] = []
            for node, entry in node_volume_map.items():
                short_name = entry["ShortName"]
                volume_name = entry["VolumeName"]
                # PG snapshot member name: <pgname>.<tag>.<volumename>
                snap_name = f"{config.CFG.ProtectionGroupName}.{snapshot_tag}.{volume_name}"

                overwrite_ok = False
                last_err = None
                # Retry the overwrite up to 3 times.
                attempt = 1
                while attempt <= 3 and not overwrite_ok:
                    try:
                        if attempt > 1:
                            config.write_host(
                                f"  Retry {attempt}/3 for {volume_name} on {short_name} ...",
                                fg=config.YELLOW,
                            )
                        config.write_host(
                            f"  Overwriting {volume_name} on {short_name} <- {snap_name} ...",
                            fg=config.CYAN,
                        )
                        # Overwrite the live volume in place from the snapshot member.
                        config._fa(
                            fa.post_volumes(
                                names=[volume_name],
                                volume={"source": {"name": snap_name}},
                                overwrite=True,
                                context_names=[short_name],
                            ),
                            allow_error=False,
                        )
                        config.write_host(f"  Restored: {volume_name}", fg=config.GREEN)
                        overwrite_ok = True
                    except Exception as e:  # noqa: BLE001 - broad catch to drive the retry loop
                        last_err = str(e)
                        config.write_host(f"  Attempt {attempt} failed: {last_err}", fg=config.RED)
                        if attempt < 3:
                            time.sleep(5)
                    attempt += 1
                if not overwrite_ok:
                    restore_failures.append(
                        f"{volume_name} on {short_name} (node {node}): {last_err}"
                    )
                    config.write_host(
                        f"  FAILED after 3 attempts: {volume_name} on {short_name}: {last_err}",
                        fg=config.RED,
                    )
            if len(restore_failures) > 0:
                joined = "\n".join(restore_failures)
                raise RuntimeError(
                    f"FlashArray volume restore failed on {len(restore_failures)} volume(s):\n{joined}"
                )
            # endregion

            # region --- STEP 5: Rescan LUN + remount /data/mongo ---
            config.write_host(
                "\n=== STEP 5: Rescanning LUN and remounting /data/mongo ===", fg=config.YELLOW
            )

            # Worker run in parallel across nodes.
            def _step5_rescan_mount(node):
                user = config.CFG.SshUser
                opts = config.SSH_OPTS
                devices = node_device
                disk = devices[node]  # e.g. 'sdb'

                # Remote shell script: only the disk name is interpolated; the rest of the
                # shell variables are evaluated on the remote host.
                cmd = (
                    "set -e\n"
                    f"DISK={disk}\n"
                    "echo 1 | sudo tee /sys/block/$DISK/device/rescan > /dev/null\n"
                    "sleep 1\n"
                    "sudo blockdev --rereadpt /dev/$DISK 2>/dev/null || sudo partprobe /dev/$DISK 2>/dev/null || true\n"
                    "sudo udevadm settle --timeout=15\n"
                    "PART=$(lsblk -lno NAME,TYPE /dev/$DISK 2>/dev/null | awk '$2 == \"part\" {print \"/dev/\" $1; exit}')\n"
                    'if [ -z "$PART" ]; then PART=/dev/$DISK; fi\n'
                    'FSTYPE=$(sudo blkid -s TYPE -o value "$PART" 2>/dev/null || echo "")\n'
                    'echo "device=$PART fstype=$FSTYPE"\n'
                    "set +e\n"
                    'case "$FSTYPE" in\n'
                    '  xfs)            sudo xfs_repair -n "$PART" >/dev/null 2>&1 ;;\n'
                    '  ext2|ext3|ext4) sudo e2fsck -n -f "$PART" >/dev/null 2>&1 ;;\n'
                    "  *)              echo \"WARN: unknown fstype '$FSTYPE' - skipping RO integrity check\"; true ;;\n"
                    "esac\n"
                    "INTEG=$?\n"
                    "if [ $INTEG -ne 0 ]; then\n"
                    '  echo "WARN: RO integrity check exit $INTEG (advisory; mount will replay journal)"\n'
                    "fi\n"
                    "set -e\n"
                    "sudo mount /data/mongo\n"
                    "mountpoint -q /data/mongo\n"
                    'echo "mounted"'
                )
                config.write_host(
                    f"  {node}: rescan + integrity check + mount /dev/{disk} ...", fg=config.CYAN
                )
                proc = subprocess.run(
                    ["ssh", *opts, f"{user}@{node}", cmd],
                    capture_output=True,
                    text=True,
                )
                out = (proc.stdout + proc.stderr)
                if proc.returncode != 0:
                    return {"Node": node, "Success": False, "Message": f"rescan/mount failed: {out.strip()}"}
                # Surface the captured device= and WARN advisory lines.
                for line in out.splitlines():
                    if re.match(r"^(WARN|device=)", line):
                        config.write_host(f"    {node}: {line}", fg=config.DARK_YELLOW)
                config.write_host(f"  {node}: mounted", fg=config.GREEN)
                return {"Node": node, "Success": True, "Message": "mounted"}

            config.invoke_parallel_or_throw(cluster_nodes, _step5_rescan_mount, "Rescan + mount")
            # endregion

            # region --- STEP 6: Start automation agents ---
            config.write_host("\n=== STEP 6: Starting automation agents ===", fg=config.YELLOW)

            # Worker run in parallel across nodes.
            def _step6_start_agent(node):
                user = config.CFG.SshUser
                opts = config.SSH_OPTS
                config.write_host(f"  Starting agent on {node} ...", fg=config.CYAN)
                proc = subprocess.run(
                    ["ssh", *opts, f"{user}@{node}", "sudo systemctl start mongodb-mms-automation-agent"],
                    capture_output=True,
                    text=True,
                )
                if proc.returncode != 0:
                    return {"Node": node, "Success": False, "Message": f"systemctl start failed (exit {proc.returncode})"}
                config.write_host(f"  Started on {node}", fg=config.GREEN)
                return {"Node": node, "Success": True, "Message": "started"}

            config.invoke_parallel_or_throw(cluster_nodes, _step6_start_agent, "Start automation agents")

            config.write_host(
                "  All agents started. WiredTiger crash recovery running on each shard...",
                fg=config.GREEN,
            )
            # endregion

            # region --- STEP 7: Wait for cluster to stabilize ---
            config.write_host("\n=== STEP 7: Waiting for cluster to stabilize ===", fg=config.YELLOW)

            expected_shards = None

            deadline = time.monotonic() + wait_timeout_sec
            ready = False

            # Replica-set deployments have no mongos / config server, so listShards does not apply.
            # Verify the single RS directly: it is stable once it has elected a writable primary.
            # The sharded path below is unchanged.
            rs_mode = config.CFG.Topology == "replicaset"

            while not ready:
                if time.monotonic() > deadline:
                    raise RuntimeError(f"Cluster did not stabilize within {wait_timeout_sec} seconds.")

                time.sleep(poll_interval_sec)
                config.write_host(
                    f"  {datetime.now().strftime('%H:%M:%S')}  Checking cluster state...",
                    fg=config.CYAN,
                )

                if rs_mode:
                    primary = _replicaset_primary(config.CFG.MongosHost, config.CFG.MongosPort)
                    if primary:
                        config.write_host(
                            f"  {datetime.now().strftime('%H:%M:%S')}  replica set up, "
                            f"primary elected: {primary}.",
                            fg=config.GREEN,
                        )
                        ready = True
                    else:
                        config.write_host(
                            "    replica set has no writable primary yet...", fg=config.DARK_GRAY
                        )
                    continue

                # Ping mongos.
                ping_remote = (
                    f"{config.CFG.MongoshPath} --quiet --eval 'db.adminCommand({{ping:1}}).ok' "
                    f"mongodb://{config.CFG.MongosHost}:{config.CFG.MongosPort} 2>/dev/null"
                )
                ping_proc = subprocess.run(
                    ["ssh", *config.SSH_OPTS, f"{config.CFG.SshUser}@{config.CFG.MongosHost}", ping_remote],
                    capture_output=True,
                    text=True,
                )
                ping = ping_proc.stdout
                if ping_proc.returncode != 0 or not re.search(r"1", ping):
                    config.write_host(
                        "    mongos not yet accepting connections...", fg=config.DARK_GRAY
                    )
                    continue

                # Read the current shard count.
                sc_remote = (
                    f"{config.CFG.MongoshPath} --quiet --eval "
                    f"'db.adminCommand({{listShards:1}}).shards.length' "
                    f"mongodb://{config.CFG.MongosHost}:{config.CFG.MongosPort} 2>/dev/null"
                )
                sc_proc = subprocess.run(
                    ["ssh", *config.SSH_OPTS, f"{config.CFG.SshUser}@{config.CFG.MongosHost}", sc_remote],
                    capture_output=True,
                    text=True,
                )
                # Parse the raw stdout as an int.
                shard_count = int(sc_proc.stdout.strip())
                if expected_shards is None:
                    expected_shards = shard_count
                    config.write_host(
                        f"    Authoritative shard count from listShards: {expected_shards}",
                        fg=config.CYAN,
                    )

                if shard_count != expected_shards:
                    config.write_host(
                        f"    Shards not fully registered yet (got: {shard_count}/{expected_shards})...",
                        fg=config.DARK_GRAY,
                    )
                    continue

                # Per-shard hello() health check via temp .js on the remote host.
                health_script = (
                    "const shards = db.adminCommand({listShards:1}).shards;\n"
                    "let ok = 0;\n"
                    "for (const s of shards) {\n"
                    "    try {\n"
                    "        const parts = s.host.split('/');\n"
                    "        const rsName = parts[0];\n"
                    "        const hosts  = parts[1];\n"
                    "        const uri = 'mongodb://' + hosts + '/?replicaSet=' + rsName;\n"
                    "        const conn = new Mongo(uri);\n"
                    "        const hello = conn.getDB('admin').runCommand({hello:1});\n"
                    "        if (hello.ok === 1 && hello.isWritablePrimary === true) { ok++; }\n"
                    "    } catch (e) { /* not yet ready */ }\n"
                    "}\n"
                    "print(ok);\n"
                )
                temp_js = f"/tmp/mongo_health_{snapshot_tag}.js"
                # Pipe the health script to a temp .js file on the mongos host.
                subprocess.run(
                    ["ssh", *config.SSH_OPTS, f"{config.CFG.SshUser}@{config.CFG.MongosHost}", f"cat > {temp_js}"],
                    input=health_script,
                    capture_output=True,
                    text=True,
                )
                healthy_remote = (
                    f"{config.CFG.MongoshPath} --quiet --file {temp_js} "
                    f"mongodb://{config.CFG.MongosHost}:{config.CFG.MongosPort} 2>/dev/null"
                )
                healthy_proc = subprocess.run(
                    ["ssh", *config.SSH_OPTS, f"{config.CFG.SshUser}@{config.CFG.MongosHost}", healthy_remote],
                    capture_output=True,
                    text=True,
                )
                healthy = healthy_proc.stdout
                if int(healthy.strip()) != expected_shards:
                    config.write_host(
                        f"    Shards reachable but not all primaries elected yet "
                        f"(got: {healthy.strip()}/{expected_shards})...",
                        fg=config.DARK_GRAY,
                    )
                    continue

                config.write_host(
                    f"  {datetime.now().strftime('%H:%M:%S')}  mongos up, {expected_shards} shards "
                    f"registered, {expected_shards} primaries reachable.",
                    fg=config.GREEN,
                )
                ready = True
            # endregion

            # region --- STEP 8: Verify data ---
            if skip_verification:
                config.write_host(
                    "\n=== STEP 8: Verification SKIPPED (-SkipVerification) ===", fg=config.DARK_YELLOW
                )
            else:
                config.write_host("\n=== STEP 8: Verifying data ===", fg=config.YELLOW)
                config.write_host("  Waiting 10s for shard metadata to propagate...", fg=config.DARK_GRAY)
                time.sleep(10)

                # Build baseline from snapshot tags.
                # snap_tags was loaded in STEP 0; reuse it here without a second FA API call.
                baseline = None
                if snap_tags.get("mongo:preSnap") or snap_tags.get("mongo:postSnap"):
                    import json as _json
                    baseline = {
                        "preSnap": _json.loads(snap_tags["mongo:preSnap"]) if snap_tags.get("mongo:preSnap") else None,
                        "postSnap": _json.loads(snap_tags["mongo:postSnap"]) if snap_tags.get("mongo:postSnap") else None,
                    }

                # Count documents in a collection via mongosh, failing hard on unparseable output.
                def get_verify_count(db: str, coll: str) -> int:
                    eval_str = f'db.getSiblingDB("{db}").{coll}.countDocuments()'
                    raw = config.invoke_mongosh_js(
                        ssh_target=config.CFG.MongosHost,
                        uri=f"mongodb://{config.CFG.MongosHost}:{config.CFG.MongosPort}",
                        js=eval_str,
                        context=f"mongosh count of {db}.{coll} (use -SkipVerification to bypass)",
                    )
                    if not re.match(r"^\d+$", raw):
                        raise RuntimeError(f"Unparseable count for {db}.{coll}: '{raw}'")
                    return int(raw.strip())

                load_test_count = get_verify_count(verify_database, "loadtest")
                payload_count = get_verify_count(verify_database, "payload")
                config.write_host(
                    f"  {verify_database}.loadtest document count: {load_test_count}", fg=config.CYAN
                )
                config.write_host(
                    f"  {verify_database}.payload  document count: {payload_count}", fg=config.CYAN
                )

                # Per-shard verification (sharded only). The certification checklist requires confirming the
                # *shards themselves* hold the data, not just the mongos-routed total. Connect to each shard's
                # RS directly, count the verify collections, and assert the per-shard sums account for the
                # mongos aggregate (>=, since a direct shard count can include orphaned docs post-migration).
                # A replica set has a single RS, so the aggregate above already is its total — no per-shard step.
                if config.CFG.Topology != "replicaset":
                    per_shard_js = (
                        "const shards = db.adminCommand({listShards:1}).shards;\n"
                        "for (const s of shards) {\n"
                        "    const parts = s.host.split('/');\n"
                        "    const uri = 'mongodb://' + parts[1] + '/?replicaSet=' + parts[0];\n"
                        "    let lt = -1, pl = -1;\n"
                        "    try {\n"
                        f"        const d = new Mongo(uri).getDB('{verify_database}');\n"
                        "        lt = d.loadtest.countDocuments();\n"
                        "        pl = d.payload.countDocuments();\n"
                        "    } catch (e) { /* unreachable shard -> -1 */ }\n"
                        "    print(s._id + ' ' + lt + ' ' + pl);\n"
                        "}\n"
                    )
                    ps_js = f"/tmp/mongo_pershard_{snapshot_tag}.js"
                    subprocess.run(
                        ["ssh", *config.SSH_OPTS, f"{config.CFG.SshUser}@{config.CFG.MongosHost}", f"cat > {ps_js}"],
                        input=per_shard_js, capture_output=True, text=True,
                    )
                    ps_remote = (
                        f"{config.CFG.MongoshPath} --quiet --file {ps_js} "
                        f"mongodb://{config.CFG.MongosHost}:{config.CFG.MongosPort} 2>/dev/null"
                    )
                    ps_proc = subprocess.run(
                        ["ssh", *config.SSH_OPTS, f"{config.CFG.SshUser}@{config.CFG.MongosHost}", ps_remote],
                        capture_output=True, text=True,
                    )
                    config.write_host("  Per-shard verification (each shard's own data):", fg=config.CYAN)
                    sum_lt = sum_pl = 0
                    shard_rows = 0
                    empty_shards: list[str] = []
                    unreachable: list[str] = []
                    for line in (ps_proc.stdout or "").splitlines():
                        cols = line.split()
                        if len(cols) != 3 or not re.match(r"^-?\d+$", cols[1]) or not re.match(r"^-?\d+$", cols[2]):
                            continue
                        sid, lt_n, pl_n = cols[0], int(cols[1]), int(cols[2])
                        shard_rows += 1
                        if lt_n < 0 or pl_n < 0:
                            unreachable.append(sid)
                            config.write_host(f"    {sid}: UNREACHABLE", fg=config.YELLOW)
                            continue
                        sum_lt += lt_n
                        sum_pl += pl_n
                        if lt_n == 0 and pl_n == 0:
                            empty_shards.append(sid)
                        config.write_host(
                            f"    {sid}: {verify_database}.loadtest={lt_n} payload={pl_n}", fg=config.GREEN
                        )
                    if shard_rows == 0:
                        config.write_host(
                            "    WARNING: listShards returned no shards - skipping per-shard check.",
                            fg=config.YELLOW,
                        )
                    elif unreachable:
                        raise RuntimeError(
                            f"Per-shard verification FAILED: shard(s) unreachable: {', '.join(unreachable)}"
                        )
                    elif sum_lt < load_test_count or sum_pl < payload_count:
                        raise RuntimeError(
                            f"Per-shard verification FAILED: per-shard totals (loadtest={sum_lt}, "
                            f"payload={sum_pl}) are LESS than the mongos aggregate (loadtest={load_test_count}, "
                            f"payload={payload_count}) - a shard is missing data."
                        )
                    else:
                        config.write_host(
                            f"  Per-shard totals account for the mongos aggregate across {shard_rows} shard(s) "
                            f"(loadtest {sum_lt}>={load_test_count}, payload {sum_pl}>={payload_count}).",
                            fg=config.GREEN,
                        )
                        if sum_lt > load_test_count or sum_pl > payload_count:
                            config.write_host(
                                "    NOTE: per-shard sum exceeds the routed total - likely orphaned documents "
                                "(benign; mongos filters them).",
                                fg=config.DARK_YELLOW,
                            )
                        if empty_shards:
                            config.write_host(
                                f"    NOTE: shard(s) with 0 docs in both collections: {', '.join(empty_shards)}.",
                                fg=config.DARK_YELLOW,
                            )

                if baseline:
                    # Look up a baseline count from a tag container; returns None if missing, else the count.
                    def get_baseline_count(container, db: str, coll: str):
                        if not container:
                            return None
                        db_val = container.get(db) if isinstance(container, dict) else None
                        if not db_val:
                            return None
                        if not isinstance(db_val, dict) or coll not in db_val:
                            return None
                        return int(db_val[coll])

                    mismatches: list[str] = []
                    found = False
                    # Compare each verified collection count against its baseline.
                    for pair in (
                        {"Coll": "loadtest", "Got": load_test_count},
                        {"Coll": "payload", "Got": payload_count},
                    ):
                        pre = get_baseline_count(baseline.get("preSnap"), verify_database, pair["Coll"])
                        post = get_baseline_count(baseline.get("postSnap"), verify_database, pair["Coll"])
                        if pre is None and post is None:
                            continue
                        found = True
                        if pre is not None and post is not None:
                            if pair["Got"] < pre or pair["Got"] > post:
                                mismatches.append(
                                    f"{verify_database}.{pair['Coll']}: expected in [{pre}, {post}], got {pair['Got']}"
                                )
                            else:
                                drift = post - pre
                                config.write_host(
                                    f"  Baseline OK : {verify_database}.{pair['Coll']} = {pair['Got']} "
                                    f"in [{pre}, {post}] (drift={drift})",
                                    fg=config.GREEN,
                                )
                        elif pre is not None:
                            if pair["Got"] < pre:
                                mismatches.append(
                                    f"{verify_database}.{pair['Coll']}: expected >= {pre} (preSnap only), got {pair['Got']}"
                                )
                            else:
                                config.write_host(
                                    f"  Baseline OK : {verify_database}.{pair['Coll']} = {pair['Got']} "
                                    f">= preSnap {pre} (postSnap missing)",
                                    fg=config.GREEN,
                                )
                        else:
                            if pair["Got"] > post:
                                mismatches.append(
                                    f"{verify_database}.{pair['Coll']}: expected <= {post} (postSnap only), got {pair['Got']}"
                                )
                            else:
                                config.write_host(
                                    f"  Baseline OK : {verify_database}.{pair['Coll']} = {pair['Got']} "
                                    f"<= postSnap {post} (preSnap missing)",
                                    fg=config.GREEN,
                                )
                    if not found:
                        config.write_host(
                            f"  Snapshot tags did not include database '{verify_database}' - skipping comparison.",
                            fg=config.DARK_YELLOW,
                        )
                    if len(mismatches) > 0:
                        joined = "\n".join(mismatches)
                        raise RuntimeError(f"Baseline verification FAILED:\n{joined}")
                else:
                    config.write_host(
                        "  No baseline in snapshot tags - reported counts are informational only.",
                        fg=config.DARK_YELLOW,
                    )
            # endregion

            # region --- Summary ---
            duration = (datetime.now() - start).total_seconds()

            config.write_host("\n=== Restore Complete ===", fg=config.GREEN)
            config.write_host(f"  Snapshot restored : {snapshot_tag}", fg="white")
            config.write_host(f"  Total duration    : {round(duration, 1)} seconds", fg="white")
            # endregion

        finally:
            # Inner finally: detach the log handlers, swallowing any teardown error.
            try:
                for h in transcript_handlers:
                    transcript_logger.removeHandler(h)
                    h.close()
            except Exception:  # noqa: BLE001 - swallow teardown errors during cleanup
                pass

    finally:
        # Outer finally: release the concurrency lock regardless of outcome.
        config.remove_script_lock(lock_path)


def main():
    typer.run(_run)


if __name__ == "__main__":
    main()
