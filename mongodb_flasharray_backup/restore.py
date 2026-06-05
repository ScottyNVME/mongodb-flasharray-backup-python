###############################################################################################################################
# Restore-MongoSnapshot - Pure Storage FlashArray + MongoDB Ops Manager Third-Party Backup API
#
# Faithful 1:1 Python port of reference/Restore-MongoSnapshot.ps1.
#
# Restores a MongoDB 8.0 sharded cluster from a set of FlashArray volume snapshots taken by
# New-MongoSnapshot.ps1. The restore overwrites each node's data volume in place,
# preserving the existing pRDM/LUN mapping - no vSphere reconfiguration required.
#
#    A single Connect-Pfa2Array call targets the gateway array (sn1-x90r2-f06-27). Cluster nodes
#    and FlashArray context names and volume mappings are discovered at runtime from Ops Manager
#    and via SCSI serial numbers. Each volume overwrite is routed to the correct array by passing
#    -ContextName with that array's short name. The target snapshot is located by querying the
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

# Original header maps lines 1-13: param() block + dot-source of Config.ps1.
# Dot-sourcing Config.ps1 is realized here via `from . import config` + config.load_config() called
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

# Import models for FlashArray (py-pure-client). Mirrors `Import-Module PureStoragePowerShellSDK2` (line 65).
from pypureclient import flasharray


app = typer.Typer(add_completion=False)


# Original line 4: [ValidatePattern('^om-\d{8}-\d{6}$')] on -SnapshotTag.
_SNAPSHOT_TAG_RE = re.compile(r"^om-\d{8}-\d{6}$")


def _validate_snapshot_tag(value: str) -> str:
    """typer callback mapping [ValidatePattern('^om-\\d{8}-\\d{6}$')]."""
    if value is None or not _SNAPSHOT_TAG_RE.match(value):
        raise typer.BadParameter(
            "Cannot validate argument on parameter 'SnapshotTag'. "
            "The argument \"{}\" does not match the \"^om-\\d{{8}}-\\d{{6}}$\" pattern.".format(value)
        )
    return value


def _run(
    # Original param() block (lines 2-12)
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
) -> None:
    # Python equivalent of dot-sourcing Config.ps1 (original line 13). Must be FIRST so running without
    # .env throws like the original (but --help still works because typer short-circuits before this).
    config.load_config()

    # Original line 70: $ErrorActionPreference = 'Stop'. In Python uncaught exceptions already terminate;
    # no analogue needed. (Comment retained for traceability.)

    # region --- Configuration --- (original lines 72-81)
    wait_timeout_sec = 600   # Max seconds to wait for cluster to stabilize
    poll_interval_sec = 10   # Seconds between readiness polls
    proc_wait_sec = 30       # Max seconds to wait for mongod TERM before SIGKILL
    # endregion

    # Concurrency lock with stale-PID detection (original lines 83-85).
    lock_path = str(Path(os.path.expanduser("~")) / ".mongo-restore.lock")
    config.new_script_lock(lock_path)

    # Logging handle so the finally-block can detach handlers (analogue of Stop-Transcript).
    transcript_handlers: list[logging.Handler] = []
    transcript_logger = logging.getLogger("restore_transcript")

    try:  # original outer try (line 87) - paired with finally that removes the lock (lines 654-657)

        # Audit log / Start-Transcript (original lines 89-93).
        log_dir = Path(os.path.expanduser("~")) / "mongo-restore-logs"
        log_dir.mkdir(parents=True, exist_ok=True)  # New-Item -ItemType Directory -Force
        log_path = log_dir / f"restore-{snapshot_tag}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"
        # Start-Transcript -Path $LogPath -Append: configure a logger writing to BOTH the file and console.
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

        try:  # original inner try (line 95) - paired with finally that Stop-Transcript (lines 650-652)

            # original line 97
            start = datetime.now()

            # region --- STEP 0: Pre-flight --- (original lines 99-236)
            config.write_host("\n=== STEP 0: Pre-flight ===", fg=config.YELLOW)

            # Discover cluster nodes from Ops Manager (fallback: CLUSTER_NODES in .env). (lines 103-105)
            config.write_host("  Discovering cluster nodes...", fg=config.CYAN)
            cluster_nodes = config.get_cluster_nodes()

            # Verify SSH connectivity to every node before stopping anything. (lines 107-114)
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

            # Connect to the gateway once. (lines 116-118)
            fa = config.connect_fa()
            config.write_host(f"  Connected to gateway: {config.CFG.FaEndpoint}", fg=config.GREEN)

            # Resolve FA context names from live fleet discovery. (lines 120-125)
            fa_context_names = config.resolve_fa_context_names(fa, config.CFG.ProtectionGroupName)
            snap_name = f"{config.CFG.ProtectionGroupName}.{snapshot_tag}"
            snap_tags = config.get_fa_snapshot_tags(fa, fa_context_names, snap_name)

            # mongo:volumes records the volume names that were part of this snapshot. (lines 127-137)
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

            # Verify the target snapshot exists on all context arrays via the gateway. (lines 139-163)
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
                    # $Snap.Name on a (possibly multi-item) result: PS pipes a single object here; take first.
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

            # Discover which FA volume backs /data/mongo on each node via SCSI serial. (lines 165-168)
            config.write_host("  Discovering node-to-volume mappings via SCSI serial...", fg=config.CYAN)
            node_volume_map = config.resolve_node_to_array_volume_map(
                fa,
                cluster_nodes,
                config.CFG.SshUser,
                config.SSH_OPTS,
                fa_context_names,
            )

            # Guard: every expected volume must have been discovered. (lines 170-176)
            if len(node_volume_map) != expected_snap_count:
                raise RuntimeError(
                    f"SCSI volume discovery found {len(node_volume_map)} of {expected_snap_count} "
                    "expected volumes - aborting before any changes are made. Verify all cluster nodes "
                    "are reachable and their data volumes are presented."
                )

            # Verify every discovered node volume is present + size matches. (lines 177-207)
            config.write_host(
                f"  Verifying all node volumes are present in snapshot '{snapshot_tag}'...",
                fg=config.CYAN,
            )
            for node, entry in node_volume_map.items():
                short_name = entry["ShortName"]
                volume_name = entry["VolumeName"]
                member_snap = f"{config.CFG.ProtectionGroupName}.{snapshot_tag}.{volume_name}"
                vol_snap = None
                # for ($attempt = 1; $attempt -le 3 -and -not $VolSnap; $attempt++)
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
                # for ($attempt = 1; $attempt -le 3 -and -not $LiveVol; $attempt++) with -ErrorAction Stop + retry
                attempt = 1
                while attempt <= 3 and not live_vol:
                    try:
                        lv_items = config._fa(
                            fa.get_volumes(context_names=[short_name], names=[volume_name]),
                            allow_error=False,
                        )
                        live_vol = lv_items[0] if lv_items else None
                    except Exception:  # noqa: BLE001 - faithful to PS catch
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

            # Discover the underlying block device backing /data/mongo on each node. (lines 209-223)
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

            # Destructive-operation confirmation. (lines 225-234)
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

            # region --- STEP 1: Stop automation agents --- (original lines 238-255)
            config.write_host("\n=== STEP 1: Stopping automation agents ===", fg=config.YELLOW)

            # Invoke-ParallelOrThrow inline block (lines 242-253).
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

            # region --- STEP 2: Force-stop mongod/mongos --- (original lines 257-297)
            config.write_host("\n=== STEP 2: Stopping mongod/mongos ===", fg=config.YELLOW)

            # Invoke-ParallelOrThrow inline block (lines 264-295).
            def _step2_stop_mongo(node):
                user = config.CFG.SshUser
                opts = config.SSH_OPTS
                max_wait = proc_wait_sec

                # Original here-string ($StopCmd, lines 270-288). In PS the here-string interpolates $Max
                # and escapes `$(seq ...). The literal remote text uses $(seq 1 <Max>) and bare $i etc.
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
                # ssh @Opts "${User}@${Node}" $StopCmd 2>&1
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

            # region --- STEP 3: Unmount /data/mongo (idempotent) --- (original lines 299-332)
            config.write_host("\n=== STEP 3: Unmounting /data/mongo ===", fg=config.YELLOW)

            # Invoke-ParallelOrThrow inline block (lines 303-330).
            def _step3_unmount(node):
                user = config.CFG.SshUser
                opts = config.SSH_OPTS
                # Original single-quoted here-string (lines 310-323): no interpolation.
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
                # Write-Host "  ${Node}: $($Out -split "`n" | Select-Object -First 1)"
                first_line = out.split("\n")[0] if out else ""
                config.write_host(f"  {node}: {first_line}", fg=config.GREEN)
                return {"Node": node, "Success": True, "Message": out}

            config.invoke_parallel_or_throw(cluster_nodes, _step3_unmount, "Unmount /data/mongo")
            # endregion

            # region --- STEP 4: Overwrite FlashArray volumes from snapshots (sequential) --- (lines 334-377)
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
                # for ($attempt = 1; $attempt -le 3 -and -not $OverwriteOk; $attempt++)
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
                        # New-Pfa2Volume -Name v -SourceName s -Overwrite $true -ErrorAction Stop
                        config._fa(
                            fa.post_volumes(
                                names=[volume_name],
                                volume=flasharray.VolumePost(
                                    source=flasharray.Reference(name=snap_name)
                                ),
                                overwrite=True,
                                context_names=[short_name],
                            ),
                            allow_error=False,
                        )
                        config.write_host(f"  Restored: {volume_name}", fg=config.GREEN)
                        overwrite_ok = True
                    except Exception as e:  # noqa: BLE001 - faithful to PS catch
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

            # region --- STEP 5: Rescan LUN + remount /data/mongo --- (original lines 379-439)
            config.write_host(
                "\n=== STEP 5: Rescanning LUN and remounting /data/mongo ===", fg=config.YELLOW
            )

            # Invoke-ParallelOrThrow inline block (lines 383-437).
            def _step5_rescan_mount(node):
                user = config.CFG.SshUser
                opts = config.SSH_OPTS
                devices = node_device
                disk = devices[node]  # e.g. 'sdb'

                # Original here-string ($Cmd, lines 401-426). It interpolates $Disk and escapes the rest.
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
                # foreach ($Line in $Out): PS iterates the captured lines.
                for line in out.splitlines():
                    if re.match(r"^(WARN|device=)", line):
                        config.write_host(f"    {node}: {line}", fg=config.DARK_YELLOW)
                config.write_host(f"  {node}: mounted", fg=config.GREEN)
                return {"Node": node, "Success": True, "Message": "mounted"}

            config.invoke_parallel_or_throw(cluster_nodes, _step5_rescan_mount, "Rescan + mount")
            # endregion

            # region --- STEP 6: Start automation agents --- (original lines 441-460)
            config.write_host("\n=== STEP 6: Starting automation agents ===", fg=config.YELLOW)

            # Invoke-ParallelOrThrow inline block (lines 445-456).
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

            # region --- STEP 7: Wait for cluster to stabilize --- (original lines 462-543)
            config.write_host("\n=== STEP 7: Waiting for cluster to stabilize ===", fg=config.YELLOW)

            expected_shards = None  # line 472

            deadline = time.monotonic() + wait_timeout_sec  # (Get-Date).AddSeconds($WaitTimeoutSec)
            ready = False

            while not ready:
                if time.monotonic() > deadline:
                    raise RuntimeError(f"Cluster did not stabilize within {wait_timeout_sec} seconds.")

                time.sleep(poll_interval_sec)
                config.write_host(
                    f"  {datetime.now().strftime('%H:%M:%S')}  Checking cluster state...",
                    fg=config.CYAN,
                )

                # Ping mongos (lines 485-491).
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

                # Read the current shard count (lines 493-501).
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
                # [int](...): PS casts the raw stdout to int.
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

                # Per-shard hello() health check via temp .js on the remote host. (lines 508-537)
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
                # $HealthScript | ssh @SshOpts "${SshUser}@${MongosHost}" "cat > $TempJs"
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

            # region --- STEP 8: Verify data --- (original lines 545-638)
            if skip_verification:
                config.write_host(
                    "\n=== STEP 8: Verification SKIPPED (-SkipVerification) ===", fg=config.DARK_YELLOW
                )
            else:
                config.write_host("\n=== STEP 8: Verifying data ===", fg=config.YELLOW)
                config.write_host("  Waiting 10s for shard metadata to propagate...", fg=config.DARK_GRAY)
                time.sleep(10)

                # Build baseline from snapshot tags. (lines 559-565)
                # $SnapTags was loaded in STEP 0; reuse it here without a second FA API call.
                baseline = None
                if snap_tags.get("mongo:preSnap") or snap_tags.get("mongo:postSnap"):
                    import json as _json
                    baseline = {
                        "preSnap": _json.loads(snap_tags["mongo:preSnap"]) if snap_tags.get("mongo:preSnap") else None,
                        "postSnap": _json.loads(snap_tags["mongo:postSnap"]) if snap_tags.get("mongo:postSnap") else None,
                    }

                # function Get-VerifyCount (lines 567-577)
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

                if baseline:
                    # function Get-BaselineCount (lines 590-597). Returns None if missing, else long.
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
                    # foreach ($Pair in @(@{Coll='loadtest';Got=...}, @{Coll='payload';Got=...}))
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

            # region --- Summary --- (original lines 640-648)
            duration = (datetime.now() - start).total_seconds()

            config.write_host("\n=== Restore Complete ===", fg=config.GREEN)
            config.write_host(f"  Snapshot restored : {snapshot_tag}", fg="white")
            config.write_host(f"  Total duration    : {round(duration, 1)} seconds", fg="white")
            # endregion

        finally:
            # original inner finally (lines 650-652): Stop-Transcript wrapped in try/catch.
            try:
                for h in transcript_handlers:
                    transcript_logger.removeHandler(h)
                    h.close()
            except Exception:  # noqa: BLE001 - faithful to PS catch {}
                pass

    finally:
        # original outer finally (lines 654-657): release the concurrency lock regardless of outcome.
        config.remove_script_lock(lock_path)


def main():
    typer.run(_run)


if __name__ == "__main__":
    main()
