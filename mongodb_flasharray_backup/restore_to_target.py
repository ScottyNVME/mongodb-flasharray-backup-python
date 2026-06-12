###############################################################################################################################
# Restore Mongo Snapshot to a DIFFERENT Replica Set - Pure Storage FlashArray + MongoDB Ops Manager
#
# Restores a full FlashArray protection-group snapshot of one replica set onto a *different*, already-existing
# replica set (the DR / clone case - certification item 1.A.1.b). This is distinct from `restore-mongo-snapshot`,
# which is in-place self-restore only (it overwrites the source cluster's own volumes).
#
# The restored data carries the SOURCE RS's local.system.replset (source RS name + source member hostnames),
# so it cannot simply be started on the destination. This command follows the MongoDB-canonical
# "restore a replica set to new hosts" procedure (seed + initial-sync):
#
#    0. Pre-flight (against the DESTINATION): discover dest node->volume map via SCSI serial; locate a source
#       PG-snapshot member co-located on the seed's array (same-arrays assumption); capture each dest node's
#       live mongod binary/dbPath/port/user BEFORE stopping anything; confirm; lock.
#    1. Stop automation agents on all destination nodes.
#    2. Force-stop mongod/mongos on all destination nodes.
#    3. Unmount /data/mongo on the SEED.
#    4. Overwrite ONLY the seed's volume from the source snapshot member (CoW pointer swap).
#    5. Remount the seed; WIPE the non-seed dest dbPaths so they perform a clean initial sync.
#    6. Rewrite the seed's RS identity OFFLINE: start a transient standalone mongod, replace
#       local.system.replset with a single-member config for the target RS name, clean-shutdown.
#    7. Start automation agents on all dest nodes -> seed self-elects primary; OM grows the RS to its
#       automationConfig member set; the wiped members initial-sync. Wait until all members are healthy.
#    8. Verify data on the destination RS against the source snapshot's preSnap/postSnap baseline.
#
# Assumptions (see docs/TODO-restore-to-different-rs.md for the documented follow-ups):
#    * Destination nodes exist and are OM-managed with TARGET_RS_NAME; storage is configured to match the
#      source (one FA data volume per dest node, on the SAME arrays that hold the source PG snapshot).
#    * Full snapshot only (no oplog/PITR).
#
# Disclaimer:
#    Provided AS-IS. This OVERWRITES the seed's data volume and WIPES the other destination members' data dirs.
#    The SOURCE replica set is never touched. Always test in a non-production environment first.
###############################################################################################################################

from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path

import typer

from . import config
from .restore import _validate_snapshot_tag  # reuse the certified om-<date>-<time> validator

app = typer.Typer(add_completion=False)

# Transient standalone port used only for the offline local.system.replset rewrite (STEP 6).
_TEMP_STANDALONE_PORT = 27099


def _parse_targets(target_nodes: str, target_seed: str = None) -> tuple[list[str], str, list[str]]:
    """Parse --target-nodes (CSV) and resolve the seed. Returns (node_list, seed, non_seed_nodes).
    The seed defaults to the first node and must be one of the targets. Raises on an empty list or a
    seed that is not in the list."""
    node_list = [n.strip() for n in target_nodes.split(",") if n.strip()]
    if not node_list:
        raise RuntimeError("--target-nodes is empty after parsing; provide a comma-separated host list.")
    seed = target_seed.strip() if target_seed else node_list[0]
    if seed not in node_list:
        raise RuntimeError(f"--target-seed '{seed}' is not one of --target-nodes ({', '.join(node_list)}).")
    return node_list, seed, [n for n in node_list if n != seed]


def _build_replset_rewrite_js(target_rs_name: str, seed_member: str) -> str:
    """Build the mongosh JS that replaces local.system.replset with a SINGLE-member config for the target
    RS (bumping version, keeping prior settings). Single-member is required so the seed can self-elect a
    primary before OM grows the RS and the wiped members initial-sync. Prints 'rewrote:<rsname>'."""
    return (
        "const local=db.getSiblingDB('local');"
        "const old=local.system.replset.findOne();"
        "local.system.replset.replaceOne({},{"
        f"_id:'{target_rs_name}',"
        "version:(old?old.version:0)+1,"
        "term:NumberLong(-1),"
        "protocolVersion:NumberLong(1),"
        f"members:[{{_id:0,host:'{seed_member}',priority:1,votes:1}}],"
        "settings:(old&&old.settings)?old.settings:{}"
        "});"
        "print('rewrote:'+local.system.replset.findOne()._id);"
    )


def _capture_mongod_info(node: str) -> dict[str, str]:
    """SSH to a node and read the live mongod's binary path, dbPath, port, run-as user, and config-file path
    from the running process (falling back to its -f/--config file). MUST be called BEFORE the agent/mongod
    is stopped. Returns {'Bin','DbPath','Port','User','Conf'}; raises if the essentials cannot be derived."""
    # Remote shell: parse the live mongod cmdline, then fill gaps from the config file. Fixed script.
    cmd = (
        "set -e\n"
        "PID=$(pgrep -x mongod | head -1)\n"
        '[ -n "$PID" ] || { echo "no-mongod"; exit 3; }\n'
        'LINE=$(tr "\\0" " " < /proc/$PID/cmdline)\n'
        'USER=$(ps -o user= -p "$PID" | tr -d " ")\n'
        "BIN=$(echo \"$LINE\" | awk '{print $1}')\n"
        "CONF=$(echo \"$LINE\" | grep -oE '(-f|--config)[ =][^ ]+' | head -1 | sed -E 's/^(-f|--config)[ =]//')\n"
        "DBPATH=$(echo \"$LINE\" | grep -oE '\\-\\-dbpath[ =][^ ]+' | sed -E 's/--dbpath[ =]//')\n"
        "PORT=$(echo \"$LINE\" | grep -oE '\\-\\-port[ =][0-9]+' | grep -oE '[0-9]+')\n"
        'if [ -n "$CONF" ]; then\n'
        '  [ -z "$DBPATH" ] && DBPATH=$(sudo grep -E "^[[:space:]]*dbPath:" "$CONF" 2>/dev/null | head -1 | awk "{print \\$2}")\n'
        '  [ -z "$PORT" ] && PORT=$(sudo grep -E "^[[:space:]]*port:" "$CONF" 2>/dev/null | head -1 | awk "{print \\$2}")\n'
        "fi\n"
        '[ -z "$PORT" ] && PORT=27017\n'
        'echo "BIN=$BIN"; echo "DBPATH=$DBPATH"; echo "PORT=$PORT"; echo "USER=$USER"; echo "CONF=$CONF"'
    )
    proc = subprocess.run(
        ["ssh", *config.SSH_OPTS, f"{config.CFG.SshUser}@{node}", cmd],
        capture_output=True,
        text=True,
    )
    out = (proc.stdout + proc.stderr).strip()
    if proc.returncode != 0:
        raise RuntimeError(f"Could not read live mongod info on {node} (exit {proc.returncode}): {out}")
    info: dict[str, str] = {}
    for line in proc.stdout.splitlines():
        m = re.match(r"^(BIN|DBPATH|PORT|USER|CONF)=(.*)$", line.strip())
        if m:
            info[m.group(1)] = m.group(2).strip()
    bin_path, dbpath, user = info.get("BIN", ""), info.get("DBPATH", ""), info.get("USER", "")
    if not bin_path or not dbpath:
        raise RuntimeError(
            f"Incomplete mongod info on {node} (bin='{bin_path}', dbPath='{dbpath}'). "
            "Verify mongod is running and managed normally before restore."
        )
    return {
        "Bin": bin_path,
        "DbPath": dbpath,
        "Port": info.get("PORT", "27017"),
        "User": user or "mongod",
        "Conf": info.get("CONF", ""),
    }


def _rs_status(member_host: str, member_port: int) -> dict:
    """Return {'primary': '<host:port>'|'', 'healthy': <#PRIMARY+SECONDARY>, 'total': <members>} from a
    directConnection rs.status() on member_host, or {'primary':'','healthy':0,'total':0} if not ready.
    Used by the STEP 7 readiness probe. Never raises."""
    js = (
        "var s=rs.status(); var prim=''; var ok=0; "
        "s.members.forEach(function(m){ if(m.stateStr==='PRIMARY'){prim=m.name;ok++;} "
        "else if(m.stateStr==='SECONDARY'){ok++;} }); "
        "print(JSON.stringify({primary:prim,healthy:ok,total:s.members.length}));"
    )
    try:
        raw = config.invoke_mongosh_js(
            ssh_target=member_host,
            uri=f"'mongodb://{member_host}:{member_port}/?directConnection=true'",
            js=js,
            context=f"rs.status via {member_host}:{member_port}",
        )
    except Exception:  # noqa: BLE001 - readiness probe; any error means "not ready yet"
        return {"primary": "", "healthy": 0, "total": 0}
    for line in reversed([ln.strip() for ln in (raw or "").splitlines() if ln.strip()]):
        try:
            obj = json.loads(line)
            if isinstance(obj, dict) and "healthy" in obj:
                return obj
        except ValueError:
            continue
    return {"primary": "", "healthy": 0, "total": 0}


def _run(
    snapshot_tag: str = typer.Option(
        ...,
        "--snapshot-tag",
        callback=_validate_snapshot_tag,
        help='Source PG snapshot tag, e.g. "om-20260505-165440".',
    ),
    target_nodes: str = typer.Option(
        ...,
        "--target-nodes",
        help="Comma-separated destination RS member hostnames (the cluster being restored ONTO).",
    ),
    target_rs_name: str = typer.Option(
        ...,
        "--target-rs-name",
        help="Destination replica set name. local.system.replset._id is rewritten to this; it MUST equal "
        "the destination cluster's Ops Manager replica-set name.",
    ),
    target_seed: str = typer.Option(
        None,
        "--target-seed",
        help="Which --target-nodes host receives the snapshot and becomes the initial primary "
        "(default: the first target node).",
    ),
    target_member_port: int = typer.Option(
        27017,
        "--target-member-port",
        help="RS member port on the destination, used for readiness/verify probes (default 27017).",
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
        help="SOURCE deployment name (selects '<NAME>__' keys in .env): provides the source protection "
        "group, fleet arrays, and the snapshot baseline tags. Omit to use the flat keys.",
    ),
) -> None:
    # Load configuration (SOURCE deployment). Must be FIRST so running without .env throws.
    config.load_config(deployment=deployment)

    # region --- Configuration ---
    wait_timeout_sec = 1800  # Max seconds to wait for the dest RS to stabilize (initial sync is data-size bound)
    poll_interval_sec = 10   # Seconds between readiness polls
    proc_wait_sec = 30       # Max seconds to wait for mongod TERM before SIGKILL
    # endregion

    target_node_list, seed, non_seed_nodes = _parse_targets(target_nodes, target_seed)

    # Concurrency lock (separate from the self-restore lock).
    lock_path = str(Path(os.path.expanduser("~")) / ".mongo-restore-to-target.lock")
    config.new_script_lock(lock_path)

    transcript_handlers: list[logging.Handler] = []
    transcript_logger = logging.getLogger("restore_to_target_transcript")

    def audit(message: str, fg: str = None) -> None:
        """Print to console (colored) and append the plain text to the audit log."""
        config.write_host(message, fg=fg)
        try:
            transcript_logger.info(message)
        except Exception:  # noqa: BLE001 - logging must never break the restore
            pass

    try:  # outer try - paired with finally that removes the lock

        log_dir = Path(os.path.expanduser("~")) / "mongo-restore-logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"restore-to-target-{target_rs_name}-{snapshot_tag}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"
        transcript_logger.setLevel(logging.INFO)
        transcript_logger.propagate = False
        _fh = logging.FileHandler(str(log_path), mode="a")
        _fh.setFormatter(logging.Formatter("%(message)s"))
        transcript_logger.addHandler(_fh)
        transcript_handlers = [_fh]

        try:  # inner try - paired with finally that detaches the log handler

            start = datetime.now()

            # region --- STEP 0: Pre-flight (DESTINATION) ---
            audit("\n=== STEP 0: Pre-flight (destination) ===", fg=config.YELLOW)
            audit(f"  Source PG       : {config.CFG.ProtectionGroupName}", fg=config.CYAN)
            audit(f"  Source snapshot : {snapshot_tag}", fg=config.CYAN)
            audit(f"  Target RS name  : {target_rs_name}", fg=config.CYAN)
            audit(f"  Target nodes    : {', '.join(target_node_list)}  (seed: {seed})", fg=config.CYAN)

            # Verify SSH connectivity to every destination node before stopping anything.
            for node in target_node_list:
                proc = subprocess.run(
                    ["ssh", *config.SSH_OPTS, f"{config.CFG.SshUser}@{node}", "true"],
                    capture_output=True,
                    text=True,
                )
                if proc.returncode != 0:
                    raise RuntimeError(
                        f"SSH connectivity check failed for {config.CFG.SshUser}@{node} (exit {proc.returncode}). "
                        "Verify key auth and reachability."
                    )
                audit(f"  SSH OK: {node}", fg=config.CYAN)

            # Capture live mongod info on every target node BEFORE anything is stopped (needed for the offline
            # rewrite on the seed and the dbPath wipe on the non-seed members).
            audit("  Capturing live mongod binary/dbPath/port/user on target nodes...", fg=config.CYAN)
            mongod_info: dict[str, dict[str, str]] = {}
            for node in target_node_list:
                info = _capture_mongod_info(node)
                mongod_info[node] = info
                audit(
                    f"    {node}: bin={info['Bin']} dbPath={info['DbPath']} port={info['Port']} user={info['User']}",
                    fg=config.DARK_GRAY,
                )

            # Connect to the gateway and resolve the SOURCE arrays + snapshot baseline tags.
            fa = config.connect_fa()
            audit(f"  Connected to gateway: {config.CFG.FaEndpoint}", fg=config.GREEN)
            fa_context_names = config.resolve_fa_context_names(fa, config.CFG.ProtectionGroupName)
            snap_name = f"{config.CFG.ProtectionGroupName}.{snapshot_tag}"
            snap_tags = config.get_fa_snapshot_tags(fa, fa_context_names, snap_name)

            # Discover which FA volume backs /data/mongo on each DESTINATION node via SCSI serial.
            audit("  Discovering destination node-to-volume mappings via SCSI serial...", fg=config.CYAN)
            node_volume_map = config.resolve_node_to_array_volume_map(
                fa,
                target_node_list,
                config.CFG.SshUser,
                config.SSH_OPTS,
                fa_context_names,
            )
            if seed not in node_volume_map:
                raise RuntimeError(f"Could not resolve a FlashArray volume for seed node '{seed}' - aborting.")
            seed_array = node_volume_map[seed]["ShortName"]
            seed_volume = node_volume_map[seed]["VolumeName"]

            # Locate a source PG-snapshot member co-located on the seed's array (same-arrays assumption). RS
            # members hold equivalent data, so ANY member snapshot present on the seed's array is a valid source.
            prefix = f"{config.CFG.ProtectionGroupName}.{snapshot_tag}."
            member_snaps = config._fa(
                fa.get_volume_snapshots(context_names=[seed_array], filter=f"name='{prefix}*'"),
                allow_error=True,
            )
            if not member_snaps:  # fallback: list all and prefix-match in Python
                all_snaps = config._fa(
                    fa.get_volume_snapshots(context_names=[seed_array]), allow_error=True
                )
                member_snaps = [s for s in all_snaps if s.name.startswith(prefix)]
            if not member_snaps:
                raise RuntimeError(
                    f"No source snapshot member '{prefix}*' found on the seed's array '{seed_array}'. "
                    "The seed's data volume must live on the same array that holds the source PG snapshot "
                    "(same-arrays assumption). Different-arrays support is a documented follow-up "
                    "(docs/TODO-restore-to-different-rs.md)."
                )
            seed_src_snap = member_snaps[0].name
            audit(f"  Seed source snapshot member: {seed_src_snap} (on {seed_array})", fg=config.GREEN)

            # Size guard: the source member snapshot must match the seed live volume size (overwrite must not resize).
            live_seed_vol = config._fa(
                fa.get_volumes(context_names=[seed_array], names=[seed_volume]), allow_error=False
            )
            if not live_seed_vol:
                raise RuntimeError(f"Could not read live seed volume '{seed_volume}' on {seed_array} - aborting.")
            if live_seed_vol[0].provisioned != member_snaps[0].provisioned:
                raise RuntimeError(
                    f"Volume size mismatch: live seed '{seed_volume}' = {live_seed_vol[0].provisioned} bytes; "
                    f"snapshot '{seed_src_snap}' = {member_snaps[0].provisioned} bytes. Restoring would resize the "
                    "live volume - aborting before any changes."
                )

            # Derive the seed's parent block device (for the LUN rescan in STEP 5). Same snippet as restore.py.
            dev_cmd = (
                'p=$(findmnt -no SOURCE /data/mongo 2>/dev/null); if [ -n "$p" ]; then '
                'lsblk -no PKNAME "$p" 2>/dev/null; else '
                "lsblk -dno NAME,SERIAL 2>/dev/null | awk '$2 ~ /^[0-9a-fA-F]{20,}$/ {print $1; exit}'; fi"
            )
            dev_proc = subprocess.run(
                ["ssh", *config.SSH_OPTS, f"{config.CFG.SshUser}@{seed}", dev_cmd],
                capture_output=True,
                text=True,
            )
            seed_disk = dev_proc.stdout.strip()
            if not seed_disk or not re.match(r"^[a-z0-9]+$", seed_disk):
                raise RuntimeError(
                    f"Could not derive parent block device for /data/mongo on seed {seed} (got: '{seed_disk}'). "
                    "Verify the Pure Storage pRDM is presented."
                )
            audit(f"  Seed device: /dev/{seed_disk}", fg=config.CYAN)

            # Destructive-operation confirmation.
            if not force:
                audit("\n  WARNING: This cross-cluster restore will:", fg=config.RED)
                audit(f"    - OVERWRITE the seed volume on {seed} (/data/mongo) from {snapshot_tag}", fg=config.RED)
                for node in non_seed_nodes:
                    audit(f"    - WIPE the data dir on {node} ({mongod_info[node]['DbPath']}) for initial sync", fg=config.RED)
                audit(f"    - Rewrite the seed's replica-set identity to '{target_rs_name}'", fg=config.RED)
                audit("  The SOURCE replica set is NOT touched.", fg=config.RED)
                confirm = input("\n  Type the snapshot tag to confirm restore: ").strip()
                if confirm != snapshot_tag:
                    raise RuntimeError("Confirmation did not match. Aborting.")
            # endregion

            # region --- STEP 1: Stop automation agents (all target nodes) ---
            audit("\n=== STEP 1: Stopping automation agents ===", fg=config.YELLOW)

            def _step1_stop_agent(node):
                audit(f"  Stopping agent on {node} ...", fg=config.CYAN)
                proc = subprocess.run(
                    ["ssh", *config.SSH_OPTS, f"{config.CFG.SshUser}@{node}", "sudo systemctl stop mongodb-mms-automation-agent"],
                    capture_output=True,
                    text=True,
                )
                if proc.returncode != 0:
                    return {"Node": node, "Success": False, "Message": f"systemctl stop failed (exit {proc.returncode})"}
                audit(f"  Stopped on {node}", fg=config.GREEN)
                return {"Node": node, "Success": True, "Message": "stopped"}

            config.invoke_parallel_or_throw(target_node_list, _step1_stop_agent, "Stop automation agents")
            # endregion

            # region --- STEP 2: Force-stop mongod/mongos (all target nodes) ---
            audit("\n=== STEP 2: Stopping mongod/mongos ===", fg=config.YELLOW)

            def _step2_stop_mongo(node):
                max_wait = proc_wait_sec
                # Identical proven script to restore.py STEP 2 (only max_wait is interpolated).
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
                    ["ssh", *config.SSH_OPTS, f"{config.CFG.SshUser}@{node}", stop_cmd],
                    capture_output=True,
                    text=True,
                )
                result = (proc.stdout + proc.stderr).strip()
                if proc.returncode != 0:
                    return {"Node": node, "Success": False, "Message": f"mongo processes still running: {result}"}
                audit(f"  {node}: {result}", fg=config.GREEN)
                return {"Node": node, "Success": True, "Message": result}

            config.invoke_parallel_or_throw(target_node_list, _step2_stop_mongo, "Stop mongod/mongos")
            # endregion

            # region --- STEP 3: Unmount /data/mongo (seed only) ---
            audit("\n=== STEP 3: Unmounting /data/mongo on seed ===", fg=config.YELLOW)
            umount_cmd = (
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
            umount_proc = subprocess.run(
                ["ssh", *config.SSH_OPTS, f"{config.CFG.SshUser}@{seed}", umount_cmd],
                capture_output=True,
                text=True,
            )
            if umount_proc.returncode != 0:
                raise RuntimeError(f"umount /data/mongo on seed {seed} failed: {(umount_proc.stdout + umount_proc.stderr).strip()}")
            audit(f"  {seed}: {(umount_proc.stdout or '').strip().splitlines()[0] if umount_proc.stdout.strip() else 'unmounted'}", fg=config.GREEN)
            # endregion

            # region --- STEP 4: Overwrite the seed volume from the source snapshot member ---
            audit(f"\n=== STEP 4: Restoring seed volume from '{seed_src_snap}' ===", fg=config.YELLOW)
            overwrite_ok = False
            last_err = None
            attempt = 1
            while attempt <= 3 and not overwrite_ok:
                try:
                    if attempt > 1:
                        audit(f"  Retry {attempt}/3 for {seed_volume} on {seed_array} ...", fg=config.YELLOW)
                    audit(f"  Overwriting {seed_volume} on {seed_array} <- {seed_src_snap} ...", fg=config.CYAN)
                    config._fa(
                        fa.post_volumes(
                            names=[seed_volume],
                            volume={"source": {"name": seed_src_snap}},
                            overwrite=True,
                            context_names=[seed_array],
                        ),
                        allow_error=False,
                    )
                    audit(f"  Restored: {seed_volume}", fg=config.GREEN)
                    overwrite_ok = True
                except Exception as e:  # noqa: BLE001 - broad catch to drive the retry loop
                    last_err = str(e)
                    audit(f"  Attempt {attempt} failed: {last_err}", fg=config.RED)
                    if attempt < 3:
                        time.sleep(5)
                attempt += 1
            if not overwrite_ok:
                raise RuntimeError(f"FlashArray seed volume restore failed: {seed_volume} on {seed_array}: {last_err}")
            # endregion

            # region --- STEP 5: Remount seed; wipe non-seed data dirs ---
            audit("\n=== STEP 5: Remounting seed + wiping non-seed data dirs ===", fg=config.YELLOW)

            # 5a: rescan LUN + remount the seed (identical proven script to restore.py STEP 5).
            rescan_cmd = (
                "set -e\n"
                f"DISK={seed_disk}\n"
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
                "set -e\n"
                "sudo mount /data/mongo\n"
                "mountpoint -q /data/mongo\n"
                'echo "mounted"'
            )
            audit(f"  {seed}: rescan + mount /dev/{seed_disk} ...", fg=config.CYAN)
            rescan_proc = subprocess.run(
                ["ssh", *config.SSH_OPTS, f"{config.CFG.SshUser}@{seed}", rescan_cmd],
                capture_output=True,
                text=True,
            )
            if rescan_proc.returncode != 0:
                raise RuntimeError(f"rescan/mount on seed {seed} failed: {(rescan_proc.stdout + rescan_proc.stderr).strip()}")
            audit(f"  {seed}: mounted", fg=config.GREEN)

            # 5b: wipe the non-seed members' dbPath contents so the automation agent triggers a clean initial sync.
            def _step5_wipe(node):
                dbpath = mongod_info[node]["DbPath"]
                muser = mongod_info[node]["User"]
                # Guard: refuse to wipe a clearly-unsafe path.
                if not dbpath or dbpath in ("/", "/data", "/data/mongo/..") or len(dbpath) < 5:
                    return {"Node": node, "Success": False, "Message": f"refusing to wipe unsafe dbPath '{dbpath}'"}
                # Remote shell: only the (validated) dbPath and user are interpolated.
                cmd = (
                    "set -e\n"
                    f'DBP="{dbpath}"\n'
                    'if mountpoint -q /data/mongo || [ -d "$DBP" ]; then\n'
                    f'  sudo find "$DBP" -mindepth 1 -maxdepth 1 -exec rm -rf {{}} +\n'
                    f'  sudo mkdir -p "$DBP" && sudo chown {muser}:{muser} "$DBP"\n'
                    '  echo wiped\n'
                    "else\n"
                    '  echo "dbPath missing"; exit 9\n'
                    "fi"
                )
                proc = subprocess.run(
                    ["ssh", *config.SSH_OPTS, f"{config.CFG.SshUser}@{node}", cmd],
                    capture_output=True,
                    text=True,
                )
                out = (proc.stdout + proc.stderr).strip()
                if proc.returncode != 0:
                    return {"Node": node, "Success": False, "Message": f"wipe failed: {out}"}
                audit(f"  {node}: data dir wiped ({dbpath})", fg=config.GREEN)
                return {"Node": node, "Success": True, "Message": "wiped"}

            if non_seed_nodes:
                config.invoke_parallel_or_throw(non_seed_nodes, _step5_wipe, "Wipe non-seed data dirs")
            else:
                audit("  (single-node target: no non-seed members to wipe)", fg=config.DARK_GRAY)
            # endregion

            # region --- STEP 6: Rewrite the seed's RS identity offline ---
            audit("\n=== STEP 6: Rewriting seed replica-set identity (offline) ===", fg=config.YELLOW)
            seed_bin = mongod_info[seed]["Bin"]
            seed_dbpath = mongod_info[seed]["DbPath"]
            seed_user = mongod_info[seed]["User"]
            seed_conf = mongod_info[seed]["Conf"]
            seed_member = f"{seed}:{target_member_port}"
            std_log = "/tmp/restore-to-target-standalone.log"
            std_conf = "/tmp/restore-to-target-standalone.conf"

            # 6a: start a transient standalone mongod (no replSet, no auth, loopback only). When the live
            # mongod has a config file, start from a STRIPPED copy of it — dropping the top-level
            # `replication`/`security`/`systemLog`/`net`/`processManagement` blocks so the CLI cleanly owns
            # port/bind/log/fork (and any TLS-on-net is removed for the loopback edit), while STORAGE options
            # (dbPath, directoryPerDB, wiredTiger tuning) are preserved. Otherwise fall back to a bare
            # --dbpath start (works for default storage layouts; see docs/TODO-restore-to-different-rs.md).
            if seed_conf:
                build_conf = (
                    f'sudo awk \'/^(replication|security|systemLog|net|processManagement):/{{skip=1;next}} '
                    f'skip&&/^[[:space:]]/{{next}} skip&&/^[^[:space:]]/{{skip=0}} {{print}}\' "{seed_conf}" '
                    f'| sudo tee {std_conf} >/dev/null\n'
                )
                # dbPath comes from the kept storage block in the stripped config.
                launch = (
                    f"sudo -u {seed_user} {seed_bin} -f {std_conf} "
                    f"--port {_TEMP_STANDALONE_PORT} --bind_ip 127.0.0.1 --fork --logpath {std_log}\n"
                )
            else:
                build_conf = ""
                launch = (
                    f"sudo -u {seed_user} {seed_bin} --dbpath {seed_dbpath} --port {_TEMP_STANDALONE_PORT} "
                    f"--bind_ip 127.0.0.1 --fork --logpath {std_log}\n"
                )
            start_std = (
                "set -e\n"
                + build_conf
                + launch
                + "for i in $(seq 1 30); do\n"
                f"  if {config.CFG.MongoshPath} --quiet "
                f"--eval 'db.adminCommand({{ping:1}}).ok' 127.0.0.1:{_TEMP_STANDALONE_PORT} 2>/dev/null | grep -q 1; then\n"
                '    echo "standalone-up"; exit 0\n'
                "  fi\n"
                "  sleep 1\n"
                "done\n"
                'echo "standalone-failed"; exit 1'
            )
            audit(f"  Starting transient standalone mongod on {seed}:{_TEMP_STANDALONE_PORT} ...", fg=config.CYAN)
            std_proc = subprocess.run(
                ["ssh", *config.SSH_OPTS, f"{config.CFG.SshUser}@{seed}", start_std],
                capture_output=True,
                text=True,
            )
            if std_proc.returncode != 0 or "standalone-up" not in std_proc.stdout:
                raise RuntimeError(
                    f"Could not start transient standalone mongod on {seed}: "
                    f"{(std_proc.stdout + std_proc.stderr).strip()} (see {std_log} on the node)"
                )
            audit("  standalone up", fg=config.GREEN)

            # 6b: replace local.system.replset with a single-member config for the target RS.
            rewrite_js = _build_replset_rewrite_js(target_rs_name, seed_member)
            rewrite_out = config.invoke_mongosh_js(
                ssh_target=seed,
                uri=f"127.0.0.1:{_TEMP_STANDALONE_PORT}",
                js=rewrite_js,
                context=f"local.system.replset rewrite on {seed}",
            )
            if f"rewrote:{target_rs_name}" not in (rewrite_out or ""):
                raise RuntimeError(f"local.system.replset rewrite did not confirm on {seed}: '{rewrite_out}'")
            audit(f"  local.system.replset._id -> '{target_rs_name}' (single member {seed_member})", fg=config.GREEN)

            # 6c: clean-shutdown the standalone.
            shutdown_cmd = (
                f"{config.CFG.MongoshPath} --quiet "
                f"--eval 'try{{db.adminCommand({{shutdown:1}})}}catch(e){{}}' 127.0.0.1:{_TEMP_STANDALONE_PORT} 2>/dev/null || true\n"
                "for i in $(seq 1 30); do\n"
                "  if ! pgrep -x mongod >/dev/null; then echo stopped; exit 0; fi\n"
                "  sleep 1\n"
                "done\n"
                "sudo pkill -KILL -x mongod 2>/dev/null || true\n"
                "sleep 2\n"
                'if pgrep -x mongod >/dev/null; then echo "still-running"; exit 1; fi\n'
                "echo stopped"
            )
            sd_proc = subprocess.run(
                ["ssh", *config.SSH_OPTS, f"{config.CFG.SshUser}@{seed}", shutdown_cmd],
                capture_output=True,
                text=True,
            )
            if sd_proc.returncode != 0:
                raise RuntimeError(f"Transient standalone did not shut down cleanly on {seed}: {(sd_proc.stdout + sd_proc.stderr).strip()}")
            audit("  transient standalone shut down", fg=config.GREEN)
            # endregion

            # region --- STEP 7: Start agents + wait for the destination RS to stabilize ---
            audit("\n=== STEP 7: Starting automation agents ===", fg=config.YELLOW)

            def _step7_start_agent(node):
                audit(f"  Starting agent on {node} ...", fg=config.CYAN)
                proc = subprocess.run(
                    ["ssh", *config.SSH_OPTS, f"{config.CFG.SshUser}@{node}", "sudo systemctl start mongodb-mms-automation-agent"],
                    capture_output=True,
                    text=True,
                )
                if proc.returncode != 0:
                    return {"Node": node, "Success": False, "Message": f"systemctl start failed (exit {proc.returncode})"}
                audit(f"  Started on {node}", fg=config.GREEN)
                return {"Node": node, "Success": True, "Message": "started"}

            config.invoke_parallel_or_throw(target_node_list, _step7_start_agent, "Start automation agents")
            audit(
                "  Agents started. Seed self-elects primary; OM grows the RS to its member set; "
                "wiped members initial-sync...",
                fg=config.GREEN,
            )

            expected_members = len(target_node_list)
            deadline = time.monotonic() + wait_timeout_sec
            ready = False
            while not ready:
                if time.monotonic() > deadline:
                    raise RuntimeError(
                        f"Destination RS '{target_rs_name}' did not stabilize within {wait_timeout_sec} seconds "
                        f"({expected_members} members healthy expected)."
                    )
                time.sleep(poll_interval_sec)
                status = _rs_status(seed, target_member_port)
                if status.get("primary") and status.get("healthy", 0) >= expected_members and status.get("total", 0) >= expected_members:
                    audit(
                        f"  {datetime.now().strftime('%H:%M:%S')}  RS up: primary {status['primary']}, "
                        f"{status['healthy']}/{expected_members} members healthy.",
                        fg=config.GREEN,
                    )
                    ready = True
                else:
                    audit(
                        f"    not ready yet (primary='{status.get('primary','')}', "
                        f"healthy={status.get('healthy',0)}/{expected_members}, members={status.get('total',0)})...",
                        fg=config.DARK_GRAY,
                    )
            # endregion

            # region --- STEP 8: Verify data on the destination RS ---
            if skip_verification:
                audit("\n=== STEP 8: Verification SKIPPED (--skip-verification) ===", fg=config.DARK_YELLOW)
            else:
                audit("\n=== STEP 8: Verifying data on the destination RS ===", fg=config.YELLOW)

                baseline = None
                if snap_tags.get("mongo:preSnap") or snap_tags.get("mongo:postSnap"):
                    baseline = {
                        "preSnap": json.loads(snap_tags["mongo:preSnap"]) if snap_tags.get("mongo:preSnap") else None,
                        "postSnap": json.loads(snap_tags["mongo:postSnap"]) if snap_tags.get("mongo:postSnap") else None,
                    }

                def get_verify_count(db: str, coll: str) -> int:
                    raw = config.invoke_mongosh_js(
                        ssh_target=seed,
                        uri=f"'mongodb://{seed}:{target_member_port}/?directConnection=true'",
                        js=f'db.getSiblingDB("{db}").{coll}.countDocuments()',
                        context=f"mongosh count of {db}.{coll} (use --skip-verification to bypass)",
                    )
                    raw = (raw or "").strip().splitlines()[-1] if (raw or "").strip() else ""
                    if not re.match(r"^\d+$", raw):
                        raise RuntimeError(f"Unparseable count for {db}.{coll}: '{raw}'")
                    return int(raw)

                load_test_count = get_verify_count(verify_database, "loadtest")
                payload_count = get_verify_count(verify_database, "payload")
                audit(f"  {verify_database}.loadtest document count: {load_test_count}", fg=config.CYAN)
                audit(f"  {verify_database}.payload  document count: {payload_count}", fg=config.CYAN)

                if baseline:
                    def get_baseline_count(container, db: str, coll: str):
                        if not container or not isinstance(container, dict):
                            return None
                        db_val = container.get(db)
                        if not isinstance(db_val, dict) or coll not in db_val:
                            return None
                        return int(db_val[coll])

                    mismatches: list[str] = []
                    found = False
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
                                audit(
                                    f"  Baseline OK : {verify_database}.{pair['Coll']} = {pair['Got']} "
                                    f"in [{pre}, {post}] (drift={post - pre})",
                                    fg=config.GREEN,
                                )
                        elif pre is not None:
                            if pair["Got"] < pre:
                                mismatches.append(f"{verify_database}.{pair['Coll']}: expected >= {pre} (preSnap only), got {pair['Got']}")
                            else:
                                audit(f"  Baseline OK : {verify_database}.{pair['Coll']} = {pair['Got']} >= preSnap {pre}", fg=config.GREEN)
                        else:
                            if pair["Got"] > post:
                                mismatches.append(f"{verify_database}.{pair['Coll']}: expected <= {post} (postSnap only), got {pair['Got']}")
                            else:
                                audit(f"  Baseline OK : {verify_database}.{pair['Coll']} = {pair['Got']} <= postSnap {post}", fg=config.GREEN)
                    if not found:
                        audit(
                            f"  Snapshot tags did not include database '{verify_database}' - skipping comparison.",
                            fg=config.DARK_YELLOW,
                        )
                    if mismatches:
                        raise RuntimeError("Baseline verification FAILED:\n" + "\n".join(mismatches))
                else:
                    audit("  No baseline in snapshot tags - reported counts are informational only.", fg=config.DARK_YELLOW)
            # endregion

            # region --- Summary ---
            duration = (datetime.now() - start).total_seconds()
            audit("\n=== Restore-to-target Complete ===", fg=config.GREEN)
            audit(f"  Snapshot restored : {snapshot_tag}", fg="white")
            audit(f"  Target RS         : {target_rs_name} ({', '.join(target_node_list)})", fg="white")
            audit(f"  Audit log         : {log_path}", fg="white")
            audit(f"  Total duration    : {round(duration, 1)} seconds", fg="white")
            # endregion

        finally:
            try:
                for h in transcript_handlers:
                    transcript_logger.removeHandler(h)
                    h.close()
            except Exception:  # noqa: BLE001 - swallow teardown errors during cleanup
                pass

    finally:
        config.remove_script_lock(lock_path)


def main():
    typer.run(_run)


if __name__ == "__main__":
    main()
