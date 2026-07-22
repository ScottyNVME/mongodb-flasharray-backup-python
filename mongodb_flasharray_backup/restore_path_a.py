#!/usr/bin/env python3
# Path A restore (OM restore API + volumeRestore) -- the consistent-by-construction restore.
#
# Every replica-set member is restored from the ONE OM-frozen secondary's snapshot, which was pre-replicated
# to the sibling-member arrays at snapshot time (new-mongo-snapshot --enable-replication). Because all members
# receive byte-identical data at one oplog point, there is no cross-member divergence -- unlike the original
# per-member restore. See docs/adr-restore-model.md and docs/path-a-implementation-plan.md.
#
# Flow: pre-flight (nodes/mongos up, sources present) -> OM POST /restore (volumeRestore:true) -> start ->
# wait for the "place files" window -> per non-arbiter node: unmount, FA clone the source onto the member's
# volume, remount, filesCopied -> wait COMPLETED -> verify.
#
# STATUS: built + unit-tested; NOT yet live-validated end-to-end (needs a complete async mesh + repl-PGs).
# Kept as a SEPARATE entrypoint so the proven original restore is untouched until cutover (Phase 6).
#
# OPEN QUESTIONS (marked OQ below) -- confirm against the live OM API / MongoDB before production:
#   OQ1  exact OM restore state that signals "vendor may place files now" (WAIT_FOR_FILES_STATES).
#   OQ2  whether OM stops mongod for us (assumed yes; we only unmount/clone/remount).
#   OQ3  snapshotsMetadata shape for a volume vendor + restoreRole values in nodes[].
#   OQ4  filesCopied body (assumed {"nodeId": ...}) -- see config.om_restore_files_copied.

import re
import subprocess
import time
from datetime import datetime

import typer

from . import config

# OQ1: the restore state(s) in which the vendor is expected to place files. Built to a best-known shape;
# adjust once confirmed. We wait for ANY of these before doing the per-node volume clones.
WAIT_FOR_FILES_STATES = {"WAITING_FOR_FILES", "READY", "COPYING_FILES", "FILES_COPY_IN_PROGRESS"}
COMPLETED_STATES = {"COMPLETED"}


def _ssh(node: str, script: str):
    return subprocess.run(["ssh", *config.SSH_OPTS, f"{config.CFG.SshUser}@{node}", script],
                          capture_output=True, text=True)


def _node_backing_disks(node: str) -> list[str]:
    """The backing block device(s) for /data/mongo (single pRDM or several LVM PVs) -- for the remount rescan."""
    cmd = (
        'src=$(findmnt -no SOURCE /data/mongo 2>/dev/null); '
        'if [ -n "$src" ]; then lsblk -s -rno NAME,TYPE "$src" 2>/dev/null '
        "| awk '$2 == \"disk\" {print $1}' | sort -u; "
        "else lsblk -dno NAME,SERIAL 2>/dev/null | awk '$2 ~ /^[0-9a-fA-F]{20,}$/ {print $1}'; fi"
    )
    proc = _ssh(node, cmd)
    return [d.strip() for d in proc.stdout.splitlines() if re.match(r"^[a-z0-9]+$", d.strip())]


def _unmount(node: str):
    return _ssh(node, "if mountpoint -q /data/mongo; then sudo umount /data/mongo && echo unmounted; "
                      "else echo already-unmounted; fi")


def _rescan_mount(node: str, disks: list[str]):
    disks_sh = " ".join(disks)
    return _ssh(node, (
        "set -e\n"
        f'for D in {disks_sh}; do\n'
        "  echo 1 | sudo tee /sys/block/$D/device/rescan >/dev/null 2>&1 || true\n"
        "  sudo blockdev --rereadpt /dev/$D 2>/dev/null || sudo partprobe /dev/$D 2>/dev/null || true\n"
        "done\n"
        "sudo udevadm settle --timeout=15 || true\n"
        "sudo pvscan --cache >/dev/null 2>&1 || true\n"
        "sudo vgchange -ay >/dev/null 2>&1 || true\n"
        "sudo mount /data/mongo\n"
        "mountpoint -q /data/mongo && echo mounted"
    ))


def _run(
    snapshot_tag: str = typer.Option(..., "--snapshot-tag", help="Snapshot tag to restore (required)."),
    force: bool = typer.Option(False, "--force", help="Skip the destructive-restore confirmation prompt."),
    deployment: str = typer.Option(None, "--deployment", help="Deployment name (selects '<NAME>__' .env keys)."),
) -> None:
    config.load_config(deployment=deployment)
    cfg = config.CFG
    TimeoutMinutes = 150

    config.write_host("\n=== Path A restore (OM restore API + volumeRestore) ===", fg=config.YELLOW)
    config.write_host(f"  Snapshot tag : {snapshot_tag}", fg=config.CYAN)

    fa = config.connect_fa()
    fa_context_names = config.resolve_fa_context_names(fa, cfg.ProtectionGroupName)
    cluster_nodes = config.get_cluster_nodes()

    # region --- STEP 0: Pre-flight (build + validate the clone plan; nodes up) ---
    config.write_host("\n=== STEP 0: Pre-flight ===", fg=config.YELLOW)

    # The single consistent source per RS, recorded on the snapshot by new-mongo-snapshot --enable-replication.
    snap_name = f"{cfg.ProtectionGroupName}.{snapshot_tag}"
    snap_tags = config.get_fa_snapshot_tags(fa, fa_context_names, snap_name)
    source_tag = snap_tags.get("mongo:sourceReplPgs")
    if not source_tag:
        raise RuntimeError(
            f"Snapshot '{snapshot_tag}' has no mongo:sourceReplPgs tag -- it was not taken with Path A "
            "replication (new-mongo-snapshot --enable-replication). Cannot do a Path A restore."
        )

    node_volume_map = config.resolve_node_volume_map(
        fa, cluster_nodes, cfg.SshUser, config.SSH_OPTS, fa_context_names, cfg.DeploymentName
    )
    rs_membership = config.get_replica_set_membership(cluster_nodes)
    rs_array_map = config.build_rs_array_map(rs_membership, node_volume_map)
    frozen_sources = config.parse_source_repl_pgs(source_tag, rs_array_map)
    clone_plan = config.build_clone_plan(rs_array_map, frozen_sources, snapshot_tag)
    if not clone_plan:
        raise RuntimeError("Empty clone plan -- could not match snapshot sources to current RS members.")

    # Validate every source member snapshot is present on the member's array BEFORE any change.
    config.write_host("  Validating replicated source snapshots are present on all member arrays...", fg=config.CYAN)
    for c in clone_plan:
        present = config._fa(
            fa.get_volume_snapshots(context_names=[c["MemberArray"]], names=[c["SourceMember"]]),
            allow_error=True,
        )
        if not present:
            raise RuntimeError(
                f"Source snapshot '{c['SourceMember']}' not present on {c['MemberArray']} (needed to restore "
                f"{c['MemberVolume']}). Ensure the snapshot replicated to all sibling arrays -- aborting."
            )
        config.write_host(f"    {c['MemberArray']}: {c['MemberVolume']} <- {c['SourceMember']}", fg=config.CYAN)

    # Pre-check: all non-arbiter nodes + mongos up with agents (OM waits indefinitely for a down node).
    # OQ: also confirm mongos routers here for a sharded cluster.
    for node in cluster_nodes:
        st = (_ssh(node, "systemctl is-active mongodb-mms-automation-agent").stdout or "").strip()
        if st != "active":
            raise RuntimeError(f"Node {node} automation agent is '{st}' -- all nodes must be up before a "
                               "Path A restore (OM will not auto-fail a down node). Aborting.")
    config.write_host(f"  All {len(cluster_nodes)} nodes' agents active.", fg=config.GREEN)

    if not force:
        config.write_host("\n  WARNING: This OVERWRITES every member's data volume from the snapshot.", fg=config.RED)
        if input("  Type the snapshot tag to confirm: ").strip() != snapshot_tag:
            raise RuntimeError("Confirmation did not match. Aborting.")
    # endregion

    restore_id = None
    try:
        # region --- STEP 1: Create + start the OM restore job (volumeRestore:true) ---
        config.write_host("\n=== STEP 1: Creating OM restore job (volumeRestore) ===", fg=config.YELLOW)
        # OQ3: snapshotsMetadata shape + restoreRole values. Built to a best-known shape.
        nodes = [{"id": n, "restoreRole": "DATA"} for n in cluster_nodes]  # OQ: arbiters -> "ARBITER"
        restore_id = config.invoke_om_restore_create(
            snapshots_metadata=snap_tags.get("snapshotMetadata") or [], nodes=nodes, volume_restore=True
        )
        config.write_host(f"  restoreId = {restore_id}", fg=config.GREEN)
        config.start_om_restore(restore_id)
        # endregion

        # region --- STEP 2: Wait for the place-files window ---
        config.write_host("\n=== STEP 2: Waiting for the restore to be ready for file placement ===", fg=config.YELLOW)
        config.wait_om_restore_state(restore_id, WAIT_FOR_FILES_STATES, timeout_minutes=TimeoutMinutes)
        # endregion

        # region --- STEP 3: Per member -- unmount, clone the source, remount, filesCopied ---
        config.write_host("\n=== STEP 3: Cloning the consistent source onto each member ===", fg=config.YELLOW)
        # OQ2: assumes OM has stopped mongod on each node; the vendor only unmounts/clones/remounts.
        disks_by_node = {n: _node_backing_disks(n) for n in cluster_nodes}
        vol_to_node = {e["VolumeName"]: n for n, entries in node_volume_map.items() for e in entries}
        signaled: set = set()
        for c in clone_plan:
            node = vol_to_node.get(c["MemberVolume"])
            config.write_host(f"  {node} [{c['MemberArray']}/{c['MemberVolume']}] <- {c['SourceMember']}", fg=config.CYAN)
            if _unmount(node).returncode != 0:
                raise RuntimeError(f"unmount /data/mongo failed on {node} -- aborting.")
            config._fa(
                fa.post_volumes(names=[c["MemberVolume"]], volume={"source": {"name": c["SourceMember"]}},
                                overwrite=True, context_names=[c["MemberArray"]]),
                allow_error=False,
            )
            mp = _rescan_mount(node, disks_by_node.get(node) or [])
            if mp.returncode != 0:
                raise RuntimeError(f"remount /data/mongo failed on {node}: {(mp.stdout + mp.stderr).strip()}")
            if node not in signaled:
                config.om_restore_files_copied(restore_id, node)   # OQ4: per-node signal
                signaled.add(node)
            config.write_host(f"    {node}: cloned + remounted + filesCopied", fg=config.GREEN)
        # endregion

        # region --- STEP 4: Wait for COMPLETED ---
        config.write_host("\n=== STEP 4: Waiting for restore COMPLETED ===", fg=config.YELLOW)
        config.wait_om_restore_state(restore_id, COMPLETED_STATES, timeout_minutes=TimeoutMinutes)
        config.write_host("\n=== Path A restore complete ===", fg=config.GREEN)
        config.write_host(f"  Snapshot restored : {snapshot_tag}", fg=config.GREEN)
        config.write_host(
            "  NOTE: run the shared count/per-shard verification (reuse restore.py STEP 8 at cutover).",
            fg=config.DARK_YELLOW,
        )
        # endregion
    except Exception:
        if restore_id:
            config.write_host(f"  Restore failed -- signaling OM /fail for {restore_id}.", fg=config.RED)
            try:
                config.om_restore_fail(restore_id)
            except Exception as fe:  # noqa: BLE001
                config.write_host(f"  (om_restore_fail also failed: {fe})", fg=config.RED)
        raise


def main() -> None:
    typer.run(_run)


if __name__ == "__main__":
    main()
