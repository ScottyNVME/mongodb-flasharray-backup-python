#!/usr/bin/env python3
"""Continuous oplog tailer using the Ops Manager third-party backup Oplog Snapshot API.

###############################################################################################################################
# Continuous Oplog Tailer - Ops Manager third-party backup Oplog Snapshot API
#
# Each iteration drives one complete OM oplog snapshot job:
#   1. POST /oplogSnapshot             -> create an oplog snapshot job
#   2. POST /oplogSnapshot/{id}/start  -> start the timeout timer
#   3. GET  /oplogSnapshot/{id}        -> poll until state = READY
#   4. For each range in the READY response, for each RS node:
#         scp each .oplogs file from the agent node to
#         ~/mongo-oplog-stream/<SnapshotTag>/<shardId>/segments/
#   5. Check range.previousEnd against stored lastEnd (gap detection)
#   6. POST /oplogSnapshot/{id}/finish -> OM deletes .oplog files asynchronously
#   7. GET  /oplogSnapshot/{id}        -> poll until state = FINISHED
#   8. Update state.json with lastEnd from this job
#
# The OM agent writes one .oplogs file per minute per RS at brs.thirdparty.baseOplogFilePath.
# Files are stored locally using the original OM filename (<startTs>_<endTs>.oplogs) so that
# lexical sort on disk equals chronological order for the replay step.
#
# Gap detection: each READY response includes previousEnd per range. If previousEnd does not
# match the stored lastEnd, an oplog gap exists and a gap-<timestamp>.json marker is written.
# With --abort-on-gap the loop terminates; without it, capture continues past the gap.
#
# Stop: the stop command writes ~/mongo-oplog-stream/<tag>/.stop. The tailer checks for
# it at the top of each iteration and exits cleanly after writing a .stopped marker.
#
# NOTE: Start this tailer BEFORE taking the first FlashArray snapshot. The OM API maintains
# coverage continuity via previousEnd - no separate anchor is needed.
# If the tailer starts after the FA snapshot, coverage begins at the first oplog snapshot's
# start time, not the snapshot point, leaving a gap.
###############################################################################################################################
"""
import json
import logging
import os
import re
import subprocess
import time
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path

import typer

from .. import config


# --- Pattern-validation callback for --snapshot-tag ---
_SNAPSHOT_TAG_RE = re.compile(r"^om-\d{8}-\d{6}$")


def _validate_snapshot_tag(value: str) -> str:
    if not _SNAPSHOT_TAG_RE.match(value):
        raise typer.BadParameter(
            r"Value must match pattern ^om-\d{8}-\d{6}$"
        )
    return value


def _run(
    snapshot_tag: str = typer.Option(
        ...,
        "--snapshot-tag",
        callback=_validate_snapshot_tag,
        help="[Mandatory] ^om-\\d{8}-\\d{6}$",
    ),
    interval_sec: int = typer.Option(
        60, "--interval-sec",
        help="seconds between consecutive oplog snapshot jobs",
    ),
    timeout_minutes: int = typer.Option(
        30, "--timeout-minutes",
        help="per-job timeout waiting for READY or FINISHED",
    ),
    poll_interval_sec: int = typer.Option(
        5, "--poll-interval-sec",
        help="seconds between GET /oplogSnapshot polls",
    ),
    abort_on_gap: bool = typer.Option(
        False, "--abort-on-gap",
        help="abort the loop when an oplog gap is detected; default: warn and continue",
    ),
    deployment: str = typer.Option(
        None, "--deployment",
        help="Deployment name to tail (selects '<NAME>__' keys in .env). Omit to use the flat keys.",
    ),
) -> None:
    # Load config first.
    config.load_config(deployment=deployment)

    # Read env-derived values used in this script
    group_id = config.CFG.GroupId
    cluster_id = config.CFG.ClusterId
    ssh_user = config.CFG.SshUser

    # Paths + ensure root exists
    home = Path(os.path.expanduser("~"))
    root = home / "mongo-oplog-stream" / snapshot_tag
    stop_file = root / ".stop"
    state_file = root / "state.json"
    root.mkdir(parents=True, exist_ok=True)

    # Log dir + logging to console and file
    log_dir = home / "mongo-oplogtailer-logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / (
        f"oplogtailer-{snapshot_tag}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"
    )
    logger = logging.getLogger("oplogtailer")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    _console = logging.StreamHandler()
    _console.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(_console)
    _file = logging.FileHandler(str(log_path), mode="a")
    _file.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(_file)

    def _now_hms() -> str:
        return datetime.now().strftime("%H:%M:%S")

    def _utc_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    try:

        # region --- Pre-flight ---

        config.write_host(
            f"\n=== Oplog Tailer (OM Oplog Snapshot API) for {snapshot_tag} ===",
            fg=config.YELLOW,
        )

        # Verify brs.thirdparty.baseOplogFilePath
        config.write_host("  Checking OM oplog path configuration...", fg=config.CYAN)
        try:
            settings = config.invoke_om_api(path="group/settings")
            oplog_base_path = None
            if isinstance(settings, dict):
                inner = settings.get("settings")
                if isinstance(inner, dict):
                    oplog_base_path = inner.get("brs.thirdparty.baseOplogFilePath")
            if not oplog_base_path:
                raise RuntimeError(
                    "brs.thirdparty.baseOplogFilePath is not configured in Ops Manager. "
                    "Navigate to Admin -> General -> Ops Manager Config and set this key to the oplog directory path on each agent node."
                )
            config.write_host(f"  OM oplog base path: {oplog_base_path}", fg=config.GREEN)
        except Exception as e:
            # Missing admin privileges to read the config is non-fatal; anything else re-raises.
            if re.search(r"USER_UNAUTHORIZED|401", str(e)):
                logger.warning(
                    "WARNING: Cannot read OM oplog path config (requires admin privileges). "
                    "Proceeding - ensure brs.thirdparty.baseOplogFilePath is set in the OM Admin UI."
                )
            else:
                raise

        # Select one tailing node per RS
        cluster_detail = config.invoke_om_api(
            path=f"group/{group_id}/clusters/{cluster_id}"
        )

        # Agent-reachability pre-check: OM's `snapshotable` flag lags a stopped automation agent by ~35s,
        # and OM does NOT auto-fail an oplog snapshot job dispatched to a dead agent — the job sits PENDING
        # (and /fail is a no-op on it), leaving an unrecoverable coverage gap. So before choosing a node as
        # the preferred oplog node, confirm its automation agent is actually running on its host (SSH
        # `systemctl is-active`, the inverse of how the agent is stopped). Result is cached per host.
        _agent_state: dict[str, str] = {}

        def _agent_active(node: dict) -> bool:
            host = (node.get("id") or "").split(":")[0]
            if host not in _agent_state:
                proc = subprocess.run(
                    ["ssh", *config.SSH_OPTS, f"{config.CFG.SshUser}@{host}",
                     "systemctl is-active mongodb-mms-automation-agent"],
                    capture_output=True,
                    text=True,
                )
                # is-active prints active/inactive/failed/...; an SSH failure yields no stdout.
                _agent_state[host] = (proc.stdout or "").strip() or "unreachable"
            st = _agent_state[host]
            if st != "active":
                config.write_host(
                    f"    skipping {node.get('id')}: automation agent is '{st}' "
                    f"(lastAgentPing={node.get('lastAgentPing')})",
                    fg=config.YELLOW,
                )
            return st == "active"

        tailing_node_ids: list[str] = []
        for rs in (cluster_detail.get("replicaSets") or []):
            # Snapshotable candidates whose automation agent is confirmed running.
            candidates = [
                n for n in (rs.get("nodes") or [])
                if n.get("snapshotable") is True and _agent_active(n)
            ]
            # Tail oplog from the PRIMARY (matches the snapshot's backup-cursor node), so the snapshot's
            # cursor-pinned member and the oplog stream come from the same node. Fall back to a secondary
            # only if the primary isn't snapshotable/agent-reachable.
            chosen = next((n for n in candidates if n.get("memberState") == "PRIMARY"), None)
            if not chosen:
                chosen = next(
                    (n for n in candidates if n.get("memberState") == "SECONDARY"),
                    None,
                )
            if not chosen:
                chosen = candidates[0] if candidates else None  # any agent-reachable snapshotable member
            if not chosen:
                raise RuntimeError(
                    f"No snapshotable node with a reachable automation agent for replica set "
                    f"{rs.get('id')}. Refusing to set a preferred oplog node on a host whose agent is down "
                    "— it would leave the oplog snapshot job stuck PENDING and create an unrecoverable "
                    "coverage gap. Restart the agent or wait for a healthy primary."
                )
            tailing_node_ids.append(chosen.get("id"))
            config.write_host(
                f"  {rs.get('id')} -> tailing on {chosen.get('id')} [{chosen.get('memberState')}] (agent active)",
                fg=config.CYAN,
            )

        # Register preferred oplog nodes (idempotent)
        config.invoke_om_api(
            method="POST",
            path=f"group/{group_id}/clusters/{cluster_id}/preferredOplogNodes",
            body={"nodeIds": list(tailing_node_ids)},
        )
        config.write_host(
            f"  Preferred oplog nodes set: {', '.join(tailing_node_ids)}",
            fg=config.GREEN,
        )

        # Map replica-set id -> canonical shard id via mongos listShards. The OM oplog API identifies
        # ranges by rsId, but snapshot and replay key per-shard artifacts by the canonical shard id
        # (e.g. the embedded config shard's rsId is "aen-shard_0" but its shard id is "config"). Writing
        # segment dirs under the shard id keeps the tailer consistent with replay. Best-effort: if mongos
        # is unreachable, fall back to rsId for the dir name (replay tolerates either).
        shard_id_by_rs: dict[str, str] = {}
        try:
            shard_json = config.invoke_mongosh_js(
                ssh_target=config.CFG.MongosHost,
                uri=f"mongodb://{config.CFG.MongosHost}:{config.CFG.MongosPort}",
                js=config.LIST_SHARDS_JS,
                context="listShards via mongos (rsId->shardId map)",
            )
            for sh in json.loads(shard_json):
                rs_name = (sh.get("rsHosts") or "").split("/")[0]
                if rs_name and sh.get("shardId"):
                    shard_id_by_rs[rs_name] = sh["shardId"]
            config.write_host(f"  Shard-id map (rsId->shardId): {shard_id_by_rs}", fg=config.CYAN)
        except Exception as e:  # noqa: BLE001 - best-effort; fall back to rsId for dir naming
            config.write_host(
                f"  WARNING: could not build rsId->shardId map ({e}); segment dirs will use rsId",
                fg=config.YELLOW,
            )

        # Load existing state
        if state_file.exists():
            state = json.loads(state_file.read_text())
        else:
            state = None
        job_seq = int(state["totalJobs"]) if state else 0

        # Write .started marker
        started = OrderedDict()
        started["snapshotTag"] = snapshot_tag
        started["pid"] = os.getpid()
        started["startedUtc"] = _utc_iso()
        started["intervalSec"] = interval_sec
        started["tailingNodes"] = list(tailing_node_ids)
        (root / ".started").write_text(json.dumps(started, indent=2))

        config.write_host(f"  Root     : {root}", fg=config.CYAN)
        config.write_host(
            f"  Interval : {interval_sec}s  timeout: {timeout_minutes}m  poll: {poll_interval_sec}s",
            fg=config.CYAN,
        )
        config.write_host(
            f"  Stop with: stop-oplog-tailer --snapshot-tag '{snapshot_tag}'",
            fg=config.CYAN,
        )
        config.write_host("", fg=None)

        # endregion

        # Loop until the stop sentinel appears.
        while not stop_file.exists():
            iter_start = datetime.now()
            job_completed = False
            oplog_snapshot_id = None

            try:
                # region --- Create and start oplog snapshot job ---

                create_resp = config.invoke_om_api(
                    method="POST",
                    path=f"group/{group_id}/clusters/{cluster_id}/oplogSnapshot",
                    body={"timeoutMinutes": timeout_minutes},
                )
                oplog_snapshot_id = create_resp.get("oplogSnapshotId")
                config.write_host(
                    f"  [{_now_hms()}]  Job {job_seq + 1}: created  id={oplog_snapshot_id}",
                    fg=config.CYAN,
                )

                config.invoke_om_api(
                    method="POST",
                    path=f"group/{group_id}/clusters/{cluster_id}/oplogSnapshot/{oplog_snapshot_id}/start",
                )

                # endregion

                # region --- Poll until READY ---

                deadline = datetime.now().timestamp() + timeout_minutes * 60
                snap_state = ""
                ready_resp = None
                while snap_state != "READY":
                    if datetime.now().timestamp() > deadline:
                        raise RuntimeError(
                            f"Oplog snapshot {oplog_snapshot_id} timed out waiting for READY."
                        )
                    if snap_state in ("FAILED", "FAILING"):
                        raise RuntimeError(
                            f"Oplog snapshot {oplog_snapshot_id} entered {snap_state} state."
                        )
                    time.sleep(poll_interval_sec)
                    ready_resp = config.invoke_om_api_with_retry(
                        path=f"group/{group_id}/clusters/{cluster_id}/oplogSnapshot/{oplog_snapshot_id}"
                    )
                    snap_state = ready_resp.get("state")
                    config.write_host(
                        f"  [{_now_hms()}]  state = {snap_state}", fg=config.DARK_GRAY
                    )

                # endregion

                # region --- Gap detection and file copy ---

                job_seq += 1
                new_last_end = None

                for range_ in (ready_resp.get("ranges") or []):
                    range_end = range_.get("end")
                    range_prev_end = range_.get("previousEnd")

                    # Gap check: a range's previousEnd must equal the lastEnd we last stored;
                    # any mismatch means oplog entries are missing for this window.
                    if state and state.get("lastEnd") and range_prev_end:
                        stored_t = int(state["lastEnd"]["time"])
                        stored_i = int(state["lastEnd"]["inc"])
                        prev_t = int(range_prev_end["time"])
                        prev_i = int(range_prev_end["inc"])
                        if stored_t != prev_t or stored_i != prev_i:
                            gap_msg = (
                                f"Oplog gap: stored lastEnd=({stored_t}:{stored_i}) but "
                                f"OM previousEnd=({prev_t}:{prev_i}). PIT in this window is unrecoverable."
                            )
                            config.write_host(
                                f"  [{_now_hms()}]  WARNING: {gap_msg}", fg=config.RED
                            )
                            # Write gap marker
                            gap = OrderedDict()
                            gap["detectedUtc"] = _utc_iso()
                            gap["storedLastEnd"] = state["lastEnd"]
                            gap["omPreviousEnd"] = range_prev_end
                            gap["jobId"] = oplog_snapshot_id
                            gap_name = "gap-{0}.json".format(
                                datetime.now().strftime("%Y%m%d-%H%M%S")
                            )
                            (root / gap_name).write_text(json.dumps(gap, indent=2))
                            if abort_on_gap:
                                raise RuntimeError(
                                    "Aborting due to oplog gap (-AbortOnGap set)."
                                )

                    # Track latest end timestamp
                    if (not new_last_end) or int(range_end["time"]) > int(new_last_end["time"]):
                        new_last_end = range_end

                    # Copy each .oplogs file
                    for range_node in (range_.get("nodes") or []):
                        node_host = range_node.get("id").split(":")[0]
                        rs_id = range_node.get("rsId")
                        shard_key = shard_id_by_rs.get(rs_id, rs_id)
                        seg_dir = root / shard_key / "segments"
                        seg_dir.mkdir(parents=True, exist_ok=True)

                        log_files = range_node.get("logFiles") or []
                        for remote_file in log_files:
                            # Store each segment under its original OM filename so on-disk
                            # lexical order matches chronological order during replay.
                            local_name = os.path.basename(remote_file)
                            local_path = seg_dir / local_name
                            config.write_host(
                                f"  [{_now_hms()}]  scp {node_host}:{remote_file} -> {shard_key}/{local_name}",
                                fg=config.CYAN,
                            )
                            proc = subprocess.run(
                                [
                                    "scp",
                                    *config.SSH_OPTS,
                                    f"{ssh_user}@{node_host}:{remote_file}",
                                    str(local_path),
                                ],
                                capture_output=True,
                                text=True,
                            )
                            if proc.returncode != 0:
                                raise RuntimeError(
                                    f"scp failed for {remote_file} from {node_host} (exit {proc.returncode})"
                                )
                        config.write_host(
                            f"  [{_now_hms()}]  {shard_key}: {len(log_files)} file(s)  "
                            f"end={int(range_end['time'])}:{int(range_end['inc'])}",
                            fg=config.GREEN,
                        )

                # endregion

                # region --- Finish and poll FINISHED ---

                config.invoke_om_api_with_retry(
                    method="POST",
                    path=f"group/{group_id}/clusters/{cluster_id}/oplogSnapshot/{oplog_snapshot_id}/finish",
                )
                deadline = datetime.now().timestamp() + timeout_minutes * 60
                snap_state = ""
                while snap_state != "FINISHED":
                    if datetime.now().timestamp() > deadline:
                        raise RuntimeError(
                            f"Oplog snapshot {oplog_snapshot_id} timed out waiting for FINISHED."
                        )
                    if snap_state in ("FAILED", "FAILING"):
                        raise RuntimeError(
                            f"Oplog snapshot {oplog_snapshot_id} entered {snap_state} after finish."
                        )
                    time.sleep(poll_interval_sec)
                    resp = config.invoke_om_api_with_retry(
                        path=f"group/{group_id}/clusters/{cluster_id}/oplogSnapshot/{oplog_snapshot_id}"
                    )
                    snap_state = resp.get("state")
                config.write_host(
                    f"  [{_now_hms()}]  Job {job_seq}: FINISHED", fg=config.GREEN
                )

                # endregion

                job_completed = True

                # Persist state
                new_state = OrderedDict()
                new_state["snapshotTag"] = snapshot_tag
                new_state["totalJobs"] = job_seq
                new_state["lastJobId"] = oplog_snapshot_id
                new_state["lastEnd"] = new_last_end
                new_state["updatedUtc"] = _utc_iso()
                state_json = json.dumps(new_state, indent=2)
                state_file.write_text(state_json)
                # Round-trip through JSON so subsequent iterations compare against the
                # same shape that was persisted.
                state = json.loads(state_json)

            except Exception as e:
                config.write_host(
                    f"  [{_now_hms()}]  ERROR: {e}", fg=config.RED
                )
                if oplog_snapshot_id and not job_completed:
                    config.write_host(
                        f"  [{_now_hms()}]  Calling /fail on {oplog_snapshot_id} ...",
                        fg=config.YELLOW,
                    )
                    try:
                        config.invoke_om_api_with_retry(
                            method="POST",
                            path=f"group/{group_id}/clusters/{cluster_id}/oplogSnapshot/{oplog_snapshot_id}/fail",
                        )
                    except Exception as e2:
                        config.write_host(
                            f"  [{_now_hms()}]  /fail also failed: {e2}", fg=config.RED
                        )

            # Sleep remainder of interval
            elapsed = (datetime.now() - iter_start).total_seconds()
            sleep = max(0, interval_sec - elapsed)
            if sleep > 0 and not stop_file.exists():
                time.sleep(sleep)

        # Stop sentinel detected
        config.write_host(
            "\n  Stop sentinel detected - exiting cleanly.", fg=config.YELLOW
        )
        stopped = OrderedDict()
        stopped["snapshotTag"] = snapshot_tag
        stopped["stoppedUtc"] = _utc_iso()
        stopped["totalJobs"] = job_seq
        stopped["lastEnd"] = state["lastEnd"] if state else None
        (root / ".stopped").write_text(json.dumps(stopped, indent=2))
        config.write_host(f"  Summary: {root / '.stopped'}", fg=config.GREEN)

    finally:
        # Flush/close logging handlers
        try:
            for h in list(logger.handlers):
                h.flush()
                h.close()
                logger.removeHandler(h)
        except Exception:
            pass


def main():
    typer.run(_run)


if __name__ == "__main__":
    main()
