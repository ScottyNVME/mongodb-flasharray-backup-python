#!/usr/bin/env python3
"""Start-OplogTailer.ps1 -> Python (faithful 1:1 translation).

###############################################################################################################################
# Continuous Oplog Tailer - Ops Manager third-party backup Oplog Snapshot API
#
# Each iteration drives one complete OM oplog snapshot job:
#   1. POST /oplogSnapshot             -> create an oplog snapshot job
#   2. POST /oplogSnapshot/{id}/start  -> start the timeout timer
#   3. GET  /oplogSnapshot/{id}        -> poll until state = READY
#   4. For each range in the READY response, for each RS node:
#         scp each .oplogs file from the agent node to
#         ~/mongo-oplog-stream/<SnapshotTag>/<rsId>/segments/
#   5. Check range.previousEnd against stored lastEnd (gap detection)
#   6. POST /oplogSnapshot/{id}/finish -> OM deletes .oplog files asynchronously
#   7. GET  /oplogSnapshot/{id}        -> poll until state = FINISHED
#   8. Update state.json with lastEnd from this job
#
# The OM agent writes one .oplogs file per minute per RS at brs.thirdparty.baseOplogFilePath.
# Files are stored locally using the original OM filename (<startTs>_<endTs>.oplogs) so that
# lexical sort on disk equals chronological order for Invoke-OplogReplay.ps1.
#
# Gap detection: each READY response includes previousEnd per range. If previousEnd does not
# match the stored lastEnd, an oplog gap exists and a gap-<timestamp>.json marker is written.
# With -AbortOnGap the loop terminates; without it, capture continues past the gap.
#
# Stop: Stop-OplogTailer.ps1 writes ~/mongo-oplog-stream/<tag>/.stop. The tailer checks for
# it at the top of each iteration and exits cleanly after writing a .stopped marker.
#
# NOTE: Start this tailer BEFORE taking the first FlashArray snapshot. The OM API maintains
# coverage continuity via previousEnd - no anchor from the snapshot sidecar is needed.
# If the tailer starts after the FA snapshot, coverage begins at the first oplog snapshot's
# start time, not the snapshot point, leaving a gap.
###############################################################################################################################
"""
# Maps original lines 1-12: param block + dot-source Config.ps1
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


# --- ValidatePattern callback for -SnapshotTag (original lines 3-5) ---
_SNAPSHOT_TAG_RE = re.compile(r"^om-\d{8}-\d{6}$")


def _validate_snapshot_tag(value: str) -> str:
    # [ValidatePattern('^om-\d{8}-\d{6}$')]
    if not _SNAPSHOT_TAG_RE.match(value):
        raise typer.BadParameter(
            r"Value must match pattern ^om-\d{8}-\d{6}$"
        )
    return value


def _run(
    # Original param() block (lines 2-11)
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
) -> None:
    # Line 12: . "$PSScriptRoot/Config.ps1"  -> load_config() first
    config.load_config()

    # Read env-derived values used in this script
    group_id = config.CFG.GroupId
    cluster_id = config.CFG.ClusterId
    ssh_user = config.CFG.SshUser

    # Lines 45-47: $ErrorActionPreference = 'Stop' (twice; Python raises by default)

    # Lines 49-52: paths + ensure $Root exists
    home = Path(os.path.expanduser("~"))
    root = home / "mongo-oplog-stream" / snapshot_tag
    stop_file = root / ".stop"
    state_file = root / "state.json"
    root.mkdir(parents=True, exist_ok=True)

    # Lines 54-57: log dir + Start-Transcript (Python logging to console + file)
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
        # (Get-Date).ToUniversalTime().ToString('o')
        return datetime.now(timezone.utc).isoformat()

    # Line 59: try {  ... line 272: } finally { Stop-Transcript }
    try:

        # region --- Pre-flight --- (lines 61-123)

        # Line 63
        config.write_host(
            f"\n=== Oplog Tailer (OM Oplog Snapshot API) for {snapshot_tag} ===",
            fg=config.YELLOW,
        )

        # Lines 65-82: verify brs.thirdparty.baseOplogFilePath
        config.write_host("  Checking OM oplog path configuration...", fg=config.CYAN)
        try:
            settings = config.invoke_om_api(path="group/settings")
            oplog_base_path = None
            if isinstance(settings, dict):
                inner = settings.get("settings")
                if isinstance(inner, dict):
                    oplog_base_path = inner.get("brs.thirdparty.baseOplogFilePath")
            if not oplog_base_path:
                # Lines 71-74
                raise RuntimeError(
                    "brs.thirdparty.baseOplogFilePath is not configured in Ops Manager. "
                    "Navigate to Admin -> General -> Ops Manager Config and set this key to the oplog directory path on each agent node."
                )
            config.write_host(f"  OM oplog base path: {oplog_base_path}", fg=config.GREEN)
        except Exception as e:  # line 76: catch
            # Line 77: if ($_.ToString() -match 'USER_UNAUTHORIZED|401')
            if re.search(r"USER_UNAUTHORIZED|401", str(e)):
                # Line 78: Write-Warning
                logger.warning(
                    "WARNING: Cannot read OM oplog path config (requires admin privileges). "
                    "Proceeding - ensure brs.thirdparty.baseOplogFilePath is set in the OM Admin UI."
                )
            else:
                # Line 80: throw
                raise

        # Lines 84-97: select one tailing node per RS
        cluster_detail = config.invoke_om_api(
            path=f"group/{group_id}/clusters/{cluster_id}"
        )
        tailing_node_ids: list[str] = []
        for rs in (cluster_detail.get("replicaSets") or []):
            # Priority: hidden secondary > secondary > primary
            candidates = [n for n in (rs.get("nodes") or []) if n.get("snapshotable") is True]
            chosen = next(
                (n for n in candidates
                 if n.get("memberState") == "SECONDARY" and n.get("hidden") is True),
                None,
            )
            if not chosen:
                chosen = next(
                    (n for n in candidates if n.get("memberState") == "SECONDARY"),
                    None,
                )
            if not chosen:
                chosen = candidates[0] if candidates else None  # fall back to primary
            if not chosen:
                raise RuntimeError(
                    f"No snapshotable node found for replica set {rs.get('id')} to use as oplog tailing node."
                )
            tailing_node_ids.append(chosen.get("id"))
            config.write_host(
                f"  {rs.get('id')} -> tailing on {chosen.get('id')} [{chosen.get('memberState')}]",
                fg=config.CYAN,
            )

        # Lines 99-103: register preferred oplog nodes (idempotent)
        config.invoke_om_api(
            method="POST",
            path=f"group/{group_id}/clusters/{cluster_id}/preferredOplogNodes",
            body={"nodeIds": list(tailing_node_ids)},
        )
        config.write_host(
            f"  Preferred oplog nodes set: {', '.join(tailing_node_ids)}",
            fg=config.GREEN,
        )

        # Lines 105-107: load existing state
        if state_file.exists():
            state = json.loads(state_file.read_text())
        else:
            state = None
        job_seq = int(state["totalJobs"]) if state else 0

        # Lines 109-116: write .started marker
        started = OrderedDict()
        started["snapshotTag"] = snapshot_tag
        started["pid"] = os.getpid()
        started["startedUtc"] = _utc_iso()
        started["intervalSec"] = interval_sec
        started["tailingNodes"] = list(tailing_node_ids)
        (root / ".started").write_text(json.dumps(started, indent=2))

        # Lines 118-121
        config.write_host(f"  Root     : {root}", fg=config.CYAN)
        config.write_host(
            f"  Interval : {interval_sec}s  timeout: {timeout_minutes}m  poll: {poll_interval_sec}s",
            fg=config.CYAN,
        )
        config.write_host(
            f"  Stop with: pwsh ./pitr/Stop-OplogTailer.ps1 -SnapshotTag '{snapshot_tag}'",
            fg=config.CYAN,
        )
        config.write_host("", fg=None)

        # endregion

        # Line 125: while (-not (Test-Path $StopFile))
        while not stop_file.exists():
            iter_start = datetime.now()  # line 126
            job_completed = False        # line 127
            oplog_snapshot_id = None     # line 128

            try:  # line 130
                # region --- Create and start oplog snapshot job --- (lines 131-140)

                # Lines 133-135
                create_resp = config.invoke_om_api(
                    method="POST",
                    path=f"group/{group_id}/clusters/{cluster_id}/oplogSnapshot",
                    body={"timeoutMinutes": timeout_minutes},
                )
                oplog_snapshot_id = create_resp.get("oplogSnapshotId")
                # Line 136
                config.write_host(
                    f"  [{_now_hms()}]  Job {job_seq + 1}: created  id={oplog_snapshot_id}",
                    fg=config.CYAN,
                )

                # Line 138
                config.invoke_om_api(
                    method="POST",
                    path=f"group/{group_id}/clusters/{cluster_id}/oplogSnapshot/{oplog_snapshot_id}/start",
                )

                # endregion

                # region --- Poll until READY --- (lines 142-156)

                # Lines 144-146
                deadline = datetime.now().timestamp() + timeout_minutes * 60
                snap_state = ""
                ready_resp = None
                # Line 147: while ($SnapState -ne 'READY')
                while snap_state != "READY":
                    if datetime.now().timestamp() > deadline:  # line 148
                        raise RuntimeError(
                            f"Oplog snapshot {oplog_snapshot_id} timed out waiting for READY."
                        )
                    if snap_state in ("FAILED", "FAILING"):  # line 149
                        raise RuntimeError(
                            f"Oplog snapshot {oplog_snapshot_id} entered {snap_state} state."
                        )
                    time.sleep(poll_interval_sec)  # line 150
                    # Line 151
                    ready_resp = config.invoke_om_api_with_retry(
                        path=f"group/{group_id}/clusters/{cluster_id}/oplogSnapshot/{oplog_snapshot_id}"
                    )
                    snap_state = ready_resp.get("state")  # line 152
                    config.write_host(
                        f"  [{_now_hms()}]  state = {snap_state}", fg=config.DARK_GRAY
                    )  # line 153

                # endregion

                # region --- Gap detection and file copy --- (lines 158-215)

                job_seq += 1          # line 160
                new_last_end = None   # line 161

                # Line 163: foreach ($Range in $ReadyResp.ranges)
                for range_ in (ready_resp.get("ranges") or []):
                    range_end = range_.get("end")            # line 164
                    range_prev_end = range_.get("previousEnd")  # line 165

                    # Lines 167-186: gap check
                    if state and state.get("lastEnd") and range_prev_end:
                        stored_t = int(state["lastEnd"]["time"])
                        stored_i = int(state["lastEnd"]["inc"])
                        prev_t = int(range_prev_end["time"])
                        prev_i = int(range_prev_end["inc"])
                        if stored_t != prev_t or stored_i != prev_i:
                            # Lines 176-177
                            gap_msg = (
                                f"Oplog gap: stored lastEnd=({stored_t}:{stored_i}) but "
                                f"OM previousEnd=({prev_t}:{prev_i}). PIT in this window is unrecoverable."
                            )
                            config.write_host(
                                f"  [{_now_hms()}]  WARNING: {gap_msg}", fg=config.RED
                            )
                            # Lines 178-183: write gap marker
                            gap = OrderedDict()
                            gap["detectedUtc"] = _utc_iso()
                            gap["storedLastEnd"] = state["lastEnd"]
                            gap["omPreviousEnd"] = range_prev_end
                            gap["jobId"] = oplog_snapshot_id
                            gap_name = "gap-{0}.json".format(
                                datetime.now().strftime("%Y%m%d-%H%M%S")
                            )
                            (root / gap_name).write_text(json.dumps(gap, indent=2))
                            if abort_on_gap:  # line 184
                                raise RuntimeError(
                                    "Aborting due to oplog gap (-AbortOnGap set)."
                                )

                    # Lines 188-191: track latest end timestamp
                    if (not new_last_end) or int(range_end["time"]) > int(new_last_end["time"]):
                        new_last_end = range_end

                    # Lines 193-212: copy each .oplogs file
                    for range_node in (range_.get("nodes") or []):
                        node_host = range_node.get("id").split(":")[0]  # line 197
                        rs_id = range_node.get("rsId")                  # line 198
                        seg_dir = root / rs_id / "segments"             # line 199
                        seg_dir.mkdir(parents=True, exist_ok=True)      # line 200

                        # Line 202: foreach ($RemoteFile in $RangeNode.logFiles)
                        log_files = range_node.get("logFiles") or []
                        for remote_file in log_files:
                            local_name = os.path.basename(remote_file)  # line 203
                            local_path = seg_dir / local_name           # line 204
                            config.write_host(
                                f"  [{_now_hms()}]  scp {node_host}:{remote_file} -> {rs_id}/{local_name}",
                                fg=config.CYAN,
                            )  # line 205
                            # Line 206: scp @SshOpts "user@host:remote" local 2>&1
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
                            if proc.returncode != 0:  # line 207
                                raise RuntimeError(
                                    f"scp failed for {remote_file} from {node_host} (exit {proc.returncode})"
                                )
                        # Lines 209-211
                        config.write_host(
                            f"  [{_now_hms()}]  {rs_id}: {len(log_files)} file(s)  "
                            f"end={int(range_end['time'])}:{int(range_end['inc'])}",
                            fg=config.GREEN,
                        )

                # endregion

                # region --- Finish and poll FINISHED --- (lines 217-231)

                # Line 219
                config.invoke_om_api_with_retry(
                    method="POST",
                    path=f"group/{group_id}/clusters/{cluster_id}/oplogSnapshot/{oplog_snapshot_id}/finish",
                )
                # Lines 220-221
                deadline = datetime.now().timestamp() + timeout_minutes * 60
                snap_state = ""
                # Line 222: while ($SnapState -ne 'FINISHED')
                while snap_state != "FINISHED":
                    if datetime.now().timestamp() > deadline:  # line 223
                        raise RuntimeError(
                            f"Oplog snapshot {oplog_snapshot_id} timed out waiting for FINISHED."
                        )
                    if snap_state in ("FAILED", "FAILING"):  # line 224
                        raise RuntimeError(
                            f"Oplog snapshot {oplog_snapshot_id} entered {snap_state} after finish."
                        )
                    time.sleep(poll_interval_sec)  # line 225
                    # Line 226
                    resp = config.invoke_om_api_with_retry(
                        path=f"group/{group_id}/clusters/{cluster_id}/oplogSnapshot/{oplog_snapshot_id}"
                    )
                    snap_state = resp.get("state")  # line 227
                config.write_host(
                    f"  [{_now_hms()}]  Job {job_seq}: FINISHED", fg=config.GREEN
                )  # line 229

                # endregion

                job_completed = True  # line 233

                # Lines 236-243: persist state
                new_state = OrderedDict()
                new_state["snapshotTag"] = snapshot_tag
                new_state["totalJobs"] = job_seq
                new_state["lastJobId"] = oplog_snapshot_id
                new_state["lastEnd"] = new_last_end
                new_state["updatedUtc"] = _utc_iso()
                state_json = json.dumps(new_state, indent=2)
                state_file.write_text(state_json)
                # Line 244: $State = $NewState | ConvertTo-Json | ConvertFrom-Json
                state = json.loads(state_json)

            except Exception as e:  # line 246: catch
                # Line 247
                config.write_host(
                    f"  [{_now_hms()}]  ERROR: {e}", fg=config.RED
                )
                # Line 248
                if oplog_snapshot_id and not job_completed:
                    config.write_host(
                        f"  [{_now_hms()}]  Calling /fail on {oplog_snapshot_id} ...",
                        fg=config.YELLOW,
                    )  # line 249
                    try:  # line 250
                        config.invoke_om_api_with_retry(
                            method="POST",
                            path=f"group/{group_id}/clusters/{cluster_id}/oplogSnapshot/{oplog_snapshot_id}/fail",
                        )
                    except Exception as e2:  # line 252
                        config.write_host(
                            f"  [{_now_hms()}]  /fail also failed: {e2}", fg=config.RED
                        )

            # Lines 258-260: sleep remainder of interval
            elapsed = (datetime.now() - iter_start).total_seconds()
            sleep = max(0, interval_sec - elapsed)
            if sleep > 0 and not stop_file.exists():
                time.sleep(sleep)

        # Lines 263-270: stop sentinel detected
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

    finally:  # lines 272-274
        # Stop-Transcript equivalent: flush/close logging handlers
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
