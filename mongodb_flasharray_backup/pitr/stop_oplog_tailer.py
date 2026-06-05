#!/usr/bin/env python3
"""Stop-OplogTailer.ps1 -> stop_oplog_tailer.py (faithful 1:1 translation).

Stop the continuous oplog tailer started by Start-OplogTailer.ps1.

Writes a .stop sentinel file that the tailer checks at the top of each iteration. Waits up
to $WaitSec for the tailer to write its .stopped marker (which contains per-shard final
lastTs and segment counts). Captures a T2-mark count file via mongos for the replay range
assertion. Prints a summary suitable for handing off to Invoke-OplogReplay.ps1.

Idempotent: if no tailer is running for this tag, exits with a notice and a non-fatal exit
code. The companion Start-OplogTailer.ps1 script removes .stop, .started and .stopped
atomically on its next start, so re-stopping a stale tag is safe.
"""

# Maps original: #Requires -Version 7 / param(...) / . "$PSScriptRoot/Config.ps1"
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import typer

from .. import config


# Maps original param block: [ValidatePattern('^om-\d{8}-\d{6}$')]$SnapshotTag
_SNAPSHOT_TAG_RE = re.compile(r"^om-\d{8}-\d{6}$")


def _validate_snapshot_tag(value: str) -> str:
    # [ValidatePattern('^om-\d{8}-\d{6}$')] enforcement
    if value is None or not _SNAPSHOT_TAG_RE.match(value):
        raise typer.BadParameter(
            "The argument does not match the \"^om-\\d{8}-\\d{6}$\" pattern."
        )
    return value


def _proc_is_running(pid: int) -> bool:
    # Maps: Get-Process -Id <pid> -ErrorAction SilentlyContinue (return $null if not running)
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, ValueError):
        return False
    except PermissionError:
        # Process exists but owned by another user.
        return True


def _run(
    snapshot_tag: str = typer.Option(
        ...,
        "--snapshot-tag",
        callback=_validate_snapshot_tag,
        help="Snapshot tag, e.g. om-YYYYMMDD-HHMMSS.",
    ),
    # max seconds to wait for the tailer to write its .stopped marker
    wait_sec: int = typer.Option(60, "--wait-sec"),
    # T2 mark capture - upper-bound for the post-replay range assertion. Captured AFTER the
    # tailer has acknowledged stop, so it reflects the cluster state at-or-after the tailer's
    # final segment. The replay assertion is `T1.preSnap <= postReplay <= T2_mark`.
    baseline_database: str = typer.Option("testdb", "--baseline-database"),
    baseline_collections: List[str] = typer.Option(
        ["loadtest", "payload"], "--baseline-collections"
    ),
) -> None:
    # Equivalent of dot-sourcing Config.ps1: load config first so running without .env throws.
    cfg = config.load_config()

    # $ErrorActionPreference = 'Stop'  -> Python exceptions propagate by default.

    # $Root / $StartedFile / $StopFile / $StoppedFile / $StateFile
    home = Path(os.path.expanduser("~"))
    root = home / "mongo-oplog-stream" / snapshot_tag
    started_file = root / ".started"
    stop_file = root / ".stop"
    stopped_file = root / ".stopped"
    state_file = root / "state.json"

    # if (-not (Test-Path $Root)) { ... return }
    if not root.exists():
        config.write_host(
            f"  No tailer state directory found at {root} - nothing to stop.",
            fg=config.YELLOW,
        )
        return

    # Write-Host "`n=== Stopping Oplog Tailer for snapshot $SnapshotTag ===" -ForegroundColor Yellow
    config.write_host(
        f"\n=== Stopping Oplog Tailer for snapshot {snapshot_tag} ===",
        fg=config.YELLOW,
    )

    # Read .started for diagnostics (pid, intervalSec). Tailer may have already exited - that
    # is not an error; we still touch .stop and report whatever state file exists.
    started = None
    if started_file.exists():
        started = json.loads(started_file.read_text())
        config.write_host(
            f"  Tailer pid={started.get('pid')}  "
            f"startedUtc={started.get('startedUtc')}  "
            f"intervalSec={started.get('intervalSec')}",
            fg=config.CYAN,
        )
    else:
        config.write_host(
            "  No .started marker found - tailer may have already exited.",
            fg=config.DARK_YELLOW,
        )

    # Touch the stop sentinel. Use New-Item with -Force so re-stopping is idempotent.
    # $null = New-Item -ItemType File -Path $StopFile -Force
    stop_file.parent.mkdir(parents=True, exist_ok=True)
    stop_file.write_text("")
    config.write_host(f"  Stop sentinel written: {stop_file}", fg=config.GREEN)

    # Wait for the tailer to acknowledge by writing .stopped. We wait up to $WaitSec, polling
    # at 1s intervals.
    # $Deadline = (Get-Date).AddSeconds($WaitSec)
    deadline = time.monotonic() + wait_sec
    acked = False
    while time.monotonic() < deadline:
        if stopped_file.exists():
            acked = True
            break
        if started and started.get("pid"):
            # If the tailer process has died without writing .stopped (e.g. crashed mid-iteration)
            # there is no point waiting further.
            proc_running = _proc_is_running(int(started.get("pid")))
            if not proc_running:
                config.write_host(
                    f"  WARNING: tailer pid={started.get('pid')} is no longer running "
                    f"but did not write .stopped.",
                    fg=config.YELLOW,
                )
                break
        # Start-Sleep -Milliseconds 1000
        time.sleep(1.0)

    if acked:
        config.write_host("  Tailer acknowledged stop.", fg=config.GREEN)
    else:
        config.write_host(
            f"  WARNING: timeout after {wait_sec}s waiting for .stopped marker.",
            fg=config.YELLOW,
        )

    # Summarize per-shard final state by reading state.json directly (authoritative).
    # Read global state.json written by Start-OplogTailer.ps1 (OM API approach).
    global_state = None
    if state_file.exists():
        global_state = json.loads(state_file.read_text())

    # Write-Host "`n  Per-RS segment summary:" -ForegroundColor Cyan
    config.write_host("\n  Per-RS segment summary:", fg=config.CYAN)
    # $ShardDirs = @(Get-ChildItem -Path $Root -Directory ...)
    try:
        shard_dirs = sorted(
            [p for p in root.iterdir() if p.is_dir()], key=lambda p: p.name
        )
    except OSError:
        shard_dirs = []
    total_segments = 0
    total_bytes = 0
    for sh in shard_dirs:
        seg_dir = sh / "segments"
        segments = []
        if seg_dir.exists():
            # Get-ChildItem -Filter '*.oplogs' -File
            try:
                segments = [
                    p for p in seg_dir.iterdir() if p.is_file() and p.name.endswith(".oplogs")
                ]
            except OSError:
                segments = []
        # $Bytes = ($Segments | Measure-Object -Property Length -Sum).Sum
        b = 0
        for seg in segments:
            b += seg.stat().st_size
        # if (-not $Bytes) { $Bytes = 0L }
        if not b:
            b = 0
        total_segments += len(segments)
        total_bytes += b
        # "    {0,-16}  segments={1,-4}  bytes={2}"
        config.write_host(
            f"    {sh.name:<16}  segments={len(segments):<4}  bytes={b}",
            fg=None,
        )

    # $Now = Get-Date
    now = datetime.now().astimezone()
    if global_state and global_state.get("lastEnd"):
        # Keep $LastEndUtc as Kind=Utc; casting through string produces Kind=Local which inflates lag.
        last_end = global_state.get("lastEnd")
        last_end_utc = datetime.fromtimestamp(int(last_end.get("time")), tz=timezone.utc)
        # $LastEndIso = $LastEndUtc.ToString('o')  -> ISO-8601 round-trip format
        last_end_iso = last_end_utc.strftime("%Y-%m-%dT%H:%M:%S.%f0Z")
        # $LagSec = [math]::Round((Now.ToUniversalTime() - LastEndUtc).TotalSeconds, 1)
        lag_sec = round(
            (now.astimezone(timezone.utc) - last_end_utc).total_seconds(), 1
        )
        config.write_host("", fg=None)
        # "  Last oplog end : {0}:{1}  ({2}, lagSec={3})"
        config.write_host(
            f"  Last oplog end : {int(last_end.get('time'))}:{int(last_end.get('inc'))}  "
            f"({last_end_iso}, lagSec={lag_sec})",
            fg=None,
        )
        config.write_host(
            f"  Total OM jobs  : {global_state.get('totalJobs')}", fg=None
        )

    config.write_host("", fg=None)
    config.write_host(f"  Total segments : {total_segments}", fg=None)
    config.write_host(f"  Total bytes    : {total_bytes}", fg=None)
    config.write_host(f"  Stream root    : {root}", fg=None)

    # Capture T2 mark for the replay range-bound assertion. This must run AFTER the tailer has
    # acknowledged stop so that any oplog entries the tailer was racing to capture are already
    # in segments. Failure here is non-fatal.
    if baseline_database and baseline_collections and len(baseline_collections) > 0:
        config.write_host(
            f"\n  Capturing T2 mark via mongos {cfg.MongosHost}:{cfg.MongosPort} ...",
            fg=config.CYAN,
        )
        try:
            db_counts = {}
            for coll in baseline_collections:
                # $Eval = "db.getSiblingDB(`"$BaselineDatabase`").$Coll.countDocuments()"
                eval_str = (
                    f'db.getSiblingDB("{baseline_database}").{coll}.countDocuments()'
                )
                raw = config.invoke_mongosh_js(
                    ssh_target=cfg.MongosHost,
                    uri=f"mongodb://{cfg.MongosHost}:{cfg.MongosPort}",
                    js=eval_str,
                    max_attempts=5,
                    context=f"T2-mark {baseline_database}.{coll}",
                )
                # if ($Raw -notmatch '^\d+$') { throw ... }
                if not re.search(r"^\d+$", raw if raw is not None else "", re.MULTILINE):
                    raise RuntimeError(
                        f"Unparseable count for {baseline_database}.{coll}: '{raw}'"
                    )
                db_counts[coll] = int(raw.strip())
                config.write_host(
                    f"    {baseline_database}.{coll} = {db_counts[coll]}",
                    fg=config.GREEN,
                )
            # $T2 = [ordered]@{ snapshotTag; capturedUtc; counts = @{ db = $DbCounts } }
            t2 = {
                "snapshotTag": snapshot_tag,
                "capturedUtc": datetime.now(timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%S.%f0Z"
                ),
                "counts": {baseline_database: db_counts},
            }
            t2_path = root / "t2-mark.json"
            # $T2 | ConvertTo-Json -Depth 5 | Set-Content $T2Path
            t2_path.write_text(json.dumps(t2, indent=2))
            config.write_host(f"  T2 mark written: {t2_path}", fg=config.GREEN)
        except Exception as e:  # noqa: BLE001 - mirrors PS catch { ... } non-fatal
            config.write_host(
                f"  WARNING: T2 mark capture failed - replay will skip the range check "
                f"unless you write t2-mark.json manually. ({e})",
                fg=config.YELLOW,
            )

    config.write_host("", fg=None)
    config.write_host(
        f"  Replay with: pwsh ./pitr/Invoke-OplogReplay.ps1 -SnapshotTag '{snapshot_tag}'",
        fg=config.CYAN,
    )


def main():
    typer.run(_run)


if __name__ == "__main__":
    main()
