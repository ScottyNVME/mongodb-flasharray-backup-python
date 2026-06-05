#!/usr/bin/env python3
"""Stop the continuous oplog tailer.

Writes a .stop sentinel file that the tailer checks at the top of each iteration. Waits up
to --wait-sec for the tailer to write its .stopped marker (which contains per-shard final
lastTs and segment counts). Captures a T2-mark count file via mongos for the replay range
assertion. Prints a summary suitable for handing off to the replay step.

Idempotent: if no tailer is running for this tag, exits with a notice and a non-fatal exit
code. The companion start command removes .stop, .started and .stopped
atomically on its next start, so re-stopping a stale tag is safe.
"""

import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import typer

from .. import config


# Enforce the ^om-\d{8}-\d{6}$ pattern on the snapshot tag.
_SNAPSHOT_TAG_RE = re.compile(r"^om-\d{8}-\d{6}$")


def _validate_snapshot_tag(value: str) -> str:
    if value is None or not _SNAPSHOT_TAG_RE.match(value):
        raise typer.BadParameter(
            "The argument does not match the \"^om-\\d{8}-\\d{6}$\" pattern."
        )
    return value


def _proc_is_running(pid: int) -> bool:
    # Return True if a process with this pid exists, False otherwise.
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
    # Load config first so running without .env throws.
    cfg = config.load_config()

    home = Path(os.path.expanduser("~"))
    root = home / "mongo-oplog-stream" / snapshot_tag
    started_file = root / ".started"
    stop_file = root / ".stop"
    stopped_file = root / ".stopped"
    state_file = root / "state.json"

    # Nothing to stop if the state directory does not exist.
    if not root.exists():
        config.write_host(
            f"  No tailer state directory found at {root} - nothing to stop.",
            fg=config.YELLOW,
        )
        return

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

    # Touch the stop sentinel. Overwriting (rather than failing if present) keeps re-stopping
    # idempotent.
    stop_file.parent.mkdir(parents=True, exist_ok=True)
    stop_file.write_text("")
    config.write_host(f"  Stop sentinel written: {stop_file}", fg=config.GREEN)

    # Wait for the tailer to acknowledge by writing .stopped. We wait up to wait_sec, polling
    # at 1s intervals.
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
        time.sleep(1.0)

    if acked:
        config.write_host("  Tailer acknowledged stop.", fg=config.GREEN)
    else:
        config.write_host(
            f"  WARNING: timeout after {wait_sec}s waiting for .stopped marker.",
            fg=config.YELLOW,
        )

    # Summarize per-shard final state by reading state.json directly (authoritative).
    global_state = None
    if state_file.exists():
        global_state = json.loads(state_file.read_text())

    config.write_host("\n  Per-RS segment summary:", fg=config.CYAN)
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
            try:
                segments = [
                    p for p in seg_dir.iterdir() if p.is_file() and p.name.endswith(".oplogs")
                ]
            except OSError:
                segments = []
        b = 0
        for seg in segments:
            b += seg.stat().st_size
        if not b:
            b = 0
        total_segments += len(segments)
        total_bytes += b
        config.write_host(
            f"    {sh.name:<16}  segments={len(segments):<4}  bytes={b}",
            fg=None,
        )

    now = datetime.now().astimezone()
    if global_state and global_state.get("lastEnd"):
        # Keep last_end_utc tz-aware as UTC; comparing against a naive/local value would
        # inflate the reported lag.
        last_end = global_state.get("lastEnd")
        last_end_utc = datetime.fromtimestamp(int(last_end.get("time")), tz=timezone.utc)
        # ISO-8601 round-trip format
        last_end_iso = last_end_utc.strftime("%Y-%m-%dT%H:%M:%S.%f0Z")
        lag_sec = round(
            (now.astimezone(timezone.utc) - last_end_utc).total_seconds(), 1
        )
        config.write_host("", fg=None)
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
                # The count output must be a bare integer; anything else is unparseable.
                if not re.search(r"^\d+$", raw if raw is not None else "", re.MULTILINE):
                    raise RuntimeError(
                        f"Unparseable count for {baseline_database}.{coll}: '{raw}'"
                    )
                db_counts[coll] = int(raw.strip())
                config.write_host(
                    f"    {baseline_database}.{coll} = {db_counts[coll]}",
                    fg=config.GREEN,
                )
            t2 = {
                "snapshotTag": snapshot_tag,
                "capturedUtc": datetime.now(timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%S.%f0Z"
                ),
                "counts": {baseline_database: db_counts},
            }
            t2_path = root / "t2-mark.json"
            t2_path.write_text(json.dumps(t2, indent=2))
            config.write_host(f"  T2 mark written: {t2_path}", fg=config.GREEN)
        except Exception as e:  # noqa: BLE001 - T2 mark capture is non-fatal
            config.write_host(
                f"  WARNING: T2 mark capture failed - replay will skip the range check "
                f"unless you write t2-mark.json manually. ({e})",
                fg=config.YELLOW,
            )

    config.write_host("", fg=None)
    config.write_host(
        f"  Replay with: invoke-oplog-replay --snapshot-tag '{snapshot_tag}'",
        fg=config.CYAN,
    )


def main():
    typer.run(_run)


if __name__ == "__main__":
    main()
