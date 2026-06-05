#!/usr/bin/env python3
"""Remove-OldArtifacts - Delete stale MongoDB backup artifacts older than N days.

Removes:
  1. FlashArray protection group snapshots (via tag BackupTimestamp, across all fleet arrays)
  2. Local oplog stream directories ~/mongo-oplog-stream/<tag>/
  3. Local log directories          ~/mongo-snapshot-logs/, ~/mongo-restore-logs/,
                                    ~/mongo-oplogtailer-logs/, ~/mongo-oplogreplay-logs/
                                    entries older than N days

Usage:
  python -m mongodb_flasharray_backup.remove_old_artifacts --older-than-days 30
  python -m mongodb_flasharray_backup.remove_old_artifacts --older-than-days 30 --what-if   # dry run
"""

import os
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import typer

from . import config


def _validate_older_than_days(value: int) -> int:
    if value < 1 or value > 365:
        raise typer.BadParameter("OlderThanDays must be between 1 and 365.")
    return value


def _run(
    older_than_days: int = typer.Option(
        ...,
        "--older-than-days",
        callback=_validate_older_than_days,
        help="Delete artifacts older than this many days",
    ),
    what_if: bool = typer.Option(
        False,
        "--what-if",
        help="Show what would be deleted without making changes",
    ),
):
    # Load config FIRST (throws without .env).
    config.load_config()

    cutoff = datetime.now() - timedelta(days=older_than_days)

    # WhatIf banner.
    if what_if:
        config.write_host(
            "\n[WhatIf] No changes will be made. Showing what would be deleted.",
            fg=config.DARK_YELLOW,
        )

    # Header.
    config.write_host("\n=== Remove-OldArtifacts ===", fg=config.YELLOW)
    config.write_host(
        f"  Cutoff date       : {cutoff.strftime('%Y-%m-%d %H:%M:%S')}", fg=config.CYAN
    )
    config.write_host(f"  Older than        : {older_than_days} days", fg=config.CYAN)

    # region --- FlashArray PG snapshots ---

    config.write_host("\n--- FlashArray PG snapshots ---", fg=config.YELLOW)

    fa = config.connect_fa()
    config.write_host(
        f"  Connected to gateway: {config.CFG.FaEndpoint}", fg=config.GREEN
    )

    # Discover context names from fleet (arrays that have the PG).
    fa_context_names = config.resolve_fa_context_names(
        fa, config.CFG.ProtectionGroupName
    )

    # Per-context iteration; counters and seen-tag set.
    fa_object_count = 0  # total physical snapshot objects deleted (N arrays x M tags)
    fa_logical_count = 0  # unique snapshot tags deleted (logical backups)
    fa_seen_tags = set()
    pg_name = config.CFG.ProtectionGroupName

    for ctx_name in fa_context_names:
        ctx_snaps = config._fa(
            fa.get_protection_group_snapshots(
                context_names=[ctx_name],
                filter=f"source.name='{pg_name}'",
            ),
            allow_error=True,
        )
        for snap in ctx_snaps:
            # Strip the PG name prefix to get the tag suffix.
            suffix = re.sub(r"^" + re.escape(pg_name) + r"\.", "", snap.name)
            # Only consider snapshots whose suffix is a backup timestamp tag.
            m = re.search(r"^om-(\d{8})-(\d{6})$", suffix)
            if not m:
                continue
            # Parse the timestamp from the tag.
            snap_date = datetime.strptime(f"{m.group(1)}{m.group(2)}", "%Y%m%d%H%M%S")
            # Skip snapshots newer than the cutoff.
            if snap_date >= cutoff:
                continue

            # WhatIf branch vs actual delete.
            if what_if:
                config.write_host(
                    f"  [WhatIf] Would delete FA snapshot: {snap.name} on {ctx_name}",
                    fg=config.DARK_YELLOW,
                )
            else:
                try:
                    config._fa(
                        fa.delete_protection_group_snapshots(
                            names=[snap.name], context_names=[ctx_name]
                        ),
                        allow_error=False,
                    )
                    config.write_host(
                        f"  Deleted FA snapshot: {snap.name} on {ctx_name}",
                        fg=config.GREEN,
                    )
                except Exception as ex:  # noqa: BLE001
                    config.write_host(
                        f"  ERROR deleting {snap.name} on {ctx_name}: {ex}",
                        fg=config.RED,
                    )
            fa_object_count += 1
            fa_seen_tags.add(suffix)

    fa_logical_count = len(fa_seen_tags)
    config.write_host(
        f"  FA snapshot objects removed : {fa_object_count} (across {len(fa_context_names)} arrays)",
        fg=config.CYAN,
    )
    config.write_host(
        f"  Logical backups expired     : {fa_logical_count}", fg=config.CYAN
    )

    # endregion

    # region --- Local oplog stream directories ---

    config.write_host("\n--- Local oplog stream directories ---", fg=config.YELLOW)

    home = Path.home()
    oplog_dir = home / "mongo-oplog-stream"
    if oplog_dir.exists():
        # Directories older than the cutoff.
        oplog_dirs = [
            d
            for d in sorted(oplog_dir.iterdir())
            if d.is_dir()
            and datetime.fromtimestamp(d.stat().st_mtime) < cutoff
        ]
        for d in oplog_dirs:
            if what_if:
                config.write_host(
                    f"  [WhatIf] Would delete: {d}", fg=config.DARK_YELLOW
                )
            else:
                shutil.rmtree(d)
                config.write_host(f"  Deleted: {d}", fg=config.GREEN)
        config.write_host(
            f"  Oplog stream dirs to remove: {len(oplog_dirs)}", fg=config.CYAN
        )
    else:
        config.write_host(f"  {oplog_dir} not found - skipping", fg=config.DARK_GRAY)

    # endregion

    # region --- Local log files ---

    config.write_host("\n--- Local log files ---", fg=config.YELLOW)

    log_dirs = [
        home / "mongo-snapshot-logs",
        home / "mongo-restore-logs",
        home / "mongo-oplogtailer-logs",
        home / "mongo-oplogreplay-logs",
    ]
    for log_dir in log_dirs:
        if not log_dir.exists():
            config.write_host(f"  {log_dir} not found - skipping", fg=config.DARK_GRAY)
            continue
        # Files older than the cutoff.
        old_logs = [
            f
            for f in sorted(log_dir.iterdir())
            if f.is_file()
            and datetime.fromtimestamp(f.stat().st_mtime) < cutoff
        ]
        for log in old_logs:
            if what_if:
                config.write_host(
                    f"  [WhatIf] Would delete: {log}", fg=config.DARK_YELLOW
                )
            else:
                log.unlink()
                config.write_host(f"  Deleted: {log}", fg=config.GREEN)
        config.write_host(
            f"  Log files to remove from {log_dir}: {len(old_logs)}", fg=config.CYAN
        )

    # endregion

    # Completion footer.
    config.write_host("\n=== Cleanup Complete ===", fg=config.GREEN)
    if what_if:
        config.write_host(
            "  WhatIf mode - no changes were made.", fg=config.DARK_YELLOW
        )


def main():
    typer.run(_run)


if __name__ == "__main__":
    main()
