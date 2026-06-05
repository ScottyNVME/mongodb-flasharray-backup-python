#!/usr/bin/env python3
"""Remove-OldArtifacts - Delete stale MongoDB backup artifacts older than N days.

1:1 faithful Python translation of reference/Remove-OldArtifacts.ps1.

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

# Maps PS lines 1-9: param() block + dot-sourcing Config.ps1.
import os
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import typer

from . import config


# Maps PS line 4: [ValidateRange(1, 365)] on $OlderThanDays.
def _validate_older_than_days(value: int) -> int:
    if value < 1 or value > 365:
        raise typer.BadParameter("OlderThanDays must be between 1 and 365.")
    return value


# Maps PS lines 2-143: the whole script body (param block + #Requires + main logic).
def _run(
    # PS line 3-5: [Parameter(Mandatory)] [ValidateRange(1,365)] [int]$OlderThanDays
    older_than_days: int = typer.Option(
        ...,
        "--older-than-days",
        callback=_validate_older_than_days,
        help="Delete artifacts older than this many days",
    ),
    # PS line 7: [switch]$WhatIf
    what_if: bool = typer.Option(
        False,
        "--what-if",
        help="Show what would be deleted without making changes",
    ),
):
    # PS line 9: . "$PSScriptRoot/Config.ps1" -> load config FIRST (throws like the original without .env).
    config.load_config()

    # PS line 25: Import-Module PureStoragePowerShellSDK2 -ErrorAction Stop
    from pypureclient import flasharray  # noqa: F401

    # PS line 27: $ErrorActionPreference = 'Stop'  (Python raises by default; no-op marker).

    # PS line 29: $Cutoff = (Get-Date).AddDays(-$OlderThanDays)
    cutoff = datetime.now() - timedelta(days=older_than_days)

    # PS lines 31-33: WhatIf banner.
    if what_if:
        config.write_host(
            "\n[WhatIf] No changes will be made. Showing what would be deleted.",
            fg=config.DARK_YELLOW,
        )

    # PS lines 35-37: header.
    config.write_host("\n=== Remove-OldArtifacts ===", fg=config.YELLOW)
    config.write_host(
        f"  Cutoff date       : {cutoff.strftime('%Y-%m-%d %H:%M:%S')}", fg=config.CYAN
    )
    config.write_host(f"  Older than        : {older_than_days} days", fg=config.CYAN)

    # region --- FlashArray PG snapshots --- (PS lines 39-86)

    # PS line 41
    config.write_host("\n--- FlashArray PG snapshots ---", fg=config.YELLOW)

    # PS line 43: $FA = Connect-Pfa2Array -EndPoint $FaEndpoint -Credential $FaCred -IgnoreCertificateError
    fa = config.connect_fa()
    # PS line 44
    config.write_host(
        f"  Connected to gateway: {config.CFG.FaEndpoint}", fg=config.GREEN
    )

    # PS line 47: Discover context names from fleet (arrays that have the PG).
    fa_context_names = config.resolve_fa_context_names(
        fa, config.CFG.ProtectionGroupName
    )

    # PS lines 49-57: per-context iteration; counters and seen-tag set.
    fa_object_count = 0  # total physical snapshot objects deleted (N arrays x M tags)
    fa_logical_count = 0  # unique snapshot tags deleted (logical backups)
    fa_seen_tags = set()
    pg_name = config.CFG.ProtectionGroupName

    # PS line 58: foreach ($CtxName in $FaContextNames)
    for ctx_name in fa_context_names:
        # PS lines 59-60: Get-Pfa2ProtectionGroupSnapshot ... -Filter ... -ErrorAction SilentlyContinue
        ctx_snaps = config._fa(
            fa.get_protection_group_snapshots(
                context_names=[ctx_name],
                filter=f"source.name='{pg_name}'",
            ),
            allow_error=True,
        )
        # PS line 61: foreach ($Snap in $CtxSnaps)
        for snap in ctx_snaps:
            # PS line 63: $Suffix = $Snap.Name -replace "^<pg>\.", ''
            suffix = re.sub(r"^" + re.escape(pg_name) + r"\.", "", snap.name)
            # PS line 64: if ($Suffix -notmatch '^om-(\d{8})-(\d{6})$') { continue }
            m = re.search(r"^om-(\d{8})-(\d{6})$", suffix)
            if not m:
                continue
            # PS line 65: $SnapDate = [datetime]::ParseExact("$($Matches[1])$($Matches[2])", 'yyyyMMddHHmmss', $null)
            snap_date = datetime.strptime(f"{m.group(1)}{m.group(2)}", "%Y%m%d%H%M%S")
            # PS line 66: if ($SnapDate -ge $Cutoff) { continue }
            if snap_date >= cutoff:
                continue

            # PS lines 68-77: WhatIf branch vs actual delete.
            if what_if:
                config.write_host(
                    f"  [WhatIf] Would delete FA snapshot: {snap.name} on {ctx_name}",
                    fg=config.DARK_YELLOW,
                )
            else:
                try:
                    # PS line 72: Remove-Pfa2ProtectionGroupSnapshot -ContextName @($CtxName) -Name $Snap.Name -ErrorAction Stop
                    config._fa(
                        fa.delete_protection_group_snapshots(
                            names=[snap.name], context_names=[ctx_name]
                        ),
                        allow_error=False,
                    )
                    # PS line 73
                    config.write_host(
                        f"  Deleted FA snapshot: {snap.name} on {ctx_name}",
                        fg=config.GREEN,
                    )
                except Exception as ex:  # noqa: BLE001 - PS catch { }
                    # PS line 75
                    config.write_host(
                        f"  ERROR deleting {snap.name} on {ctx_name}: {ex}",
                        fg=config.RED,
                    )
            # PS line 78: $FaObjectCount++
            fa_object_count += 1
            # PS line 79: $null = $FaSeenTags.Add($Suffix)
            fa_seen_tags.add(suffix)

    # PS line 82: $FaLogicalCount = $FaSeenTags.Count
    fa_logical_count = len(fa_seen_tags)
    # PS lines 83-84
    config.write_host(
        f"  FA snapshot objects removed : {fa_object_count} (across {len(fa_context_names)} arrays)",
        fg=config.CYAN,
    )
    config.write_host(
        f"  Logical backups expired     : {fa_logical_count}", fg=config.CYAN
    )

    # endregion

    # region --- Local oplog stream directories --- (PS lines 88-109)

    # PS line 90
    config.write_host("\n--- Local oplog stream directories ---", fg=config.YELLOW)

    # PS line 92: $OplogDir = Join-Path $HOME 'mongo-oplog-stream'
    home = Path.home()
    oplog_dir = home / "mongo-oplog-stream"
    # PS line 93: if (Test-Path $OplogDir)
    if oplog_dir.exists():
        # PS lines 94-95: Get-ChildItem -Directory | Where LastWriteTime -lt Cutoff
        oplog_dirs = [
            d
            for d in sorted(oplog_dir.iterdir())
            if d.is_dir()
            and datetime.fromtimestamp(d.stat().st_mtime) < cutoff
        ]
        # PS line 96: foreach ($Dir in $OplogDirs)
        for d in oplog_dirs:
            # PS lines 97-102
            if what_if:
                config.write_host(
                    f"  [WhatIf] Would delete: {d}", fg=config.DARK_YELLOW
                )
            else:
                # PS line 100: Remove-Item -Recurse -Force
                shutil.rmtree(d)
                # PS line 101
                config.write_host(f"  Deleted: {d}", fg=config.GREEN)
        # PS line 104
        config.write_host(
            f"  Oplog stream dirs to remove: {len(oplog_dirs)}", fg=config.CYAN
        )
    else:
        # PS line 106
        config.write_host(f"  {oplog_dir} not found - skipping", fg=config.DARK_GRAY)

    # endregion

    # region --- Local log files --- (PS lines 111-138)

    # PS line 113
    config.write_host("\n--- Local log files ---", fg=config.YELLOW)

    # PS lines 115-120: $LogDirs array.
    log_dirs = [
        home / "mongo-snapshot-logs",
        home / "mongo-restore-logs",
        home / "mongo-oplogtailer-logs",
        home / "mongo-oplogreplay-logs",
    ]
    # PS line 121: foreach ($LogDir in $LogDirs)
    for log_dir in log_dirs:
        # PS lines 122-125: if (-not (Test-Path $LogDir)) { ...; continue }
        if not log_dir.exists():
            config.write_host(f"  {log_dir} not found - skipping", fg=config.DARK_GRAY)
            continue
        # PS line 126: Get-ChildItem -File | Where LastWriteTime -lt Cutoff
        old_logs = [
            f
            for f in sorted(log_dir.iterdir())
            if f.is_file()
            and datetime.fromtimestamp(f.stat().st_mtime) < cutoff
        ]
        # PS line 127: foreach ($Log in $OldLogs)
        for log in old_logs:
            # PS lines 128-133
            if what_if:
                config.write_host(
                    f"  [WhatIf] Would delete: {log}", fg=config.DARK_YELLOW
                )
            else:
                # PS line 131: Remove-Item -Force
                log.unlink()
                # PS line 132
                config.write_host(f"  Deleted: {log}", fg=config.GREEN)
        # PS line 135
        config.write_host(
            f"  Log files to remove from {log_dir}: {len(old_logs)}", fg=config.CYAN
        )

    # endregion

    # PS lines 140-143: completion footer.
    config.write_host("\n=== Cleanup Complete ===", fg=config.GREEN)
    if what_if:
        config.write_host(
            "  WhatIf mode - no changes were made.", fg=config.DARK_YELLOW
        )


def main():
    typer.run(_run)


if __name__ == "__main__":
    main()
