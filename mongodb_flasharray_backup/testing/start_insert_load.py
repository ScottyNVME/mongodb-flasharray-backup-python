"""Start-InsertLoad.ps1 -> Python 1:1 translation.

Insert Load Script - Continuous background writer for snapshot/restore testing.

Inserts documents into testdb.loadtest on the cluster until stopped (Ctrl-C) or
-MaxDocs reached. Each batch is committed and logged so the caller can compare
counts before/after snapshot.

Usage:
  start_insert_load                      # run until Ctrl-C
  start_insert_load --max-docs 50000     # stop after 50,000 docs
  start_insert_load --batch-size 500     # tune batch size
"""
# Maps original: header comment block (lines 7-17) and #Requires -Version 7 (line 1)

import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone

import typer

# Original line 6: . "$PSScriptRoot/../Config.ps1"  (dot-source Config.ps1).
from .. import config


def _run(
    # Original param block lines 2-5
    # [int]$MaxDocs = [int]::MaxValue  (comment in source: "0 = unlimited")
    max_docs: int = typer.Option(
        2147483647, "--max-docs",
        help="Maximum number of documents to insert (default: int max)",
    ),
    # [int]$BatchSize = 200  # inserts per batch
    batch_size: int = typer.Option(
        200, "--batch-size",
        help="Inserts per batch",
    ),
) -> None:
    # Original line 6: . "$PSScriptRoot/../Config.ps1"  -> load config first.
    config.load_config()
    cfg = config.CFG

    # Original line 19: $Collection = 'loadtest'
    collection = "loadtest"

    # Original lines 21-25: counters / timing state
    total = 0
    batch = 0
    start_time = datetime.now()
    last_print_total = 0
    last_print_time = start_time

    # Original lines 27-28
    config.write_host(
        f"=== Insert Load - testdb.{collection} ===", fg=config.YELLOW
    )
    config.write_host(
        f"  MaxDocs={max_docs}  BatchSize={batch_size}  Press Ctrl-C to stop",
        fg=config.CYAN,
    )

    # Original lines 30-38: Ensure collection is sharded (hashed _id)
    # $SetupScript here-string (verbatim, newlines preserved).
    setup_script = (
        'var db2 = db.getSiblingDB("testdb");\n'
        "try {\n"
        '    db.getSiblingDB("admin").runCommand({enableSharding:"testdb"});\n'
        '    db.getSiblingDB("admin").runCommand({shardCollection:"testdb.loadtest", key:{_id:"hashed"}});\n'
        "} catch(e) { /* already sharded */ }\n"
        'print("ready");'
    )
    # Original line 39:
    # $null = ssh @SshOpts "${SshUser}@${MongosHost}" "$MongoshPath --quiet --eval '$SetupScript' mongodb://${MongosHost}:${MongosPort} 2>/dev/null"
    setup_remote = (
        f"{cfg.MongoshPath} --quiet --eval '{setup_script}' "
        f"mongodb://{cfg.MongosHost}:{cfg.MongosPort} 2>/dev/null"
    )
    subprocess.run(
        ["ssh", *config.SSH_OPTS, f"{cfg.SshUser}@{cfg.MongosHost}", setup_remote],
        capture_output=True,
        text=True,
    )

    # Original lines 41-80: try / catch [PipelineStoppedException] / finally
    try:
        # Original line 42: while ($Total -lt $MaxDocs)
        while total < max_docs:
            # Original line 43
            this_batch = min(batch_size, max_docs - total)
            # Original lines 44-58: $BatchScript here-string (verbatim).
            batch_script = (
                'var db2 = db.getSiblingDB("testdb");\n'
                "var docs = [];\n"
                f"var base = {total};\n"
                f"for (var i = 0; i < {this_batch}; i++) " + "{\n"
                "    docs.push({\n"
                "        seq:       base + i,\n"
                "        ts:        new Date(),\n"
                '        payload:   new Array(201).join("x"),\n'
                f"        batchNum:  {batch}\n"
                "    });\n"
                "}\n"
                "var r = db2.loadtest.insertMany(docs, {ordered:false});\n"
                "print(Object.keys(r.insertedIds).length);"
            )
            # Original line 59:
            # $Inserted = ssh @SshOpts "${SshUser}@${MongosHost}" "$MongoshPath --quiet --eval '$BatchScript' mongodb://${MongosHost}:${MongosPort} 2>/dev/null"
            batch_remote = (
                f"{cfg.MongoshPath} --quiet --eval '{batch_script}' "
                f"mongodb://{cfg.MongosHost}:{cfg.MongosPort} 2>/dev/null"
            )
            proc = subprocess.run(
                ["ssh", *config.SSH_OPTS, f"{cfg.SshUser}@{cfg.MongosHost}", batch_remote],
                capture_output=True,
                text=True,
            )
            inserted = proc.stdout or ""

            # Original line 60:
            # $n = [int]($Inserted -replace '[^0-9]', '' | Select-Object -First 1)
            # PS captures ssh output as an array of lines; -replace strips non-digits
            # per element, Select-Object -First 1 takes the first line's digits.
            lines = inserted.splitlines()
            first = lines[0] if lines else ""
            digits = re.sub(r"[^0-9]", "", first)
            n = int(digits) if digits else 0

            # Original line 61
            if n == 0:
                time.sleep(0.5)
                continue
            # Original lines 62-63
            total += n
            batch += 1
            # Original line 64
            now = datetime.now()
            # Original line 65
            rate = round(total / (now - start_time).total_seconds(), 0)
            # Original lines 66-70
            if (total - last_print_total) >= 10000 or (now - last_print_time).total_seconds() >= 30:
                # PS: ${Rate}/s  with Round(...,0) -> e.g. "1234/s" (no decimal)
                config.write_host(
                    f"  {now.strftime('%H:%M:%S')}  total={total}  batch={batch}  rate={_fmt_rate(rate)}/s",
                    fg=config.CYAN,
                )
                last_print_total = total
                last_print_time = now
    except KeyboardInterrupt:
        # Original lines 72-73: catch [System.Management.Automation.PipelineStoppedException]
        # Ctrl-C
        pass
    finally:
        # Original lines 74-79: finally block
        config.write_host(
            f"\n  Stopped. Total inserted: {total}  Batches: {batch}",
            fg=config.GREEN,
        )
        # Write final count to a status file so the snapshot/restore scripts can read it
        # Original line 77: $StatusPath = Join-Path $HOME 'mongo-loadtest-status.json'
        status_path = os.path.join(os.path.expanduser("~"), "mongo-loadtest-status.json")
        # Original line 78:
        # @{ totalInserted = $Total; stoppedAt = (Get-Date -Format 'o') } | ConvertTo-Json | Set-Content $StatusPath
        status_obj = {
            "totalInserted": total,
            "stoppedAt": _get_date_iso8601(),
        }
        with open(status_path, "w") as fh:
            fh.write(json.dumps(status_obj, indent=4))
        # Original line 79
        config.write_host(f"  Status written: {status_path}", fg=config.GREEN)


def _fmt_rate(rate: float) -> str:
    """PS Round(value, 0) yields a whole number; render without a trailing .0."""
    # [math]::Round($Total / ... , 0) interpolated as ${Rate} renders as an integer
    # (PS prints "1234", not "1234.0").
    return str(int(rate))


def _get_date_iso8601() -> str:
    """PS Get-Date -Format 'o' -> round-trip ISO 8601 with offset and 7 fractional
    digits (e.g. 2026-06-05T12:34:56.1234567+00:00)."""
    # [CLARIFICATION NEEDED: PowerShell's 'o' format emits 7-digit fractional
    # seconds and a local timezone offset; Python datetime.isoformat() emits up to
    # 6 digits. We reproduce a 7-digit, offset-bearing ISO string using the local
    # timezone for closest fidelity.]
    now = datetime.now(timezone.utc).astimezone()
    # Build 7-digit fractional seconds from microseconds (6 digits) + trailing 0.
    micro = now.microsecond
    frac7 = f"{micro:06d}0"  # 6 -> 7 digits
    offset = now.strftime("%z")  # e.g. +0000
    offset_fmt = offset[:3] + ":" + offset[3:] if offset else "+00:00"
    return now.strftime("%Y-%m-%dT%H:%M:%S") + "." + frac7 + offset_fmt


def main():
    typer.run(_run)


if __name__ == "__main__":
    main()
