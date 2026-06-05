"""Insert Load Script - Continuous background writer for snapshot/restore testing.

Inserts documents into testdb.loadtest on the cluster until stopped (Ctrl-C) or
-MaxDocs reached. Each batch is committed and logged so the caller can compare
counts before/after snapshot.

Usage:
  start_insert_load                      # run until Ctrl-C
  start_insert_load --max-docs 50000     # stop after 50,000 docs
  start_insert_load --batch-size 500     # tune batch size
"""

import json
import os
import re
import subprocess
import time
from datetime import datetime, timezone

import typer

from .. import config


def _run(
    max_docs: int = typer.Option(
        2147483647, "--max-docs",
        help="Maximum number of documents to insert (default: int max)",
    ),
    batch_size: int = typer.Option(
        200, "--batch-size",
        help="Inserts per batch",
    ),
) -> None:
    # Load config first.
    config.load_config()
    cfg = config.CFG

    collection = "loadtest"

    # Counters / timing state.
    total = 0
    batch = 0
    start_time = datetime.now()
    last_print_total = 0
    last_print_time = start_time

    config.write_host(
        f"=== Insert Load - testdb.{collection} ===", fg=config.YELLOW
    )
    config.write_host(
        f"  MaxDocs={max_docs}  BatchSize={batch_size}  Press Ctrl-C to stop",
        fg=config.CYAN,
    )

    # Ensure collection is sharded (hashed _id).
    setup_script = (
        'var db2 = db.getSiblingDB("testdb");\n'
        "try {\n"
        '    db.getSiblingDB("admin").runCommand({enableSharding:"testdb"});\n'
        '    db.getSiblingDB("admin").runCommand({shardCollection:"testdb.loadtest", key:{_id:"hashed"}});\n'
        "} catch(e) { /* already sharded */ }\n"
        'print("ready");'
    )
    setup_remote = (
        f"{cfg.MongoshPath} --quiet --eval '{setup_script}' "
        f"mongodb://{cfg.MongosHost}:{cfg.MongosPort} 2>/dev/null"
    )
    subprocess.run(
        ["ssh", *config.SSH_OPTS, f"{cfg.SshUser}@{cfg.MongosHost}", setup_remote],
        capture_output=True,
        text=True,
    )

    try:
        while total < max_docs:
            this_batch = min(batch_size, max_docs - total)
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

            # ssh output is split into lines; strip non-digits from the first
            # line and take that count.
            lines = inserted.splitlines()
            first = lines[0] if lines else ""
            digits = re.sub(r"[^0-9]", "", first)
            n = int(digits) if digits else 0

            if n == 0:
                time.sleep(0.5)
                continue
            total += n
            batch += 1
            now = datetime.now()
            rate = round(total / (now - start_time).total_seconds(), 0)
            if (total - last_print_total) >= 10000 or (now - last_print_time).total_seconds() >= 30:
                config.write_host(
                    f"  {now.strftime('%H:%M:%S')}  total={total}  batch={batch}  rate={_fmt_rate(rate)}/s",
                    fg=config.CYAN,
                )
                last_print_total = total
                last_print_time = now
    except KeyboardInterrupt:
        # Ctrl-C
        pass
    finally:
        config.write_host(
            f"\n  Stopped. Total inserted: {total}  Batches: {batch}",
            fg=config.GREEN,
        )
        # Write final count to a status file so the snapshot/restore scripts can read it
        status_path = os.path.join(os.path.expanduser("~"), "mongo-loadtest-status.json")
        status_obj = {
            "totalInserted": total,
            "stoppedAt": _get_date_iso8601(),
        }
        with open(status_path, "w") as fh:
            fh.write(json.dumps(status_obj, indent=4))
        config.write_host(f"  Status written: {status_path}", fg=config.GREEN)


def _fmt_rate(rate: float) -> str:
    """Render a rounded whole-number rate without a trailing .0."""
    return str(int(rate))


def _get_date_iso8601() -> str:
    """Round-trip ISO 8601 timestamp with offset and 7 fractional digits
    (e.g. 2026-06-05T12:34:56.1234567+00:00)."""
    # Python datetime.isoformat() emits up to 6 fractional digits; reproduce a
    # 7-digit, offset-bearing ISO string using the local timezone.
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
