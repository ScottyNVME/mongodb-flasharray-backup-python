"""Initialize-TestData.ps1 -> Python 1:1 translation.

Seed testdb.loadtest and testdb.payload for demo and testing.

Enables sharding on testdb, shards both collections on {_id: "hashed"} to
distribute documents evenly across all three shards, then inserts documents in
$BatchSize batches.

Safe to re-run: skips collections that already have >= the target count.
Use --force to drop both collections and re-seed from scratch.
"""
# Maps original: header comment block (lines 9-23) and #Requires -Version 7 (line 1)

import math
import subprocess
import time

import typer

# Original line 8: . "$PSScriptRoot/Config.ps1"  (dot-source Config.ps1).
from .. import config

# PowerShell "DarkCyan" -> typer "cyan".  config does not expose a DarkCyan
# constant, so define the faithful mapping locally for the progress lines.
DARK_CYAN = "cyan"


# Local helper that matches the Invoke-Mongos pattern used in tests/Run-AllTests.ps1.
# Original lines 29-36.  $Eval must use only double-quotes internally (it is wrapped
# in single-quotes by the SSH command).
# Behaviorally identical to config.invoke_mongos; reimplemented here to mirror the
# script-local function definition verbatim (uses load_config-derived CFG values).
def invoke_mongos(eval_str: str) -> str:
    cfg = config.CFG
    mongos_uri = f"mongodb://{cfg.MongosHost}:{cfg.MongosPort}"
    remote = (
        f"{cfg.MongoshPath} --quiet --eval '{eval_str}' {mongos_uri} 2>/dev/null"
    )
    proc = subprocess.run(
        ["ssh", *config.SSH_OPTS, f"{cfg.SshUser}@{cfg.MongosHost}", remote],
        capture_output=True,
        text=True,
    )
    out = (proc.stdout or "")
    if proc.returncode != 0:
        # Original: if ($LASTEXITCODE -ne 0) { throw "mongosh failed (exit $LASTEXITCODE): $Out" }
        raise RuntimeError(f"mongosh failed (exit {proc.returncode}): {out}")
    # Original: return ($Out | Out-String).Trim()
    return out.strip()


# Original lines 88-140: function Invoke-SeedCollection
def invoke_seed_collection(coll_name: str, target_count: int, batch_sz: int) -> None:
    # Original line 96
    existing = int(invoke_mongos(
        f'db.getSiblingDB("testdb").{coll_name}.countDocuments()'
    ))
    # Original line 97
    needed = target_count - existing
    # Original lines 98-101
    if needed <= 0:
        config.write_host(
            f"  testdb.{coll_name} already has {existing} docs - skipping.",
            fg=config.DARK_GRAY,
        )
        return

    # Original line 103
    config.write_host(
        f"\n-- Seeding testdb.{coll_name} ({existing} -> {target_count}, "
        f"inserting {needed} docs) --",
        fg=config.YELLOW,
    )

    # Original lines 105-107
    batches = int(math.ceil(needed / batch_sz))
    inserted = 0
    start_time = time.time()

    # Original line 109
    for b in range(0, batches):
        # Original line 110
        offset = existing + b * batch_sz
        # Original line 111
        this_batch = min(batch_sz, needed - b * batch_sz)

        # Original lines 115-122: build the JS insert for this batch (here-string
        # with CRLF/LF replaced by single spaces).
        js = (
            "var docs = [];"
            f" for(var i = 0; i < {this_batch}; i++)" + "{"
            f"   docs.push({{seq: {offset} + i, v: \"{coll_name}\", ts: new Date()}});"
            " }"
            f" db.getSiblingDB(\"testdb\").{coll_name}.insertMany(docs, {{ordered: false}});"
            f" print({this_batch}) "
        )

        # Original line 124
        result = invoke_mongos(js)  # noqa: F841  (matches PS: $Result captured, unused)
        # Original line 125
        inserted += this_batch

        # Original lines 127-132: progress every 10 batches or on the last batch
        if b % 10 == 9 or b == (batches - 1):
            pct = int(inserted / needed * 100)
            elapsed = int(time.time() - start_time)
            config.write_host(
                "  [{0:>3}%] {1:>8}/{2}  ({3}s)".format(pct, inserted, needed, elapsed),
                fg=DARK_CYAN,
            )

    # Original lines 135-138
    final = int(invoke_mongos(
        f'db.getSiblingDB("testdb").{coll_name}.countDocuments()'
    ))
    if final < target_count:
        raise RuntimeError(
            f"testdb.{coll_name} ended with {final} docs (expected >= {target_count})"
        )
    # Original line 139
    config.write_host(f"  testdb.{coll_name} : {final} docs", fg=config.GREEN)


def _run(
    # Original param block lines 2-7
    loadtest_docs: int = typer.Option(
        100000, "--loadtest-docs",
        help="Target document count for testdb.loadtest",
    ),
    payload_docs: int = typer.Option(
        200000, "--payload-docs",
        help="Target document count for testdb.payload",
    ),
    batch_size: int = typer.Option(
        5000, "--batch-size",
        help="Documents per insertMany call",
    ),
    force: bool = typer.Option(
        False, "--force",
        help="Drop and re-seed even if data already exists",
    ),
) -> None:
    # Original line 8: . "$PSScriptRoot/Config.ps1"  -> load config first.
    config.load_config()
    cfg = config.CFG

    # Original line 25: $ErrorActionPreference = 'Stop'  (Python raises by default).

    # Original line 27: $MongosUri = "mongodb://${MongosHost}:${MongosPort}"
    mongos_uri = f"mongodb://{cfg.MongosHost}:{cfg.MongosPort}"

    # Original lines 38-42
    config.write_host("\n=== Initialize Test Data ===", fg=config.YELLOW)
    config.write_host(f"  testdb.loadtest target : {loadtest_docs}", fg=config.CYAN)
    config.write_host(f"  testdb.payload  target : {payload_docs}", fg=config.CYAN)
    config.write_host(f"  Batch size             : {batch_size}", fg=config.CYAN)
    config.write_host(f"  Mongos                 : {mongos_uri}", fg=config.CYAN)

    # ── Check existing counts ──  Original lines 44-49
    existing_loadtest = int(invoke_mongos(
        'db.getSiblingDB("testdb").loadtest.countDocuments()'
    ))
    existing_payload = int(invoke_mongos(
        'db.getSiblingDB("testdb").payload.countDocuments()'
    ))
    config.write_host("")
    config.write_host(
        f"  Existing  testdb.loadtest = {existing_loadtest}", fg=config.DARK_GRAY
    )
    config.write_host(
        f"  Existing  testdb.payload  = {existing_payload}", fg=config.DARK_GRAY
    )

    # Original lines 51-52
    seed_loadtest = existing_loadtest < loadtest_docs
    seed_payload = existing_payload < payload_docs

    # Original lines 54-60: -Force
    if force:
        config.write_host(
            "\n  -Force specified - dropping both collections.", fg=config.YELLOW
        )
        invoke_mongos('db.getSiblingDB("testdb").loadtest.drop()')
        invoke_mongos('db.getSiblingDB("testdb").payload.drop()')
        seed_loadtest = loadtest_docs > 0
        seed_payload = payload_docs > 0

    # Original lines 62-66
    if not seed_loadtest and not seed_payload:
        config.write_host(
            "\n  Both collections already meet the target counts. Nothing to do.",
            fg=config.GREEN,
        )
        config.write_host(
            "  Use -Force to drop and re-seed.", fg=config.DARK_GRAY
        )
        raise typer.Exit(code=0)

    # ── Enable sharding on testdb ──  Original lines 68-71
    config.write_host("\n-- Enabling sharding on testdb --", fg=config.YELLOW)
    sh_result = invoke_mongos('sh.enableSharding("testdb")')
    config.write_host(f"  {sh_result}", fg=config.DARK_GRAY)

    # ── Shard each collection on {_id: "hashed"} ──  Original lines 73-86
    for col_spec in (
        {"Name": "loadtest", "Seed": seed_loadtest},
        {"Name": "payload", "Seed": seed_payload},
    ):
        # Original line 81
        if not col_spec["Seed"]:
            continue
        coll = col_spec["Name"]
        config.write_host(
            f"\n-- Sharding testdb.{coll} on {{_id: hashed}} --", fg=config.YELLOW
        )
        # Original line 84:
        # sh.shardCollection("testdb.$Coll", {_id: "hashed"})
        shard_result = invoke_mongos(
            f'sh.shardCollection("testdb.{coll}", {{_id: "hashed"}})'
        )
        config.write_host(f"  {shard_result}", fg=config.DARK_GRAY)

    # ── Insert documents ──  Original lines 142-143
    if seed_loadtest:
        invoke_seed_collection(
            coll_name="loadtest", target_count=loadtest_docs, batch_sz=batch_size
        )
    if seed_payload:
        invoke_seed_collection(
            coll_name="payload", target_count=payload_docs, batch_sz=batch_size
        )

    # ── Final verification ──  Original lines 145-152
    config.write_host("\n-- Final verification --", fg=config.YELLOW)

    final_loadtest = int(invoke_mongos(
        'db.getSiblingDB("testdb").loadtest.countDocuments()'
    ))
    final_payload = int(invoke_mongos(
        'db.getSiblingDB("testdb").payload.countDocuments()'
    ))

    config.write_host(
        "  testdb.loadtest = {0:>8}".format(final_loadtest), fg=config.GREEN
    )
    config.write_host(
        "  testdb.payload  = {0:>8}".format(final_payload), fg=config.GREEN
    )

    # Shard distribution summary  Original lines 154-159
    config.write_host("\n-- Shard distribution (loadtest) --", fg=config.YELLOW)
    config.write_host(
        invoke_mongos('db.getSiblingDB("testdb").loadtest.getShardDistribution()')
    )

    config.write_host("\n-- Shard distribution (payload) --", fg=config.YELLOW)
    config.write_host(
        invoke_mongos('db.getSiblingDB("testdb").payload.getShardDistribution()')
    )

    # Original lines 161-168
    loadtest_ok = final_loadtest >= loadtest_docs
    payload_ok = final_payload >= payload_docs

    if loadtest_ok and payload_ok:
        config.write_host("\n=== Test data ready ===", fg=config.GREEN)
    else:
        raise RuntimeError(
            f"Seeding incomplete: loadtest={final_loadtest} (want>={loadtest_docs})  "
            f"payload={final_payload} (want>={payload_docs})"
        )


def main():
    typer.run(_run)


if __name__ == "__main__":
    main()
