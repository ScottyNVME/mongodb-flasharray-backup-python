"""Seed testdb.loadtest and testdb.payload for demo and testing.

Enables sharding on testdb, shards both collections on {_id: "hashed"} to
distribute documents evenly across all three shards, then inserts documents in
$BatchSize batches.

Safe to re-run: skips collections that already have >= the target count.
Use --force to drop both collections and re-seed from scratch.
"""

import math
import subprocess
import time

import typer

from .. import config

# config does not expose a DarkCyan constant, so define it locally for the
# progress lines.
DARK_CYAN = "cyan"


# Local mongos helper.  eval_str must use only double-quotes internally (it is
# wrapped in single-quotes by the SSH command).
# Behaviorally identical to config.invoke_mongos; reimplemented here as a
# script-local function that uses load_config-derived CFG values.
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
        raise RuntimeError(f"mongosh failed (exit {proc.returncode}): {out}")
    return out.strip()


def invoke_seed_collection(coll_name: str, target_count: int, batch_sz: int) -> None:
    existing = int(invoke_mongos(
        f'db.getSiblingDB("testdb").{coll_name}.countDocuments()'
    ))
    needed = target_count - existing
    if needed <= 0:
        config.write_host(
            f"  testdb.{coll_name} already has {existing} docs - skipping.",
            fg=config.DARK_GRAY,
        )
        return

    config.write_host(
        f"\n-- Seeding testdb.{coll_name} ({existing} -> {target_count}, "
        f"inserting {needed} docs) --",
        fg=config.YELLOW,
    )

    batches = int(math.ceil(needed / batch_sz))
    inserted = 0
    start_time = time.time()

    for b in range(0, batches):
        offset = existing + b * batch_sz
        this_batch = min(batch_sz, needed - b * batch_sz)

        # Build the JS insert for this batch (newlines replaced by single spaces).
        js = (
            "var docs = [];"
            f" for(var i = 0; i < {this_batch}; i++)" + "{"
            f"   docs.push({{seq: {offset} + i, v: \"{coll_name}\", ts: new Date()}});"
            " }"
            f" db.getSiblingDB(\"testdb\").{coll_name}.insertMany(docs, {{ordered: false}});"
            f" print({this_batch}) "
        )

        result = invoke_mongos(js)  # noqa: F841  (captured, unused)
        inserted += this_batch

        # Progress every 10 batches or on the last batch.
        if b % 10 == 9 or b == (batches - 1):
            pct = int(inserted / needed * 100)
            elapsed = int(time.time() - start_time)
            config.write_host(
                "  [{0:>3}%] {1:>8}/{2}  ({3}s)".format(pct, inserted, needed, elapsed),
                fg=DARK_CYAN,
            )

    final = int(invoke_mongos(
        f'db.getSiblingDB("testdb").{coll_name}.countDocuments()'
    ))
    if final < target_count:
        raise RuntimeError(
            f"testdb.{coll_name} ended with {final} docs (expected >= {target_count})"
        )
    config.write_host(f"  testdb.{coll_name} : {final} docs", fg=config.GREEN)


def _run(
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
    # Load config first.
    config.load_config()
    cfg = config.CFG

    mongos_uri = f"mongodb://{cfg.MongosHost}:{cfg.MongosPort}"

    config.write_host("\n=== Initialize Test Data ===", fg=config.YELLOW)
    config.write_host(f"  testdb.loadtest target : {loadtest_docs}", fg=config.CYAN)
    config.write_host(f"  testdb.payload  target : {payload_docs}", fg=config.CYAN)
    config.write_host(f"  Batch size             : {batch_size}", fg=config.CYAN)
    config.write_host(f"  Mongos                 : {mongos_uri}", fg=config.CYAN)

    # ── Check existing counts ──
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

    seed_loadtest = existing_loadtest < loadtest_docs
    seed_payload = existing_payload < payload_docs

    if force:
        config.write_host(
            "\n  -Force specified - dropping both collections.", fg=config.YELLOW
        )
        invoke_mongos('db.getSiblingDB("testdb").loadtest.drop()')
        invoke_mongos('db.getSiblingDB("testdb").payload.drop()')
        seed_loadtest = loadtest_docs > 0
        seed_payload = payload_docs > 0

    if not seed_loadtest and not seed_payload:
        config.write_host(
            "\n  Both collections already meet the target counts. Nothing to do.",
            fg=config.GREEN,
        )
        config.write_host(
            "  Use -Force to drop and re-seed.", fg=config.DARK_GRAY
        )
        raise typer.Exit(code=0)

    # ── Enable sharding on testdb ──
    config.write_host("\n-- Enabling sharding on testdb --", fg=config.YELLOW)
    sh_result = invoke_mongos('sh.enableSharding("testdb")')
    config.write_host(f"  {sh_result}", fg=config.DARK_GRAY)

    # ── Shard each collection on {_id: "hashed"} ──
    for col_spec in (
        {"Name": "loadtest", "Seed": seed_loadtest},
        {"Name": "payload", "Seed": seed_payload},
    ):
        if not col_spec["Seed"]:
            continue
        coll = col_spec["Name"]
        config.write_host(
            f"\n-- Sharding testdb.{coll} on {{_id: hashed}} --", fg=config.YELLOW
        )
        shard_result = invoke_mongos(
            f'sh.shardCollection("testdb.{coll}", {{_id: "hashed"}})'
        )
        config.write_host(f"  {shard_result}", fg=config.DARK_GRAY)

    # ── Insert documents ──
    if seed_loadtest:
        invoke_seed_collection(
            coll_name="loadtest", target_count=loadtest_docs, batch_sz=batch_size
        )
    if seed_payload:
        invoke_seed_collection(
            coll_name="payload", target_count=payload_docs, batch_sz=batch_size
        )

    # ── Final verification ──
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

    # Shard distribution summary
    config.write_host("\n-- Shard distribution (loadtest) --", fg=config.YELLOW)
    config.write_host(
        invoke_mongos('db.getSiblingDB("testdb").loadtest.getShardDistribution()')
    )

    config.write_host("\n-- Shard distribution (payload) --", fg=config.YELLOW)
    config.write_host(
        invoke_mongos('db.getSiblingDB("testdb").payload.getShardDistribution()')
    )

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
