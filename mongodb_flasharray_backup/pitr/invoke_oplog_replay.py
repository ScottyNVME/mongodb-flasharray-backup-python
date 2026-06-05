#!/usr/bin/env python3
"""Invoke-OplogReplay - Apply captured oplog segments to a cluster restored from FA snapshot.

1:1 faithful Python translation of reference/pitr/Invoke-OplogReplay.ps1.

After restoring from a FlashArray snapshot (which puts the cluster at time T1), this script
applies the oplog segments captured by Start-OplogTailer up to a target timestamp T2,
advancing the cluster to exactly T2 without any additional human intervention.

Each shard's .oplogs segments are applied in capture order directly to that shard's primary
using mongorestore --oplogReplay --oplogFile. --oplogLimit is applied to any segment whose
end timestamp extends beyond -TargetTimestamp. Replay is per-shard (not via mongos) because
the oplog is shard-local.

Usage:
  python -m mongodb_flasharray_backup.pitr.invoke_oplog_replay --snapshot-tag "om-20260505-200455"
  python -m mongodb_flasharray_backup.pitr.invoke_oplog_replay --snapshot-tag "om-20260505-200455" --target-timestamp 1778030500
"""

# Maps PS lines 1-15: param() block + #Requires + dot-sourcing Config.ps1.
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import typer

from .. import config


# ------------------------------------------------------------------------------------------------
# Colors used by the source that are not exported by config.py. Faithful PowerShell -> click map:
#   White    -> bright_white   (PS "White" renders bright)
#   DarkCyan -> cyan           (PS "DarkCyan" renders as standard cyan)
#   Gray     -> white          (PS "Gray" renders as standard/light gray)
# ------------------------------------------------------------------------------------------------
WHITE = "bright_white"
DARK_CYAN = "cyan"
GRAY = "white"


# Maps PS line 4: [ValidatePattern('^om-\d{8}-\d{6}$')] on $SnapshotTag.
def _validate_snapshot_tag(value: str) -> str:
    if not re.match(r"^om-\d{8}-\d{6}$", value):
        raise typer.BadParameter("SnapshotTag must match pattern ^om-\\d{8}-\\d{6}$")
    return value


# ------------------------------------------------------------------------------------------------
# Start-Transcript equivalent: tee stdout + stderr to the log file while still writing to console.
# This captures Write-Host (config.write_host / typer.secho) and command output exactly like
# PowerShell's Start-Transcript -Append.
# ------------------------------------------------------------------------------------------------
class _Tee:
    def __init__(self, stream, log_handle):
        self._stream = stream
        self._log = log_handle

    def write(self, data):
        self._stream.write(data)
        self._log.write(data)
        return len(data)

    def flush(self):
        self._stream.flush()
        self._log.flush()

    def __getattr__(self, name):
        return getattr(self._stream, name)


# Maps PS lines 2-298: the whole script body (param block + main logic in try/finally).
def _run(
    # PS lines 3-5: [Parameter(Mandatory)] [ValidatePattern('^om-\d{8}-\d{6}$')] [string]$SnapshotTag
    snapshot_tag: str = typer.Option(
        ...,
        "--snapshot-tag",
        callback=_validate_snapshot_tag,
        help="Snapshot tag to replay (pattern: om-YYYYMMDD-HHMMSS)",
    ),
    # PS line 7: [long]$TargetTimestamp = 0  (0 = replay all; Unix epoch seconds otherwise)
    target_timestamp: int = typer.Option(
        0,
        "--target-timestamp",
        help="0 = replay all; Unix epoch seconds otherwise",
    ),
    # PS line 9: [string]$VerifyDatabase = 'testdb'
    verify_database: str = typer.Option(
        "testdb",
        "--verify-database",
        help="database to count docs in during post-replay smoke test",
    ),
    # PS line 11: [string]$T2MarkPath = ''
    t2_mark_path: str = typer.Option(
        "",
        "--t2-mark-path",
        help="optional: t2-mark.json with counts captured after stop-load (default: <OplogDir>/t2-mark.json)",
    ),
    # PS line 13: [switch]$SkipVerification
    skip_verification: bool = typer.Option(
        False,
        "--skip-verification",
        help="opt out of post-replay range-bound assertion (counts are still printed)",
    ),
):
    # PS line 15: . "$PSScriptRoot/Config.ps1" -> load config FIRST (throws like the original without .env).
    config.load_config()

    # PS lines 42-45: $LogDir / New-Item / $LogPath / Start-Transcript -Path $LogPath -Append.
    log_dir = Path(os.path.expanduser("~")) / "mongo-oplogreplay-logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"oplogreplay-{snapshot_tag}-{datetime.now().strftime('%Y%m%d-%H%M%S')}.log"
    log_handle = open(log_path, "a", encoding="utf-8")
    orig_stdout = sys.stdout
    orig_stderr = sys.stderr
    sys.stdout = _Tee(orig_stdout, log_handle)
    sys.stderr = _Tee(orig_stderr, log_handle)

    # PS line 47: try {
    try:

        # PS lines 49-53: $OplogDir + existence check.
        oplog_dir = Path(os.path.expanduser("~")) / "mongo-oplog-stream" / snapshot_tag
        if not oplog_dir.exists():
            raise RuntimeError(
                f"Oplog stream directory not found at {oplog_dir}. Run Start-OplogTailer.ps1 first."
            )

        # PS lines 55-62: header + replay target description.
        config.write_host(f"\n=== Oplog Replay for snapshot {snapshot_tag} ===", fg=config.YELLOW)
        config.write_host(f"  Source : {oplog_dir}", fg=config.CYAN)
        if target_timestamp > 0:
            # PS line 58: [System.DateTimeOffset]::FromUnixTimeSeconds($TargetTimestamp).DateTime.ToString('o')
            target_dt = (
                datetime.fromtimestamp(target_timestamp, tz=timezone.utc)
                .replace(tzinfo=None)
                .isoformat()
            )
            config.write_host(f"  Replay to: {target_dt} (Unix {target_timestamp})", fg=config.CYAN)
        else:
            config.write_host("  Replay to: end of captured oplog (all entries)", fg=config.CYAN)

        # PS lines 64-69: Connect to FlashArray + read snapshot metadata tags.
        config.write_host(
            f"  Connecting to FlashArray gateway {config.CFG.FaEndpoint} ...", fg=config.CYAN
        )
        fa = config.connect_fa()
        ctx_names = config.resolve_fa_context_names(fa, config.CFG.ProtectionGroupName)
        snap_tags = config.get_fa_snapshot_tags(
            fa, ctx_names, f"{config.CFG.ProtectionGroupName}.{snapshot_tag}"
        )
        config.write_host("  FlashArray connected.", fg=config.GREEN)

        # PS lines 71-79: Discover current shard primaries (post-restore).
        shard_json = config.invoke_mongosh_js(
            ssh_target=config.CFG.MongosHost,
            uri=f"mongodb://{config.CFG.MongosHost}:{config.CFG.MongosPort}",
            js=config.LIST_SHARDS_JS,
            context="listShards via mongos",
        )
        shards = json.loads(shard_json)

        config.write_host("  Current shard primaries:", fg=config.CYAN)
        for s in shards:
            config.write_host(f"    {s['shardId']} -> {s['host']}", fg=WHITE)

        # PS lines 81-102: Warm up each shard's routing cache via mongos before per-shard replay.
        config.write_host("\n  Warming up shard routing cache via mongos...", fg=config.CYAN)
        warmup_script = (
            "var dbs=db.adminCommand({listDatabases:1}).databases; "
            "for(var i=0;i<dbs.length;i++){try{db.getSiblingDB(dbs[i].name).getCollectionNames();}catch(e){}} "
            "print('routing-cache-warmed');"
        )
        # PS lines 90-102: best-effort (non-fatal) warm-up.
        try:
            warmup_out = config.invoke_mongosh_js(
                ssh_target=config.CFG.MongosHost,
                uri=f"mongodb://{config.CFG.MongosHost}:{config.CFG.MongosPort}",
                js=warmup_script,
                context="routing-cache warm-up",
            )
            if re.search("routing-cache-warmed", warmup_out):
                config.write_host("  Routing cache warm-up complete", fg=config.GREEN)
            else:
                config.write_host(
                    f"  WARNING: routing cache warm-up returned unexpected output: {warmup_out}",
                    fg=config.YELLOW,
                )
        except Exception as e:  # noqa: BLE001
            config.write_host(
                f"  WARNING: routing cache warm-up failed (non-fatal): {e}", fg=config.YELLOW
            )

        # PS line 104: $Errors = [System.Collections.Generic.List[string]]::new()
        errors: list[str] = []

        # PS lines 106-113: Deploy the .oplogs decoder script to every replay node.
        # decode_oplogs.py lives in the same directory as this module.
        decoder_src = Path(__file__).with_name("decode_oplogs.py")
        if not decoder_src.exists():
            raise RuntimeError(
                f"decode_oplogs.py not found at {decoder_src} — it must be in the same directory as this script."
            )
        remote_decoder = "/tmp/decode_oplogs.py"

        # PS lines 115-125: Load T1 atClusterTime from snapshot tag for pre-snapshot segment filtering.
        t1_at_cluster_time = 0
        if snap_tags.get("mongo:t1ts"):
            t1_at_cluster_time = int(snap_tags["mongo:t1ts"])
            config.write_host(
                f"  T1 atClusterTime (segment filter): {t1_at_cluster_time}", fg=config.CYAN
            )
        else:
            config.write_host(
                "  WARNING: mongo:t1ts tag absent — pre-T1 segment filtering disabled",
                fg=config.YELLOW,
            )

        # PS lines 127-165: Build the per-shard work list (flat $Plan list of work units).
        plan: list[dict] = []
        for s in shards:
            shard_id = s["shardId"]
            seg_dir = oplog_dir / shard_id / "segments"
            if not seg_dir.exists():
                config.write_host(
                    f"  WARNING: no segments dir for {shard_id} at {seg_dir} - skipping shard",
                    fg=config.YELLOW,
                )
                continue
            # PS line 139: Get-ChildItem $SegDir -Filter '*.oplogs' -File | Sort-Object Name
            files = sorted(
                [f for f in seg_dir.iterdir() if f.is_file() and f.name.endswith(".oplogs")],
                key=lambda f: f.name,
            )
            if len(files) == 0:
                config.write_host(
                    f"  WARNING: no segments found for {shard_id} in {seg_dir} - skipping shard",
                    fg=config.YELLOW,
                )
                continue
            for f in files:
                # PS lines 145-148: Parse timestamps from the OM filename: <startTs>_<endTs>.oplogs
                parts = f.stem.split("_")
                start_ts = int(parts[0])
                end_ts = int(parts[1])
                # PS line 152: Skip segments entirely before or at the T1 snapshot atClusterTime.
                if t1_at_cluster_time > 0 and end_ts <= t1_at_cluster_time:
                    continue
                # PS line 154: Skip files whose entire window is beyond the PIT target.
                if target_timestamp > 0 and start_ts > target_timestamp:
                    continue
                # PS lines 155-163: $Plan.Add([pscustomobject]@{...})
                plan.append(
                    {
                        "ShardId": shard_id,
                        "RsHosts": s["rsHosts"],
                        "Node": s["host"].split(":")[0],
                        "LocalPath": str(f),
                        "SegLabel": f.stem,
                        "StartTs": start_ts,
                        "EndTs": end_ts,
                    }
                )

        # PS lines 167-171: plan summary.
        if len(plan) == 0:
            config.write_host(
                "\n  No post-T1 oplog segments to replay (all available segments pre-date the T1 snapshot). Cluster remains at T1 restore state.",
                fg=config.YELLOW,
            )
        else:
            unique_shards = len({u["ShardId"] for u in plan})
            config.write_host(
                f"\n  Replay plan: {len(plan)} segment(s) across {unique_shards} shard(s)",
                fg=config.CYAN,
            )

        # PS lines 173-183: SCP the decoder to each distinct agent node once before the replay loop.
        deployed_nodes: set[str] = set()
        for unit in plan:
            if unit["Node"] not in deployed_nodes:
                deployed_nodes.add(unit["Node"])
                config.write_host(f"  Deploying decoder to {unit['Node']}...", fg=config.CYAN)
                # PS line 178: scp @SshOpts $DecoderSrc "${SshUser}@$($Unit.Node):${RemoteDecoder}" 2>&1
                proc = subprocess.run(
                    [
                        "scp",
                        *config.SSH_OPTS,
                        str(decoder_src),
                        f"{config.CFG.SshUser}@{unit['Node']}:{remote_decoder}",
                    ],
                    capture_output=True,
                    text=True,
                )
                if proc.returncode != 0:
                    raise RuntimeError(f"Failed to SCP decode_oplogs.py to {unit['Node']}")

        # PS lines 185-230: Replay loop.
        prev_shard_id = None
        for unit in plan:
            if unit["ShardId"] != prev_shard_id:
                config.write_host(
                    f"\n  {unit['ShardId']}: replaying on {unit['RsHosts']} ...", fg=config.CYAN
                )
                prev_shard_id = unit["ShardId"]

            remote_file = (
                f"/tmp/oplog-replay-{snapshot_tag}-{unit['ShardId']}-{unit['SegLabel']}.oplogs"
            )
            # PS line 196: $Bytes = (Get-Item $Unit.LocalPath).Length
            num_bytes = Path(unit["LocalPath"]).stat().st_size
            # PS lines 200-201: --oplogLimit applied only when file extends beyond the PIT target.
            apply_limit = target_timestamp > 0 and unit["EndTs"] > target_timestamp
            oplog_limit = f"--oplogLimit '{target_timestamp}:1'" if apply_limit else ""

            # PS line 203: "    seg {0}  {1:N1} KB -> {2}" -f SegLabel, (Bytes/1KB), Node
            kb = num_bytes / 1024
            config.write_host(
                f"    seg {unit['SegLabel']}  {kb:,.1f} KB -> {unit['Node']}", fg=DARK_CYAN
            )
            # PS line 204: scp @SshOpts $Unit.LocalPath "${SshUser}@$($Unit.Node):${RemoteFile}" 2>&1
            proc = subprocess.run(
                [
                    "scp",
                    *config.SSH_OPTS,
                    unit["LocalPath"],
                    f"{config.CFG.SshUser}@{unit['Node']}:{remote_file}",
                ],
                capture_output=True,
                text=True,
            )
            if proc.returncode != 0:
                # PS line 206
                errors.append(f"{unit['ShardId']}/{unit['SegLabel']}: scp to {unit['Node']} failed")
                continue

            # PS lines 211-220: build the remote replay command (here-string).
            replay_cmd = f"""set +e
TMPDIR=$(mktemp -d)
python3 {remote_decoder} {remote_file} > $TMPDIR/oplog.bson
if [ $? -ne 0 ]; then rm -rf $TMPDIR {remote_file}; exit 1; fi
{config.CFG.MongorestorePath} --host '{unit['RsHosts']}' --oplogReplay $TMPDIR {oplog_limit} 2>&1
EC=$?
rm -rf $TMPDIR {remote_file}
exit $EC
"""
            # PS line 221: $Out = ssh @SshOpts "${SshUser}@$($Unit.Node)" $ReplayCmd 2>&1
            replay_proc = subprocess.run(
                [
                    "ssh",
                    *config.SSH_OPTS,
                    f"{config.CFG.SshUser}@{unit['Node']}",
                    replay_cmd,
                ],
                capture_output=True,
                text=True,
            )
            # PS line 222: $RestoreExit = $LASTEXITCODE
            restore_exit = replay_proc.returncode
            # PS line 223: Write-Host ($Out -join "`n") -ForegroundColor Gray
            # 2>&1 in PS merges stderr into the captured output stream.
            out = (replay_proc.stdout or "") + (replay_proc.stderr or "")
            config.write_host(out, fg=GRAY)
            if restore_exit == 0:
                # PS line 225
                config.write_host(f"    seg {unit['SegLabel']}: OK", fg=config.GREEN)
            else:
                # PS lines 227-228
                errors.append(
                    f"{unit['ShardId']}/{unit['SegLabel']}: mongorestore exit {restore_exit} on {unit['Node']}"
                )
                config.write_host(
                    f"    seg {unit['SegLabel']}: mongorestore exit {restore_exit}",
                    fg=config.YELLOW,
                )

        # PS lines 232-234: fail if any segment errored.
        if len(errors) > 0:
            joined = "\n".join(errors)
            raise RuntimeError(f"Oplog replay failed on {len(errors)} segment(s):\n{joined}")

        # PS lines 236-248: Post-replay verification: count docs as a sanity check.
        config.write_host("\n=== Post-Replay Verification ===", fg=config.YELLOW)
        load_test_count = config.invoke_mongosh_js(
            ssh_target=config.CFG.MongosHost,
            uri=f"mongodb://{config.CFG.MongosHost}:{config.CFG.MongosPort}",
            js=f'db.getSiblingDB("{verify_database}").loadtest.countDocuments()',
            context=f"post-replay count of {verify_database}.loadtest",
        )
        payload_count = config.invoke_mongosh_js(
            ssh_target=config.CFG.MongosHost,
            uri=f"mongodb://{config.CFG.MongosHost}:{config.CFG.MongosPort}",
            js=f'db.getSiblingDB("{verify_database}").payload.countDocuments()',
            context=f"post-replay count of {verify_database}.payload",
        )

        config.write_host(f"  {verify_database}.loadtest : {load_test_count.strip()}", fg=config.CYAN)
        config.write_host(f"  {verify_database}.payload  : {payload_count.strip()}", fg=config.CYAN)

        # PS lines 250-258: Range-bound assertion setup ($Counts hashtable).
        counts = {
            "loadtest": int(load_test_count.strip()),
            "payload": int(payload_count.strip()),
        }
        # PS line 259: default $T2MarkPath to <OplogDir>/t2-mark.json when not supplied.
        if not t2_mark_path:
            t2_mark_path = str(oplog_dir / "t2-mark.json")

        # PS lines 261-292: verification branches.
        if skip_verification:
            config.write_host("  Verification skipped (-SkipVerification)", fg=config.YELLOW)
        elif not snap_tags.get("mongo:preSnap"):
            config.write_host(
                "  mongo:preSnap tag absent — skipping range check (counts printed above)",
                fg=config.YELLOW,
            )
        elif not Path(t2_mark_path).exists():
            config.write_host(
                f"  T2 mark not found at {t2_mark_path} - skipping range check (provide -T2MarkPath or write the file to enable)",
                fg=config.YELLOW,
            )
        else:
            # PS lines 268-271: parse preSnap tag JSON and t2-mark.json.
            pre_snap = json.loads(snap_tags["mongo:preSnap"])
            with open(t2_mark_path, "r", encoding="utf-8") as fh:
                t2_mark = json.loads(fh.read())
            pre_db = pre_snap.get(verify_database)
            t2_mark_counts = t2_mark.get("counts") if isinstance(t2_mark, dict) else None
            t2_db = t2_mark_counts.get(verify_database) if isinstance(t2_mark_counts, dict) else None
            # PS lines 272-273: skip if entries missing for VerifyDatabase.
            if pre_db is None or t2_db is None:
                config.write_host(
                    f"  WARNING: mongo:preSnap tag or t2-mark missing entries for '{verify_database}' - skipping range check",
                    fg=config.YELLOW,
                )
            else:
                # PS lines 275-291: per-collection range check.
                mismatches: list[str] = []
                for coll in counts.keys():
                    got = counts[coll]
                    lo = int(pre_db[coll])
                    hi = int(t2_db[coll])
                    if got < lo or got > hi:
                        # PS lines 281-282
                        mismatches.append(f"{verify_database}.{coll}: {got} NOT IN [{lo}, {hi}]")
                        config.write_host(
                            f"  Baseline FAIL : {verify_database}.{coll} = {got} NOT IN [{lo}, {hi}]",
                            fg=config.RED,
                        )
                    else:
                        # PS lines 284-285
                        tail = hi - got
                        config.write_host(
                            f"  Baseline OK   : {verify_database}.{coll} = {got} in [{lo}, {hi}] (unrecoveredTail={tail})",
                            fg=config.GREEN,
                        )
                # PS lines 288-290: raise if any mismatch.
                if len(mismatches) > 0:
                    joined = "\n".join(mismatches)
                    raise RuntimeError(f"Post-replay verification failed:\n{joined}")

        # PS line 294
        config.write_host("\n=== Oplog Replay Complete ===", fg=config.GREEN)

    # PS lines 296-298: finally { try { Stop-Transcript } catch {} }
    finally:
        try:
            sys.stdout = orig_stdout
            sys.stderr = orig_stderr
            log_handle.close()
        except Exception:
            pass


def main():
    typer.run(_run)


if __name__ == "__main__":
    main()
