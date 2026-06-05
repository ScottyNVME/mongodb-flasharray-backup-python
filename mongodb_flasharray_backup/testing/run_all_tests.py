"""Run-AllTests.ps1 -> Python 1:1 translation.

Runs Tests 1, 2, and 3 from tests/Test-SnapshotRestore.md end-to-end.

  Test 1 - Basic snapshot restore (no load).
  Test 2 - Snapshot restore under load.
  Test 3 - PITR under load (oplog tailer + replay).

Prints a summary table at the end and exits 0 if all tests pass, 1 if any fail.

Original .ps1 invoked sibling .ps1 scripts via `pwsh -File ...`.  This faithful
translation invokes the corresponding Python console entrypoints instead, using
the SAME interpreter:  sys.executable -m mongodb_flasharray_backup.<module>.
  New-MongoSnapshot.ps1     -> mongodb_flasharray_backup.snapshot          (new-mongo-snapshot)
  Restore-MongoSnapshot.ps1 -> mongodb_flasharray_backup.restore           (restore-mongo-snapshot)
  Start-OplogTailer.ps1     -> mongodb_flasharray_backup.pitr.start_oplog_tailer  (start-oplog-tailer)
  Stop-OplogTailer.ps1      -> mongodb_flasharray_backup.pitr.stop_oplog_tailer   (stop-oplog-tailer)
  Invoke-OplogReplay.ps1    -> mongodb_flasharray_backup.pitr.invoke_oplog_replay (invoke-oplog-replay)
  tests/Start-InsertLoad.ps1-> mongodb_flasharray_backup.testing.start_insert_load(start-insert-load)
"""
# Maps original: #Requires -Version 7 (line 2) and the SYNOPSIS/DESCRIPTION
# comment block (lines 3-30).

import collections
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import typer

# Original line 44: . "$ScriptsDir/Config.ps1"  (dot-source Config.ps1).
from .. import config


# Module names corresponding to the original sibling .ps1 scripts.  These are
# the Python equivalents invoked via `sys.executable -m <module>`.
_MOD_SNAPSHOT = "mongodb_flasharray_backup.snapshot"
_MOD_RESTORE = "mongodb_flasharray_backup.restore"
_MOD_TAILER = "mongodb_flasharray_backup.pitr.start_oplog_tailer"
_MOD_STOP_TAILER = "mongodb_flasharray_backup.pitr.stop_oplog_tailer"
_MOD_REPLAY = "mongodb_flasharray_backup.pitr.invoke_oplog_replay"
_MOD_INSERT_LOAD = "mongodb_flasharray_backup.testing.start_insert_load"


# ── Helpers ────────────────────────────────────────────────────────────────

# Original lines 46-51: function Invoke-Mongos ([string]$Eval)
def invoke_mongos(eval_str: str) -> str:
    cfg = config.CFG
    # Original lines 47-48:
    #   $Out = ssh @SshOpts "${SshUser}@${MongosHost}" \
    #       "$MongoshPath --quiet --eval '$Eval' mongodb://${MongosHost}:${MongosPort} 2>/dev/null"
    remote = (
        f"{cfg.MongoshPath} --quiet --eval '{eval_str}' "
        f"mongodb://{cfg.MongosHost}:{cfg.MongosPort} 2>/dev/null"
    )
    proc = subprocess.run(
        ["ssh", *config.SSH_OPTS, f"{cfg.SshUser}@{cfg.MongosHost}", remote],
        capture_output=True,
        text=True,
    )
    out = proc.stdout or ""
    # Original line 49: if ($LASTEXITCODE -ne 0) { throw "mongosh failed (exit $LASTEXITCODE): $Out" }
    if proc.returncode != 0:
        raise RuntimeError(f"mongosh failed (exit {proc.returncode}): {out}")
    # Original line 50: return ($Out | Out-String).Trim()
    return out.strip()


# Original lines 53-58: function Banner ([string]$Msg)
def banner(msg: str) -> None:
    line = "─" * (len(msg) + 4)
    config.write_host(f"\n{line}", config.CYAN)
    config.write_host(f"  {msg}", config.CYAN)
    config.write_host(f"{line}", config.CYAN)


# Original lines 60-62: function Step ([string]$Msg)
def step(msg: str) -> None:
    # Get-Date -Format 'HH:mm:ss'
    config.write_host(f"  [{datetime.now().strftime('%H:%M:%S')}]  {msg}", None)


# Original lines 64-66: function Pass ([string]$Msg)
def pass_(msg: str) -> None:
    config.write_host(f"  ✔  {msg}", config.GREEN)


# Original lines 68-70: function Fail ([string]$Msg)
def fail(msg: str) -> None:
    config.write_host(f"  ✘  {msg}", config.RED)


# Original lines 72-82: function Start-PwshBackground.
# Starts a background process and returns the Popen object.  stdout/stderr are
# written to $LogPath / "${LogPath}.err".  Instead of launching a sibling .ps1
# via `pwsh -NonInteractive -File`, this launches the corresponding Python
# module via `sys.executable -m <module>` plus the extra args.
def start_pwsh_background(module: str, extra_args, log_path: str):
    # Original line 75: $AllArgs = @('-NonInteractive', '-File', $Script) + $ExtraArgs
    all_args = [sys.executable, "-m", module] + list(extra_args)
    # Original lines 76-80: Start-Process ... -RedirectStandardOutput $LogPath
    #   -RedirectStandardError "${LogPath}.err" -PassThru
    out_f = open(log_path, "w")
    err_f = open(f"{log_path}.err", "w")
    proc = subprocess.Popen(all_args, stdout=out_f, stderr=err_f)
    # Track the file handles so they survive for the life of the process.
    proc._stdout_file = out_f  # type: ignore[attr-defined]
    proc._stderr_file = err_f  # type: ignore[attr-defined]
    # Original line 81: return $proc
    return proc


# Helper mirroring `& pwsh -NonInteractive -File <Script> <args>` followed by the
# `if ($LASTEXITCODE -ne 0) { throw ... }` checks used throughout the script.
def _invoke_module(module: str, args, fail_label: str) -> None:
    proc = subprocess.run([sys.executable, "-m", module] + list(args))
    if proc.returncode != 0:
        raise RuntimeError(f"{fail_label} exited {proc.returncode}")


# Helpers to mirror PowerShell process control (.HasExited / .Kill() /
# .WaitForExit(ms)).
def _has_exited(proc) -> bool:
    return proc.poll() is not None


def _kill(proc) -> None:
    try:
        proc.kill()
    except Exception:
        pass


def _wait_for_exit(proc, timeout_ms: int) -> None:
    try:
        proc.wait(timeout=timeout_ms / 1000.0)
    except subprocess.TimeoutExpired:
        pass


def _range_callback(value: int) -> int:
    # [ValidateRange(1, 3)] for both -StartAtTest and -StopAfterTest.
    if value < 1 or value > 3:
        raise typer.BadParameter("value must be in range 1..3")
    return value


# Original lines 31-318: param() block + script body.
def _run(
    start_at_test: int = typer.Option(
        1, "--start-at-test", callback=_range_callback,
        help="Start execution from test N (1, 2, or 3). Tests before N are skipped.",
    ),
    stop_after_test: int = typer.Option(
        3, "--stop-after-test", callback=_range_callback,
        help="Stop after test N (1, 2, or 3). Tests after N are skipped.",
    ),
) -> None:
    # Equivalent of dot-sourcing Config.ps1 (line 44); call FIRST so running
    # without .env throws like the original.
    config.load_config()

    # Original line 38: $ErrorActionPreference = 'Stop'  (Python exceptions propagate)

    # Original lines 84-87: $Results / $AllPass / $LogDir / New-Item.
    results = collections.OrderedDict()
    all_pass = True
    # $LogDir = Join-Path $PSScriptRoot 'run-alltests-logs'
    log_dir = str(Path(__file__).resolve().parent / "run-alltests-logs")
    # $null = New-Item -ItemType Directory -Path $LogDir -Force
    os.makedirs(log_dir, exist_ok=True)

    # Original lines 89-92: opening banner box.
    config.write_host("", None)
    config.write_host("╔" + "═" * 50 + "╗", config.CYAN)
    config.write_host("║  MongoDB FlashArray Backup — Test Suite 1/2/3    ║", config.CYAN)
    config.write_host("╚" + "═" * 50 + "╝", config.CYAN)

    # ─────────────────────────────────────────────────────────────────────
    #  TEST 1 — Basic snapshot restore (no load)   (original lines 94-135)
    # ─────────────────────────────────────────────────────────────────────
    banner("TEST 1: Basic snapshot restore (no load)")
    if start_at_test > 1:
        # Original lines 99-100
        config.write_host("  Skipped (-StartAtTest > 1)", config.DARK_GRAY)
        results["Test1"] = "SKIP"
    else:
        try:
            # Original lines 103-106
            step("Checking pre-snapshot counts...")
            t1_pre_payload = invoke_mongos('db.getSiblingDB("testdb").payload.countDocuments()')
            t1_pre_loadtest = invoke_mongos('db.getSiblingDB("testdb").loadtest.countDocuments()')
            pass_(f"testdb.payload={t1_pre_payload}  testdb.loadtest={t1_pre_loadtest}")

            # Original lines 108-111
            tag1 = "om-" + datetime.now().strftime("%Y%m%d-%H%M%S")
            step(f"Taking snapshot {tag1} ...")
            _invoke_module(_MOD_SNAPSHOT, ["--snapshot-tag", tag1], "New-MongoSnapshot.ps1")

            # Original lines 113-117
            step("Dropping testdb (simulate disaster)...")
            invoke_mongos('db.getSiblingDB("testdb").dropDatabase()')
            after_drop = invoke_mongos('db.getSiblingDB("testdb").payload.countDocuments()')
            if after_drop != "0":
                raise RuntimeError(f"Drop check failed: payload={after_drop} (expected 0)")
            pass_("testdb dropped")

            # Original lines 119-121
            step(f"Restoring from {tag1} ...")
            _invoke_module(_MOD_RESTORE, ["--snapshot-tag", tag1, "--force"], "Restore-MongoSnapshot.ps1")

            # Original lines 123-126
            step("Verifying post-restore counts...")
            t1_post_payload = invoke_mongos('db.getSiblingDB("testdb").payload.countDocuments()')
            t1_post_loadtest = invoke_mongos('db.getSiblingDB("testdb").loadtest.countDocuments()')
            pass_(f"testdb.payload={t1_post_payload}  testdb.loadtest={t1_post_loadtest}")

            # Original lines 128-129
            results["Test1"] = (
                f"PASS | payload: pre={t1_pre_payload} post={t1_post_payload} | "
                f"loadtest: pre={t1_pre_loadtest} post={t1_post_loadtest} | tag={tag1}"
            )
            pass_("TEST 1 PASSED")
        except Exception as e:  # Original lines 130-134: catch
            results["Test1"] = f"FAIL | {e}"
            all_pass = False
            fail(f"TEST 1 FAILED: {e}")

    # ─────────────────────────────────────────────────────────────────────
    #  TEST 2 — Snapshot restore under load   (original lines 137-195)
    # ─────────────────────────────────────────────────────────────────────
    banner("TEST 2: Snapshot restore under load")
    if start_at_test > 2 or stop_after_test < 2:
        # Original lines 142-143
        config.write_host("  Skipped (-StartAtTest > 2)", config.DARK_GRAY)
        results["Test2"] = "SKIP"
    else:
        # Original line 145
        load_proc2 = None
        try:
            # Original lines 147-150
            load_log2 = str(Path(log_dir) / "test2-load.log")
            step(f"Starting load generator (log → {load_log2})...")
            load_proc2 = start_pwsh_background(_MOD_INSERT_LOAD, [], load_log2)
            pass_(f"Load generator PID={load_proc2.pid}")

            # Original lines 152-153
            step("Waiting 30s before snapshot (load accumulating)...")
            _sleep(30)

            # Original lines 155-158
            tag2 = "om-" + datetime.now().strftime("%Y%m%d-%H%M%S")
            step(f"Taking snapshot {tag2} while load is running...")
            _invoke_module(_MOD_SNAPSHOT, ["--snapshot-tag", tag2], "New-MongoSnapshot.ps1")

            # Original lines 160-161
            step("Waiting 30s after snapshot (post-snapshot writes accumulating)...")
            _sleep(30)

            # Original lines 163-166
            step("Stopping load generator...")
            if not _has_exited(load_proc2):
                _kill(load_proc2)
                _wait_for_exit(load_proc2, 10000)
            load_proc2 = None
            pass_("Load generator stopped")

            # Original lines 168-170
            step("Getting post-load count before drop...")
            t2_before_drop = invoke_mongos('db.getSiblingDB("testdb").loadtest.countDocuments()')
            pass_(f"testdb.loadtest before drop = {t2_before_drop} (these post-snapshot writes will be lost)")

            # Original lines 172-174
            step("Dropping testdb...")
            invoke_mongos('db.getSiblingDB("testdb").dropDatabase()')
            pass_("testdb dropped")

            # Original lines 176-178
            step(f"Restoring from {tag2} ...")
            _invoke_module(_MOD_RESTORE, ["--snapshot-tag", tag2, "--force"], "Restore-MongoSnapshot.ps1")

            # Original lines 180-183
            step("Verifying post-restore counts...")
            t2_post_loadtest = invoke_mongos('db.getSiblingDB("testdb").loadtest.countDocuments()')
            t2_post_payload = invoke_mongos('db.getSiblingDB("testdb").payload.countDocuments()')
            pass_(f"testdb.loadtest={t2_post_loadtest}  testdb.payload={t2_post_payload}")

            # Original lines 185-188
            results["Test2"] = (
                f"PASS | loadtest post-restore={t2_post_loadtest} "
                f"(pre-drop was {t2_before_drop}, diff lost as expected) | tag={tag2}"
            )
            pass_("TEST 2 PASSED")
        except Exception as e:  # Original lines 189-194: catch
            results["Test2"] = f"FAIL | {e}"
            all_pass = False
            fail(f"TEST 2 FAILED: {e}")
            if load_proc2 is not None and not _has_exited(load_proc2):
                _kill(load_proc2)

    # ─────────────────────────────────────────────────────────────────────
    #  TEST 3 — PITR under load   (original lines 197-298)
    # ─────────────────────────────────────────────────────────────────────
    banner("TEST 3: PITR under load (oplog tailer + replay)")
    if stop_after_test < 3:
        # Original lines 202-203
        config.write_host("  Skipped (-StopAfterTest < 3)", config.DARK_GRAY)
        results["Test3"] = "SKIP"
    else:
        # Original lines 205-206
        load_proc3 = None
        tailer_proc3 = None
        # Original line 284 references $T2Mark even when t2-mark.json is missing;
        # initialize to None (PS would leave it unset / $null in that path).
        t2_mark = None
        try:
            # Original lines 208-210
            tag3 = "om-" + datetime.now().strftime("%Y%m%d-%H%M%S")
            step(f"Snapshot tag for Test 3: {tag3}")

            # Original lines 212-213
            load_log3 = str(Path(log_dir) / "test3-load.log")
            tailer_log3 = str(Path(log_dir) / "test3-tailer.log")

            # Original lines 215-217
            step(f"Starting load generator (log → {load_log3})...")
            load_proc3 = start_pwsh_background(_MOD_INSERT_LOAD, [], load_log3)
            pass_(f"Load generator PID={load_proc3.pid}")

            # Original lines 219-221
            step(f"Starting oplog tailer for {tag3} (log → {tailer_log3})...")
            tailer_proc3 = start_pwsh_background(_MOD_TAILER, ["--snapshot-tag", tag3], tailer_log3)
            pass_(f"Oplog tailer PID={tailer_proc3.pid}")

            # Original lines 223-224
            step("Waiting 90s for tailer first job and load to accumulate before snapshot...")
            _sleep(90)

            # Original lines 226-228
            step(f"Taking T1 snapshot {tag3} while load and tailer are running...")
            _invoke_module(_MOD_SNAPSHOT, ["--snapshot-tag", tag3], "New-MongoSnapshot.ps1")

            # Original lines 230-231
            step("Waiting 90s (T1 → T2 window, tailer collecting segments)...")
            _sleep(90)

            # Original lines 233-236
            step("Stopping load generator...")
            if not _has_exited(load_proc3):
                _kill(load_proc3)
                _wait_for_exit(load_proc3, 10000)
            load_proc3 = None
            pass_("Load generator stopped")

            # Original lines 238-240
            step("Stopping oplog tailer (writing T2 mark and sentinel)...")
            _invoke_module(_MOD_STOP_TAILER, ["--snapshot-tag", tag3], "Stop-OplogTailer.ps1")

            # Original lines 242-251
            step("Waiting for tailer process to exit...")
            if tailer_proc3 is not None and not _has_exited(tailer_proc3):
                _wait_for_exit(tailer_proc3, 120000)
                if not _has_exited(tailer_proc3):
                    config.write_host("  WARNING: tailer did not exit in 120s — killing", config.YELLOW)
                    _kill(tailer_proc3)
            tailer_proc3 = None
            pass_("Oplog tailer exited")

            # Original lines 253-260
            step("Getting T2 mark counts...")
            t2_mark_file = str(Path(os.path.expanduser("~")) / "mongo-oplog-stream" / tag3 / "t2-mark.json")
            if os.path.exists(t2_mark_file):
                with open(t2_mark_file, "r") as _f:
                    t2_mark = json.load(_f)
                _t2_loadtest = t2_mark["counts"]["testdb"]["loadtest"]
                _t2_payload = t2_mark["counts"]["testdb"]["payload"]
                pass_(f"T2 mark: testdb.loadtest={_t2_loadtest}  testdb.payload={_t2_payload}")
            else:
                config.write_host("  WARNING: t2-mark.json not found — Stop-OplogTailer may have failed", config.YELLOW)

            # Original lines 262-264
            step("Dropping testdb (simulate disaster)...")
            invoke_mongos('db.getSiblingDB("testdb").dropDatabase()')
            pass_("testdb dropped")

            # Original lines 266-268
            step(f"Restoring to T1 from {tag3} ...")
            _invoke_module(_MOD_RESTORE, ["--snapshot-tag", tag3, "--force"], "Restore-MongoSnapshot.ps1")

            # Original lines 270-272
            step("Verifying T1 baseline (should match snapshot tags)...")
            t3_post_restore = invoke_mongos('db.getSiblingDB("testdb").loadtest.countDocuments()')
            pass_(f"testdb.loadtest after T1 restore = {t3_post_restore}")

            # Original lines 274-276
            step("Replaying oplog segments to advance to T2...")
            _invoke_module(_MOD_REPLAY, ["--snapshot-tag", tag3], "Invoke-OplogReplay.ps1")

            # Original lines 278-281
            step("Verifying post-replay counts...")
            t3_post_replay = invoke_mongos('db.getSiblingDB("testdb").loadtest.countDocuments()')
            t3_post_payload = invoke_mongos('db.getSiblingDB("testdb").payload.countDocuments()')
            pass_(f"testdb.loadtest post-replay={t3_post_replay}  testdb.payload={t3_post_payload}")

            # Original lines 283-287
            tail_str = ""
            if t2_mark:
                tail = int(t2_mark["counts"]["testdb"]["loadtest"]) - int(t3_post_replay)
                tail_str = f"  unrecoveredTail={tail}"

            # Original lines 289-290
            results["Test3"] = (
                f"PASS | loadtest post-replay={t3_post_replay}  payload={t3_post_payload}{tail_str} | tag={tag3}"
            )
            pass_("TEST 3 PASSED")
        except Exception as e:  # Original lines 291-297: catch
            results["Test3"] = f"FAIL | {e}"
            all_pass = False
            fail(f"TEST 3 FAILED: {e}")
            if load_proc3 is not None and not _has_exited(load_proc3):
                _kill(load_proc3)
            if tailer_proc3 is not None and not _has_exited(tailer_proc3):
                _kill(tailer_proc3)

    # ─────────────────────────────────────────────────────────────────────
    #  SUMMARY   (original lines 300-318)
    # ─────────────────────────────────────────────────────────────────────
    config.write_host("", None)
    config.write_host("╔" + "═" * 66 + "╗", config.CYAN)
    config.write_host("║  RESULTS                                                         ║", config.CYAN)
    config.write_host("╚" + "═" * 66 + "╝", config.CYAN)
    # Original lines 307-310: foreach over $Results.
    for key, value in results.items():
        if value.startswith("PASS"):
            color = config.GREEN
        elif value == "SKIP":
            color = config.DARK_GRAY
        else:
            color = config.RED
        config.write_host(f"  {key}: {value}", color)
    config.write_host("", None)
    # Original lines 312-318
    if all_pass:
        config.write_host("All tests PASSED", config.GREEN)
        raise typer.Exit(0)
    else:
        config.write_host("One or more tests FAILED", config.RED)
        raise typer.Exit(1)


# Local Start-Sleep -Seconds equivalent.  Original uses Start-Sleep -Seconds N.
def _sleep(seconds: int) -> None:
    import time
    time.sleep(seconds)


def main():
    typer.run(_run)


if __name__ == "__main__":
    main()
