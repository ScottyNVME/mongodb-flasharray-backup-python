"""Unit tests for restore_to_target.py.

These cover the pure logic that needs no live cluster/array: target-node/seed parsing, the
local.system.replset rewrite-JS construction, the captured-mongod-info parser, and the rs.status()
readiness-probe parser. Live behavior (FA overwrite, OM reconcile, initial sync) is exercised by the
1.A.1.b certification run, not here.
"""

import sys
from pathlib import Path

import pytest

# Make the package importable when running from the repo root without an editable install.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mongodb_flasharray_backup import restore_to_target as rtt  # noqa: E402


# --------------------------------------------------------------------------------------------------------
# _parse_targets
# --------------------------------------------------------------------------------------------------------
def test_parse_targets_default_seed_is_first():
    nodes, seed, non_seed = rtt._parse_targets("a.ex,b.ex,c.ex")
    assert nodes == ["a.ex", "b.ex", "c.ex"]
    assert seed == "a.ex"
    assert non_seed == ["b.ex", "c.ex"]


def test_parse_targets_trims_and_drops_blanks():
    nodes, seed, non_seed = rtt._parse_targets("  a.ex , , b.ex ", target_seed="b.ex")
    assert nodes == ["a.ex", "b.ex"]
    assert seed == "b.ex"
    assert non_seed == ["a.ex"]


def test_parse_targets_single_node_has_no_non_seed():
    nodes, seed, non_seed = rtt._parse_targets("only.ex")
    assert (nodes, seed, non_seed) == (["only.ex"], "only.ex", [])


def test_parse_targets_empty_raises():
    with pytest.raises(RuntimeError, match="--target-nodes is empty"):
        rtt._parse_targets("   , ,")


def test_parse_targets_seed_not_in_list_raises():
    with pytest.raises(RuntimeError, match="is not one of --target-nodes"):
        rtt._parse_targets("a.ex,b.ex", target_seed="z.ex")


# --------------------------------------------------------------------------------------------------------
# _build_replset_rewrite_js
# --------------------------------------------------------------------------------------------------------
def test_build_rewrite_js_contains_target_identity():
    js = rtt._build_replset_rewrite_js("aen-rs-dst", "dst1.ex:27017")
    # New RS name and single seed member with quorum on the seed.
    assert "_id:'aen-rs-dst'" in js
    assert "host:'dst1.ex:27017'" in js
    assert "votes:1" in js and "priority:1" in js
    # Exactly one member object is written (single-member config so the seed can self-elect).
    assert js.count("_id:0") == 1
    # Version is bumped from the prior config, not hard-coded.
    assert "version:(old?old.version:0)+1" in js
    # Confirmation marker the caller asserts on.
    assert "print('rewrote:'+local.system.replset.findOne()._id)" in js


# --------------------------------------------------------------------------------------------------------
# _capture_mongod_info (subprocess mocked)
# --------------------------------------------------------------------------------------------------------
class _Proc:
    def __init__(self, stdout="", stderr="", returncode=0):
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode


def _load_dummy_cfg(monkeypatch):
    # _capture_mongod_info reads config.CFG.SshUser; give it a minimal stub.
    monkeypatch.setattr(rtt.config, "CFG", type("C", (), {"SshUser": "ec2-user"}))


def test_capture_mongod_info_parses_fields(monkeypatch):
    _load_dummy_cfg(monkeypatch)
    out = (
        "BIN=/var/lib/mms/mongodb/bin/mongod\nDBPATH=/data/mongo/data\nPORT=27017\n"
        "USER=mongod\nCONF=/data/mongo/automation-mongod.conf\n"
    )
    monkeypatch.setattr(rtt.subprocess, "run", lambda *a, **k: _Proc(stdout=out))
    info = rtt._capture_mongod_info("node1")
    assert info == {
        "Bin": "/var/lib/mms/mongodb/bin/mongod",
        "DbPath": "/data/mongo/data",
        "Port": "27017",
        "User": "mongod",
        "Conf": "/data/mongo/automation-mongod.conf",
    }


def test_capture_mongod_info_incomplete_raises(monkeypatch):
    _load_dummy_cfg(monkeypatch)
    # Missing DBPATH -> must raise rather than proceed with a half-known target.
    out = "BIN=/usr/bin/mongod\nDBPATH=\nPORT=27017\nUSER=mongod\n"
    monkeypatch.setattr(rtt.subprocess, "run", lambda *a, **k: _Proc(stdout=out))
    with pytest.raises(RuntimeError, match="Incomplete mongod info"):
        rtt._capture_mongod_info("node1")


def test_capture_mongod_info_ssh_failure_raises(monkeypatch):
    _load_dummy_cfg(monkeypatch)
    monkeypatch.setattr(rtt.subprocess, "run", lambda *a, **k: _Proc(stderr="no-mongod", returncode=3))
    with pytest.raises(RuntimeError, match="Could not read live mongod info"):
        rtt._capture_mongod_info("node1")


# --------------------------------------------------------------------------------------------------------
# _rs_status (invoke_mongosh_js mocked)
# --------------------------------------------------------------------------------------------------------
def test_rs_status_parses_json(monkeypatch):
    monkeypatch.setattr(
        rtt.config,
        "invoke_mongosh_js",
        lambda **k: 'some noise\n{"primary":"d1:27017","healthy":3,"total":3}\n',
    )
    st = rtt._rs_status("d1", 27017)
    assert st == {"primary": "d1:27017", "healthy": 3, "total": 3}


def test_rs_status_not_ready_returns_zeros(monkeypatch):
    monkeypatch.setattr(rtt.config, "invoke_mongosh_js", lambda **k: "")
    assert rtt._rs_status("d1", 27017) == {"primary": "", "healthy": 0, "total": 0}


def test_rs_status_probe_error_returns_zeros(monkeypatch):
    def _boom(**k):
        raise RuntimeError("connection refused")

    monkeypatch.setattr(rtt.config, "invoke_mongosh_js", _boom)
    assert rtt._rs_status("d1", 27017) == {"primary": "", "healthy": 0, "total": 0}
