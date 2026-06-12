"""Unit tests for config.py.

These cover the pure logic that needs no live cluster/array: Digest hashing, the .env loader, script
locking (incl. stale-PID reclaim and the O_EXCL race), the parallel runner's failure aggregation, and the
SCSI-serial selection logic of resolve_node_to_array_volume_map.
"""

import os
import sys
from pathlib import Path

import pytest

# Make the package importable when running from the repo root without an editable install.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mongodb_flasharray_backup import config  # noqa: E402


# --------------------------------------------------------------------------------------------------------
# MD5 hashing
# --------------------------------------------------------------------------------------------------------
def test_get_md5_hash_known_vectors():
    # RFC 1321 / well-known MD5 hex digests (lowercase).
    assert config.get_md5_hash("") == "d41d8cd98f00b204e9800998ecf8427e"
    assert config.get_md5_hash("abc") == "900150983cd24fb0d6963f7d28e17f72"


# --------------------------------------------------------------------------------------------------------
# .env loader
# --------------------------------------------------------------------------------------------------------
def _write_full_env(p: Path):
    p.write_text(
        "# comment line, skipped\n"
        "   \n"  # blank-ish line, skipped (first non-space char rule)
        "MONGOSH_PATH=/usr/bin/mongosh\n"
        "MONGOS_HOST=mongos.example.com\n"
        "MONGOS_PORT=27017\n"
        "SSH_USER=ec2-user\n"
        "MONGO_TOOLS_BASE=/opt/mongo-tools\n"
        "FA_ENDPOINT=10.0.0.1\n"
        "FA_USERNAME=pureuser\n"
        "FA_APITOKEN=secret=with=equals\n"  # value contains '=' -> split on first only
        "FA_PROTECTION_GROUP=mongo-pg\n"
        "FA_CLUSTER_NAME=aen\n"
        "OM_BASE_URL=http://om.example.com:8080/\n"  # trailing slash trimmed
        "OM_API_VERSION=v1.0\n"
        "OM_GROUP_ID=grp1\n"
        "OM_CLUSTER_ID=cl1\n"
        "OM_PUBLIC_KEY=pub\n"
        "OM_PRIVATE_KEY=priv\n"
    )


def test_load_config_full(tmp_path):
    env = tmp_path / ".env"
    _write_full_env(env)
    cfg = config.load_config(env)
    assert cfg.MongoshPath == "/usr/bin/mongosh"
    assert cfg.MongosPort == 27017
    assert cfg.MongodumpPath == "/opt/mongo-tools/mongodump"
    assert cfg.MongorestorePath == "/opt/mongo-tools/mongorestore"
    # value with '=' preserved after first split (split('=', 1))
    assert cfg.FaApiToken == "secret=with=equals"
    # OM base URL trailing slash trimmed + version appended
    assert cfg.OpsManagerBaseUrl == "http://om.example.com:8080/api/public/v1.0"
    # CLUSTER_NODES optional + absent -> None fallback
    assert cfg.ClusterNodesFallback is None


def test_load_config_cluster_nodes_split(tmp_path):
    env = tmp_path / ".env"
    _write_full_env(env)
    with env.open("a") as f:
        f.write("CLUSTER_NODES=a.example.com,b.example.com,c.example.com\n")
    cfg = config.load_config(env)
    assert cfg.ClusterNodesFallback == ["a.example.com", "b.example.com", "c.example.com"]


def test_load_config_missing_required_key_raises(tmp_path):
    env = tmp_path / ".env"
    env.write_text("MONGOSH_PATH=/usr/bin/mongosh\n")  # everything else missing
    with pytest.raises(RuntimeError, match="missing required key"):
        config.load_config(env)


def test_load_config_missing_file_raises(tmp_path):
    with pytest.raises(RuntimeError, match="\\.env file not found"):
        config.load_config(tmp_path / "does-not-exist.env")


# --------------------------------------------------------------------------------------------------------
# Script locking
# --------------------------------------------------------------------------------------------------------
def test_lock_acquire_and_live_holder_blocks(tmp_path):
    lock = str(tmp_path / "x.lock")
    config.new_script_lock(lock)
    assert Path(lock).exists()
    # The lock file records our own (live) PID -> a second acquire must refuse.
    with pytest.raises(RuntimeError, match="Lock held by live PID"):
        config.new_script_lock(lock)
    config.remove_script_lock(lock)
    assert not Path(lock).exists()
    # idempotent removal
    config.remove_script_lock(lock)


def test_lock_stale_pid_is_reclaimed(tmp_path):
    lock = Path(tmp_path / "stale.lock")
    # Write a lock owned by a PID that is almost certainly dead.
    lock.write_text("pid=999999999\nhost=ghost\nstarted=2020-01-01T00:00:00+00:00\n")
    # Should detect the stale PID, remove it, and acquire cleanly.
    config.new_script_lock(str(lock))
    assert lock.exists()
    body = lock.read_text()
    assert f"pid={os.getpid()}" in body
    config.remove_script_lock(str(lock))


def test_lock_unparseable_pid_raises(tmp_path):
    lock = Path(tmp_path / "bad.lock")
    lock.write_text("nothing-useful-here\n")
    with pytest.raises(RuntimeError, match="no parseable PID"):
        config.new_script_lock(str(lock))


# --------------------------------------------------------------------------------------------------------
# invoke_parallel_or_throw
# --------------------------------------------------------------------------------------------------------
def test_parallel_success_passthrough():
    items = [1, 2, 3]
    res = config.invoke_parallel_or_throw(
        items, lambda n: {"Success": True, "Message": "ok", "Node": f"n{n}"}, "Step", throttle_limit=3
    )
    assert len(res) == 3
    assert all(r["Success"] for r in res)


def test_parallel_aggregates_failures():
    items = ["a", "b", "c"]

    def block(x):
        if x == "b":
            return {"Success": False, "Message": "boom", "Node": x}
        return {"Success": True, "Message": "ok", "Node": x}

    with pytest.raises(RuntimeError) as ei:
        config.invoke_parallel_or_throw(items, block, "MyStep", throttle_limit=3)
    msg = str(ei.value)
    assert "MyStep failed on 1 item(s)" in msg
    assert "[b] boom" in msg


def test_parallel_uses_key_when_no_node():
    with pytest.raises(RuntimeError, match=r"\[K1\] nope"):
        config.invoke_parallel_or_throw(
            ["z"], lambda x: {"Success": False, "Message": "nope", "Key": "K1"}, "S"
        )


# --------------------------------------------------------------------------------------------------------
# resolve_node_to_array_volume_map serial selection (subprocess + _fa mocked)
# --------------------------------------------------------------------------------------------------------
class _FakeVol:
    def __init__(self, name):
        self.name = name


def test_resolve_node_to_array_volume_map_selects_last_valid_serial(tmp_path, monkeypatch):
    # Need a loaded CFG for write_host etc.; load_config not strictly required here but harmless.
    serial = "624A9370ABCDEF0123456789"  # 24 hex chars, FA NAA format

    class _Proc:
        # findmnt/lsblk output: a noise line then the real serial.
        stdout = f"ok\n{serial}\n"

    monkeypatch.setattr(config.subprocess, "run", lambda *a, **k: _Proc())

    captured = {}

    def fake_fa(resp, allow_error=False):
        # resp is whatever fake fa.get_volumes returned; here it's the filter string.
        captured["filter"] = resp
        return [_FakeVol("mongo-vol-1")]

    monkeypatch.setattr(config, "_fa", fake_fa)

    class _FakeFA:
        def get_volumes(self, context_names=None, filter=None):
            return filter  # passed straight to fake_fa

    result = config.resolve_node_to_array_volume_map(
        _FakeFA(), ["node1.example.com"], "ec2-user", config.SSH_OPTS, ["arr-07"]
    )
    assert result == {
        "node1.example.com": {"ShortName": "arr-07", "VolumeName": "mongo-vol-1", "Serial": serial.lower()}
    }
    # serial lower-cased then upper-cased in the FA filter (FA stores uppercase hex)
    assert captured["filter"] == f"serial='{serial.upper()}'"


def test_resolve_node_to_array_volume_map_bad_serial_raises(monkeypatch):
    class _Proc:
        stdout = "garbage\nok\n"  # nothing matches the 20+ hex rule

    monkeypatch.setattr(config.subprocess, "run", lambda *a, **k: _Proc())
    monkeypatch.setattr(config, "time", type("T", (), {"sleep": staticmethod(lambda s: None)}))

    class _FakeFA:
        def get_volumes(self, **k):
            return None

    with pytest.raises(RuntimeError, match="Could not read FA volume serial"):
        config.resolve_node_to_array_volume_map(_FakeFA(), ["n1"], "u", config.SSH_OPTS, ["c"])


# --------------------------------------------------------------------------------------------------------
# Tag-based volume-map parsing (parse_volume_map_tags) — pure logic, no live array.
# --------------------------------------------------------------------------------------------------------
class _TagRow:
    """Mimics a FA GET /volumes/tags row: .resource (volume name), .key, .value."""
    def __init__(self, resource, key, value):
        self.resource, self.key, self.value = resource, key, value


def test_parse_volume_map_tags_groups_and_filters_by_deployment():
    rows = [
        _TagRow("aen-mongo-01-data", config.VOLMAP_TAG_DEPLOYMENT, "aen-cluster"),
        _TagRow("aen-mongo-01-data", config.VOLMAP_TAG_NODE, "aen-mongo-01"),
        _TagRow("aen-mongo-01-data", config.VOLMAP_TAG_SERIAL, "1071bf0a0a224a050019bf3b"),
        # different deployment -> excluded
        _TagRow("aen-mongo-09-data", config.VOLMAP_TAG_DEPLOYMENT, "other-dep"),
        _TagRow("aen-mongo-09-data", config.VOLMAP_TAG_NODE, "aen-mongo-09"),
        # non-mongo tag -> ignored
        _TagRow("vol-x", "purity:thing", "ignored"),
    ]
    m = config.parse_volume_map_tags(rows, "sn1-x90r2-f07-27", "aen-cluster")
    assert m == {
        "aen-mongo-01": [{
            "ShortName": "sn1-x90r2-f07-27",
            "VolumeName": "aen-mongo-01-data",
            "Serial": "1071bf0a0a224a050019bf3b",
            "PvIndex": 0,
        }]
    }


def test_parse_volume_map_tags_empty_and_missing_node():
    assert config.parse_volume_map_tags([], "arr", "dep") == {}
    # deployment matches but no node tag -> skipped
    rows = [_TagRow("v1", config.VOLMAP_TAG_DEPLOYMENT, "dep")]
    assert config.parse_volume_map_tags(rows, "arr", "dep") == {}


# --------------------------------------------------------------------------------------------------------
# Multi-volume discovery parser (parse_fa_volume_serials) — pure, no live node.
# --------------------------------------------------------------------------------------------------------
def test_parse_fa_volume_serials_single_volume_prdm():
    # lsblk -s -rno TYPE,NAME,SERIAL,WWN for /data/mongo on a single pRDM (/dev/sdb1 -> sdb).
    out = (
        "part sdb1\n"
        "disk sdb 1071bf0a0a224a050019bf3b 0x624a93701071bf0a0a224a050019bf3b\n"
    )
    assert config.parse_fa_volume_serials(out) == ["1071bf0a0a224a050019bf3b"]


def test_parse_fa_volume_serials_multivolume_lvm_multipath():
    # LVM VG over two FA volumes, each a multipath device with two paths (modeled on the KVM customer:
    # WWN 0x624a9370<serial> on the mpath dm; bare 24-hex SERIAL on each sd path; de-dup to 2 volumes).
    out = (
        "lvm  vg_database-lv_u01data\n"
        "lvm  vg_database-lv_u01data\n"
        "mpath mpatha  0x624a937033f4fcdef857436f000339e6\n"
        "disk  sdc 33f4fcdef857436f000339e6\n"
        "disk  sdd 33f4fcdef857436f000339e6\n"
        "mpath mpathb  0x624a937033f4fcdef857436f000339e7\n"
        "disk  sde 33f4fcdef857436f000339e7\n"
    )
    assert config.parse_fa_volume_serials(out) == [
        "33f4fcdef857436f000339e6",
        "33f4fcdef857436f000339e7",
    ]


def test_parse_fa_volume_serials_ignores_non_fa_rows_and_empties():
    out = "lvm vg-lv\nvg  vg\ndisk sda ATA_SOMEDISK_123\n\n"
    assert config.parse_fa_volume_serials(out) == []
