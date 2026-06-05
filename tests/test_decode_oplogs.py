"""Round-trip test for the shipped-verbatim pitr/decode_oplogs.py.

Builds a synthetic OM .oplogs file (file-header doc + one snappy block) and confirms the decoder reproduces
the original raw oplog BSON on stdout. Skipped if libsnappy is not available on the host.
"""

import ctypes
import importlib.util
import struct
import subprocess
import sys
from pathlib import Path

import pytest

DECODER = Path(__file__).resolve().parents[1] / "mongodb_flasharray_backup" / "pitr" / "decode_oplogs.py"


def _load_decoder_module():
    spec = importlib.util.spec_from_file_location("decode_oplogs_under_test", DECODER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # runs _load_snappy() at import; raises if libsnappy missing
    return mod


@pytest.fixture(scope="module")
def snappy_compress():
    try:
        mod = _load_decoder_module()
    except OSError:
        pytest.skip("libsnappy not available on this host")
    lib = mod.lib
    lib.snappy_max_compressed_length.restype = ctypes.c_size_t
    lib.snappy_max_compressed_length.argtypes = [ctypes.c_size_t]
    lib.snappy_compress.restype = ctypes.c_int
    lib.snappy_compress.argtypes = [
        ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p, ctypes.POINTER(ctypes.c_size_t)
    ]

    def _compress(data: bytes) -> bytes:
        out_len = ctypes.c_size_t(lib.snappy_max_compressed_length(len(data)))
        buf = ctypes.create_string_buffer(out_len.value)
        rc = lib.snappy_compress(data, len(data), buf, ctypes.byref(out_len))
        assert rc == 0
        return buf.raw[: out_len.value]

    return _compress


def _bson_block_header(comp_size: int) -> bytes:
    # Minimal BSON document carrying an int32 field "size" = comp_size.
    # element: type 0x10 (int32) + key "size\x00" + 4-byte LE value
    element = b"\x10" + b"size\x00" + struct.pack("<i", comp_size)
    body = element + b"\x00"  # document terminator
    total_len = 4 + len(body)
    return struct.pack("<i", total_len) + body


def test_decode_oplogs_roundtrip(tmp_path, snappy_compress):
    # The "raw oplog BSON" payload (decoder is byte-transparent; any bytes round-trip).
    payload = b"\x16\x00\x00\x00\x02hello\x00\x06\x00\x00\x00world\x00\x00"  # a tiny valid-ish BSON doc
    compressed = snappy_compress(payload)

    file_header = b"\x05\x00\x00\x00\x00"  # empty BSON doc (len 5) — decoder skips by size
    block_header = _bson_block_header(len(compressed))
    data = file_header + block_header + compressed

    oplogs = tmp_path / "00001_00002.oplogs"
    oplogs.write_bytes(data)

    proc = subprocess.run([sys.executable, str(DECODER), str(oplogs)], capture_output=True)
    assert proc.returncode == 0, proc.stderr.decode(errors="replace")
    assert proc.stdout == payload


def test_decode_oplogs_multi_block(tmp_path, snappy_compress):
    payloads = [b"AAAA" * 10, b"BBBBBB" * 7]
    file_header = b"\x05\x00\x00\x00\x00"
    data = bytearray(file_header)
    for p in payloads:
        c = snappy_compress(p)
        data += _bson_block_header(len(c))
        data += c
    oplogs = tmp_path / "00003_00004.oplogs"
    oplogs.write_bytes(bytes(data))

    proc = subprocess.run([sys.executable, str(DECODER), str(oplogs)], capture_output=True)
    assert proc.returncode == 0, proc.stderr.decode(errors="replace")
    assert proc.stdout == b"".join(payloads)
