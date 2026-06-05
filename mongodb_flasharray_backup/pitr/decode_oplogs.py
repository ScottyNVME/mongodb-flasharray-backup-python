#!/usr/bin/env python3
"""Decode an Ops Manager .oplogs file to raw oplog BSON on stdout.

An .oplogs file has the structure:
  [file-header BSON doc]  {version, encrypted, hashed_encryption_key}
  For each block:
    [block-header BSON doc]  {start, end, uncompressed_size, size, encoding:"snappy"}
    [snappy-compressed oplog BSON bytes, length = size]

The decompressed bytes are concatenated raw BSON oplog documents suitable
for mongorestore --oplogReplay.

Usage:  python3 decode_oplogs.py <file.oplogs>  (writes to stdout)
"""
import sys
import struct
import ctypes
import ctypes.util

def _load_snappy():
    for name in ('snappy', 'libsnappy.so.1', 'libsnappy.so.1.1.8'):
        path = ctypes.util.find_library(name) or name
        try:
            lib = ctypes.CDLL(path)
            # quick sanity check
            lib.snappy_uncompressed_length
            return lib
        except (OSError, AttributeError):
            pass
    # last-resort hard path for RHEL/Rocky 9
    return ctypes.CDLL('/usr/lib64/libsnappy.so.1.1.8')

lib = _load_snappy()
lib.snappy_uncompressed_length.restype  = ctypes.c_int
lib.snappy_uncompressed_length.argtypes = [
    ctypes.c_char_p, ctypes.c_size_t, ctypes.POINTER(ctypes.c_size_t)]
lib.snappy_uncompress.restype  = ctypes.c_int
lib.snappy_uncompress.argtypes = [
    ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p, ctypes.POINTER(ctypes.c_size_t)]


def doc_size(data, off):
    """Return the BSON document size (little-endian int32) at offset."""
    return struct.unpack_from('<i', data, off)[0]


def find_int32(doc, key):
    """Find the value of an int32 field by key name in a BSON document."""
    needle = b'\x10' + key.encode() + b'\x00'
    i = doc.find(needle)
    if i < 0:
        return None
    return struct.unpack_from('<i', doc, i + len(needle))[0]


def decompress_block(compressed):
    out_len = ctypes.c_size_t(0)
    rc = lib.snappy_uncompressed_length(compressed, len(compressed), ctypes.byref(out_len))
    if rc != 0:
        raise RuntimeError(f'snappy_uncompressed_length returned {rc}')
    buf = ctypes.create_string_buffer(out_len.value)
    rc = lib.snappy_uncompress(compressed, len(compressed), buf, ctypes.byref(out_len))
    if rc != 0:
        raise RuntimeError(f'snappy_uncompress returned {rc}')
    return buf.raw[:out_len.value]


def main():
    if len(sys.argv) != 2:
        print(f'Usage: {sys.argv[0]} <file.oplogs>', file=sys.stderr)
        sys.exit(1)

    with open(sys.argv[1], 'rb') as f:
        data = f.read()

    off = 0

    # Skip the file-level header document (version, encrypted, hashed_encryption_key)
    off += doc_size(data, off)

    # Process one or more data blocks
    while off < len(data):
        bh_size = doc_size(data, off)
        block_header = data[off:off + bh_size]
        comp_size = find_int32(block_header, 'size')
        if comp_size is None or comp_size <= 0:
            sys.exit(f'Could not read "size" from block header at offset {off}')
        off += bh_size

        compressed = data[off:off + comp_size]
        raw_bson = decompress_block(compressed)
        sys.stdout.buffer.write(raw_bson)
        off += comp_size


if __name__ == '__main__':
    main()
