#!/usr/bin/env python3
"""Replace the pinned Python generator's per-byte ctypes buffer writer."""

from __future__ import annotations

import argparse
import ast
import hashlib
from pathlib import Path


BUILDER_SHA256 = "a64e20b65ffa65e45d63c3633f2faa7c439a323efba5b36f01f77aaeba95a8a0"
PREFIX = '''class _UniffiRustBufferBuilder:
    """Stage serialized bytes before transferring one Rust-owned buffer."""

    def __init__(self):
        self._bytes = bytearray()

    def finalize(self):
        if self._bytes is None:
            raise RuntimeError("Rust buffer builder already finalized or discarded")
        data = bytes(self._bytes)
        rbuf = _UniffiRustBuffer.alloc(len(data))
        try:
            if data:
                ctypes.memmove(rbuf.data, data, len(data))
            rbuf.len = len(data)
        except BaseException:
            rbuf.free()
            raise
        self._bytes = None
        return rbuf

    def discard(self):
        self._bytes = None

    def _pack_into(self, size, format, value):
        data = struct.pack(format, value)
        if len(data) != size:
            raise ValueError("packed size does not match generated writer")
        self.write(data)

    def write(self, value):
        if self._bytes is None:
            raise RuntimeError("Rust buffer builder already finalized or discarded")
        self._bytes.extend(value)

'''


def optimize(source: str) -> str:
    tree = ast.parse(source)
    builders = [node for node in tree.body
                if isinstance(node, ast.ClassDef) and node.name == "_UniffiRustBufferBuilder"]
    if len(builders) != 1:
        raise ValueError("expected exactly one generated Python Rust buffer builder")
    builder = builders[0]
    lines = source.splitlines(keepends=True)
    original = "".join(lines[builder.lineno - 1:builder.end_lineno])
    if hashlib.sha256(original.encode()).hexdigest() != BUILDER_SHA256:
        raise ValueError("unexpected Python buffer builder; review the pinned generator before updating")
    for node in ast.walk(tree):
        if (isinstance(node, ast.Attribute)
                and not builder.lineno <= node.lineno <= builder.end_lineno
                and node.attr in ("rbuf", "_reserve", "_pack_into")):
            raise ValueError(f"unexpected external buffer-builder access: {node.attr}")
    first_scalar = next(node for node in builder.body
                        if isinstance(node, ast.FunctionDef) and node.name == "write_i8")
    replacement = PREFIX + "".join(lines[first_scalar.lineno - 1:builder.end_lineno])
    result = "".join(lines[:builder.lineno - 1]) + replacement + "".join(lines[builder.end_lineno:])
    ast.parse(result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binding", type=Path)
    args = parser.parse_args()
    try:
        source = args.binding.read_text(encoding="utf-8")
        result = optimize(source)
    except (OSError, ValueError, SyntaxError) as error:
        parser.exit(1, f"Python buffer optimization failed: {error}\n")
    args.binding.write_text(result, encoding="utf-8")


if __name__ == "__main__":
    main()
