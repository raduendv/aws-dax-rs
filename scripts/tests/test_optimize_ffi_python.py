#!/usr/bin/env python3
"""Pinned-template and Rust-buffer ownership tests, without a native dependency."""

import ast
import ctypes
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from optimize_ffi_python import optimize

FIXTURE = (Path(__file__).parent / "fixtures/python_buffer_builder.py").read_text()


class WriterTests(unittest.TestCase):
    def setUp(self):
        self.allocations = []
        self.frees = []

        def allocate(size):
            storage = ctypes.create_string_buffer(max(size, 1))
            buffer = SimpleNamespace(
                storage=storage, data=ctypes.cast(storage, ctypes.POINTER(ctypes.c_char)),
                len=0, free=lambda: self.frees.append(size))
            self.allocations.append(size)
            return buffer

        self.namespace = {"ctypes": ctypes, "struct": struct,
                          "_UniffiRustBuffer": SimpleNamespace(alloc=allocate)}
        exec(compile(optimize(FIXTURE), "<optimized-builder>", "exec"), self.namespace)
        self.builder = self.namespace["_UniffiRustBufferBuilder"]

    def test_determinism_and_fail_closed_template_checks(self):
        self.assertEqual(optimize(FIXTURE), optimize(FIXTURE))
        for source in ("", FIXTURE + "\n" + FIXTURE,
                       FIXTURE.replace("alloc(16)", "alloc(32)"),
                       optimize(FIXTURE), FIXTURE + "\nexternal.rbuf\n",
                       FIXTURE + "\nexternal._reserve(1)\n"):
            with self.subTest(source=source[:50]), self.assertRaises(ValueError):
                optimize(source)
        source = "import struct\n\n" + FIXTURE + "\n\nclass Unrelated:\n    pass\n"
        result = optimize(source)
        self.assertTrue(result.startswith("import struct\n\n"))
        self.assertTrue(result.endswith("\n\nclass Unrelated:\n    pass\n"))
        ast.parse(result)

    def test_cli_preserves_file_when_generator_template_changes(self):
        with tempfile.TemporaryDirectory() as folder:
            binding = Path(folder) / "aws_dax_ffi.py"
            original = FIXTURE.replace("alloc(16)", "alloc(32)")
            binding.write_text(original)
            result = subprocess.run(
                [sys.executable, str(Path(__file__).resolve().parents[1] / "optimize_ffi_python.py"),
                 str(binding)], capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("unexpected Python buffer builder", result.stderr)
            self.assertEqual(binding.read_text(), original)

    def test_all_scalar_writers_preserve_byte_order(self):
        writer = self.builder()
        expected = bytearray()
        for suffix, fmt, values in (
            ("i8", ">b", [-128, 127]), ("u8", ">B", [0, 255]),
            ("i16", ">h", [-32768, 32767]), ("u16", ">H", [0, 65535]),
            ("i32", ">i", [-(2**31), 2**31 - 1]), ("u32", ">I", [0, 2**32 - 1]),
            ("i64", ">q", [-(2**63), 2**63 - 1]), ("u64", ">Q", [0, 2**64 - 1]),
            ("float", ">f", [-1.5, 0.0]), ("double", ">d", [1.5, float("inf")]),
            ("c_size_t", "@N", [0, 42]),
        ):
            for value in values:
                getattr(writer, "write_" + suffix)(value)
                expected.extend(struct.pack(fmt, value))
        writer.write(b"\0\xff" + b"large" * 10000)
        expected.extend(b"\0\xff" + b"large" * 10000)
        self.assertEqual(self.allocations, [])
        buffer = writer.finalize()
        self.assertEqual(self.allocations, [len(expected)])
        self.assertEqual(ctypes.string_at(buffer.data, buffer.len), bytes(expected))
        buffer.free()
        self.assertEqual(self.frees, [len(expected)])

    def test_empty_finalize_and_discard_are_safe(self):
        writer = self.builder()
        writer.write(b"")
        buffer = writer.finalize()
        self.assertEqual(buffer.len, 0)
        self.assertEqual(self.allocations, [0])
        writer.discard()
        writer.discard()
        with self.assertRaises(RuntimeError):
            writer.finalize()
        with self.assertRaises(RuntimeError):
            writer.write(b"after finalize")
        discarded = self.builder()
        discarded.write(b"abandoned")
        discarded.discard()
        with self.assertRaises(RuntimeError):
            discarded.finalize()
        self.assertEqual(self.allocations, [0])

    def test_conversion_failure_does_not_allocate_or_accept_partial_scalar(self):
        writer = self.builder()
        writer.write(b"prefix")
        with self.assertRaises(struct.error):
            writer.write_i32(2**40)
        with self.assertRaisesRegex(ValueError, "packed size"):
            writer._pack_into(8, ">i", 1)
        buffer = writer.finalize()
        self.assertEqual(ctypes.string_at(buffer.data, buffer.len), b"prefix")
        self.assertEqual(self.allocations, [6])

    def test_failed_copy_frees_allocation(self):
        writer = self.builder()
        writer.write(b"abc")
        with patch.object(ctypes, "memmove", side_effect=RuntimeError("injected")):
            with self.assertRaisesRegex(RuntimeError, "injected"):
                writer.finalize()
        writer.discard()
        self.assertEqual(self.allocations, [3])
        self.assertEqual(self.frees, [3])

    def test_allocation_failure_does_not_free_unowned_memory(self):
        writer = self.builder()
        writer.write(b"abc")
        with patch.object(self.namespace["_UniffiRustBuffer"], "alloc",
                          side_effect=MemoryError("injected")):
            with self.assertRaises(MemoryError):
                writer.finalize()
        writer.discard()
        self.assertEqual(self.frees, [])


if __name__ == "__main__":
    unittest.main()
