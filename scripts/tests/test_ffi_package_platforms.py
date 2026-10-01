#!/usr/bin/env python3
"""Validate every Phase 8 package platform and native binary architecture."""

from __future__ import annotations

import importlib.util
import json
import pathlib
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[2]
ASSEMBLER_PATH = ROOT / "scripts" / "assemble_ffi_packages.py"
SPEC = importlib.util.spec_from_file_location("assemble_ffi_packages", ASSEMBLER_PATH)
assert SPEC is not None and SPEC.loader is not None
ASSEMBLER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ASSEMBLER)


def elf(machine: int) -> bytes:
    return b"\x7fELF\x02\x01" + (b"\0" * 12) + machine.to_bytes(2, "little")


def macho(cpu: int) -> bytes:
    return b"\xcf\xfa\xed\xfe" + cpu.to_bytes(4, "little")


def pe(machine: int) -> bytes:
    payload = bytearray(0x86)
    payload[:2] = b"MZ"
    payload[0x3C:0x40] = (0x80).to_bytes(4, "little")
    payload[0x80:0x84] = b"PE\0\0"
    payload[0x84:0x86] = machine.to_bytes(2, "little")
    return bytes(payload)


class PackagePlatformTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temporary.name)

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def validate(self, target: str, name: str, payload: bytes) -> None:
        native = self.root / name
        native.write_bytes(payload)
        ASSEMBLER.validate_native_library(native, target)

    def assert_rejected(self, target: str, name: str, payload: bytes) -> None:
        with self.assertRaises(SystemExit):
            self.validate(target, name, payload)

    def test_metadata_covers_exact_phase_eight_matrix(self) -> None:
        metadata = json.loads(
            (ROOT / "src/aws-dax-ffi/packaging/package-metadata.json").read_text()
        )
        self.assertEqual(
            set(metadata["packaging_platforms"]),
            set(metadata["phase8_native_targets"]),
        )
        for platform in metadata["packaging_platforms"].values():
            self.assertIn("go_native_filename", platform)
            self.assertIn("dotnet_rid", platform)
            self.assertIn("python_wheel_tag", platform)
            self.assertIn("npm_os", platform)
            self.assertIn("npm_cpu", platform)

    def test_accepts_each_expected_native_architecture(self) -> None:
        self.validate("linux-x86_64", "libaws_dax_ffi.so", elf(62))
        self.validate("linux-aarch64", "libaws_dax_ffi.so", elf(183))
        self.validate("osx-x86_64", "libaws_dax_ffi.dylib", macho(0x01000007))
        self.validate("osx-arm64", "libaws_dax_ffi.dylib", macho(0x0100000C))
        self.validate("windows-x86_64", "aws_dax_ffi.dll", pe(0x8664))

    def test_rejects_relabelled_or_wrong_architecture_binaries(self) -> None:
        self.assert_rejected("linux-aarch64", "libaws_dax_ffi.so", elf(62))
        self.assert_rejected("osx-x86_64", "libaws_dax_ffi.dylib", macho(0x0100000C))
        self.assert_rejected("windows-x86_64", "aws_dax_ffi.dll", elf(62))
        self.assert_rejected("linux-x86_64", "libaws_dax_ffi.so", pe(0x8664))


if __name__ == "__main__":
    unittest.main()
