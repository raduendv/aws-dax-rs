#!/usr/bin/env python3
"""Static assertions for assembled FFI package layouts."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import tarfile
import zipfile


ROOT = pathlib.Path(__file__).resolve().parents[2]
METADATA = json.loads(
    (ROOT / "src/aws-dax-ffi/packaging/package-metadata.json").read_text()
)
VERSION = "0.1.0"


def zip_entries(path: pathlib.Path) -> dict[str, bytes]:
    with zipfile.ZipFile(path) as archive:
        return {name: archive.read(name) for name in archive.namelist()}


def assert_contains(payload: bytes, expected: str, context: str) -> None:
    assert expected.encode() in payload, f"{context} does not contain {expected!r}"


def validate_packages(
    root: pathlib.Path,
    target: str,
    expected_native: bytes,
    expected_go_native: bytes,
) -> None:
    config = METADATA["packaging_platforms"][target]
    native = config["native_filename"]
    go_native_name = config["go_native_filename"]

    go = zip_entries(root / "go" / f"aws-dax-ffi-go-{VERSION}.zip")
    go_native = f"aws_dax_ffi/native/{target}/{go_native_name}"
    assert go[go_native] == expected_go_native
    go_linkage = go[f"aws_dax_ffi/{config['go_source']}"]
    assert_contains(go_linkage, f"//go:build {config['go_build_constraint']}", "Go linkage")
    assert_contains(go_linkage, str(config["go_ldflags"]), "Go linkage")

    dotnet = zip_entries(root / "dotnet" / f"Aws.Dax.Ffi.{VERSION}.nupkg")
    assert dotnet[f"runtimes/{config['dotnet_rid']}/native/{native}"] == expected_native

    java = zip_entries(root / "java" / f"aws-dax-ffi-{VERSION}.jar")
    assert java[f"native/{target}/{native}"] == expected_native
    assert_contains(
        java["software/amazon/dax/DaxPackageNative.class"],
        f"/native/{target}/{native}",
        "Java loader",
    )
    java_sources = zip_entries(root / "java" / f"aws-dax-ffi-{VERSION}-sources.jar")
    assert_contains(
        java_sources["software/amazon/dax/DaxPackageNative.java"],
        f"/native/{target}/{native}",
        "Java loader source",
    )

    kotlin = zip_entries(root / "kotlin" / f"aws-dax-ffi-kotlin-{VERSION}.jar")
    assert kotlin[f"native/{target}/{native}"] == expected_native
    assert_contains(
        kotlin["software/amazon/dax/DaxPackageNative.class"],
        f"/native/{target}/{native}",
        "Kotlin/JVM loader",
    )

    wheel_tag = config["python_wheel_tag"]
    wheel_path = root / "python" / f"aws_dax_ffi-{VERSION}-py3-none-{wheel_tag}.whl"
    python = zip_entries(wheel_path)
    assert python[f"aws_dax_ffi/native/{target}/{native}"] == expected_native
    assert_contains(
        python["aws_dax_ffi/__init__.py"],
        f"native/{target}/{native}",
        "Python loader",
    )
    assert_contains(
        python[f"aws_dax_ffi-{VERSION}.dist-info/WHEEL"],
        f"Tag: py3-none-{wheel_tag}",
        "wheel metadata",
    )

    with tarfile.open(root / "javascript" / f"aws-dax-ffi-{VERSION}.tgz", "r:gz") as archive:
        javascript = {
            member.name: archive.extractfile(member).read()
            for member in archive.getmembers()
            if member.isfile()
        }
    assert javascript[f"package/native/{target}/{native}"] == expected_native
    package_json = json.loads(javascript["package/package.json"])
    assert package_json["os"] == [config["npm_os"]]
    assert package_json["cpu"] == [config["npm_cpu"]]
    assert_contains(
        javascript["package/dist/index.js"],
        f"../native/{target}/{native}",
        "Node/N-API loader",
    )

    for ecosystem in ("go", "dotnet", "java", "kotlin", "python", "javascript"):
        metadata_name = (
            "aws_dax_ffi_package.json"
            if ecosystem != "python"
            else "aws_dax_ffi/aws_dax_ffi_package.json"
        )
        if ecosystem == "go":
            payload = go[metadata_name]
        elif ecosystem == "dotnet":
            payload = dotnet[metadata_name]
        elif ecosystem == "java":
            payload = java[metadata_name]
        elif ecosystem == "kotlin":
            payload = kotlin[metadata_name]
        elif ecosystem == "python":
            payload = python[metadata_name]
        else:
            payload = javascript[f"package/{metadata_name}"]
        metadata = json.loads(payload)
        assert metadata["native_platform"] == target
        packaged_native = expected_go_native if ecosystem == "go" else expected_native
        packaged_name = go_native_name if ecosystem == "go" else native
        assert metadata["native_filename"] == packaged_name
        assert metadata["native_sha256"] == hashlib.sha256(packaged_native).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("artifacts", type=pathlib.Path)
    parser.add_argument("native", type=pathlib.Path)
    parser.add_argument("--platform", default="linux-x86_64", choices=METADATA["packaging_platforms"])
    parser.add_argument("--go-native", type=pathlib.Path)
    args = parser.parse_args()
    go_native = args.go_native or args.native
    validate_packages(
        args.artifacts,
        args.platform,
        args.native.read_bytes(),
        go_native.read_bytes(),
    )
    print(f"DAX_FFI_PACKAGE_LAYOUT platform={args.platform} status=ok")


if __name__ == "__main__":
    main()
