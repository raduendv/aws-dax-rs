#!/usr/bin/env python3
"""Assemble deterministic, host-specific packages from pinned UniFFI output."""

from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import json
import os
import pathlib
import platform
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
import zipfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
FFI = ROOT / "src" / "aws-dax-ffi"
METADATA = json.loads((FFI / "packaging" / "package-metadata.json").read_text())
EPOCH = (1980, 1, 1, 0, 0, 0)


def platform_metadata(target: str) -> dict[str, object]:
    try:
        return METADATA["packaging_platforms"][target]
    except KeyError:
        supported = ", ".join(sorted(METADATA["packaging_platforms"]))
        raise SystemExit(f"unsupported packaging platform {target!r}; expected one of: {supported}") from None


def host_platform() -> str:
    system, machine = platform.system(), platform.machine().lower()
    machine = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "x86_64"}.get(machine, machine)
    if system == "Darwin":
        return f"osx-{machine}"
    if system == "Linux":
        return f"linux-{machine}"
    if system == "Windows":
        return f"windows-{machine}"
    raise SystemExit(f"unsupported host platform: {system}/{machine}")


def native_name(target: str) -> str:
    return str(platform_metadata(target)["native_filename"])


def validate_native_library(path: pathlib.Path, target: str) -> None:
    if path.name != native_name(target):
        raise SystemExit(
            f"native library for {target} must be named {native_name(target)}, not {path.name}"
        )
    if target.startswith("linux-"):
        header = path.read_bytes()[:20]
        if len(header) < 20 or header[:4] != b"\x7fELF":
            raise SystemExit(f"native library for {target} is not an ELF binary: {path}")
        if header[4:6] != b"\x02\x01":
            raise SystemExit(f"native library for {target} must be a 64-bit little-endian ELF binary: {path}")
        expected_machine = {"linux-x86_64": 62, "linux-aarch64": 183}[target]
        if int.from_bytes(header[18:20], "little") != expected_machine:
            raise SystemExit(f"native library has the wrong ELF architecture for {target}: {path}")
    elif target.startswith("osx-"):
        header = path.read_bytes()[:8]
        if len(header) < 8 or header[:4] != b"\xcf\xfa\xed\xfe":
            raise SystemExit(f"native library for {target} is not a 64-bit little-endian Mach-O binary: {path}")
        expected_cpu = {"osx-x86_64": 0x01000007, "osx-arm64": 0x0100000C}[target]
        if int.from_bytes(header[4:8], "little") != expected_cpu:
            raise SystemExit(f"native library has the wrong Mach-O architecture for {target}: {path}")
    elif target == "windows-x86_64":
        payload = path.read_bytes()
        if len(payload) < 0x40 or payload[:2] != b"MZ":
            raise SystemExit(f"native library for {target} is not a PE binary: {path}")
        pe_offset = int.from_bytes(payload[0x3C:0x40], "little")
        if len(payload) < pe_offset + 6 or payload[pe_offset:pe_offset + 4] != b"PE\0\0":
            raise SystemExit(f"native library for {target} has an invalid PE header: {path}")
        if int.from_bytes(payload[pe_offset + 4:pe_offset + 6], "little") != 0x8664:
            raise SystemExit(f"native library for {target} is not an x86-64 PE binary: {path}")


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def copy(source: pathlib.Path, destination: pathlib.Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    os.chmod(destination, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)


def write(path: pathlib.Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        stream.write(text)
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IROTH)


def package_metadata(target: str, native: pathlib.Path) -> str:
    return json.dumps(
        {
            **METADATA,
            "native_platform": target,
            "native_filename": native.name,
            "native_sha256": sha256(native),
        },
        indent=2,
        sort_keys=True,
    ) + "\n"


def zip_tree(source: pathlib.Path, output: pathlib.Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted(p for p in source.rglob("*") if p.is_file()):
            entry = zipfile.ZipInfo(path.relative_to(source).as_posix(), EPOCH)
            entry.compress_type = zipfile.ZIP_DEFLATED
            entry.external_attr = (0o100644 << 16)
            archive.writestr(entry, path.read_bytes())


def tar_tree(source: pathlib.Path, output: pathlib.Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode="w") as archive:
                for path in sorted(p for p in source.rglob("*") if p.is_file()):
                    info = archive.gettarinfo(path, arcname=path.relative_to(source).as_posix())
                    info.uid = info.gid = 0
                    info.uname = info.gname = ""
                    info.mtime = 0
                    info.mode = 0o644
                    with path.open("rb") as payload:
                        archive.addfile(info, payload)


def generate(bindings: pathlib.Path) -> None:
    subprocess.run([ROOT / "scripts" / "generate_ffi_bindings.sh"], check=True, cwd=ROOT,
                   env={**os.environ, "DAX_FFI_OUT_DIR": str(bindings)})


def assemble_go(
    stage: pathlib.Path,
    bindings: pathlib.Path,
    native: pathlib.Path,
    go_native: pathlib.Path,
    target: str,
    output: pathlib.Path,
) -> None:
    root = stage / "go"
    shutil.copytree(bindings / "go" / "aws_dax_ffi", root / "aws_dax_ffi")
    config = platform_metadata(target)
    copy(go_native, root / "aws_dax_ffi" / "native" / target / go_native.name)
    write(root / "go.mod", "module github.com/raduendv/aws-dax-ffi-go\n\ngo 1.24\n")
    write(root / "aws_dax_ffi" / str(config["go_source"]), f"""//go:build {config["go_build_constraint"]}

package aws_dax_ffi

// #cgo LDFLAGS: {config["go_ldflags"]}
import "C"
""")
    write(root / "aws_dax_ffi_package.json", package_metadata(target, go_native))
    zip_tree(root, output / "go" / f"aws-dax-ffi-go-{METADATA['package_version']}.zip")


def assemble_dotnet(stage: pathlib.Path, bindings: pathlib.Path, native: pathlib.Path, target: str, output: pathlib.Path) -> None:
    root = stage / "dotnet"
    build_root = ROOT / "target" / "ffi-package-dotnet-build"
    shutil.rmtree(build_root, ignore_errors=True)
    project = build_root / "Aws.Dax.Ffi.csproj"
    copy(bindings / "dotnet" / "aws_dax_ffi.cs", build_root / "aws_dax_ffi.cs")
    write(project, """<Project Sdk="Microsoft.NET.Sdk"><PropertyGroup>
<TargetFramework>net8.0</TargetFramework><AllowUnsafeBlocks>true</AllowUnsafeBlocks>
<AssemblyName>Aws.Dax.Ffi</AssemblyName><Deterministic>true</Deterministic>
<PathMap>$(MSBuildProjectDirectory)=/_/aws-dax-ffi</PathMap>
<DeterministicSourcePaths>true</DeterministicSourcePaths><ContinuousIntegrationBuild>true</ContinuousIntegrationBuild>
<DebugType>none</DebugType><DebugSymbols>false</DebugSymbols>
</PropertyGroup></Project>
""")
    subprocess.run(["dotnet", "build", project, "--nologo", "-v:q"], check=True)
    copy(build_root / "bin" / "Debug" / "net8.0" / "Aws.Dax.Ffi.dll", root / "lib" / "net8.0" / "Aws.Dax.Ffi.dll")
    copy(bindings / "dotnet" / "aws_dax_ffi.cs", root / "src" / "Aws.Dax.Ffi.cs")
    rid = str(platform_metadata(target)["dotnet_rid"])
    copy(native, root / "runtimes" / rid / "native" / native.name)
    write(root / "Aws.Dax.Ffi.nuspec", f"""<?xml version="1.0"?>
<package><metadata><id>Aws.Dax.Ffi</id><version>{METADATA["package_version"]}</version>
<authors>aws-dax-rs</authors><description>Generated AWS DAX UniFFI bindings.</description>
</metadata></package>
""")
    write(root / "aws_dax_ffi_package.json", package_metadata(target, native))
    zip_tree(root, output / "dotnet" / f"Aws.Dax.Ffi.{METADATA['package_version']}.nupkg")


def jvm_loader(package: str, target: str, native: pathlib.Path) -> str:
    resource = f"/native/{target}/{native.name}"
    suffix = native.suffix
    return f"""package {package};

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/** Loads the package-owned native asset without global JVM search paths. */
public final class DaxPackageNative {{
  private DaxPackageNative() {{}}
  public static String libraryPath() {{
    try (InputStream input = DaxPackageNative.class.getResourceAsStream("{resource}")) {{
      if (input == null) throw new UnsatisfiedLinkError("missing packaged {target} native library");
      Path file = Files.createTempFile("aws-dax-ffi-", "{suffix}");
      Files.copy(input, file, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
      file.toFile().deleteOnExit();
      return file.toAbsolutePath().toString();
    }} catch (Exception error) {{
      throw new UnsatisfiedLinkError("cannot extract packaged AWS DAX native library: " + error);
    }}
  }}
}}
"""


def patch_jvm_loader(source: pathlib.Path) -> None:
    namespace = source / "software" / "amazon" / "dax" / "NamespaceLibrary.java"
    text = namespace.read_text()
    expected = 'return "aws_dax_ffi";'
    if text.count(expected) != 1:
        raise SystemExit(f"unexpected generated JVM loader shape: {namespace}")
    write(namespace, text.replace(expected, "return DaxPackageNative.libraryPath();"))


def assemble_jvm(kind: str, stage: pathlib.Path, bindings: pathlib.Path, native: pathlib.Path, target: str, output: pathlib.Path) -> None:
    root = stage / kind
    source = root / "src"
    shutil.copytree(bindings / kind / "software", source / "software")
    package = "software.amazon.dax"
    loader = source / "software" / "amazon" / "dax" / "DaxPackageNative.java"
    write(loader, jvm_loader(package, target, native))
    if kind == "java":
        patch_jvm_loader(source)
        classes = root / "classes"
        jna = os.environ.get("JNA_JAR", str(pathlib.Path.home() / ".m2/repository/net/java/dev/jna/jna/5.17.0/jna-5.17.0.jar"))
        if not pathlib.Path(jna).is_file():
            raise SystemExit("JNA_JAR must identify the pinned JNA 5.17.0 jar")
        subprocess.run(["javac", "--release", "21", "-cp", jna, "-d", classes,
                        *map(str, sorted(source.rglob("*.java")))], check=True)
        copy(native, classes / "native" / target / native.name)
        write(classes / "aws_dax_ffi_package.json", package_metadata(target, native))
        zip_tree(classes, output / "java" / f"aws-dax-ffi-{METADATA['package_version']}.jar")
        zip_tree(source, output / "java" / f"aws-dax-ffi-{METADATA['package_version']}-sources.jar")
    else:
        generated = source / "software" / "amazon" / "dax" / "aws_dax_ffi.kt"
        text = generated.read_text()
        expected = 'return "aws_dax_ffi"'
        if text.count(expected) != 1:
            raise SystemExit("unexpected generated Kotlin loader shape")
        write(generated, text.replace(expected, "return DaxPackageNative.libraryPath()"))
        write(root / "pom.xml", """<project xmlns="http://maven.apache.org/POM/4.0.0">
<modelVersion>4.0.0</modelVersion><groupId>software.amazon.dax</groupId>
<artifactId>aws-dax-ffi-kotlin</artifactId><version>0.1.0</version>
<properties><kotlin.version>2.1.20</kotlin.version><maven.compiler.release>21</maven.compiler.release></properties>
<dependencies><dependency><groupId>net.java.dev.jna</groupId><artifactId>jna</artifactId><version>5.17.0</version></dependency>
<dependency><groupId>org.jetbrains.kotlin</groupId><artifactId>kotlin-stdlib</artifactId><version>${kotlin.version}</version></dependency>
<dependency><groupId>org.jetbrains.kotlinx</groupId><artifactId>kotlinx-coroutines-core-jvm</artifactId><version>1.10.1</version></dependency></dependencies>
<build><sourceDirectory>src</sourceDirectory><plugins>
<plugin><groupId>org.jetbrains.kotlin</groupId><artifactId>kotlin-maven-plugin</artifactId><version>${kotlin.version}</version><executions><execution><goals><goal>compile</goal></goals></execution></executions></plugin>
<plugin><groupId>org.apache.maven.plugins</groupId><artifactId>maven-compiler-plugin</artifactId><version>3.13.0</version><configuration><release>21</release></configuration></plugin>
</plugins></build></project>
""")
        subprocess.run(["mvn", "-q", "-f", root / "pom.xml", "compile"], check=True)
        classes = root / "target" / "classes"
        copy(native, classes / "native" / target / native.name)
        write(classes / "aws_dax_ffi_package.json", package_metadata(target, native))
        zip_tree(classes, output / "kotlin" / f"aws-dax-ffi-kotlin-{METADATA['package_version']}.jar")
        copy(root / "pom.xml", output / "kotlin" / f"aws-dax-ffi-kotlin-{METADATA['package_version']}.pom")


def assemble_python(stage: pathlib.Path, bindings: pathlib.Path, native: pathlib.Path, target: str, output: pathlib.Path) -> None:
    root = stage / "python"
    package = root / "aws_dax_ffi"
    copy(bindings / "python" / "aws_dax_ffi.py", package / "__init__.py")
    copy(native, package / "native" / target / native.name)
    init = package / "__init__.py"
    text = init.read_text()
    needle = "import ctypes\n"
    shim = f"""import ctypes
import importlib.resources

# Package-relative loading; no PYTHONPATH or loader-path environment variable.
_dax_native = importlib.resources.files(__package__).joinpath("native/{target}/{native.name}")
_dax_native_path = str(_dax_native)
"""
    if text.count(needle) != 1:
        raise SystemExit("unexpected generated Python loader shape")
    write(init, text.replace(needle, shim))
    text = init.read_text()
    expected = 'path = os.path.join(os.path.dirname(__file__), libname)'
    if text.count(expected) != 1:
        raise SystemExit("unexpected generated Python CDLL shape")
    write(init, text.replace(expected, "path = _dax_native_path"))
    dist = root / f"aws_dax_ffi-{METADATA['package_version']}.dist-info"
    wheel_tag = str(platform_metadata(target)["python_wheel_tag"])
    write(dist / "METADATA", f"Metadata-Version: 2.1\nName: aws-dax-ffi\nVersion: {METADATA['package_version']}\n")
    write(dist / "WHEEL", f"Wheel-Version: 1.0\nGenerator: aws-dax-rs\nRoot-Is-Purelib: false\nTag: py3-none-{wheel_tag}\n")
    write(package / "aws_dax_ffi_package.json", package_metadata(target, native))
    record = []
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        relative = path.relative_to(root).as_posix()
        digest = base64.urlsafe_b64encode(hashlib.sha256(path.read_bytes()).digest()).rstrip(b"=").decode()
        record.append(f"{relative},sha256={digest},{path.stat().st_size}")
    record.append(f"{dist.name}/RECORD,,")
    write(dist / "RECORD", "\n".join(record) + "\n")
    zip_tree(root, output / "python" / f"aws_dax_ffi-{METADATA['package_version']}-py3-none-{wheel_tag}.whl")


def assemble_javascript(stage: pathlib.Path, bindings: pathlib.Path, native: pathlib.Path, target: str, output: pathlib.Path) -> None:
    root = stage / "javascript" / "package"
    for generated in ("aws_dax_ffi.ts", "aws_dax_ffi-ffi.ts", "index.ts"):
        copy(bindings / "javascript-napi" / generated, root / "generated" / generated)
    copy(native, root / "native" / target / native.name)
    ffi = root / "generated" / "aws_dax_ffi-ffi.ts"
    text = ffi.read_text()
    text = text.replace(
        'import lib from "@ubjs/node";',
        'import lib from "@ubjs/node";\nimport { fileURLToPath } from "node:url";',
    )
    expected = """const libPath = resolveLibPath({
      crateName: "aws_dax_ffi",
      callerUrl: import.meta.url,
    });"""
    replacement = f'const libPath = fileURLToPath(new URL("../native/{target}/{native.name}", import.meta.url));'
    if text.count(expected) != 1:
        raise SystemExit("unexpected generated JavaScript native resolver shape")
    write(ffi, text.replace(expected, replacement))
    runtime_modules = FFI / "packaging" / "javascript-tools" / "node_modules"
    compiler = runtime_modules / ".bin" / "tsc"
    bundler = runtime_modules / ".bin" / "esbuild"
    if not compiler.is_file() or not bundler.is_file():
        raise SystemExit(
            "pinned JavaScript packaging tools are unavailable; "
            "run npm ci --prefix src/aws-dax-ffi/packaging/javascript-tools"
        )
    package_modules = root / "node_modules"
    if os.name == "nt":
        shutil.copytree(runtime_modules, package_modules, symlinks=True)
    else:
        package_modules.symlink_to(runtime_modules)
    try:
        subprocess.run([
            compiler, "--declaration", "--emitDeclarationOnly", "--module", "esnext",
            "--moduleResolution", "bundler", "--target", "es2022", "--outDir", root / "dist",
            "--skipLibCheck", root / "generated" / "index.ts",
        ], check=True)
        subprocess.run([
            bundler, "generated/index.ts", "--bundle", "--format=esm",
            "--platform=node", "--external:@ubjs/core", "--external:@ubjs/node",
            "--outfile=dist/index.js",
        ], check=True, cwd=root)
    finally:
        if package_modules.is_symlink():
            package_modules.unlink()
        else:
            shutil.rmtree(package_modules, ignore_errors=True)
    config = platform_metadata(target)
    write(root / "package.json", json.dumps({
        "name": "@raduendv/aws-dax-ffi", "version": METADATA["package_version"], "type": "module",
        "main": "./dist/index.js", "types": "./dist/index.d.ts",
        "os": [config["npm_os"]], "cpu": [config["npm_cpu"]],
        "dependencies": {"@ubjs/core": "0.31.0-6", "@ubjs/node": "0.31.0-6"},
        "description": "Generated AWS DAX UniFFI N-API bindings; Node only, not browser or WASM.",
    }, indent=2, sort_keys=True) + "\n")
    write(root / "aws_dax_ffi_package.json", package_metadata(target, native))
    tar_tree(stage / "javascript", output / "javascript" / f"aws-dax-ffi-{METADATA['package_version']}.tgz")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform", default=host_platform(), choices=METADATA["packaging_platforms"])
    parser.add_argument("--native-library", type=pathlib.Path)
    parser.add_argument("--go-native-library", type=pathlib.Path)
    parser.add_argument("--output", type=pathlib.Path, default=ROOT / "target" / "ffi-packages")
    parser.add_argument("--skip-generate", action="store_true")
    args = parser.parse_args()
    if args.platform != host_platform() and args.native_library is None:
        raise SystemExit("--native-library is required for a non-host Phase 8 platform")
    native = args.native_library or ROOT / "target" / "debug" / native_name(args.platform)
    if not native.is_file():
        raise SystemExit(f"native library does not exist: {native}")
    validate_native_library(native, args.platform)
    config = platform_metadata(args.platform)
    go_native_name = str(config["go_native_filename"])
    if args.go_native_library is not None:
        go_native = args.go_native_library
    elif go_native_name == native.name:
        go_native = native
    else:
        go_native = ROOT / "target" / "debug" / go_native_name
    if not go_native.is_file():
        raise SystemExit(
            f"Go native library for {args.platform} does not exist: {go_native}; "
            "provide --go-native-library"
        )
    if go_native.name != go_native_name:
        raise SystemExit(
            f"Go native library for {args.platform} must be named {go_native_name}, "
            f"not {go_native.name}"
        )
    with tempfile.TemporaryDirectory(prefix="aws-dax-ffi-package-") as temporary:
        temporary_path = pathlib.Path(temporary)
        bindings = temporary_path / "bindings"
        if args.skip_generate:
            shutil.copytree(ROOT / "target" / "ffi-bindings", bindings)
        else:
            generate(bindings)
        output = args.output / args.platform
        if output.exists():
            shutil.rmtree(output)
        assemble_go(temporary_path, bindings, native, go_native, args.platform, output)
        assemble_dotnet(temporary_path, bindings, native, args.platform, output)
        assemble_jvm("java", temporary_path, bindings, native, args.platform, output)
        assemble_jvm("kotlin", temporary_path, bindings, native, args.platform, output)
        assemble_python(temporary_path, bindings, native, args.platform, output)
        assemble_javascript(temporary_path, bindings, native, args.platform, output)
        print(f"DAX_FFI_PACKAGE_ASSEMBLY platform={args.platform} output={output}")


if __name__ == "__main__":
    main()
