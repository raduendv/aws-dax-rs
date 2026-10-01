# Release checklist

The repository is kept release-ready without publishing or tagging releases.
Leave the version and release-date items unchecked until an actual publication
is approved. The validation and package-integrity checks below should remain
green on the working tree.

Before publishing a release:

- [ ] Update the version in `src/aws-dax-sdk/Cargo.toml` and `Cargo.lock`.
- [ ] Update [CHANGELOG.md](CHANGELOG.md) with the release date and changes.
- [ ] Review [MIGRATION.md](MIGRATION.md) and [README.md](README.md) for
      compatibility changes.
- [ ] Run `cargo fmt --check`.
- [ ] Run the validation matrix on Rust 1.85 and stable.
- [ ] Run `cargo clippy --all-targets --all-features -- -D warnings`.
- [ ] Run `cargo test --all-targets`.
- [ ] Run `cargo test --all-targets -- --ignored` when the Go reference fixture
      is available.
- [ ] Run `cargo deny check`.
- [ ] Run `cargo doc --no-deps`.
- [ ] Run `cargo package -p aws-dax --allow-dirty --list` and inspect the package contents.
- [ ] Run `git diff --check`.
- [ ] Confirm no credentials, generated build output, or local Go checkout is
      included in the package.

## Generated FFI packages

Do not publish any generated package until explicit approval is given.

- [ ] Confirm the `aws-dax`, `aws-dax-ffi`, and six generated-package versions
      are identical.
- [ ] Run `scripts/check_ffi_bindings.sh`; review any intentional API snapshot
      change in `src/aws-dax-ffi/api-snapshots`.
- [ ] Run `cargo check -p aws-dax-fuzz --all-targets`; do not use
      `cargo test --workspace --all-targets`, which executes fuzz binaries
      indefinitely.
- [ ] Run bounded scheduled fuzz campaigns for every target in
      `src/aws-dax-fuzz/Cargo.toml`.
- [ ] Build and package Linux x86_64, Linux aarch64, macOS x86_64, macOS arm64,
      and Windows x86_64.
- [ ] Confirm each package's native container and architecture validation
      passes; never relabel a native artifact.
- [ ] Install the Go module ZIP into a clean module and run its artifact-only
      example.
- [ ] Install the .NET NuGet package into a clean .NET 8 project and run its
      artifact-only example.
- [ ] Compile and run the Java JAR example on JDK 21 with the pinned JNA.
- [ ] Compile and run the Kotlin/JVM JAR/POM example with the pinned Kotlin,
      JNA, and coroutine dependencies.
- [ ] Install the Python wheel into a clean virtual environment and run its
      artifact-only example.
- [ ] Install the npm tarball into a clean Node project and run its
      artifact-only N-API example.
- [ ] Confirm none of the six examples uses `LD_LIBRARY_PATH`,
      `DYLD_LIBRARY_PATH`, `PYTHONPATH`, `NODE_PATH`, or a JNA path override.
- [ ] Compare two fixed-input assemblies byte-for-byte.
- [ ] Verify `SHA256SUMS`, native checksums in package metadata, CycloneDX
      SBOMs, source/license policy, secret scanning, and build provenance.
- [ ] Run packaged-artifact plaintext and TLS Put/Get/Delete validation for all
      six ecosystems on a VPC-reachable Linux runner.
- [ ] Run persistent generated consumers for direct, batch, transaction,
      paginator, routing, cancellation, shutdown, and structured-error parity.
- [ ] Run the rollback-safe one-node TLS fault matrix and verify firewall rule
      cleanup after every language.
- [ ] Inspect every release candidate from downloaded CI artifacts only; do
      not substitute source-tree bindings or native libraries.
- [ ] Confirm release notes do not claim Kotlin/Native, Kotlin Multiplatform,
      browser JavaScript, or WebAssembly support.
- [ ] Publish only after a separate explicit approval.
