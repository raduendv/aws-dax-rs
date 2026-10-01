# Release checklist

The repository is kept release-ready without publishing or tagging releases.
Leave the version and release-date items unchecked until an actual publication
is approved. The validation and package-integrity checks below should remain
green on the working tree.

Before publishing a release:

- [ ] Update the version in `Cargo.toml` and `Cargo.lock`.
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
- [ ] Run `cargo package --allow-dirty --list` and inspect the package contents.
- [ ] Run `git diff --check`.
- [ ] Confirm no credentials, generated build output, or local Go checkout is
      included in the package.
