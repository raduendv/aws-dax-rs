//! Exports deterministic metadata fixtures from a pinned Go reference checkout.

use std::{
    env,
    error::Error,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use aws_dax::fixture::{
    Fixture, FixtureEncoding, FixtureExpectation, FixtureFormat, FixtureKind, FixtureRedaction,
    FixtureSource, FixtureStatus, SCHEMA_VERSION,
};
use serde_json::json;

const REFERENCE_REPOSITORY: &str = "github.com/aws/aws-dax-go-v2";
const REFERENCE_REVISION: &str = "287eea5d36462faac54175d1e89878804b524928";
const REFERENCE_TAG: &str = "v1.0.3";

fn main() -> Result<(), Box<dyn Error>> {
    let reference = reference_path(env::args_os().skip(1))?;
    verify_revision(&reference)?;

    let tube_path = reference.join("dax/internal/client/tube.go");
    let tube = fs::read_to_string(&tube_path)?;
    let magic = extract_string_constant(&tube, "magic")?;
    let user_agent = extract_string_constant(&tube, "agent")?;

    let fixture = Fixture {
        schema_version: SCHEMA_VERSION.into(),
        fixture_id: "metadata.protocol.handshake".into(),
        kind: FixtureKind::Metadata,
        source: FixtureSource {
            repository: REFERENCE_REPOSITORY.into(),
            revision: REFERENCE_REVISION.into(),
            tag: REFERENCE_TAG.into(),
            path: "dax/internal/client/tube.go".into(),
            symbol: "magic; agent; optional".into(),
            generator: "reference-fixture-export".into(),
            generator_version: env!("CARGO_PKG_VERSION").into(),
        },
        inputs: json!({
            "operation": "connection_handshake_metadata",
        }),
        expected: FixtureExpectation {
            status: FixtureStatus::Ok,
            value: Some(json!({
                "magic": magic,
                "user_agent": user_agent,
            })),
            error: None,
        },
        encoding: FixtureEncoding {
            input_format: FixtureFormat::Structured,
            output_format: FixtureFormat::Structured,
            canonicalization: "object keys are compared without ordering".into(),
        },
        redaction: FixtureRedaction {
            credentials: false,
            endpoints: false,
            customer_data: false,
        },
    };
    fixture.validate()?;

    println!("{}", serde_json::to_string_pretty(&fixture)?);
    Ok(())
}

fn reference_path(
    mut arguments: impl Iterator<Item = std::ffi::OsString>,
) -> Result<PathBuf, Box<dyn Error>> {
    match (arguments.next(), arguments.next()) {
        (None, None) => Ok(PathBuf::from("aws-dax-go-v2")),
        (Some(flag), Some(path)) if flag == "--reference" => Ok(PathBuf::from(path)),
        _ => Err("usage: reference-fixture-export [--reference <path>]".into()),
    }
}

fn verify_revision(reference: &Path) -> Result<(), Box<dyn Error>> {
    let output = Command::new("git")
        .args(["-C"])
        .arg(reference)
        .args(["rev-parse", "HEAD"])
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "could not determine reference revision at {}",
            reference.display()
        )
        .into());
    }

    let found = std::str::from_utf8(&output.stdout)?.trim();
    if found != REFERENCE_REVISION {
        return Err(format!(
            "reference revision mismatch: expected {REFERENCE_REVISION}, found {found}"
        )
        .into());
    }
    Ok(())
}

fn extract_string_constant(source: &str, constant: &str) -> Result<String, Box<dyn Error>> {
    let prefix = format!("const {constant} = \"");
    let line = source
        .lines()
        .find(|line| line.starts_with(&prefix))
        .ok_or_else(|| format!("missing `{constant}` constant"))?;
    let value = line
        .strip_prefix(&prefix)
        .and_then(|value| value.strip_suffix('"'))
        .ok_or_else(|| format!("invalid `{constant}` constant declaration"))?;
    Ok(value.into())
}
