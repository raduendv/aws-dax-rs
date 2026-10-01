use std::process::Command;

use aws_dax::fixture::Fixture;

const HANDSHAKE_FIXTURE: &str = include_str!("fixtures/metadata.protocol.handshake.json");

#[test]
#[ignore = "requires the pinned aws-dax-go-v2 checkout; run explicitly before updating fixtures"]
fn checked_in_handshake_metadata_matches_the_reference_exporter() {
    let output = Command::new(env!("CARGO_BIN_EXE_reference-fixture-export"))
        .arg("--reference")
        .arg("aws-dax-go-v2")
        .output()
        .expect("reference fixture exporter must start");

    assert!(
        output.status.success(),
        "exporter failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let exported = Fixture::from_json(
        std::str::from_utf8(&output.stdout).expect("exporter output must be UTF-8 JSON"),
    )
    .expect("exported fixture must be valid");
    let checked_in = Fixture::from_json(HANDSHAKE_FIXTURE).expect("fixture must be valid");

    assert_eq!(exported, checked_in, "checked-in fixture is stale");
}
