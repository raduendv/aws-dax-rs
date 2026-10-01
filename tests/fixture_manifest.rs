use aws_dax::fixture::{Fixture, FixtureError, FixtureStatus, SCHEMA_VERSION};

const HANDSHAKE_FIXTURE: &str = include_str!("fixtures/metadata.protocol.handshake.json");

#[test]
fn parses_a_versioned_fixture_with_reference_provenance() {
    let fixture = Fixture::from_json(HANDSHAKE_FIXTURE).expect("fixture must be valid");

    assert_eq!(fixture.schema_version, SCHEMA_VERSION);
    assert_eq!(fixture.fixture_id, "metadata.protocol.handshake");
    assert_eq!(fixture.expected.status, FixtureStatus::Ok);
    assert_eq!(
        fixture
            .expected
            .value
            .as_ref()
            .and_then(|value| value.get("magic"))
            .and_then(|value| value.as_str()),
        Some("J7yne5G")
    );
}

#[test]
fn rejects_an_unknown_schema() {
    let json = HANDSHAKE_FIXTURE.replace(SCHEMA_VERSION, "dax-fixture/v999");

    assert!(matches!(
        Fixture::from_json(&json),
        Err(FixtureError::UnsupportedSchema { .. })
    ));
}

#[test]
fn rejects_an_error_fixture_without_an_error_expectation() {
    let json = HANDSHAKE_FIXTURE
        .replace("\"status\": \"ok\"", "\"status\": \"error\"")
        .replace(
            "\"value\": {\n      \"magic\": \"J7yne5G\",\n      \"user_agent\": \"DaxGoV2Client-1.0.3\"\n    }",
            "\"value\": null",
        );

    assert!(matches!(
        Fixture::from_json(&json),
        Err(FixtureError::InvalidExpectation(_))
    ));
}
