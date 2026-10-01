//! Versioned conformance-fixture manifests for the DAX port.
//!
//! Fixtures are deliberately data-only. They let the Go reference implementation
//! and the Rust port compare deterministic behavior without embedding Go-specific
//! test code in the Rust crate.

use std::{error::Error, fmt};

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The only fixture-manifest schema currently accepted by this crate.
pub const SCHEMA_VERSION: &str = "dax-fixture/v1";

/// A versioned, language-neutral conformance fixture.
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Fixture {
    /// Fixture schema identifier.
    pub schema_version: String,
    /// Stable, dot-separated identifier for this case.
    pub fixture_id: String,
    /// Behavior represented by the fixture.
    pub kind: FixtureKind,
    /// Go reference provenance.
    pub source: FixtureSource,
    /// Operation-specific input, encoded according to [`FixtureEncoding`].
    pub inputs: Value,
    /// Expected outcome.
    pub expected: FixtureExpectation,
    /// Serialization and normalization rules for input and expected values.
    pub encoding: FixtureEncoding,
    /// Assertion that the fixture excludes sensitive data.
    pub redaction: FixtureRedaction,
}

impl Fixture {
    /// Parses and validates a fixture manifest from UTF-8 JSON.
    pub fn from_json(json: &str) -> Result<Self, FixtureError> {
        let fixture = serde_json::from_str::<Self>(json).map_err(FixtureError::InvalidJson)?;
        fixture.validate()?;
        Ok(fixture)
    }

    /// Validates schema and cross-field requirements that JSON alone cannot express.
    pub fn validate(&self) -> Result<(), FixtureError> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(FixtureError::UnsupportedSchema {
                found: self.schema_version.clone(),
            });
        }
        require_non_empty("fixture_id", &self.fixture_id)?;
        self.source.validate()?;
        self.encoding.validate()?;

        match (&self.expected.status, &self.expected.error) {
            (FixtureStatus::Ok, Some(_)) => Err(FixtureError::InvalidExpectation(
                "successful fixtures must not include an error".into(),
            )),
            (FixtureStatus::Error, None) => Err(FixtureError::InvalidExpectation(
                "error fixtures must include an error expectation".into(),
            )),
            _ => Ok(()),
        }
    }
}

/// Behavior category covered by a fixture.
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FixtureKind {
    /// CBOR encoding or decoding behavior.
    Cbor,
    /// DynamoDB expression parsing or validation behavior.
    Expression,
    /// Pagination behavior.
    Pagination,
    /// Client configuration behavior.
    Config,
    /// Error mapping behavior.
    Error,
    /// Fixed compatibility metadata.
    Metadata,
}

/// Provenance for a fixture exported from the Go implementation.
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureSource {
    /// Reference repository identifier.
    pub repository: String,
    /// Immutable reference revision.
    pub revision: String,
    /// Human-readable reference tag.
    pub tag: String,
    /// Reference-relative source path.
    pub path: String,
    /// Source test or symbol establishing the behavior.
    pub symbol: String,
    /// Deterministic fixture exporter identifier.
    pub generator: String,
    /// Fixture exporter version.
    pub generator_version: String,
}

impl FixtureSource {
    fn validate(&self) -> Result<(), FixtureError> {
        for (field, value) in [
            ("source.repository", &self.repository),
            ("source.revision", &self.revision),
            ("source.tag", &self.tag),
            ("source.path", &self.path),
            ("source.symbol", &self.symbol),
            ("source.generator", &self.generator),
            ("source.generator_version", &self.generator_version),
        ] {
            require_non_empty(field, value)?;
        }
        Ok(())
    }
}

/// Expected fixture result.
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureExpectation {
    /// Whether the fixture must succeed or fail.
    pub status: FixtureStatus,
    /// Operation-specific output for successful fixtures.
    #[serde(default)]
    pub value: Option<Value>,
    /// Required error expectation for failed fixtures.
    #[serde(default)]
    pub error: Option<FixtureErrorExpectation>,
}

/// Expected operation status.
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FixtureStatus {
    /// The operation succeeds.
    Ok,
    /// The operation fails.
    Error,
}

/// Stable error details suitable for cross-language comparison.
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureErrorExpectation {
    /// Language-neutral error category.
    pub category: String,
    /// Optional stable substring required in the error message.
    #[serde(default)]
    pub message_contains: Option<String>,
}

/// Input and output serialization rules for a fixture.
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureEncoding {
    /// Serialization used for `inputs`.
    pub input_format: FixtureFormat,
    /// Serialization used for the successful expected value.
    pub output_format: FixtureFormat,
    /// Rule used to make values comparable across languages.
    pub canonicalization: String,
}

impl FixtureEncoding {
    fn validate(&self) -> Result<(), FixtureError> {
        require_non_empty("encoding.canonicalization", &self.canonicalization)
    }
}

/// Supported fixture value encodings.
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FixtureFormat {
    /// JSON data.
    Json,
    /// Lowercase hexadecimal bytes.
    Hex,
    /// UTF-8 text.
    Utf8,
    /// Operation-specific structured JSON.
    Structured,
}

/// Redaction assertions for a fixture.
#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FixtureRedaction {
    /// Whether the fixture contains credentials.
    pub credentials: bool,
    /// Whether the fixture contains a real endpoint.
    pub endpoints: bool,
    /// Whether the fixture contains customer item data.
    pub customer_data: bool,
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), FixtureError> {
    if value.trim().is_empty() {
        return Err(FixtureError::MissingField(field));
    }
    Ok(())
}

/// A fixture parsing or validation failure.
#[derive(Debug)]
pub enum FixtureError {
    /// JSON did not match the fixture schema.
    InvalidJson(serde_json::Error),
    /// The fixture uses a schema this crate cannot interpret.
    UnsupportedSchema {
        /// Schema identifier supplied by the fixture.
        found: String,
    },
    /// A required string field was absent or blank.
    MissingField(&'static str),
    /// The fixture result shape conflicts with its status.
    InvalidExpectation(String),
}

impl fmt::Display for FixtureError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJson(error) => write!(formatter, "invalid fixture JSON: {error}"),
            Self::UnsupportedSchema { found } => {
                write!(formatter, "unsupported fixture schema `{found}`")
            }
            Self::MissingField(field) => write!(formatter, "fixture field `{field}` is required"),
            Self::InvalidExpectation(reason) => {
                write!(formatter, "invalid fixture expectation: {reason}")
            }
        }
    }
}

impl Error for FixtureError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidJson(error) => Some(error),
            Self::UnsupportedSchema { .. }
            | Self::MissingField(_)
            | Self::InvalidExpectation(_) => None,
        }
    }
}
