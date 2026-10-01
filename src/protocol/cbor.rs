//! DAX CBOR primitives and the initial DynamoDB AttributeValue codec.

use std::{collections::BTreeMap, collections::HashMap, error::Error, fmt};

use aws_sdk_dynamodb::{
    primitives::Blob,
    types::{AttributeDefinition, AttributeValue, Capacity, ConsumedCapacity, ScalarAttributeType},
};
use num_bigint::{BigInt, Sign};

const MAJOR_UNSIGNED: u8 = 0x00;
const MAJOR_NEGATIVE: u8 = 0x20;
const MAJOR_BYTES: u8 = 0x40;
const MAJOR_TEXT: u8 = 0x60;
const MAJOR_ARRAY: u8 = 0x80;
const MAJOR_MAP: u8 = 0xa0;
const MAJOR_TAG: u8 = 0xc0;
const MAJOR_SIMPLE: u8 = 0xe0;
const ADDITIONAL_ONE_BYTE: u8 = 24;
const ADDITIONAL_TWO_BYTES: u8 = 25;
const ADDITIONAL_FOUR_BYTES: u8 = 26;
const ADDITIONAL_EIGHT_BYTES: u8 = 27;
const MAX_VALUE_BYTES: usize = 16 * 1024 * 1024;
const MAX_CONTAINER_ELEMENTS: usize = 100_000;
const MAX_NESTING_DEPTH: usize = 64;
const MAX_DECODED_VALUES: usize = MAX_CONTAINER_ELEMENTS + 1;
const TAG_POSITIVE_BIGNUM: u64 = 2;
const TAG_NEGATIVE_BIGNUM: u64 = 3;
const TAG_DECIMAL: u64 = 4;
const TAG_STRING_SET: u64 = 3321;
const TAG_NUMBER_SET: u64 = 3322;
const TAG_BINARY_SET: u64 = 3323;

/// A CBOR encoding or decoding failure.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum CborError {
    /// The input ended before a complete CBOR item was available.
    UnexpectedEnd,
    /// The CBOR major type did not match the requested value.
    UnexpectedType {
        /// Expected major type.
        expected: &'static str,
        /// Received initial byte.
        found: u8,
    },
    /// The item uses an unsupported CBOR form.
    UnsupportedForm(&'static str),
    /// A declared string or byte-string length exceeds the decoder limit.
    ValueTooLarge(usize),
    /// A declared array or map length exceeds the decoder limit.
    ContainerTooLarge(usize),
    /// Nested values exceed the decoder depth limit.
    NestingTooDeep,
    /// Text bytes are not valid UTF-8.
    InvalidUtf8,
    /// A DAX AttributeValue is invalid.
    InvalidAttributeValue(&'static str),
    /// A DAX AttributeValue variant belongs to a later codec slice.
    UnsupportedAttributeValue(&'static str),
    /// A DAX item key does not match its key definition.
    InvalidItemKey(&'static str),
    /// A schema-compressed item referred to an unknown attribute-name list ID.
    UnknownAttributeListId(i64),
    /// A transaction cancellation reason did not contain an item payload.
    MissingItemPayload,
    /// Bytes remained after decoding one complete value.
    TrailingData,
}

#[cfg(test)]
mod endpoint_tests {
    use super::decode_endpoints_body;

    #[test]
    fn decodes_endpoint_roster() {
        let bytes = [
            0x81, 0xa8, 0x00, 0x01, 0x01, 0x63, b'n', b'o', b'd', 0x02, 0x44, 10, 0, 0, 1, 0x03,
            0x18, 0x91, 0x04, 0x01, 0x05, 0x61, b'a', 0x06, 0x07, 0x07, 0x80,
        ];
        let endpoints = decode_endpoints_body(&bytes).expect("valid endpoint roster");
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].node_id, 1);
        assert_eq!(endpoints[0].hostname, "nod");
        assert_eq!(endpoints[0].address, vec![10, 0, 0, 1]);
        assert_eq!(endpoints[0].port, 145);
        assert_eq!(endpoints[0].role, 1);
        assert_eq!(endpoints[0].availability_zone.as_deref(), Some("a"));
        assert_eq!(endpoints[0].leader_session_id, Some(7));
    }

    #[test]
    fn rejects_unknown_endpoint_role() {
        let bytes = [0x81, 0xa1, 0x04, 0x03];
        assert!(decode_endpoints_body(&bytes).is_err());
    }

    #[test]
    fn accepts_endpoint_without_optional_hostname() {
        let bytes = [
            0x81, 0xa4, 0x00, 0x01, 0x02, 0x44, 10, 0, 0, 1, 0x03, 0x18, 0x91, 0x04, 0x01,
        ];
        let endpoints = decode_endpoints_body(&bytes).expect("valid endpoint without hostname");
        assert_eq!(endpoints[0].hostname, "");
        assert_eq!(endpoints[0].node_id, 1);
    }
}

impl fmt::Display for CborError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEnd => write!(formatter, "unexpected end of CBOR input"),
            Self::UnexpectedType { expected, found } => {
                write!(
                    formatter,
                    "expected {expected}, found CBOR initial byte 0x{found:02x}"
                )
            }
            Self::UnsupportedForm(form) => write!(formatter, "unsupported CBOR form: {form}"),
            Self::ValueTooLarge(length) => {
                write!(formatter, "CBOR value exceeds size limit: {length}")
            }
            Self::ContainerTooLarge(length) => {
                write!(formatter, "CBOR container exceeds element limit: {length}")
            }
            Self::NestingTooDeep => write!(formatter, "CBOR nesting exceeds depth limit"),
            Self::InvalidUtf8 => write!(formatter, "CBOR text string is not valid UTF-8"),
            Self::InvalidAttributeValue(reason) => {
                write!(formatter, "invalid attribute value: {reason}")
            }
            Self::UnsupportedAttributeValue(variant) => {
                write!(formatter, "unsupported AttributeValue variant: {variant}")
            }
            Self::InvalidItemKey(reason) => write!(formatter, "invalid DAX item key: {reason}"),
            Self::UnknownAttributeListId(id) => {
                write!(formatter, "unknown DAX attribute-name list ID: {id}")
            }
            Self::MissingItemPayload => write!(formatter, "missing DAX transaction item payload"),
            Self::TrailingData => write!(formatter, "trailing CBOR data after attribute value"),
        }
    }
}

impl Error for CborError {}

/// A decoded DAX response envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ResponseEnvelope<'a> {
    /// The response is successful; the remaining bytes are operation-specific.
    Success(&'a [u8]),
    /// The response is a DAX failure.
    Error(DaxResponseError),
}

/// Structured metadata from a DAX error envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DaxResponseError {
    pub(crate) code_sequence: Vec<i64>,
    pub(crate) message: String,
    pub(crate) request_id: Option<String>,
    pub(crate) error_code: Option<String>,
    pub(crate) status_code: i64,
    pub(crate) cancellation_reasons: Option<Box<[CancellationReasonMetadata]>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CancellationReasonMetadata {
    pub(crate) code: Option<String>,
    pub(crate) message: Option<String>,
    pub(crate) item_cbor: Option<Box<[u8]>>,
}

/// Decoded DAX BatchWriteItem response fields supported by this slice.
#[derive(Debug, Default)]
pub(crate) struct BatchWriteResponse {
    pub(crate) unprocessed_items: HashMap<String, Vec<aws_sdk_dynamodb::types::WriteRequest>>,
    pub(crate) consumed_capacity: Option<Vec<ConsumedCapacity>>,
    pub(crate) item_collection_metrics:
        Option<HashMap<String, Vec<aws_sdk_dynamodb::types::ItemCollectionMetrics>>>,
}

#[derive(Debug)]
pub(crate) struct BatchGetResponse {
    pub(crate) responses: HashMap<String, Vec<HashMap<String, AttributeValue>>>,
    pub(crate) unprocessed_keys: HashMap<String, aws_sdk_dynamodb::types::KeysAndAttributes>,
    pub(crate) consumed_capacity: Option<Vec<ConsumedCapacity>>,
}

/// A node returned by the DAX cluster endpoint-discovery operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiscoveredEndpoint {
    /// Stable DAX node identifier.
    pub node_id: i64,
    /// Node hostname returned by DAX.
    pub hostname: String,
    /// Raw node address bytes returned by DAX.
    pub address: Vec<u8>,
    /// Node service port.
    pub port: i64,
    /// DAX role code: `1` leader or `2` replica.
    pub role: i64,
    /// Optional availability-zone identifier.
    pub availability_zone: Option<String>,
    /// Optional leader-session identifier.
    pub leader_session_id: Option<i64>,
}

pub(crate) fn decode_endpoints_body(input: &[u8]) -> Result<Vec<DiscoveredEndpoint>, CborError> {
    let mut reader = Reader::new(input);
    let count = reader.read_length(MAJOR_ARRAY, "DAX endpoint list")?;
    ensure_container_length(count)?;
    let mut endpoints = Vec::with_capacity(count);
    for _ in 0..count {
        let mut endpoint = DiscoveredEndpoint {
            node_id: 0,
            hostname: String::new(),
            address: Vec::new(),
            port: 0,
            role: 0,
            availability_zone: None,
            leader_session_id: None,
        };
        let mut seen = [false; 4];
        reader.consume_response_map(|key, reader| {
            match key {
                0 => {
                    endpoint.node_id = read_integer(reader)?.try_into().map_err(|_| {
                        CborError::InvalidAttributeValue("DAX endpoint node ID is out of range")
                    })?;
                    seen[0] = true;
                }
                1 => {
                    endpoint.hostname = reader.read_text()?;
                }
                2 => {
                    endpoint.address = reader.read_bytes()?;
                    seen[1] = true;
                }
                3 => {
                    endpoint.port = read_integer(reader)?.try_into().map_err(|_| {
                        CborError::InvalidAttributeValue("DAX endpoint port is out of range")
                    })?;
                    seen[2] = true;
                }
                4 => {
                    endpoint.role = read_integer(reader)?.try_into().map_err(|_| {
                        CborError::InvalidAttributeValue("DAX endpoint role is out of range")
                    })?;
                    if !matches!(endpoint.role, 1 | 2) {
                        return Err(CborError::InvalidAttributeValue(
                            "unknown DAX endpoint role",
                        ));
                    }
                    seen[3] = true;
                }
                5 => endpoint.availability_zone = Some(reader.read_text()?),
                6 => {
                    endpoint.leader_session_id =
                        Some(read_integer(reader)?.try_into().map_err(|_| {
                            CborError::InvalidAttributeValue(
                                "DAX leader session ID is out of range",
                            )
                        })?)
                }
                _ => reader.skip_value()?,
            }
            Ok(())
        })?;
        if !seen.into_iter().all(|field| field) {
            return Err(CborError::InvalidAttributeValue(
                "DAX endpoint is missing a required field",
            ));
        }
        if !matches!(endpoint.address.len(), 4 | 16) {
            return Err(CborError::InvalidAttributeValue(
                "DAX endpoint address must be IPv4 or IPv6 bytes",
            ));
        }
        if !(1..=u16::MAX as i64).contains(&endpoint.port) {
            return Err(CborError::InvalidAttributeValue(
                "DAX endpoint port is out of range",
            ));
        }
        endpoints.push(endpoint);
    }
    if !reader.is_empty() {
        return Err(CborError::TrailingData);
    }
    Ok(endpoints)
}

type ItemCollectionMetricsMap =
    HashMap<String, Vec<aws_sdk_dynamodb::types::ItemCollectionMetrics>>;

pub(crate) fn decode_batch_write_body(
    input: &[u8],
    schemas: &HashMap<String, Vec<AttributeDefinition>>,
    attribute_lists: &HashMap<i64, Vec<String>>,
) -> Result<BatchWriteResponse, CborError> {
    let mut reader = Reader::new(input);
    let mut output = BatchWriteResponse::default();
    if reader.consume_null()? {
        return Ok(output);
    }
    if input == [MAJOR_MAP] || input == [MAJOR_ARRAY] {
        return Ok(output);
    }

    let table_count = reader.read_length(MAJOR_MAP, "BatchWrite unprocessed-items map")?;
    ensure_container_length(table_count)?;
    for _ in 0..table_count {
        let table = reader.read_text()?;
        let schema = schemas
            .get(&table)
            .ok_or(CborError::InvalidItemKey("missing BatchWrite key schema"))?;
        let pair_count = reader.read_length(MAJOR_ARRAY, "BatchWrite request array")?;
        ensure_container_length(pair_count)?;
        if pair_count % 2 != 0 {
            return Err(CborError::InvalidAttributeValue(
                "BatchWrite request array must contain key/value pairs",
            ));
        }
        let mut requests = Vec::with_capacity(pair_count / 2);
        for _ in 0..pair_count / 2 {
            let key_payload = reader.read_bytes()?;
            let mut key_frame = Vec::new();
            write_bytes(&mut key_frame, &key_payload);
            let key = decode_item_key(&key_frame, schema)?;
            if reader.consume_null()? {
                requests.push(
                    aws_sdk_dynamodb::types::WriteRequest::builder()
                        .delete_request(
                            aws_sdk_dynamodb::types::DeleteRequest::builder()
                                .set_key(Some(key))
                                .build()
                                .map_err(|_| {
                                    CborError::InvalidAttributeValue(
                                        "invalid BatchWrite delete request",
                                    )
                                })?,
                        )
                        .build(),
                );
            } else {
                let compressed = reader.read_bytes()?;
                let mut item = decode_item_non_key_attributes(&compressed, attribute_lists)?;
                item.extend(key);
                requests.push(
                    aws_sdk_dynamodb::types::WriteRequest::builder()
                        .put_request(
                            aws_sdk_dynamodb::types::PutRequest::builder()
                                .set_item(Some(item))
                                .build()
                                .map_err(|_| {
                                    CborError::InvalidAttributeValue(
                                        "invalid BatchWrite put request",
                                    )
                                })?,
                        )
                        .build(),
                );
            }
        }
        output.unprocessed_items.insert(table, requests);
    }

    let capacity_count = reader.read_length(MAJOR_ARRAY, "BatchWrite consumed capacity")?;
    ensure_container_length(capacity_count)?;
    if capacity_count > 0 {
        let mut capacities = Vec::with_capacity(capacity_count);
        for _ in 0..capacity_count {
            capacities.push(decode_consumed_capacity(&mut reader)?.ok_or(
                CborError::InvalidAttributeValue("null BatchWrite consumed capacity"),
            )?);
        }
        output.consumed_capacity = Some(capacities);
    }

    let item_collection_metrics = decode_item_collection_metrics_map(&mut reader, schemas)?;
    if !reader.is_empty() {
        return Err(CborError::TrailingData);
    }
    output.item_collection_metrics = item_collection_metrics;
    Ok(output)
}

pub(crate) fn decode_batch_get_body(
    input: &[u8],
    request: &aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemInput,
    schemas: &HashMap<String, Vec<AttributeDefinition>>,
    attribute_lists: &HashMap<i64, Vec<String>>,
) -> Result<BatchGetResponse, CborError> {
    let mut reader = Reader::new(input);
    if reader.consume_null()? {
        return Ok(BatchGetResponse {
            responses: HashMap::new(),
            unprocessed_keys: HashMap::new(),
            consumed_capacity: None,
        });
    }
    let outer = reader.peek()?;
    if outer & 0xe0 != MAJOR_ARRAY {
        return Err(CborError::UnexpectedType {
            expected: "BatchGet response",
            found: outer,
        });
    }
    let indefinite_outer = outer & 0x1f == 31;
    if indefinite_outer {
        reader.read_byte()?;
    } else if reader.read_length(MAJOR_ARRAY, "BatchGet response")? != 2 {
        return Err(CborError::InvalidAttributeValue(
            "BatchGet response must contain two sections",
        ));
    }
    let mut responses = HashMap::new();
    reader.consume_map("BatchGet responses", |reader| {
        let table = reader.read_text()?;
        let schema = schemas
            .get(&table)
            .ok_or(CborError::InvalidItemKey("missing BatchGet key schema"))?;
        let mut items = Vec::new();
        let projection = request
            .request_items()
            .and_then(|items| items.get(&table))
            .and_then(|keys| {
                keys.projection_expression().map(|expression| {
                    build_projection_paths(expression, keys.expression_attribute_names())
                })
            })
            .transpose()?;
        let mut pending_key = None;
        reader.consume_array("BatchGet response items", |reader| {
            if let Some(projection) = projection.as_deref() {
                items.push(decode_projected_attributes(reader, Some(projection))?);
            } else if pending_key.is_none() {
                pending_key = Some(reader.read_bytes()?);
            } else {
                let key_payload = pending_key.take().ok_or(CborError::UnexpectedEnd)?;
                let mut key_frame = Vec::new();
                write_bytes(&mut key_frame, &key_payload);
                let key = decode_item_key(&key_frame, schema)?;
                let mut item = if reader.peek()? & 0xe0 == MAJOR_BYTES {
                    let compressed = reader.read_bytes()?;
                    decode_item_non_key_attributes(&compressed, attribute_lists)?
                } else {
                    decode_item_non_key_attributes_reader(reader, attribute_lists)?
                };
                item.extend(key);
                items.push(item);
            }
            Ok(())
        })?;
        if pending_key.is_some() {
            return Err(CborError::InvalidAttributeValue(
                "BatchGet response item pair is incomplete",
            ));
        }
        responses.insert(table, items);
        Ok(())
    })?;
    let mut unprocessed = HashMap::new();
    reader.consume_map("BatchGet unprocessed keys", |reader| {
        let table = reader.read_text()?;
        let schema = schemas
            .get(&table)
            .ok_or(CborError::InvalidItemKey("missing BatchGet key schema"))?;
        let mut keys = Vec::new();
        reader.consume_array("BatchGet unprocessed keys", |reader| {
            let payload = reader.read_bytes()?;
            let mut frame = Vec::new();
            write_bytes(&mut frame, &payload);
            keys.push(decode_item_key(&frame, schema)?);
            Ok(())
        })?;
        if keys.is_empty() {
            return Ok(());
        }
        let original = request.request_items().and_then(|items| items.get(&table));
        let mut builder =
            aws_sdk_dynamodb::types::KeysAndAttributes::builder().set_keys(Some(keys));
        if let Some(original) = original {
            builder = builder
                .set_consistent_read(original.consistent_read())
                .set_projection_expression(original.projection_expression().map(str::to_owned))
                .set_expression_attribute_names(original.expression_attribute_names().cloned());
        }
        unprocessed.insert(
            table,
            builder
                .build()
                .map_err(|_| CborError::InvalidAttributeValue("invalid BatchGet keys"))?,
        );
        Ok(())
    })?;
    let mut capacities = Vec::new();
    if !reader.is_empty() {
        reader.consume_array("BatchGet consumed capacity", |reader| {
            capacities.push(decode_consumed_capacity(reader)?.ok_or(
                CborError::InvalidAttributeValue("null BatchGet consumed capacity"),
            )?);
            Ok(())
        })?;
    }
    let consumed_capacity = (!capacities.is_empty()).then_some(capacities);
    if indefinite_outer {
        if reader.peek()? != 0xff {
            return Err(CborError::InvalidAttributeValue(
                "BatchGet response must contain two sections",
            ));
        }
        reader.read_byte()?;
    }
    if !reader.is_empty() {
        return Err(CborError::TrailingData);
    }
    Ok(BatchGetResponse {
        responses,
        unprocessed_keys: unprocessed,
        consumed_capacity,
    })
}

pub(crate) fn decode_transact_get_body(
    input: &[u8],
    request: &aws_sdk_dynamodb::operation::transact_get_items::TransactGetItemsInput,
    schemas: &HashMap<String, Vec<AttributeDefinition>>,
    attribute_lists: &HashMap<i64, Vec<String>>,
) -> Result<
    (
        Vec<aws_sdk_dynamodb::types::ItemResponse>,
        Option<Vec<ConsumedCapacity>>,
    ),
    CborError,
> {
    let mut reader = Reader::new(input);
    if reader.consume_null()? {
        return Ok((Vec::new(), None));
    }

    if reader.read_length(MAJOR_ARRAY, "TransactGet response")? != 2 {
        return Err(CborError::InvalidAttributeValue(
            "TransactGet response must contain two sections",
        ));
    }
    let expected = request.transact_items().len();
    let count = reader.read_length(MAJOR_ARRAY, "TransactGet responses")?;
    if count != expected {
        return Err(CborError::InvalidAttributeValue(
            "TransactGet response count does not match request",
        ));
    }
    let mut responses = Vec::with_capacity(count);
    for item in request.transact_items() {
        let get = item.get().ok_or(CborError::InvalidAttributeValue(
            "TransactGet request item must contain Get",
        ))?;
        let projection = get
            .projection_expression()
            .map(|expression| build_projection_paths(expression, get.expression_attribute_names()))
            .transpose()?;
        if reader.consume_null()? {
            responses.push(aws_sdk_dynamodb::types::ItemResponse::builder().build());
            continue;
        }
        if let Some(projection) = projection {
            let item = decode_projected_attributes(&mut reader, Some(&projection))?;
            responses.push(
                aws_sdk_dynamodb::types::ItemResponse::builder()
                    .set_item(Some(item))
                    .build(),
            );
            continue;
        }
        let table = get.table_name();
        let schema = schemas
            .get(table)
            .ok_or(CborError::InvalidItemKey("missing TransactGet key schema"))?;
        let compressed = reader.read_bytes()?;
        let mut attributes = decode_item_non_key_attributes(&compressed, attribute_lists)?;
        attributes.extend(get.key().clone());
        responses.push(
            aws_sdk_dynamodb::types::ItemResponse::builder()
                .set_item(Some(attributes))
                .build(),
        );
        let _ = schema;
    }
    let capacity_count = reader.read_length(MAJOR_ARRAY, "TransactGet consumed capacity")?;
    ensure_container_length(capacity_count)?;
    let consumed_capacity = if capacity_count == 0 {
        None
    } else {
        let mut capacities = Vec::with_capacity(capacity_count);
        for _ in 0..capacity_count {
            capacities.push(decode_consumed_capacity(&mut reader)?.ok_or(
                CborError::InvalidAttributeValue("null TransactGet consumed capacity"),
            )?);
        }
        Some(capacities)
    };
    if !reader.is_empty() {
        return Err(CborError::TrailingData);
    }
    Ok((responses, consumed_capacity))
}

#[derive(Debug, Clone)]
enum ProjectionSegment {
    Attribute(String),
    Index(usize),
}

fn build_projection_paths(
    expression: &str,
    names: Option<&HashMap<String, String>>,
) -> Result<Vec<Vec<ProjectionSegment>>, CborError> {
    expression
        .split(',')
        .map(|term| {
            let term = term.trim();
            if term.is_empty() {
                return Err(CborError::InvalidAttributeValue("empty projection path"));
            }
            let mut segments = Vec::new();
            for component in term.split('.') {
                if component.is_empty() {
                    return Err(CborError::InvalidAttributeValue("invalid projection path"));
                }
                let (attribute, indexes) = component
                    .split_once('[')
                    .map_or((component, ""), |(attribute, indexes)| (attribute, indexes));
                if attribute.is_empty() {
                    return Err(CborError::InvalidAttributeValue("invalid projection path"));
                }
                let attribute = names
                    .and_then(|names| names.get(attribute))
                    .map_or(attribute, String::as_str);
                segments.push(ProjectionSegment::Attribute(attribute.to_owned()));
                let mut remainder = indexes;
                while !remainder.is_empty() {
                    let end = remainder
                        .find(']')
                        .ok_or(CborError::InvalidAttributeValue("invalid projection path"))?;
                    let index = remainder[..end].parse::<usize>().map_err(|_| {
                        CborError::InvalidAttributeValue("invalid projection index")
                    })?;
                    segments.push(ProjectionSegment::Index(index));
                    remainder = &remainder[end + 1..];
                    if remainder.is_empty() {
                        break;
                    }
                    remainder = remainder
                        .strip_prefix('[')
                        .ok_or(CborError::InvalidAttributeValue("invalid projection path"))?;
                }
            }
            Ok(segments)
        })
        .collect()
}

fn decode_projected_attributes(
    reader: &mut Reader<'_>,
    paths: Option<&[Vec<ProjectionSegment>]>,
) -> Result<HashMap<String, AttributeValue>, CborError> {
    let paths = paths.ok_or(CborError::InvalidAttributeValue("missing projection paths"))?;
    let mut root = ProjectionNode::default();
    reader.consume_response_map(|ordinal, reader| {
        let path = paths
            .get(usize::try_from(ordinal).map_err(|_| {
                CborError::InvalidAttributeValue("DAX response ordinal is out of range")
            })?)
            .ok_or(CborError::InvalidAttributeValue(
                "DAX response ordinal exceeds projection",
            ))?;
        let value = decode_value(reader, 0, &mut DecodeBudget::new())?;
        root.insert(path, value)
    })?;
    root.into_item()
}

#[derive(Default)]
struct ProjectionNode {
    value: Option<AttributeValue>,
    children: BTreeMap<ProjectionKey, Self>,
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
enum ProjectionKey {
    Attribute(String),
    Index(usize),
}

impl ProjectionNode {
    fn insert(
        &mut self,
        path: &[ProjectionSegment],
        value: AttributeValue,
    ) -> Result<(), CborError> {
        let Some(segment) = path.first() else {
            self.value = Some(value);
            return Ok(());
        };
        let key = match segment {
            ProjectionSegment::Attribute(name) => ProjectionKey::Attribute(name.clone()),
            ProjectionSegment::Index(index) => ProjectionKey::Index(*index),
        };
        self.children
            .entry(key)
            .or_default()
            .insert(&path[1..], value)
    }

    fn into_attribute(self) -> Result<AttributeValue, CborError> {
        if let Some(value) = self.value {
            return Ok(value);
        }
        if self
            .children
            .keys()
            .any(|key| matches!(key, ProjectionKey::Index(_)))
        {
            let mut values = Vec::with_capacity(self.children.len());
            for (_, child) in self.children {
                values.push(child.into_attribute()?);
            }
            return Ok(AttributeValue::L(values));
        }
        let mut values = HashMap::with_capacity(self.children.len());
        for (key, child) in self.children {
            let ProjectionKey::Attribute(name) = key else {
                return Err(CborError::InvalidAttributeValue(
                    "conflicting projected document paths",
                ));
            };
            values.insert(name, child.into_attribute()?);
        }
        Ok(AttributeValue::M(values))
    }

    fn into_item(self) -> Result<HashMap<String, AttributeValue>, CborError> {
        let mut item = HashMap::with_capacity(self.children.len());
        for (key, child) in self.children {
            let ProjectionKey::Attribute(name) = key else {
                return Err(CborError::InvalidAttributeValue(
                    "projection must start with an attribute",
                ));
            };
            item.insert(name, child.into_attribute()?);
        }
        Ok(item)
    }
}

pub(crate) fn decode_transact_write_body(
    input: &[u8],
    schemas: &HashMap<String, Vec<AttributeDefinition>>,
) -> Result<
    (
        Option<Vec<ConsumedCapacity>>,
        Option<ItemCollectionMetricsMap>,
    ),
    CborError,
> {
    let mut reader = Reader::new(input);
    if reader.consume_null()? {
        return Ok((None, None));
    }
    if input == [MAJOR_MAP] || input == [MAJOR_ARRAY] {
        return Ok((None, None));
    }
    if reader.read_length(MAJOR_ARRAY, "TransactWrite response")? != 3 {
        return Err(CborError::InvalidAttributeValue(
            "TransactWrite response must contain three sections",
        ));
    }
    let return_count = reader.read_length(MAJOR_ARRAY, "TransactWrite return values")?;
    ensure_container_length(return_count)?;
    for _ in 0..return_count {
        if !reader.consume_null()? {
            return Err(CborError::UnsupportedForm(
                "TransactWrite returned attributes",
            ));
        }
    }
    let capacity_count = reader.read_length(MAJOR_ARRAY, "TransactWrite consumed capacity")?;
    ensure_container_length(capacity_count)?;
    let consumed_capacity = if capacity_count == 0 {
        None
    } else {
        let mut capacities = Vec::with_capacity(capacity_count);
        for _ in 0..capacity_count {
            capacities.push(decode_consumed_capacity(&mut reader)?.ok_or(
                CborError::InvalidAttributeValue("null TransactWrite consumed capacity"),
            )?);
        }
        Some(capacities)
    };
    let item_collection_metrics = decode_item_collection_metrics_map(&mut reader, schemas)?;
    if !reader.is_empty() {
        return Err(CborError::TrailingData);
    }
    Ok((consumed_capacity, item_collection_metrics))
}

fn decode_item_collection_metrics_map(
    reader: &mut Reader<'_>,
    schemas: &HashMap<String, Vec<AttributeDefinition>>,
) -> Result<Option<ItemCollectionMetricsMap>, CborError> {
    let count = reader.read_length(MAJOR_MAP, "item collection metrics")?;
    ensure_container_length(count)?;
    if count == 0 {
        return Ok(None);
    }

    let mut output = HashMap::with_capacity(count);
    for _ in 0..count {
        let table = reader.read_text()?;
        let schema = schemas
            .get(&table)
            .ok_or(CborError::InvalidItemKey("missing item metrics key schema"))?;
        let partition_key = schema
            .first()
            .ok_or(CborError::InvalidItemKey("missing partition key schema"))?
            .attribute_name()
            .to_owned();
        let metric_count = reader.read_length(MAJOR_ARRAY, "item collection metrics entries")?;
        ensure_container_length(metric_count)?;
        let mut metrics = Vec::with_capacity(metric_count);
        for _ in 0..metric_count {
            if reader.consume_null()? {
                return Err(CborError::InvalidAttributeValue(
                    "null item collection metric",
                ));
            }
            let _key = reader.read_bytes()?;
            let value = decode_value(reader, 0, &mut DecodeBudget::new())?;
            let lower = reader.read_f64()?;
            let upper = reader.read_f64()?;
            metrics.push(
                aws_sdk_dynamodb::types::ItemCollectionMetrics::builder()
                    .item_collection_key(partition_key.clone(), value)
                    .size_estimate_range_gb(lower)
                    .size_estimate_range_gb(upper)
                    .build(),
            );
        }
        output.insert(table, metrics);
    }
    Ok(Some(output))
}

#[cfg(test)]
mod transact_write_tests {
    use std::collections::HashMap;

    use aws_sdk_dynamodb::types::{AttributeDefinition, AttributeValue};

    use super::{
        CborError, build_projection_paths, decode_projected_attributes, decode_transact_write_body,
    };

    #[test]
    fn decodes_empty_success_response() {
        let response = [0x83, 0x80, 0x80, 0xa0];
        assert_eq!(
            decode_transact_write_body(&response, &HashMap::new()).unwrap(),
            (None, None)
        );
    }

    #[test]
    fn rejects_non_null_transact_write_return_values() {
        let response = [0x83, 0x81, 0x80, 0x80, 0xa0];
        assert_eq!(
            decode_transact_write_body(&response, &HashMap::new()),
            Err(CborError::UnsupportedForm(
                "TransactWrite returned attributes"
            ))
        );
    }

    #[test]
    fn decodes_consumed_capacity() {
        let response = [
            0x83, 0x80, 0x81, 0x40, 0x61, 0x74, 0xfb, 0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0xf6, 0xf6, 0xf6, 0xa0,
        ];
        let capacities = decode_transact_write_body(&response, &HashMap::new())
            .unwrap()
            .0
            .unwrap();
        assert_eq!(capacities.len(), 1);
        assert_eq!(capacities[0].table_name(), Some("t"));
        assert_eq!(capacities[0].capacity_units(), Some(1.0));
    }

    #[test]
    fn rejects_item_collection_metrics() {
        let response = [0x83, 0x80, 0x80, 0xa1, 0x61, 0x74, 0x80];
        assert!(matches!(
            decode_transact_write_body(&response, &HashMap::new()),
            Err(CborError::InvalidItemKey(_))
        ));
    }

    #[test]
    fn decodes_item_collection_metrics() {
        let schema = AttributeDefinition::builder()
            .attribute_name("pk")
            .attribute_type("S".into())
            .build()
            .unwrap();
        let schemas = HashMap::from([("t".to_owned(), vec![schema])]);
        let response = [
            0x83, 0x80, 0x80, 0xa1, 0x61, b't', 0x81, 0x40, 0x61, b'k', 0xfb, 0x3f, 0xf0, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0xfb, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let (_, metrics) = decode_transact_write_body(&response, &schemas).unwrap();
        let metrics = metrics.unwrap();
        assert_eq!(
            metrics["t"][0].item_collection_key().unwrap()["pk"],
            AttributeValue::S("k".into())
        );
        assert_eq!(metrics["t"][0].size_estimate_range_gb(), &[1.0, 2.0]);
    }

    #[test]
    fn decodes_projected_top_level_attribute() {
        let paths = build_projection_paths("status", None).unwrap();
        let mut reader = super::Reader::new(&[0xa1, 0x00, 0x62, b'o', b'k']);
        let item = decode_projected_attributes(&mut reader, Some(&paths)).unwrap();
        assert_eq!(item.get("status"), Some(&AttributeValue::S("ok".into())));
    }

    #[test]
    fn decodes_projected_nested_path() {
        let paths = build_projection_paths("profile.email", None).unwrap();
        let mut reader = super::Reader::new(&[0xa1, 0x00, 0x63, b'a', b'@', b'b']);
        let item = decode_projected_attributes(&mut reader, Some(&paths)).unwrap();
        let profile = item.get("profile").and_then(|value| match value {
            AttributeValue::M(map) => map.get("email"),
            _ => None,
        });
        assert_eq!(profile, Some(&AttributeValue::S("a@b".into())));
    }

    #[test]
    fn decodes_projected_lists_compactly_and_in_source_order() {
        let paths = build_projection_paths("a[3],a[2]", None).unwrap();
        let mut reader = super::Reader::new(&[
            0xa2, 0x00, 0x63, b't', b'h', b'r', 0x01, 0x63, b't', b'w', b'o',
        ]);
        let item = decode_projected_attributes(&mut reader, Some(&paths)).unwrap();
        assert_eq!(
            item.get("a"),
            Some(&AttributeValue::L(vec![
                AttributeValue::S("two".into()),
                AttributeValue::S("thr".into()),
            ]))
        );
    }

    #[test]
    fn decodes_nested_projected_list_and_map_paths() {
        let paths = build_projection_paths("a[2].b.c,a[2].b.d,a[1].b.e", None).unwrap();
        let mut reader =
            super::Reader::new(&[0xa3, 0x00, 0x61, b'c', 0x01, 0x61, b'd', 0x02, 0x61, b'e']);
        let item = decode_projected_attributes(&mut reader, Some(&paths)).unwrap();
        let AttributeValue::L(list) = item.get("a").expect("projected list") else {
            panic!("expected projected list");
        };
        assert_eq!(list.len(), 2);
        let AttributeValue::M(first) = &list[0] else {
            panic!("expected first list element map");
        };
        let AttributeValue::M(first_b) = first.get("b").expect("first b map") else {
            panic!("expected first b map");
        };
        assert_eq!(first_b.get("e"), Some(&AttributeValue::S("e".into())));
        let AttributeValue::M(second) = &list[1] else {
            panic!("expected second list element map");
        };
        let AttributeValue::M(second_b) = second.get("b").expect("second b map") else {
            panic!("expected second b map");
        };
        assert_eq!(second_b.get("c"), Some(&AttributeValue::S("c".into())));
        assert_eq!(second_b.get("d"), Some(&AttributeValue::S("d".into())));
    }

    #[test]
    fn preserves_go_projection_overwrite_for_duplicate_or_parent_paths() {
        let duplicate = build_projection_paths("status,status", None).unwrap();
        let mut reader = super::Reader::new(&[
            0xa2, 0x00, 0x63, b'o', b'l', b'd', 0x01, 0x63, b'n', b'e', b'w',
        ]);
        let item = decode_projected_attributes(&mut reader, Some(&duplicate)).unwrap();
        assert_eq!(item.get("status"), Some(&AttributeValue::S("new".into())));

        let parent = build_projection_paths("profile,profile.email", None).unwrap();
        let mut reader = super::Reader::new(&[
            0xa2, 0x00, 0x63, b'r', b'a', b'w', 0x01, 0x63, b'n', b'e', b'w',
        ]);
        let item = decode_projected_attributes(&mut reader, Some(&parent)).unwrap();
        assert_eq!(item.get("profile"), Some(&AttributeValue::S("raw".into())));
    }

    #[test]
    fn resolves_projection_aliases() {
        let names = HashMap::from([("#s".to_owned(), "status".to_owned())]);
        let paths = build_projection_paths("#s", Some(&names)).unwrap();
        let mut reader = super::Reader::new(&[0xa1, 0x00, 0x61, b'o']);
        let item = decode_projected_attributes(&mut reader, Some(&paths)).unwrap();
        assert_eq!(item.get("status"), Some(&AttributeValue::S("o".into())));
    }

    #[test]
    fn preserves_dots_inside_response_projection_aliases() {
        let names = HashMap::from([
            ("#a".to_owned(), "with.dot".to_owned()),
            ("#b".to_owned(), "sub.field".to_owned()),
        ]);
        let paths = build_projection_paths("#a.#b", Some(&names)).unwrap();
        let mut reader = super::Reader::new(&[0xa1, 0x00, 0x61, b'o']);
        let item = decode_projected_attributes(&mut reader, Some(&paths)).unwrap();
        let nested = item.get("with.dot").and_then(|value| match value {
            AttributeValue::M(map) => map.get("sub.field"),
            _ => None,
        });
        assert_eq!(nested, Some(&AttributeValue::S("o".into())));
    }
}

/// Decodes the mandatory DAX success/error envelope before an operation payload.
#[allow(
    dead_code,
    reason = "Phase 3 response envelopes are validated before their transport integration"
)]
pub(crate) fn decode_response_envelope(input: &[u8]) -> Result<ResponseEnvelope<'_>, CborError> {
    if input.first() == Some(&0xf6) {
        return Ok(ResponseEnvelope::Success(&input[1..]));
    }
    let mut reader = Reader::new(input);
    let length = reader.read_length(MAJOR_ARRAY, "error code array")?;
    ensure_container_length(length)?;
    if length == 0 {
        return Ok(ResponseEnvelope::Success(reader.remaining()));
    }

    let mut code_sequence = Vec::new();
    for _ in 0..length {
        code_sequence.push(
            i64::try_from(read_integer(&mut reader)?)
                .map_err(|_| CborError::InvalidAttributeValue("DAX error code is out of range"))?,
        );
    }
    let message = reader.read_text()?;
    let (request_id, error_code, status_code, cancellation_reasons) = if reader.peek()? == 0xf6 {
        reader.read_byte()?;
        (None, None, infer_error_status(&code_sequence), None)
    } else {
        let detail_length = reader.read_length(MAJOR_ARRAY, "DAX error detail")?;
        if !(3..=4).contains(&detail_length) {
            return Err(CborError::InvalidAttributeValue(
                "DAX error detail must contain three or four values",
            ));
        }
        let request_id = read_optional_text(&mut reader)?;
        let error_code = read_optional_text(&mut reader)?;
        let status_code =
            read_optional_i64(&mut reader)?.unwrap_or_else(|| infer_error_status(&code_sequence));
        let cancellation_reasons = if detail_length == 4 {
            let reason_count = reader.read_length(MAJOR_ARRAY, "DAX cancellation reasons")?;
            ensure_container_length(reason_count)?;
            if reason_count % 3 != 0 {
                return Err(CborError::InvalidAttributeValue(
                    "DAX cancellation reasons must contain triples",
                ));
            }
            let mut reasons = Vec::with_capacity(reason_count / 3);
            for _ in 0..reason_count / 3 {
                let code = read_optional_text(&mut reader)?;
                let message = read_optional_text(&mut reader)?;
                let item_cbor = if reader.peek()? == 0xf6 {
                    reader.read_byte()?;
                    None
                } else if reader.peek()? & 0xe0 == MAJOR_BYTES {
                    Some(reader.read_bytes()?.into_boxed_slice())
                } else {
                    let start = reader.position();
                    reader.skip_value()?;
                    let end = reader.position();
                    Some(reader.bytes_between(start, end)?.into())
                };
                reasons.push(CancellationReasonMetadata {
                    code,
                    message,
                    item_cbor,
                });
            }
            Some(reasons.into_boxed_slice())
        } else {
            None
        };
        (request_id, error_code, status_code, cancellation_reasons)
    };
    if !reader.is_empty() {
        return Err(CborError::TrailingData);
    }
    Ok(ResponseEnvelope::Error(DaxResponseError {
        code_sequence,
        message,
        request_id,
        error_code,
        status_code,
        cancellation_reasons,
    }))
}

/// Decodes an expression-free GetItem response body after a successful envelope.
#[allow(
    dead_code,
    reason = "Phase 3 response codecs are validated before their transport integration"
)]
pub(crate) fn decode_get_item_body(
    input: &[u8],
    request_key: &HashMap<String, AttributeValue>,
    attribute_lists: &HashMap<i64, Vec<String>>,
) -> Result<Option<HashMap<String, AttributeValue>>, CborError> {
    let mut reader = Reader::new(input);
    if reader.consume_null()? {
        return Ok(None);
    }

    let mut item = None;
    reader.consume_response_map(|ordinal, reader| match ordinal {
        0 => {
            let compressed = reader.read_bytes()?;
            let mut decoded = decode_item_non_key_attributes(&compressed, attribute_lists)?;
            decoded.extend(request_key.clone());
            item = Some(decoded);
            Ok(())
        }
        1 => Err(CborError::UnsupportedForm("DAX consumed capacity response")),
        _ => Err(CborError::UnsupportedForm(
            "unknown GetItem response ordinal",
        )),
    })?;
    if !reader.is_empty() {
        return Err(CborError::TrailingData);
    }
    Ok(item)
}

/// Decodes an expression-free Scan response body after a successful envelope.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ScanResponse {
    pub(crate) items: Vec<HashMap<String, AttributeValue>>,
    pub(crate) consumed_capacity: Option<ConsumedCapacity>,
    pub(crate) count: Option<i32>,
    pub(crate) scanned_count: Option<i32>,
    pub(crate) last_evaluated_key: Option<HashMap<String, AttributeValue>>,
}

pub(crate) fn decode_scan_body(
    input: &[u8],
    key_definition: &[AttributeDefinition],
    attribute_lists: &HashMap<i64, Vec<String>>,
) -> Result<ScanResponse, CborError> {
    let mut reader = Reader::new(input);
    if reader.consume_null()? {
        return Ok(ScanResponse {
            items: Vec::new(),
            consumed_capacity: None,
            count: None,
            scanned_count: None,
            last_evaluated_key: None,
        });
    }

    let mut items = Vec::new();
    let mut consumed_capacity = None;
    let mut count = None;
    let mut scanned_count = None;
    let mut last_evaluated_key = None;
    reader.consume_response_map(|ordinal, reader| match ordinal {
        7 => {
            reader.consume_array("Scan item array", |reader| {
                let mut pair = Vec::with_capacity(2);
                reader.consume_array("Scan item pair", |reader| {
                    pair.push(reader.read_bytes()?);
                    Ok(())
                })?;
                if pair.len() != 2 {
                    return Err(CborError::InvalidAttributeValue(
                        "DAX Scan item must contain a key and non-key attributes",
                    ));
                }
                let key_payload = pair.remove(0);
                let compressed = pair.remove(0);
                let mut key_frame = Vec::new();
                write_bytes(&mut key_frame, &key_payload);
                let key = decode_item_key(&key_frame, key_definition)?;
                let mut item = decode_item_non_key_attributes(&compressed, attribute_lists)?;
                item.extend(key);
                items.push(item);
                Ok(())
            })?;
            Ok(())
        }
        1 => {
            consumed_capacity = decode_consumed_capacity(reader)?;
            Ok(())
        }
        8 => {
            count =
                Some(i32::try_from(read_integer(reader)?).map_err(|_| {
                    CborError::InvalidAttributeValue("DAX Scan count is out of range")
                })?);
            Ok(())
        }
        9 => {
            if reader.consume_null()? {
                return Ok(());
            }
            let key_payload = reader.read_bytes()?;
            let mut key_frame = Vec::new();
            write_bytes(&mut key_frame, &key_payload);
            last_evaluated_key = Some(decode_item_key(&key_frame, key_definition)?);
            Ok(())
        }
        10 => {
            scanned_count = Some(i32::try_from(read_integer(reader)?).map_err(|_| {
                CborError::InvalidAttributeValue("DAX Scan scanned count is out of range")
            })?);
            Ok(())
        }
        _ => Err(CborError::UnsupportedForm("unknown Scan response ordinal")),
    })?;
    if !reader.is_empty() {
        return Err(CborError::TrailingData);
    }
    Ok(ScanResponse {
        items,
        consumed_capacity,
        count,
        scanned_count,
        last_evaluated_key,
    })
}

fn decode_consumed_capacity(
    reader: &mut Reader<'_>,
) -> Result<Option<ConsumedCapacity>, CborError> {
    if reader.consume_null()? {
        return Ok(None);
    }

    let _metadata = reader.read_bytes()?;
    let table_name = reader.read_text()?;
    let capacity_units = reader.read_f64()?;
    let table = if reader.consume_null()? {
        None
    } else {
        Some(
            Capacity::builder()
                .capacity_units(reader.read_f64()?)
                .build(),
        )
    };
    let global_secondary_indexes = decode_consumed_capacity_indexes(reader)?;
    let local_secondary_indexes = decode_consumed_capacity_indexes(reader)?;

    Ok(Some(
        ConsumedCapacity::builder()
            .table_name(table_name)
            .capacity_units(capacity_units)
            .set_table(table)
            .set_global_secondary_indexes(global_secondary_indexes)
            .set_local_secondary_indexes(local_secondary_indexes)
            .build(),
    ))
}

fn decode_consumed_capacity_indexes(
    reader: &mut Reader<'_>,
) -> Result<Option<HashMap<String, Capacity>>, CborError> {
    if reader.consume_null()? {
        return Ok(None);
    }

    let mut indexes = HashMap::new();
    reader.consume_map("DAX consumed-capacity index map", |reader| {
        let name = reader.read_text()?;
        indexes.insert(
            name,
            Capacity::builder()
                .capacity_units(reader.read_f64()?)
                .build(),
        );
        Ok(())
    })?;
    Ok(Some(indexes))
}

/// Decodes an expression-free PutItem response body after a successful envelope.
#[allow(
    dead_code,
    reason = "Phase 3 response codecs are validated before their transport integration"
)]
pub(crate) fn decode_put_item_body(
    input: &[u8],
    request_item: &HashMap<String, AttributeValue>,
    key_definition: &[AttributeDefinition],
    attribute_lists: &HashMap<i64, Vec<String>>,
) -> Result<Option<HashMap<String, AttributeValue>>, CborError> {
    decode_old_attributes_body(
        input,
        request_item,
        key_definition,
        attribute_lists,
        "PutItem",
    )
}

/// Decodes an expression-free DeleteItem response body after a successful envelope.
pub(crate) fn decode_delete_item_body(
    input: &[u8],
    request_key: &HashMap<String, AttributeValue>,
    key_definition: &[AttributeDefinition],
    attribute_lists: &HashMap<i64, Vec<String>>,
) -> Result<Option<HashMap<String, AttributeValue>>, CborError> {
    decode_old_attributes_body(
        input,
        request_key,
        key_definition,
        attribute_lists,
        "DeleteItem",
    )
}

fn decode_old_attributes_body(
    input: &[u8],
    request_item: &HashMap<String, AttributeValue>,
    key_definition: &[AttributeDefinition],
    attribute_lists: &HashMap<i64, Vec<String>>,
    operation: &'static str,
) -> Result<Option<HashMap<String, AttributeValue>>, CborError> {
    if input.is_empty() {
        return Ok(None);
    }
    let mut reader = Reader::new(input);
    if reader.consume_null()? {
        return Ok(None);
    }
    if reader.peek()? == MAJOR_ARRAY {
        let length = reader.read_length(MAJOR_ARRAY, "DAX item mutation response")?;
        if length == 0 {
            return Ok(None);
        }
        return Err(CborError::UnexpectedType {
            expected: "DAX item mutation response map",
            found: MAJOR_ARRAY,
        });
    }

    let mut attributes = None;
    reader.consume_response_map(|ordinal, reader| match ordinal {
        2 => {
            let compressed = reader.read_bytes()?;
            let mut decoded = decode_item_non_key_attributes(&compressed, attribute_lists)?;
            for definition in validated_key_definition(key_definition)? {
                let name = definition.attribute_name();
                let value = request_item
                    .get(name)
                    .ok_or(CborError::InvalidItemKey("a required key is missing"))?;
                decoded.insert(name.to_owned(), value.clone());
            }
            attributes = Some(decoded);
            Ok(())
        }
        1 => Err(CborError::UnsupportedForm("DAX consumed capacity response")),
        3 => Err(CborError::UnsupportedForm(
            "DAX item collection metrics response",
        )),
        _ => Err(CborError::UnsupportedForm(match operation {
            "PutItem" => "unknown PutItem response ordinal",
            "DeleteItem" => "unknown DeleteItem response ordinal",
            _ => "unknown item mutation response ordinal",
        })),
    })?;
    if !reader.is_empty() {
        return Err(CborError::TrailingData);
    }
    Ok(attributes)
}

/// Decodes a successful DefineKeySchema response body.
#[allow(
    dead_code,
    reason = "Phase 3 control codecs are validated before their transport integration"
)]
pub(crate) fn decode_define_key_schema_body(
    input: &[u8],
) -> Result<Vec<AttributeDefinition>, CborError> {
    let mut reader = Reader::new(input);
    let length = reader.read_length(MAJOR_MAP, "key schema map")?;
    ensure_container_length(length)?;
    let mut definitions = Vec::new();
    for _ in 0..length {
        definitions.push(
            AttributeDefinition::builder()
                .attribute_name(reader.read_text()?)
                .attribute_type(ScalarAttributeType::from(reader.read_text()?.as_str()))
                .build()
                .map_err(|_| CborError::InvalidAttributeValue("invalid key schema"))?,
        );
    }
    if !reader.is_empty() {
        return Err(CborError::TrailingData);
    }
    Ok(definitions)
}

/// Decodes a successful DefineAttributeListId response body.
#[allow(
    dead_code,
    reason = "Phase 3 control codecs are validated before their transport integration"
)]
pub(crate) fn decode_define_attribute_list_id_body(input: &[u8]) -> Result<i64, CborError> {
    let mut reader = Reader::new(input);
    let id = i64::try_from(read_integer(&mut reader)?)
        .map_err(|_| CborError::InvalidAttributeValue("attribute-list ID is out of range"))?;
    if !reader.is_empty() {
        return Err(CborError::TrailingData);
    }
    Ok(id)
}

/// Decodes a successful DefineAttributeList response body.
#[allow(
    dead_code,
    reason = "Phase 3 control codecs are validated before their transport integration"
)]
pub(crate) fn decode_define_attribute_list_body(input: &[u8]) -> Result<Vec<String>, CborError> {
    let mut reader = Reader::new(input);
    let length = reader.read_length(MAJOR_ARRAY, "attribute-name array")?;
    ensure_container_length(length)?;
    let mut names = Vec::new();
    for _ in 0..length {
        names.push(reader.read_text()?);
    }
    if !reader.is_empty() {
        return Err(CborError::TrailingData);
    }
    Ok(names)
}

fn read_optional_text(reader: &mut Reader<'_>) -> Result<Option<String>, CborError> {
    if reader.peek()? == 0xf6 {
        reader.read_byte()?;
        Ok(None)
    } else {
        reader.read_text().map(Some)
    }
}

fn read_optional_i64(reader: &mut Reader<'_>) -> Result<Option<i64>, CborError> {
    if reader.peek()? == 0xf6 {
        reader.read_byte()?;
        Ok(None)
    } else {
        let value = read_integer(reader)?;
        i64::try_from(value)
            .map(Some)
            .map_err(|_| CborError::InvalidAttributeValue("DAX error status is out of range"))
    }
}

fn infer_error_status(code_sequence: &[i64]) -> i64 {
    if code_sequence.first() == Some(&4) {
        400
    } else {
        500
    }
}

/// Encodes a DynamoDB AttributeValue subset into DAX CBOR.
pub fn encode_attribute_value(value: &AttributeValue) -> Result<Vec<u8>, CborError> {
    let mut output = Vec::new();
    encode_value(value, &mut output, 0)?;
    Ok(output)
}

/// Decodes one complete DynamoDB AttributeValue subset from DAX CBOR.
pub fn decode_attribute_value(input: &[u8]) -> Result<AttributeValue, CborError> {
    let mut reader = Reader::new(input);
    let mut budget = DecodeBudget::new();
    let value = decode_value(&mut reader, 0, &mut budget)?;
    if !reader.is_empty() {
        return Err(CborError::TrailingData);
    }
    Ok(value)
}

/// Encodes a DynamoDB item key using the DAX CBOR key framing.
pub fn encode_item_key(
    item: &HashMap<String, AttributeValue>,
    key_definition: &[AttributeDefinition],
) -> Result<Vec<u8>, CborError> {
    let keys = validated_key_definition(key_definition)?;
    let mut key = Vec::new();
    encode_hash_key(
        required_key(item, &keys[0])?,
        keys[0].attribute_type(),
        keys.len() == 2,
        &mut key,
    )?;
    if keys.len() == 2 {
        encode_range_key(
            required_key(item, &keys[1])?,
            keys[1].attribute_type(),
            &mut key,
        )?;
    }

    let mut output = Vec::new();
    write_bytes(&mut output, &key);
    Ok(output)
}

/// Decodes a DAX CBOR key frame using a DynamoDB key definition.
pub fn decode_item_key(
    input: &[u8],
    key_definition: &[AttributeDefinition],
) -> Result<HashMap<String, AttributeValue>, CborError> {
    let keys = validated_key_definition(key_definition)?;
    let mut frame = Reader::new(input);
    let payload = frame.read_bytes()?;
    if !frame.is_empty() {
        return Err(CborError::TrailingData);
    }

    let mut payload = Reader::new(&payload);
    let mut budget = DecodeBudget::new();
    let mut item = HashMap::with_capacity(keys.len());
    let hash_value = if keys.len() == 1 {
        decode_single_hash_key(&mut payload, keys[0].attribute_type())?
    } else {
        decode_composite_hash_key(&mut payload, keys[0].attribute_type(), &mut budget)?
    };
    item.insert(keys[0].attribute_name().to_owned(), hash_value);
    if keys.len() == 2 {
        item.insert(
            keys[1].attribute_name().to_owned(),
            decode_range_key(&mut payload, keys[1].attribute_type())?,
        );
    }
    if !payload.is_empty() {
        return Err(CborError::TrailingData);
    }
    Ok(item)
}

/// Encodes non-key item attributes using a DAX attribute-name list ID.
///
/// The caller owns schema-list cache lookup. Attribute names are sorted to
/// match the reference client's cache key before their values are encoded.
pub fn encode_item_non_key_attributes(
    item: &HashMap<String, AttributeValue>,
    key_definition: &[AttributeDefinition],
    attribute_list_id: i64,
) -> Result<Vec<u8>, CborError> {
    let keys = validated_key_definition(key_definition)?;
    let mut names = item
        .keys()
        .filter(|name| {
            name.as_str() != keys[0].attribute_name()
                && (keys.len() == 1 || name.as_str() != keys[1].attribute_name())
        })
        .collect::<Vec<_>>();
    names.sort_unstable();

    let mut output = Vec::new();
    write_bigint(&mut output, &BigInt::from(attribute_list_id));
    for name in names {
        encode_value(
            item.get(name)
                .ok_or(CborError::InvalidItemKey("item attribute disappeared"))?,
            &mut output,
            0,
        )?;
    }
    Ok(output)
}

/// Decodes schema-compressed non-key item attributes using known attribute-name lists.
pub fn decode_item_non_key_attributes(
    input: &[u8],
    attribute_lists: &HashMap<i64, Vec<String>>,
) -> Result<HashMap<String, AttributeValue>, CborError> {
    let mut reader = Reader::new(input);
    let attributes = decode_item_non_key_attributes_reader(&mut reader, attribute_lists)?;
    if !reader.is_empty() {
        return Err(CborError::TrailingData);
    }
    Ok(attributes)
}

fn decode_item_non_key_attributes_reader(
    reader: &mut Reader<'_>,
    attribute_lists: &HashMap<i64, Vec<String>>,
) -> Result<HashMap<String, AttributeValue>, CborError> {
    let id = i64::try_from(read_integer(reader)?)
        .map_err(|_| CborError::InvalidAttributeValue("attribute-name list ID is out of range"))?;
    let names = attribute_lists
        .get(&id)
        .ok_or(CborError::UnknownAttributeListId(id))?;
    ensure_container_length(names.len())?;

    let mut attributes = HashMap::with_capacity(names.len());
    let mut budget = DecodeBudget::new();
    if reader.peek()? & 0xe0 == MAJOR_BYTES {
        let payload = reader.read_bytes()?;
        let mut nested = Reader::new(&payload);
        for name in names {
            attributes.insert(name.clone(), decode_value(&mut nested, 0, &mut budget)?);
        }
        if !nested.is_empty() {
            return Err(CborError::TrailingData);
        }
    } else {
        for name in names {
            attributes.insert(name.clone(), decode_value(reader, 0, &mut budget)?);
        }
    }
    Ok(attributes)
}

/// Decodes a transaction cancellation item using the request key and caches
/// supplied by the caller.
pub fn decode_transaction_cancellation_item(
    input: Option<&[u8]>,
    key: &HashMap<String, AttributeValue>,
    key_definition: &[AttributeDefinition],
    attribute_lists: &HashMap<i64, Vec<String>>,
) -> Result<HashMap<String, AttributeValue>, CborError> {
    encode_item_key(key, key_definition)?;
    let mut item = decode_item_non_key_attributes(
        input.ok_or(CborError::MissingItemPayload)?,
        attribute_lists,
    )?;
    item.extend(key.clone());
    Ok(item)
}

fn validated_key_definition(
    key_definition: &[AttributeDefinition],
) -> Result<&[AttributeDefinition], CborError> {
    if !(1..=2).contains(&key_definition.len()) {
        return Err(CborError::InvalidItemKey(
            "key definition must contain one or two attributes",
        ));
    }
    Ok(key_definition)
}

fn required_key<'a>(
    item: &'a HashMap<String, AttributeValue>,
    definition: &AttributeDefinition,
) -> Result<&'a AttributeValue, CborError> {
    item.get(definition.attribute_name())
        .ok_or(CborError::InvalidItemKey("a required key is missing"))
}

fn encode_hash_key(
    value: &AttributeValue,
    key_type: &ScalarAttributeType,
    composite: bool,
    output: &mut Vec<u8>,
) -> Result<(), CborError> {
    match (key_type.as_str(), value) {
        ("S", AttributeValue::S(value)) if composite => write_text(output, value),
        ("S", AttributeValue::S(value)) => output.extend_from_slice(value.as_bytes()),
        ("N", AttributeValue::N(_)) => encode_value(value, output, 0)?,
        ("B", AttributeValue::B(value)) if composite => write_bytes(output, value.as_ref()),
        ("B", AttributeValue::B(value)) => output.extend_from_slice(value.as_ref()),
        ("S" | "N" | "B", _) => {
            return Err(CborError::InvalidItemKey(
                "key value does not match its key definition",
            ));
        }
        _ => return Err(CborError::InvalidItemKey("unsupported key attribute type")),
    }
    Ok(())
}

fn encode_range_key(
    value: &AttributeValue,
    key_type: &ScalarAttributeType,
    output: &mut Vec<u8>,
) -> Result<(), CborError> {
    match (key_type.as_str(), value) {
        ("S", AttributeValue::S(value)) => output.extend_from_slice(value.as_bytes()),
        ("N", AttributeValue::N(value)) => output.extend_from_slice(&encode_lex_decimal(value)?),
        ("B", AttributeValue::B(value)) => output.extend_from_slice(value.as_ref()),
        ("S" | "N" | "B", _) => {
            return Err(CborError::InvalidItemKey(
                "key value does not match its key definition",
            ));
        }
        _ => return Err(CborError::InvalidItemKey("unsupported key attribute type")),
    }
    Ok(())
}

fn decode_single_hash_key(
    payload: &mut Reader<'_>,
    key_type: &ScalarAttributeType,
) -> Result<AttributeValue, CborError> {
    let bytes = payload.read_exact(payload.input.len() - payload.offset)?;
    match key_type.as_str() {
        "S" => String::from_utf8(bytes.to_vec())
            .map(AttributeValue::S)
            .map_err(|_| CborError::InvalidUtf8),
        "N" => decode_attribute_value(bytes),
        "B" => Ok(AttributeValue::B(Blob::new(bytes))),
        _ => Err(CborError::InvalidItemKey("unsupported key attribute type")),
    }
}

fn decode_composite_hash_key(
    payload: &mut Reader<'_>,
    key_type: &ScalarAttributeType,
    budget: &mut DecodeBudget,
) -> Result<AttributeValue, CborError> {
    match key_type.as_str() {
        "S" => Ok(AttributeValue::S(payload.read_text()?)),
        "N" => match decode_value(payload, 0, budget)? {
            value @ AttributeValue::N(_) => Ok(value),
            _ => Err(CborError::InvalidItemKey(
                "key value does not match its key definition",
            )),
        },
        "B" => Ok(AttributeValue::B(Blob::new(payload.read_bytes()?))),
        _ => Err(CborError::InvalidItemKey("unsupported key attribute type")),
    }
}

fn decode_range_key(
    payload: &mut Reader<'_>,
    key_type: &ScalarAttributeType,
) -> Result<AttributeValue, CborError> {
    let bytes = payload.read_exact(payload.input.len() - payload.offset)?;
    match key_type.as_str() {
        "S" => String::from_utf8(bytes.to_vec())
            .map(AttributeValue::S)
            .map_err(|_| CborError::InvalidUtf8),
        "N" => {
            let (value, consumed) = decode_lex_decimal(bytes)?;
            if consumed != bytes.len() {
                return Err(CborError::TrailingData);
            }
            Ok(AttributeValue::N(value.to_string()))
        }
        "B" => Ok(AttributeValue::B(Blob::new(bytes))),
        _ => Err(CborError::InvalidItemKey("unsupported key attribute type")),
    }
}
fn encode_value(
    value: &AttributeValue,
    output: &mut Vec<u8>,
    depth: usize,
) -> Result<(), CborError> {
    if depth > MAX_NESTING_DEPTH {
        return Err(CborError::NestingTooDeep);
    }
    match value {
        AttributeValue::S(value) => write_text(output, value),
        AttributeValue::B(value) => write_bytes(output, value.as_ref()),
        AttributeValue::N(value) => write_number(output, value)?,
        AttributeValue::Bool(value) => output.push(if *value { 0xf5 } else { 0xf4 }),
        AttributeValue::Null(true) => output.push(0xf6),
        AttributeValue::Null(false) => {
            return Err(CborError::InvalidAttributeValue("NULL must be true"));
        }
        AttributeValue::L(values) => {
            write_type(output, MAJOR_ARRAY, values.len() as u64);
            for value in values {
                encode_value(value, output, depth + 1)?;
            }
        }
        AttributeValue::M(values) => {
            write_type(output, MAJOR_MAP, values.len() as u64);
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_unstable_by_key(|(key, _)| *key);
            for (key, value) in entries {
                write_text(output, key);
                encode_value(value, output, depth + 1)?;
            }
        }
        AttributeValue::Ss(values) => {
            write_set_header(output, TAG_STRING_SET, values.len(), "SS")?;
            for value in values {
                write_text(output, value);
            }
        }
        AttributeValue::Ns(values) => {
            write_set_header(output, TAG_NUMBER_SET, values.len(), "NS")?;
            for value in values {
                write_number(output, value)?;
            }
        }
        AttributeValue::Bs(values) => {
            write_set_header(output, TAG_BINARY_SET, values.len(), "BS")?;
            for value in values {
                write_bytes(output, value.as_ref());
            }
        }
        _ => return Err(CborError::UnsupportedAttributeValue("unknown SDK variant")),
    }
    Ok(())
}

struct DecodeBudget {
    remaining_values: usize,
}

impl DecodeBudget {
    const fn new() -> Self {
        Self {
            remaining_values: MAX_DECODED_VALUES,
        }
    }

    fn consume(&mut self) -> Result<(), CborError> {
        self.remaining_values = self
            .remaining_values
            .checked_sub(1)
            .ok_or(CborError::ContainerTooLarge(MAX_DECODED_VALUES))?;
        Ok(())
    }
}

fn decode_value(
    reader: &mut Reader<'_>,
    depth: usize,
    budget: &mut DecodeBudget,
) -> Result<AttributeValue, CborError> {
    if depth > MAX_NESTING_DEPTH {
        return Err(CborError::NestingTooDeep);
    }
    budget.consume()?;
    let initial = reader.peek()?;
    match initial & 0xe0 {
        MAJOR_TEXT => Ok(AttributeValue::S(reader.read_text()?)),
        MAJOR_BYTES => Ok(AttributeValue::B(Blob::new(reader.read_bytes()?))),
        MAJOR_ARRAY => {
            let mut values = Vec::new();
            reader.consume_array("array", |reader| {
                values.push(decode_value(reader, depth + 1, budget)?);
                Ok(())
            })?;
            Ok(AttributeValue::L(values))
        }
        MAJOR_MAP => {
            let mut values = HashMap::new();
            reader.consume_map("map", |reader| {
                let key = reader.read_text()?;
                let value = decode_value(reader, depth + 1, budget)?;
                values.insert(key, value);
                Ok(())
            })?;
            Ok(AttributeValue::M(values))
        }
        MAJOR_SIMPLE => match reader.read_byte()? {
            0xf4 => Ok(AttributeValue::Bool(false)),
            0xf5 => Ok(AttributeValue::Bool(true)),
            0xf6 => Ok(AttributeValue::Null(true)),
            found => Err(CborError::UnexpectedType {
                expected: "DynamoDB boolean or null",
                found,
            }),
        },
        MAJOR_UNSIGNED | MAJOR_NEGATIVE => Ok(AttributeValue::N(read_integer(reader)?.to_string())),
        MAJOR_TAG => decode_tagged_value(reader, depth, budget),
        _ => Err(CborError::UnexpectedType {
            expected: "DynamoDB AttributeValue",
            found: initial,
        }),
    }
}

fn decode_tagged_value(
    reader: &mut Reader<'_>,
    depth: usize,
    budget: &mut DecodeBudget,
) -> Result<AttributeValue, CborError> {
    let tag = reader.read_length(MAJOR_TAG, "tag")? as u64;
    match tag {
        TAG_POSITIVE_BIGNUM | TAG_NEGATIVE_BIGNUM => {
            Ok(AttributeValue::N(read_bignum(reader, tag)?.to_string()))
        }
        TAG_DECIMAL => Ok(AttributeValue::N(read_decimal(reader)?.to_string())),
        TAG_STRING_SET => {
            let mut values = Vec::new();
            reader.consume_array("array", |reader| {
                budget.consume()?;
                values.push(reader.read_text()?);
                Ok(())
            })?;
            Ok(AttributeValue::Ss(values))
        }
        TAG_NUMBER_SET => {
            let mut values = Vec::new();
            reader.consume_array("array", |reader| {
                match decode_value(reader, depth + 1, budget)? {
                    AttributeValue::N(value) => values.push(value),
                    _ => {
                        return Err(CborError::InvalidAttributeValue(
                            "NS member is not a number",
                        ));
                    }
                }
                Ok(())
            })?;
            Ok(AttributeValue::Ns(values))
        }
        TAG_BINARY_SET => {
            let mut values = Vec::new();
            reader.consume_array("array", |reader| {
                budget.consume()?;
                values.push(Blob::new(reader.read_bytes()?));
                Ok(())
            })?;
            Ok(AttributeValue::Bs(values))
        }
        _ => Err(CborError::UnsupportedForm("unknown DAX AttributeValue tag")),
    }
}

fn write_set_header(
    output: &mut Vec<u8>,
    tag: u64,
    length: usize,
    variant: &'static str,
) -> Result<(), CborError> {
    if length == 0 {
        return Err(CborError::InvalidAttributeValue(match variant {
            "SS" => "SS must not be empty",
            "NS" => "NS must not be empty",
            "BS" => "BS must not be empty",
            _ => "set must not be empty",
        }));
    }
    write_type(output, MAJOR_TAG, tag);
    write_type(output, MAJOR_ARRAY, length as u64);
    Ok(())
}

fn write_number(output: &mut Vec<u8>, value: &str) -> Result<(), CborError> {
    if value.contains(['.', 'e', 'E']) {
        let decimal = Decimal::parse(value)?;
        write_type(output, MAJOR_TAG, TAG_DECIMAL);
        write_type(output, MAJOR_ARRAY, 2);
        write_bigint(output, &BigInt::from(decimal.exponent));
        write_bigint(output, &decimal.unscaled);
        return Ok(());
    }
    let integer = value
        .parse::<BigInt>()
        .map_err(|_| CborError::InvalidAttributeValue("N is not a valid integer"))?;
    write_bigint(output, &integer);
    Ok(())
}

fn write_bigint(output: &mut Vec<u8>, value: &BigInt) {
    if value.sign() != Sign::Minus {
        let (_, bytes) = value.to_bytes_be();
        if bytes.len() <= 8 {
            let magnitude = bytes
                .iter()
                .fold(0_u64, |current, byte| (current << 8) | u64::from(*byte));
            write_type(output, MAJOR_UNSIGNED, magnitude);
        } else {
            write_type(output, MAJOR_TAG, TAG_POSITIVE_BIGNUM);
            write_bytes(output, &bytes);
        }
        return;
    }

    let magnitude = -value - 1_u8;
    let (_, bytes) = magnitude.to_bytes_be();
    if bytes.len() <= 8 {
        let encoded = bytes
            .iter()
            .fold(0_u64, |current, byte| (current << 8) | u64::from(*byte));
        write_type(output, MAJOR_NEGATIVE, encoded);
    } else {
        write_type(output, MAJOR_TAG, TAG_NEGATIVE_BIGNUM);
        write_bytes(output, &bytes);
    }
}

fn read_integer(reader: &mut Reader<'_>) -> Result<BigInt, CborError> {
    let initial = reader.read_byte()?;
    let major = initial & 0xe0;
    let value = reader.read_argument(initial)?;
    match major {
        MAJOR_UNSIGNED => Ok(BigInt::from(value)),
        MAJOR_NEGATIVE => Ok(-BigInt::from(value) - 1_u8),
        _ => Err(CborError::UnexpectedType {
            expected: "CBOR integer",
            found: initial,
        }),
    }
}

fn read_bignum(reader: &mut Reader<'_>, tag: u64) -> Result<BigInt, CborError> {
    let bytes = reader.read_bytes()?;
    let magnitude = BigInt::from_bytes_be(Sign::Plus, &bytes);
    if tag == TAG_POSITIVE_BIGNUM {
        Ok(magnitude)
    } else {
        Ok(-magnitude - 1_u8)
    }
}

fn read_decimal(reader: &mut Reader<'_>) -> Result<Decimal, CborError> {
    let length = reader.read_length(MAJOR_ARRAY, "decimal fraction array")?;
    if length != 2 {
        return Err(CborError::InvalidAttributeValue(
            "decimal fraction must contain two values",
        ));
    }
    let exponent = read_numeric(reader)?;
    let exponent = i64::try_from(exponent)
        .map_err(|_| CborError::InvalidAttributeValue("decimal exponent is out of range"))?;
    let unscaled = read_numeric(reader)?;
    Ok(Decimal { exponent, unscaled })
}

fn read_numeric(reader: &mut Reader<'_>) -> Result<BigInt, CborError> {
    match reader.peek()? & 0xe0 {
        MAJOR_UNSIGNED | MAJOR_NEGATIVE => read_integer(reader),
        MAJOR_TAG => {
            let tag = reader.read_length(MAJOR_TAG, "numeric tag")? as u64;
            match tag {
                TAG_POSITIVE_BIGNUM | TAG_NEGATIVE_BIGNUM => read_bignum(reader, tag),
                _ => Err(CborError::InvalidAttributeValue(
                    "expected integer or bignum",
                )),
            }
        }
        _ => Err(CborError::InvalidAttributeValue(
            "expected integer or bignum",
        )),
    }
}

struct Decimal {
    exponent: i64,
    unscaled: BigInt,
}

impl Decimal {
    fn parse(value: &str) -> Result<Self, CborError> {
        let (significand, exponent) = match value.find(['e', 'E']) {
            Some(index) => {
                let exponent = value[index + 1..]
                    .parse::<i32>()
                    .map_err(|_| CborError::InvalidAttributeValue("N is not a valid decimal"))?;
                (&value[..index], i64::from(exponent))
            }
            None => (value, 0),
        };
        if significand.is_empty() || significand.matches(['e', 'E']).count() > 0 {
            return Err(CborError::InvalidAttributeValue("N is not a valid decimal"));
        }

        let (unscaled, fraction_digits) = match significand.rsplit_once('.') {
            Some((whole, fraction)) => {
                let signless_whole = whole.strip_prefix(['+', '-']).unwrap_or(whole);
                if signless_whole.contains(['+', '-']) || fraction.contains(['+', '-']) {
                    return Err(CborError::InvalidAttributeValue("N is not a valid decimal"));
                }
                let joined = format!("{whole}{fraction}");
                let unscaled = joined
                    .parse::<BigInt>()
                    .map_err(|_| CborError::InvalidAttributeValue("N is not a valid decimal"))?;
                (unscaled, fraction.len() as i64)
            }
            None => (
                significand
                    .parse::<BigInt>()
                    .map_err(|_| CborError::InvalidAttributeValue("N is not a valid decimal"))?,
                0,
            ),
        };
        Ok(Self {
            exponent: exponent - fraction_digits,
            unscaled,
        })
    }
}

impl fmt::Display for Decimal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.exponent == 0 {
            self.unscaled.fmt(formatter)
        } else {
            write!(formatter, "{}E{}", self.unscaled, self.exponent)
        }
    }
}

fn encode_lex_decimal(value: &str) -> Result<Vec<u8>, CborError> {
    let decimal = Decimal::parse(value)?;
    let mut output = Vec::new();
    write_lex_decimal(&decimal, &mut output)?;
    Ok(output)
}

fn write_lex_decimal(decimal: &Decimal, output: &mut Vec<u8>) -> Result<(), CborError> {
    if decimal.unscaled == BigInt::from(0_u8) {
        output.push(0x80);
        return Ok(());
    }

    let negative = decimal.unscaled.sign() == Sign::Minus;
    let digits = if negative {
        (-&decimal.unscaled).to_string()
    } else {
        decimal.unscaled.to_string()
    };
    let precision =
        i64::try_from(digits.len()).map_err(|_| CborError::ValueTooLarge(digits.len()))?;
    let exponent =
        precision
            .checked_add(decimal.exponent)
            .ok_or(CborError::InvalidAttributeValue(
                "lexdecimal exponent is out of range",
            ))?;
    let exponent = i32::try_from(exponent)
        .map_err(|_| CborError::InvalidAttributeValue("lexdecimal exponent is out of range"))?;

    if (-62..62).contains(&exponent) {
        output.push(if negative {
            (0x3f_i32 - exponent) as u8
        } else {
            (0xc0_i32 + exponent) as u8
        });
    } else {
        output.push(match (negative, exponent < 0) {
            (true, true) => 0x7e,
            (true, false) => 0x01,
            (false, true) => 0x81,
            (false, false) => 0xfe,
        });
        let encoded = if negative {
            exponent ^ 0x7fff_ffff
        } else {
            exponent ^ i32::MIN
        };
        output.extend_from_slice(&encoded.to_be_bytes());
    }

    let remainder = digits.len() % 3;
    let (digits, terminator) = match remainder {
        0 => (digits, 2_u16),
        1 => (format!("{digits}00"), 0),
        2 => (format!("{digits}0"), 1),
        _ => unreachable!(),
    };
    let digit_adjust = if negative { 1011_u16 } else { 12 };
    let terminator = if negative {
        1023 - terminator
    } else {
        terminator
    };

    let mut accumulator = 0_u32;
    let mut bits = 0_u8;
    for chunk in digits.as_bytes().chunks_exact(3) {
        let group = u16::from(chunk[0] - b'0') * 100
            + u16::from(chunk[1] - b'0') * 10
            + u16::from(chunk[2] - b'0');
        write_lex_bits(
            output,
            &mut accumulator,
            &mut bits,
            if negative {
                digit_adjust - group
            } else {
                digit_adjust + group
            },
        );
    }
    write_lex_bits(output, &mut accumulator, &mut bits, terminator);
    if bits != 0 {
        output.push((accumulator << (8 - bits)) as u8);
    }
    Ok(())
}

fn write_lex_bits(output: &mut Vec<u8>, accumulator: &mut u32, bits: &mut u8, value: u16) {
    *accumulator = (*accumulator << 10) | u32::from(value);
    *bits += 10;
    while *bits >= 8 {
        *bits -= 8;
        output.push((*accumulator >> *bits) as u8);
        *accumulator &= (1_u32 << *bits).saturating_sub(1);
    }
}

fn decode_lex_decimal(input: &[u8]) -> Result<(Decimal, usize), CborError> {
    if input.is_empty() {
        return Err(CborError::UnexpectedEnd);
    }
    let header = input[0];
    if header == 0x80 || header == 0x7f {
        return Ok((
            Decimal {
                exponent: 0,
                unscaled: BigInt::from(0_u8),
            },
            1,
        ));
    }
    if header == 0 || header == 0xff {
        return Err(CborError::UnsupportedForm("lexdecimal null"));
    }

    let (negative, exponent, mut offset) = match header {
        0x01 | 0x7e | 0x81 | 0xfe => {
            let exponent_bytes = input.get(1..5).ok_or(CborError::UnexpectedEnd)?;
            let encoded = i32::from_be_bytes(exponent_bytes.try_into().expect("four bytes"));
            let exponent = match header {
                0x01 | 0x7e => encoded ^ 0x7fff_ffff,
                0x81 | 0xfe => encoded ^ i32::MIN,
                _ => unreachable!(),
            };
            (matches!(header, 0x01 | 0x7e), exponent, 5)
        }
        0x82..=0xfd => (false, i32::from(header) - 0xc0, 1),
        _ => (true, 0x3f - i32::from(header), 1),
    };
    let digit_adjust = if negative { 1011_i32 } else { 12 };
    let terminal_digits = if negative {
        [1023_i32, 1022, 1021]
    } else {
        [0_i32, 1, 2]
    };

    let mut accumulator = 0_u32;
    let mut bits = 0_u8;
    let mut last_digit = None;
    let mut unscaled = BigInt::from(0_u8);
    let mut precision = 0_i64;

    loop {
        let byte = *input.get(offset).ok_or(CborError::UnexpectedEnd)?;
        offset += 1;
        accumulator = (accumulator << 8) | u32::from(byte);
        bits += 8;
        while bits >= 10 {
            bits -= 10;
            let digit = ((accumulator >> bits) & 0x3ff) as i32;
            accumulator &= (1_u32 << bits).saturating_sub(1);
            if let Some(terminal) = terminal_digits.iter().position(|value| *value == digit) {
                let last_digit = last_digit.ok_or(CborError::InvalidAttributeValue(
                    "lexdecimal has no significand",
                ))?;
                let divisor = match terminal {
                    0 => 100_i32,
                    1 => 10,
                    2 => 1,
                    _ => unreachable!(),
                };
                append_lex_digit(
                    &mut unscaled,
                    last_digit / divisor,
                    [10_u16, 100, 1000][terminal],
                );
                precision += [1_i64, 2, 3][terminal];
                return Ok((
                    Decimal {
                        exponent: i64::from(exponent) - precision,
                        unscaled,
                    },
                    offset,
                ));
            }
            if let Some(last_digit) = last_digit.replace(digit - digit_adjust) {
                append_lex_digit(&mut unscaled, last_digit, 1000);
                precision += 3;
            }
        }
    }
}

fn append_lex_digit(value: &mut BigInt, digit: i32, multiplier: u16) {
    *value *= multiplier;
    *value += digit;
}

pub(crate) fn write_text(output: &mut Vec<u8>, value: &str) {
    write_type(output, MAJOR_TEXT, value.len() as u64);
    output.extend_from_slice(value.as_bytes());
}

pub(crate) fn write_i64(output: &mut Vec<u8>, value: i64) {
    if value >= 0 {
        write_type(output, MAJOR_UNSIGNED, value as u64);
    } else {
        write_type(output, MAJOR_NEGATIVE, value.unsigned_abs() - 1);
    }
}

pub(crate) fn write_bytes(output: &mut Vec<u8>, value: &[u8]) {
    write_type(output, MAJOR_BYTES, value.len() as u64);
    output.extend_from_slice(value);
}

pub(crate) fn write_type(output: &mut Vec<u8>, major: u8, value: u64) {
    if value <= 23 {
        output.push(major | value as u8);
    } else if value <= u64::from(u8::MAX) {
        output.extend_from_slice(&[major | ADDITIONAL_ONE_BYTE, value as u8]);
    } else if value <= u64::from(u16::MAX) {
        output.push(major | ADDITIONAL_TWO_BYTES);
        output.extend_from_slice(&(value as u16).to_be_bytes());
    } else if value <= u64::from(u32::MAX) {
        output.push(major | ADDITIONAL_FOUR_BYTES);
        output.extend_from_slice(&(value as u32).to_be_bytes());
    } else {
        output.push(major | ADDITIONAL_EIGHT_BYTES);
        output.extend_from_slice(&value.to_be_bytes());
    }
}

fn ensure_value_length(length: usize) -> Result<(), CborError> {
    if length > MAX_VALUE_BYTES {
        return Err(CborError::ValueTooLarge(length));
    }
    Ok(())
}

fn ensure_container_length(length: usize) -> Result<(), CborError> {
    if length > MAX_CONTAINER_ELEMENTS {
        return Err(CborError::ContainerTooLarge(length));
    }
    Ok(())
}

struct Reader<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn is_empty(&self) -> bool {
        self.offset == self.input.len()
    }

    fn remaining(&self) -> &'a [u8] {
        &self.input[self.offset..]
    }

    fn peek(&self) -> Result<u8, CborError> {
        self.input
            .get(self.offset)
            .copied()
            .ok_or(CborError::UnexpectedEnd)
    }

    fn read_byte(&mut self) -> Result<u8, CborError> {
        let byte = self.peek()?;
        self.offset += 1;
        Ok(byte)
    }

    fn read_length(
        &mut self,
        expected_major: u8,
        expected: &'static str,
    ) -> Result<usize, CborError> {
        let initial = self.read_byte()?;
        if initial & 0xe0 != expected_major {
            return Err(CborError::UnexpectedType {
                expected,
                found: initial,
            });
        }
        let value = self.read_argument(initial)?;
        usize::try_from(value).map_err(|_| CborError::ValueTooLarge(usize::MAX))
    }

    fn read_argument(&mut self, initial: u8) -> Result<u64, CborError> {
        let additional = initial & 0x1f;
        let value = match additional {
            0..=23 => additional as u64,
            ADDITIONAL_ONE_BYTE => self.read_fixed(1)?,
            ADDITIONAL_TWO_BYTES => self.read_fixed(2)?,
            ADDITIONAL_FOUR_BYTES => self.read_fixed(4)?,
            ADDITIONAL_EIGHT_BYTES => self.read_fixed(8)?,
            28..=30 => {
                return Err(CborError::UnsupportedForm(
                    "reserved additional information",
                ));
            }

            31 => return Err(CborError::UnsupportedForm("indefinite-length item")),
            _ => unreachable!(),
        };
        Ok(value)
    }

    fn position(&self) -> usize {
        self.offset
    }

    fn bytes_between(&self, start: usize, end: usize) -> Result<&'a [u8], CborError> {
        self.input.get(start..end).ok_or(CborError::TrailingData)
    }

    fn read_fixed(&mut self, length: usize) -> Result<u64, CborError> {
        let bytes = self.read_exact(length)?;
        let mut value = 0_u64;
        for byte in bytes {
            value = (value << 8) | u64::from(*byte);
        }
        Ok(value)
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], CborError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(CborError::UnexpectedEnd)?;
        let bytes = self
            .input
            .get(self.offset..end)
            .ok_or(CborError::UnexpectedEnd)?;
        self.offset = end;
        Ok(bytes)
    }

    fn read_text(&mut self) -> Result<String, CborError> {
        let length = self.read_length(MAJOR_TEXT, "text string")?;
        ensure_value_length(length)?;
        let bytes = self.read_exact(length)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| CborError::InvalidUtf8)
    }

    fn read_bytes(&mut self) -> Result<Vec<u8>, CborError> {
        let length = self.read_length(MAJOR_BYTES, "byte string")?;
        ensure_value_length(length)?;
        Ok(self.read_exact(length)?.to_vec())
    }

    fn consume_null(&mut self) -> Result<bool, CborError> {
        if self.peek()? == 0xf6 {
            self.read_byte()?;
            return Ok(true);
        }
        Ok(false)
    }

    fn read_f64(&mut self) -> Result<f64, CborError> {
        let initial = self.read_byte()?;
        if initial != (MAJOR_SIMPLE | ADDITIONAL_EIGHT_BYTES) {
            return Err(CborError::UnexpectedType {
                expected: "64-bit floating-point value",
                found: initial,
            });
        }
        Ok(f64::from_bits(self.read_fixed(8)?))
    }

    fn skip_value(&mut self) -> Result<(), CborError> {
        let initial = self.read_byte()?;
        match initial & 0xe0 {
            MAJOR_UNSIGNED | MAJOR_NEGATIVE => {
                self.read_argument(initial)?;
            }
            MAJOR_BYTES | MAJOR_TEXT => {
                let length = usize::try_from(self.read_argument(initial)?)
                    .map_err(|_| CborError::ValueTooLarge(usize::MAX))?;
                ensure_value_length(length)?;
                self.read_exact(length)?;
            }
            MAJOR_ARRAY | MAJOR_MAP => {
                let indefinite = initial & 0x1f == 31;
                if indefinite {
                    loop {
                        if self.peek()? == 0xff {
                            self.read_byte()?;
                            break;
                        }
                        self.skip_value()?;
                        if initial & 0xe0 == MAJOR_MAP {
                            self.skip_value()?;
                        }
                    }
                } else {
                    let length = usize::try_from(self.read_argument(initial)?)
                        .map_err(|_| CborError::ContainerTooLarge(usize::MAX))?;
                    ensure_container_length(length)?;
                    let items = if initial & 0xe0 == MAJOR_MAP {
                        length
                            .checked_mul(2)
                            .ok_or(CborError::ContainerTooLarge(usize::MAX))?
                    } else {
                        length
                    };
                    for _ in 0..items {
                        self.skip_value()?;
                    }
                }
            }
            MAJOR_TAG => {
                self.read_argument(initial)?;
                self.skip_value()?;
            }
            MAJOR_SIMPLE => match initial & 0x1f {
                24 => {
                    self.read_exact(1)?;
                }
                25 => {
                    self.read_exact(2)?;
                }
                26 => {
                    self.read_exact(4)?;
                }
                27 => {
                    self.read_exact(8)?;
                }
                31 => return Err(CborError::UnsupportedForm("unexpected break")),
                _ => {}
            },
            _ => return Err(CborError::UnsupportedForm("unknown CBOR major type")),
        }
        Ok(())
    }

    fn consume_response_map(
        &mut self,
        mut consumer: impl FnMut(i64, &mut Self) -> Result<(), CborError>,
    ) -> Result<(), CborError> {
        let initial = self.read_byte()?;
        if initial & 0xe0 != MAJOR_MAP {
            return Err(CborError::UnexpectedType {
                expected: "DAX response map",
                found: initial,
            });
        }
        let indefinite = initial & 0x1f == 31;
        let length = if indefinite {
            None
        } else {
            Some(
                usize::try_from(self.read_argument(initial)?)
                    .map_err(|_| CborError::ContainerTooLarge(usize::MAX))?,
            )
        };
        if let Some(length) = length {
            ensure_container_length(length)?;
            for _ in 0..length {
                let ordinal = i64::try_from(read_integer(self)?).map_err(|_| {
                    CborError::InvalidAttributeValue("DAX response ordinal is out of range")
                })?;
                consumer(ordinal, self)?;
            }
        } else {
            let mut pairs = 0_usize;
            while self.peek()? != 0xff {
                pairs = pairs
                    .checked_add(1)
                    .ok_or(CborError::ContainerTooLarge(usize::MAX))?;
                ensure_container_length(pairs)?;
                let ordinal = i64::try_from(read_integer(self)?).map_err(|_| {
                    CborError::InvalidAttributeValue("DAX response ordinal is out of range")
                })?;
                consumer(ordinal, self)?;
            }
            self.read_byte()?;
        }
        Ok(())
    }

    fn consume_array(
        &mut self,
        expected: &'static str,
        mut consumer: impl FnMut(&mut Self) -> Result<(), CborError>,
    ) -> Result<(), CborError> {
        let initial = self.read_byte()?;
        if initial & 0xe0 != MAJOR_ARRAY {
            return Err(CborError::UnexpectedType {
                expected,
                found: initial,
            });
        }
        if initial & 0x1f == 31 {
            let mut length = 0_usize;
            while self.peek()? != 0xff {
                length = length
                    .checked_add(1)
                    .ok_or(CborError::ContainerTooLarge(usize::MAX))?;
                ensure_container_length(length)?;
                consumer(self)?;
            }
            self.read_byte()?;
            return Ok(());
        }
        let length = usize::try_from(self.read_argument(initial)?)
            .map_err(|_| CborError::ContainerTooLarge(usize::MAX))?;
        ensure_container_length(length)?;
        for _ in 0..length {
            consumer(self)?;
        }
        Ok(())
    }

    fn consume_map(
        &mut self,
        expected: &'static str,
        mut consumer: impl FnMut(&mut Self) -> Result<(), CborError>,
    ) -> Result<(), CborError> {
        let initial = self.read_byte()?;
        if initial & 0xe0 != MAJOR_MAP {
            return Err(CborError::UnexpectedType {
                expected,
                found: initial,
            });
        }
        if initial & 0x1f == 31 {
            let mut length = 0_usize;
            while self.peek()? != 0xff {
                length = length
                    .checked_add(1)
                    .ok_or(CborError::ContainerTooLarge(usize::MAX))?;
                ensure_container_length(length)?;
                consumer(self)?;
            }
            self.read_byte()?;
            return Ok(());
        }
        let length = usize::try_from(self.read_argument(initial)?)
            .map_err(|_| CborError::ContainerTooLarge(usize::MAX))?;
        ensure_container_length(length)?;
        for _ in 0..length {
            consumer(self)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod response_envelope_tests {
    use std::collections::HashMap;

    use aws_sdk_dynamodb::types::{AttributeDefinition, AttributeValue, ScalarAttributeType};

    use super::{
        CborError, ResponseEnvelope, decode_batch_get_body, decode_batch_write_body,
        decode_define_attribute_list_body, decode_define_attribute_list_id_body,
        decode_define_key_schema_body, decode_delete_item_body, decode_get_item_body,
        decode_put_item_body, decode_response_envelope, decode_scan_body, decode_transact_get_body,
        decode_transaction_cancellation_item,
    };

    #[test]
    fn separates_success_payloads_from_error_envelopes() {
        assert_eq!(
            decode_response_envelope(&hex("80f6")).unwrap(),
            ResponseEnvelope::Success(&[0xf6])
        );
        assert_eq!(
            decode_response_envelope(&hex("8104617883f6f6f6")).unwrap(),
            ResponseEnvelope::Error(super::DaxResponseError {
                code_sequence: vec![4],
                message: "x".into(),
                request_id: None,
                error_code: None,
                status_code: 400,
                cancellation_reasons: None,
            })
        );
        assert_eq!(
            decode_response_envelope(&hex("81046178f6")).unwrap(),
            ResponseEnvelope::Error(super::DaxResponseError {
                code_sequence: vec![4],
                message: "x".into(),
                request_id: None,
                error_code: None,
                status_code: 400,
                cancellation_reasons: None,
            })
        );
    }

    #[test]
    fn rejects_invalid_error_details() {
        assert_eq!(
            decode_response_envelope(&hex("8104617882f6f6")).unwrap_err(),
            CborError::InvalidAttributeValue("DAX error detail must contain three or four values")
        );
        assert_eq!(
            decode_response_envelope(&hex("8104617884f6f6f680")).unwrap(),
            ResponseEnvelope::Error(super::DaxResponseError {
                code_sequence: vec![4],
                message: "x".into(),
                request_id: None,
                error_code: None,
                status_code: 400,
                cancellation_reasons: Some(Vec::new().into_boxed_slice()),
            })
        );
    }

    #[test]
    fn infers_server_status_and_preserves_explicit_status() {
        assert_eq!(
            decode_response_envelope(&hex("81016178f6")).unwrap(),
            ResponseEnvelope::Error(super::DaxResponseError {
                code_sequence: vec![1],
                message: "x".into(),
                request_id: None,
                error_code: None,
                status_code: 500,
                cancellation_reasons: None,
            })
        );
        assert_eq!(
            decode_response_envelope(&hex("8104617883f661791901f4")).unwrap(),
            ResponseEnvelope::Error(super::DaxResponseError {
                code_sequence: vec![4],
                message: "x".into(),
                request_id: None,
                error_code: Some("y".into()),
                status_code: 500,
                cancellation_reasons: None,
            })
        );
    }

    #[test]
    fn rejects_trailing_error_envelope_bytes() {
        assert_eq!(
            decode_response_envelope(&hex("81046178f600")).unwrap_err(),
            CborError::TrailingData
        );
    }

    #[test]
    fn decodes_transaction_cancellation_reason_metadata() {
        let envelope = hex("8204183a617884f6f6f683f6617881f6");
        let ResponseEnvelope::Error(error) =
            decode_response_envelope(&envelope).expect("valid cancellation envelope")
        else {
            panic!("expected cancellation error");
        };
        assert_eq!(error.code_sequence, vec![4, 58]);
        assert_eq!(
            error.cancellation_reasons,
            Some(
                vec![super::CancellationReasonMetadata {
                    code: None,
                    message: Some("x".into()),
                    item_cbor: Some(vec![0x81, 0xf6].into_boxed_slice()),
                }]
                .into_boxed_slice()
            )
        );
    }

    #[test]
    fn preserves_non_null_transaction_cancellation_item_payloads() {
        let envelope = hex("8204183a617884f6f6f6836143614da0");
        let ResponseEnvelope::Error(error) =
            decode_response_envelope(&envelope).expect("valid cancellation envelope")
        else {
            panic!("expected cancellation error");
        };
        let reason = &error.cancellation_reasons.expect("reasons")[0];
        assert_eq!(reason.code.as_deref(), Some("C"));
        assert_eq!(reason.message.as_deref(), Some("M"));
        assert_eq!(reason.item_cbor.as_deref(), Some([0xa0].as_slice()));
    }

    #[test]
    fn decodes_compressed_transaction_cancellation_item_from_error_envelope() {
        let envelope = hex("8204183a617884f6f6f683f6f6451930396130");
        let ResponseEnvelope::Error(error) =
            decode_response_envelope(&envelope).expect("valid cancellation envelope")
        else {
            panic!("expected cancellation error");
        };
        let reason = &error.cancellation_reasons.expect("reasons")[0];
        let key = HashMap::from([("hk".to_owned(), AttributeValue::N("0".into()))]);
        let schema = [AttributeDefinition::builder()
            .attribute_name("hk")
            .attribute_type(ScalarAttributeType::N)
            .build()
            .unwrap()];
        let attributes = HashMap::from([(12_345_i64, vec!["attr".to_owned()])]);

        let item = decode_transaction_cancellation_item(
            reason.item_cbor.as_deref(),
            &key,
            &schema,
            &attributes,
        )
        .expect("compressed cancellation item");
        assert_eq!(
            item,
            HashMap::from([
                ("hk".to_owned(), AttributeValue::N("0".into())),
                ("attr".to_owned(), AttributeValue::S("0".into())),
            ])
        );
    }

    #[test]
    fn rejects_malformed_transaction_cancellation_item_payloads() {
        let envelope = hex("8204183a617884f6f6f6836143614d18");
        assert_eq!(
            decode_response_envelope(&envelope).unwrap_err(),
            CborError::UnexpectedEnd
        );
    }

    #[test]
    fn rejects_non_triple_transaction_cancellation_reasons() {
        let envelope = hex("8104617884f6f6f682f6f6");
        assert_eq!(
            decode_response_envelope(&envelope).unwrap_err(),
            CborError::InvalidAttributeValue("DAX cancellation reasons must contain triples")
        );
    }

    #[test]
    fn rejects_oversized_transaction_cancellation_reasons() {
        let envelope = hex("8104617884f6f6f69a000186a1");
        assert_eq!(
            decode_response_envelope(&envelope).unwrap_err(),
            CborError::ContainerTooLarge(100_001)
        );
    }

    #[test]
    fn decodes_compressed_get_and_put_response_attributes() {
        let lists = HashMap::from([(9_i64, vec!["value".into()])]);
        let key = HashMap::from([("pk".into(), AttributeValue::S("key".into()))]);
        assert_eq!(
            decode_get_item_body(&hex("a10047096576616c7565"), &key, &lists).unwrap(),
            Some(HashMap::from([
                ("pk".into(), AttributeValue::S("key".into())),
                ("value".into(), AttributeValue::S("value".into())),
            ]))
        );
        assert_eq!(
            decode_get_item_body(&hex("f6"), &key, &lists).unwrap(),
            None
        );

        let schema = vec![
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("key schema is complete"),
        ];
        let request_item = HashMap::from([
            ("pk".into(), AttributeValue::S("key".into())),
            ("new".into(), AttributeValue::S("value".into())),
        ]);
        assert_eq!(
            decode_put_item_body(&hex("a10247096576616c7565"), &request_item, &schema, &lists)
                .unwrap(),
            Some(HashMap::from([
                ("pk".into(), AttributeValue::S("key".into())),
                ("value".into(), AttributeValue::S("value".into())),
            ]))
        );
        assert_eq!(
            decode_delete_item_body(&hex("a10247096576616c7565"), &key, &schema, &lists).unwrap(),
            Some(HashMap::from([
                ("pk".into(), AttributeValue::S("key".into())),
                ("value".into(), AttributeValue::S("value".into())),
            ]))
        );
        assert_eq!(
            decode_scan_body(&hex("a1078182436b657947096576616c7565"), &schema, &lists).unwrap(),
            super::ScanResponse {
                items: vec![HashMap::from([
                    ("pk".into(), AttributeValue::S("key".into())),
                    ("value".into(), AttributeValue::S("value".into())),
                ])],
                consumed_capacity: None,
                count: None,
                scanned_count: None,
                last_evaluated_key: None,
            }
        );
        assert_eq!(
            decode_scan_body(
                &hex("a1079f9f436b657947096576616c7565ffff"),
                &schema,
                &lists
            )
            .unwrap()
            .items,
            vec![HashMap::from([
                ("pk".into(), AttributeValue::S("key".into())),
                ("value".into(), AttributeValue::S("value".into())),
            ])]
        );
    }

    #[test]
    fn decodes_batch_get_unprocessed_keys_and_preserves_request_options() {
        let key = HashMap::from([("pk".into(), AttributeValue::S("v".into()))]);
        let keys = aws_sdk_dynamodb::types::KeysAndAttributes::builder()
            .set_keys(Some(vec![key.clone()]))
            .consistent_read(true)
            .projection_expression("pk")
            .expression_attribute_names("#pk", "pk")
            .build()
            .expect("keys and attributes are complete");
        let request = aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemInput::builder()
            .request_items("Table", keys)
            .build()
            .expect("batch get input is complete");
        let schema = vec![
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("key schema is complete"),
        ];
        let schemas = HashMap::from([("Table".to_owned(), schema)]);
        let body = hex("82a0a1655461626c6581417680");

        let decoded = decode_batch_get_body(&body, &request, &schemas, &HashMap::new())
            .expect("batch get response is valid");
        let unprocessed = decoded
            .unprocessed_keys
            .get("Table")
            .expect("table has unprocessed keys");
        assert_eq!(unprocessed.keys(), &[key]);
        assert_eq!(unprocessed.consistent_read(), Some(true));
        assert_eq!(unprocessed.projection_expression(), Some("pk"));
        assert_eq!(
            unprocessed.expression_attribute_names(),
            Some(&HashMap::from([("#pk".to_owned(), "pk".to_owned())]))
        );
    }

    #[test]
    fn omits_empty_batch_get_unprocessed_key_tables() {
        let request = aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemInput::builder()
            .build()
            .expect("batch get input is complete");
        let schema = AttributeDefinition::builder()
            .attribute_name("pk")
            .attribute_type(ScalarAttributeType::S)
            .build()
            .expect("key schema is complete");
        let schemas = HashMap::from([("Table".to_owned(), vec![schema])]);
        let body = hex("82a0a1655461626c658080");

        let decoded = decode_batch_get_body(&body, &request, &schemas, &HashMap::new())
            .expect("batch get response is valid");
        assert!(decoded.unprocessed_keys.is_empty());
    }

    #[test]
    fn decodes_batch_get_response_items_and_restores_key_attributes() {
        let key = HashMap::from([("pk".into(), AttributeValue::S("v".into()))]);
        let keys = aws_sdk_dynamodb::types::KeysAndAttributes::builder()
            .set_keys(Some(vec![key]))
            .build()
            .expect("keys and attributes are complete");
        let request = aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemInput::builder()
            .request_items("Table", keys)
            .build()
            .expect("batch get input is complete");
        let schema = vec![
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("key schema is complete"),
        ];
        let schemas = HashMap::from([("Table".to_owned(), schema)]);
        let lists = HashMap::from([(9_i64, vec!["status".to_owned()])]);
        let body = hex("82a1655461626c658241764509636f6c64a080");

        let decoded = decode_batch_get_body(&body, &request, &schemas, &lists)
            .expect("batch get response is valid");
        assert_eq!(
            decoded.responses["Table"][0],
            HashMap::from([
                ("pk".into(), AttributeValue::S("v".into())),
                ("status".into(), AttributeValue::S("old".into())),
            ])
        );
    }

    #[test]
    fn decodes_batch_get_items_and_unprocessed_keys_in_one_page() {
        let keys = aws_sdk_dynamodb::types::KeysAndAttributes::builder()
            .set_keys(Some(vec![
                HashMap::from([("pk".into(), AttributeValue::S("v".into()))]),
                HashMap::from([("pk".into(), AttributeValue::S("w".into()))]),
            ]))
            .consistent_read(true)
            .build()
            .expect("keys and attributes are complete");
        let request = aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemInput::builder()
            .request_items("Table", keys)
            .build()
            .expect("batch get input is complete");
        let schema = vec![
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("key schema is complete"),
        ];
        let schemas = HashMap::from([("Table".to_owned(), schema)]);
        let lists = HashMap::from([(9_i64, vec!["status".to_owned()])]);
        let body = hex("82a1655461626c658241764509636f6c64a1655461626c6581417780");

        let decoded = decode_batch_get_body(&body, &request, &schemas, &lists)
            .expect("batch get page with pending keys is valid");
        assert_eq!(
            decoded.responses["Table"][0],
            HashMap::from([
                ("pk".into(), AttributeValue::S("v".into())),
                ("status".into(), AttributeValue::S("old".into())),
            ])
        );
        assert_eq!(
            decoded.unprocessed_keys["Table"].keys(),
            &[HashMap::from([(
                "pk".into(),
                AttributeValue::S("w".into())
            )])]
        );
        assert_eq!(
            decoded.unprocessed_keys["Table"].consistent_read(),
            Some(true)
        );
    }

    #[test]
    fn decodes_batch_get_projected_response_with_expression_alias() {
        let key = HashMap::from([("pk".into(), AttributeValue::S("v".into()))]);
        let keys = aws_sdk_dynamodb::types::KeysAndAttributes::builder()
            .set_keys(Some(vec![key]))
            .projection_expression("#status")
            .expression_attribute_names("#status", "status")
            .build()
            .expect("keys and attributes are complete");
        let request = aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemInput::builder()
            .request_items("Table", keys)
            .build()
            .expect("batch get input is complete");
        let schema = vec![
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("key schema is complete"),
        ];
        let schemas = HashMap::from([("Table".to_owned(), schema)]);
        let body = hex("82a1655461626c6581a100636e6577a080");

        let decoded = decode_batch_get_body(&body, &request, &schemas, &HashMap::new())
            .expect("projected BatchGet response is valid");
        assert_eq!(
            decoded.responses["Table"][0],
            HashMap::from([("status".into(), AttributeValue::S("new".into()))])
        );
    }

    #[test]
    fn rejects_batch_get_response_with_trailing_data() {
        let request = aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemInput::builder()
            .build()
            .expect("batch get input is complete");
        let error = decode_batch_get_body(
            &hex("82a0a080ff"),
            &request,
            &HashMap::new(),
            &HashMap::new(),
        )
        .expect_err("trailing data must be rejected");
        assert_eq!(error, CborError::TrailingData);
    }

    #[test]
    fn rejects_oversized_batch_get_response_arrays() {
        let request = aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemInput::builder()
            .build()
            .expect("batch get input is complete");
        let response_items = hex("82a1655461626c659a000186a1");
        assert_eq!(
            decode_batch_get_body(
                &response_items,
                &request,
                &HashMap::from([("Table".to_owned(), Vec::new())]),
                &HashMap::new(),
            )
            .unwrap_err(),
            CborError::ContainerTooLarge(100_001)
        );

        let unprocessed_keys = hex("82a0a1655461626c659a000186a100");
        assert_eq!(
            decode_batch_get_body(
                &unprocessed_keys,
                &request,
                &HashMap::from([("Table".to_owned(), Vec::new())]),
                &HashMap::new(),
            )
            .unwrap_err(),
            CborError::ContainerTooLarge(100_001)
        );
    }

    #[test]
    fn rejects_oversized_batch_write_request_arrays() {
        let schema = AttributeDefinition::builder()
            .attribute_name("pk")
            .attribute_type(ScalarAttributeType::S)
            .build()
            .expect("key schema is complete");
        let schemas = HashMap::from([("Table".to_owned(), vec![schema])]);
        let body = hex("a1655461626c659a000186a1");

        assert_eq!(
            decode_batch_write_body(&body, &schemas, &HashMap::new()).unwrap_err(),
            CborError::ContainerTooLarge(100_001)
        );
    }

    #[test]
    fn rejects_transact_get_response_count_mismatch_before_decoding_items() {
        let get = aws_sdk_dynamodb::types::Get::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("v".into()))
            .build()
            .expect("transact get item is complete");
        let item = aws_sdk_dynamodb::types::TransactGetItem::builder()
            .get(get)
            .build();
        let request =
            aws_sdk_dynamodb::operation::transact_get_items::TransactGetItemsInput::builder()
                .transact_items(item)
                .build()
                .expect("transact get input is complete");

        assert_eq!(
            decode_transact_get_body(&hex("828080"), &request, &HashMap::new(), &HashMap::new())
                .unwrap_err(),
            CborError::InvalidAttributeValue("TransactGet response count does not match request")
        );
    }

    #[test]
    fn decodes_transact_get_null_and_compressed_items_with_request_keys() {
        let get = aws_sdk_dynamodb::types::Get::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("v".into()))
            .build()
            .expect("transact get item is complete");
        let item = aws_sdk_dynamodb::types::TransactGetItem::builder()
            .get(get.clone())
            .build();
        let request =
            aws_sdk_dynamodb::operation::transact_get_items::TransactGetItemsInput::builder()
                .transact_items(item.clone())
                .transact_items(item)
                .build()
                .expect("transact get input is complete");
        let schema = AttributeDefinition::builder()
            .attribute_name("pk")
            .attribute_type(ScalarAttributeType::S)
            .build()
            .expect("key schema is complete");
        let schemas = HashMap::from([("Table".to_owned(), vec![schema])]);
        let attributes = HashMap::from([(12_345_i64, vec!["status".to_owned()])]);

        let (responses, capacity) = decode_transact_get_body(
            &hex("8282f645193039613080"),
            &request,
            &schemas,
            &attributes,
        )
        .expect("transact get response is valid");
        assert!(responses[0].item().is_none());
        assert_eq!(
            responses[1].item(),
            Some(&HashMap::from([
                ("pk".to_owned(), AttributeValue::S("v".into())),
                ("status".to_owned(), AttributeValue::S("0".into())),
            ]))
        );
        assert!(capacity.is_none());
    }

    #[test]
    fn decodes_scan_consumed_capacity() {
        let schema = vec![
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("key schema is complete"),
        ];
        let response = decode_scan_body(
            &hex(
                "a40140655461626c65fb3ff8000000000000fb3fe0000000000000a163677369fb4000000000000000a1636c7369fb4008000000000000078182436b657947096576616c7565080109416b",
            ),
            &schema,
            &HashMap::from([(9_i64, vec!["value".into()])]),
        )
        .unwrap();
        assert_eq!(response.items.len(), 1);
        assert_eq!(
            response
                .last_evaluated_key
                .as_ref()
                .and_then(|key| key.get("pk")),
            Some(&AttributeValue::S("k".into()))
        );
        let capacity = response.consumed_capacity.expect("capacity is present");
        assert_eq!(capacity.table_name(), Some("Table"));
        assert_eq!(capacity.capacity_units(), Some(1.5));
        assert_eq!(
            capacity.table().and_then(|table| table.capacity_units()),
            Some(0.5)
        );
        assert_eq!(
            capacity
                .global_secondary_indexes()
                .and_then(|indexes| indexes.get("gsi"))
                .and_then(|index| index.capacity_units()),
            Some(2.0)
        );
        assert_eq!(
            capacity
                .local_secondary_indexes()
                .and_then(|indexes| indexes.get("lsi"))
                .and_then(|index| index.capacity_units()),
            Some(3.0)
        );

        assert!(
            decode_scan_body(&hex("a201f60801"), &schema, &HashMap::new())
                .unwrap()
                .consumed_capacity
                .is_none()
        );
    }

    #[test]
    fn accepts_indefinite_response_maps_and_rejects_unported_ordinals() {
        let lists = HashMap::from([(9_i64, vec!["value".into()])]);
        let key = HashMap::from([("pk".into(), AttributeValue::S("key".into()))]);
        assert_eq!(
            decode_get_item_body(&hex("bf0047096576616c7565ff"), &key, &lists).unwrap(),
            Some(HashMap::from([
                ("pk".into(), AttributeValue::S("key".into())),
                ("value".into(), AttributeValue::S("value".into())),
            ]))
        );
        assert_eq!(
            decode_get_item_body(&hex("a101f6"), &key, &lists).unwrap_err(),
            CborError::UnsupportedForm("DAX consumed capacity response")
        );
        let schema = vec![
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .expect("key schema is complete"),
        ];
        assert_eq!(
            decode_delete_item_body(&hex("a100f6"), &key, &schema, &lists).unwrap_err(),
            CborError::UnsupportedForm("unknown DeleteItem response ordinal")
        );
    }

    #[test]
    fn decodes_control_response_bodies() {
        let schema = decode_define_key_schema_body(&hex("a162706b6153")).unwrap();
        assert_eq!(schema.len(), 1);
        assert_eq!(schema[0].attribute_name(), "pk");
        assert_eq!(schema[0].attribute_type().as_str(), "S");
        assert_eq!(decode_define_attribute_list_id_body(&hex("09")).unwrap(), 9);
        assert_eq!(
            decode_define_attribute_list_body(&hex("826161617a")).unwrap(),
            vec!["a".to_owned(), "z".to_owned()]
        );
        assert_eq!(
            decode_define_attribute_list_id_body(&hex("0900")).unwrap_err(),
            CborError::TrailingData
        );
    }

    fn hex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                u8::from_str_radix(std::str::from_utf8(pair).expect("hex bytes are UTF-8"), 16)
                    .expect("fixture is valid hexadecimal")
            })
            .collect()
    }
}
