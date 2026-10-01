#![allow(
    dead_code,
    reason = "Phase 3 conversions are wired into exported operations in phases 4 and 5"
)]

use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
};

use aws_sdk_dynamodb::{primitives::Blob, types::AttributeValue};

use crate::{DaxAttributeValue, DaxFfiError};

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(crate) struct ValueConversionError(pub(crate) String);

impl From<ValueConversionError> for DaxFfiError {
    fn from(error: ValueConversionError) -> Self {
        Self::validation(error.0)
    }
}

pub(crate) const MAX_ATTRIBUTE_DEPTH: usize = 32;
pub(crate) const MAX_ATTRIBUTE_NODES: usize = 100_000;
pub(crate) const MAX_COLLECTION_ENTRIES: usize = 100_000;
pub(crate) const MAX_VALUE_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_TOTAL_VALUE_BYTES: usize = 16 * 1024 * 1024;

pub(crate) fn into_sdk_attribute_value(
    value: DaxAttributeValue,
) -> Result<AttributeValue, ValueConversionError> {
    convert_into_sdk(value, 1, &mut ConversionBudget::default())
}

pub(crate) fn from_sdk_attribute_value(
    value: AttributeValue,
) -> Result<DaxAttributeValue, ValueConversionError> {
    convert_from_sdk(value, 1, &mut ConversionBudget::default())
}

pub(crate) fn into_sdk_item(
    item: HashMap<String, DaxAttributeValue>,
) -> Result<HashMap<String, AttributeValue>, ValueConversionError> {
    let AttributeValue::M(item) =
        into_sdk_attribute_value(DaxAttributeValue::MapValue { values: item })?
    else {
        unreachable!("map input always converts to an SDK map")
    };
    Ok(item)
}

pub(crate) fn from_sdk_item(
    item: HashMap<String, AttributeValue>,
) -> Result<HashMap<String, DaxAttributeValue>, ValueConversionError> {
    let DaxAttributeValue::MapValue { values } = from_sdk_attribute_value(AttributeValue::M(item))?
    else {
        unreachable!("SDK map input always converts to an FFI map")
    };
    Ok(values)
}

#[derive(Default)]
struct ConversionBudget {
    nodes: usize,
    collection_entries: usize,
    bytes: usize,
}

impl ConversionBudget {
    fn enter(&mut self, depth: usize) -> Result<(), ValueConversionError> {
        if depth > MAX_ATTRIBUTE_DEPTH {
            return Err(validation(format!(
                "attribute value nesting exceeds {MAX_ATTRIBUTE_DEPTH}"
            )));
        }
        self.nodes = self
            .nodes
            .checked_add(1)
            .ok_or_else(|| validation("attribute value node count overflow"))?;
        if self.nodes > MAX_ATTRIBUTE_NODES {
            return Err(validation(format!(
                "attribute value node count exceeds {MAX_ATTRIBUTE_NODES}"
            )));
        }
        Ok(())
    }

    fn add_entries(&mut self, count: usize) -> Result<(), ValueConversionError> {
        if count > MAX_COLLECTION_ENTRIES {
            return Err(validation(format!(
                "attribute value collection exceeds {MAX_COLLECTION_ENTRIES} entries"
            )));
        }
        self.collection_entries = self
            .collection_entries
            .checked_add(count)
            .ok_or_else(|| validation("attribute value collection count overflow"))?;
        if self.collection_entries > MAX_COLLECTION_ENTRIES {
            return Err(validation(format!(
                "attribute value aggregate collection entries exceed {MAX_COLLECTION_ENTRIES}"
            )));
        }
        Ok(())
    }

    fn add_bytes(&mut self, count: usize, kind: &str) -> Result<(), ValueConversionError> {
        if count > MAX_VALUE_BYTES {
            return Err(validation(format!(
                "attribute value {kind} exceeds {MAX_VALUE_BYTES} bytes"
            )));
        }
        self.bytes = self
            .bytes
            .checked_add(count)
            .ok_or_else(|| validation("attribute value byte count overflow"))?;
        if self.bytes > MAX_TOTAL_VALUE_BYTES {
            return Err(validation(format!(
                "attribute value aggregate data exceeds {MAX_TOTAL_VALUE_BYTES} bytes"
            )));
        }
        Ok(())
    }
}

fn convert_into_sdk(
    value: DaxAttributeValue,
    depth: usize,
    budget: &mut ConversionBudget,
) -> Result<AttributeValue, ValueConversionError> {
    budget.enter(depth)?;
    match value {
        DaxAttributeValue::String { string } => {
            budget.add_bytes(string.len(), "string")?;
            Ok(AttributeValue::S(string))
        }
        DaxAttributeValue::Number { number } => {
            validate_number(&number)?;
            budget.add_bytes(number.len(), "number")?;
            Ok(AttributeValue::N(number))
        }
        DaxAttributeValue::Binary { binary } => {
            budget.add_bytes(binary.len(), "binary")?;
            Ok(AttributeValue::B(Blob::new(binary)))
        }
        DaxAttributeValue::Boolean { boolean } => Ok(AttributeValue::Bool(boolean)),
        DaxAttributeValue::Null => Ok(AttributeValue::Null(true)),
        DaxAttributeValue::StringSet { values } => {
            validate_nonempty_set(&values, "string")?;
            budget.add_entries(values.len())?;
            for value in &values {
                budget.add_bytes(value.len(), "string set member")?;
            }
            Ok(AttributeValue::Ss(values))
        }
        DaxAttributeValue::NumberSet { values } => {
            validate_nonempty_set(&values, "number")?;
            budget.add_entries(values.len())?;
            for value in &values {
                validate_number(value)?;
                budget.add_bytes(value.len(), "number set member")?;
            }
            Ok(AttributeValue::Ns(values))
        }
        DaxAttributeValue::BinarySet { values } => {
            validate_nonempty_set(&values, "binary")?;
            budget.add_entries(values.len())?;
            let values = values
                .into_iter()
                .map(|value| {
                    budget.add_bytes(value.len(), "binary set member")?;
                    Ok(Blob::new(value))
                })
                .collect::<Result<Vec<_>, ValueConversionError>>()?;
            Ok(AttributeValue::Bs(values))
        }
        DaxAttributeValue::ListValue { values } => {
            budget.add_entries(values.len())?;
            let values = values
                .into_iter()
                .map(|value| convert_into_sdk(value, depth + 1, budget))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(AttributeValue::L(values))
        }
        DaxAttributeValue::MapValue { values } => {
            budget.add_entries(values.len())?;
            let values = values
                .into_iter()
                .map(|(key, value)| {
                    budget.add_bytes(key.len(), "map key")?;
                    Ok((key, convert_into_sdk(value, depth + 1, budget)?))
                })
                .collect::<Result<HashMap<_, _>, ValueConversionError>>()?;
            Ok(AttributeValue::M(values))
        }
    }
}

fn convert_from_sdk(
    value: AttributeValue,
    depth: usize,
    budget: &mut ConversionBudget,
) -> Result<DaxAttributeValue, ValueConversionError> {
    budget.enter(depth)?;
    match value {
        AttributeValue::S(string) => {
            budget.add_bytes(string.len(), "string")?;
            Ok(DaxAttributeValue::String { string })
        }
        AttributeValue::N(number) => {
            validate_number(&number)?;
            budget.add_bytes(number.len(), "number")?;
            Ok(DaxAttributeValue::Number { number })
        }
        AttributeValue::B(binary) => {
            budget.add_bytes(binary.as_ref().len(), "binary")?;
            Ok(DaxAttributeValue::Binary {
                binary: binary.into_inner(),
            })
        }
        AttributeValue::Bool(boolean) => Ok(DaxAttributeValue::Boolean { boolean }),
        AttributeValue::Null(_) => Ok(DaxAttributeValue::Null),
        AttributeValue::Ss(values) => {
            validate_nonempty_set(&values, "string")?;
            budget.add_entries(values.len())?;
            for value in &values {
                budget.add_bytes(value.len(), "string set member")?;
            }
            Ok(DaxAttributeValue::StringSet { values })
        }
        AttributeValue::Ns(values) => {
            validate_nonempty_set(&values, "number")?;
            budget.add_entries(values.len())?;
            for value in &values {
                validate_number(value)?;
                budget.add_bytes(value.len(), "number set member")?;
            }
            Ok(DaxAttributeValue::NumberSet { values })
        }
        AttributeValue::Bs(values) => {
            validate_nonempty_set(&values, "binary")?;
            budget.add_entries(values.len())?;
            let values = values
                .into_iter()
                .map(|value| {
                    budget.add_bytes(value.as_ref().len(), "binary set member")?;
                    Ok(value.into_inner())
                })
                .collect::<Result<Vec<_>, ValueConversionError>>()?;
            Ok(DaxAttributeValue::BinarySet { values })
        }
        AttributeValue::L(values) => {
            budget.add_entries(values.len())?;
            let values = values
                .into_iter()
                .map(|value| convert_from_sdk(value, depth + 1, budget))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(DaxAttributeValue::ListValue { values })
        }
        AttributeValue::M(values) => {
            budget.add_entries(values.len())?;
            let values = values
                .into_iter()
                .map(|(key, value)| {
                    budget.add_bytes(key.len(), "map key")?;
                    Ok((key, convert_from_sdk(value, depth + 1, budget)?))
                })
                .collect::<Result<HashMap<_, _>, ValueConversionError>>()?;
            Ok(DaxAttributeValue::MapValue { values })
        }
        _ => Err(validation(
            "unknown AWS SDK AttributeValue variant cannot cross the FFI boundary",
        )),
    }
}

fn validate_nonempty_set<T: Eq + Hash>(
    values: &[T],
    kind: &str,
) -> Result<(), ValueConversionError> {
    if values.is_empty() {
        return Err(validation(format!(
            "attribute value {kind} set must not be empty"
        )));
    }
    let mut unique = HashSet::with_capacity(values.len());
    if values.iter().any(|value| !unique.insert(value)) {
        return Err(validation(format!(
            "attribute value {kind} set contains a duplicate"
        )));
    }
    Ok(())
}

fn validate_number(value: &str) -> Result<(), ValueConversionError> {
    let bytes = value.as_bytes();
    let mut index = 0;
    if matches!(bytes.first(), Some(b'-' | b'+')) {
        index += 1;
    }
    let significand_start = index;
    let mut integer_digits = 0usize;
    while bytes.get(index).is_some_and(u8::is_ascii_digit) {
        integer_digits += 1;
        index += 1;
    }
    let mut fractional_digits = 0usize;
    if bytes.get(index) == Some(&b'.') {
        index += 1;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            fractional_digits += 1;
            index += 1;
        }
    }
    if integer_digits + fractional_digits == 0 {
        return Err(invalid_number(value));
    }

    let significand_end = index;
    let mut exponent = 0i64;
    if matches!(bytes.get(index), Some(b'e' | b'E')) {
        index += 1;
        let exponent_negative = bytes.get(index) == Some(&b'-');
        if matches!(bytes.get(index), Some(b'-' | b'+')) {
            index += 1;
        }
        let exponent_start = index;
        while bytes.get(index).is_some_and(u8::is_ascii_digit) {
            exponent = exponent
                .checked_mul(10)
                .and_then(|current| current.checked_add(i64::from(bytes[index] - b'0')))
                .ok_or_else(|| invalid_number(value))?;
            index += 1;
        }
        if exponent_start == index {
            return Err(invalid_number(value));
        }
        if exponent_negative {
            exponent = -exponent;
        }
    }
    if index != bytes.len() {
        return Err(invalid_number(value));
    }
    if i32::try_from(exponent).is_err() {
        return Err(invalid_number(value));
    }

    let digits = bytes[significand_start..significand_end]
        .iter()
        .filter(|byte| byte.is_ascii_digit())
        .copied()
        .collect::<Vec<_>>();
    let Some(first_nonzero) = digits.iter().position(|digit| *digit != b'0') else {
        return Ok(());
    };
    let last_nonzero = digits
        .iter()
        .rposition(|digit| *digit != b'0')
        .expect("first nonzero digit exists");
    if last_nonzero - first_nonzero + 1 > 38 {
        return Err(validation(
            "attribute value number exceeds DynamoDB's 38-digit precision",
        ));
    }

    let adjusted_exponent = exponent
        .checked_add(integer_digits as i64)
        .and_then(|value| value.checked_sub(first_nonzero as i64))
        .and_then(|value| value.checked_sub(1))
        .ok_or_else(|| invalid_number(value))?;
    if !(-130..=125).contains(&adjusted_exponent) {
        return Err(validation(
            "attribute value number exceeds DynamoDB's supported magnitude",
        ));
    }
    Ok(())
}

fn invalid_number(value: &str) -> ValueConversionError {
    validation(format!("invalid DynamoDB number string: {value:?}"))
}

fn validation(message: impl Into<String>) -> ValueConversionError {
    ValueConversionError(message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn variants() -> Vec<DaxAttributeValue> {
        vec![
            DaxAttributeValue::String {
                string: "text".into(),
            },
            DaxAttributeValue::Number {
                number: "-1.25e2".into(),
            },
            DaxAttributeValue::Binary {
                binary: vec![0, 1, 255],
            },
            DaxAttributeValue::Boolean { boolean: true },
            DaxAttributeValue::Null,
            DaxAttributeValue::StringSet {
                values: vec!["a".into(), "b".into()],
            },
            DaxAttributeValue::NumberSet {
                values: vec!["1".into(), "2.5".into()],
            },
            DaxAttributeValue::BinarySet {
                values: vec![vec![1], vec![2]],
            },
            DaxAttributeValue::ListValue {
                values: vec![DaxAttributeValue::Null],
            },
            DaxAttributeValue::MapValue {
                values: HashMap::from([(
                    "nested".into(),
                    DaxAttributeValue::Boolean { boolean: false },
                )]),
            },
        ]
    }

    #[test]
    fn round_trips_every_variant() {
        for value in variants() {
            let sdk = into_sdk_attribute_value(value.clone()).unwrap();
            assert_eq!(from_sdk_attribute_value(sdk).unwrap(), value);
        }
    }

    #[test]
    fn round_trips_items_with_one_shared_budget() {
        let item = variants()
            .into_iter()
            .enumerate()
            .map(|(index, value)| (format!("field-{index}"), value))
            .collect::<HashMap<_, _>>();
        let sdk = into_sdk_item(item.clone()).unwrap();
        assert_eq!(from_sdk_item(sdk).unwrap(), item);
    }

    #[test]
    fn validates_number_precision_and_magnitude() {
        for valid in ["0", "-0.0", ".5", "1.", "1e-130", "9.9e125"] {
            assert!(validate_number(valid).is_ok(), "{valid}");
        }
        for invalid in [
            "",
            ".",
            "1e",
            "1 2",
            "NaN",
            "Infinity",
            "123456789012345678901234567890123456789",
            "1e-131",
            "1e126",
            "0e2147483648",
        ] {
            assert!(validate_number(invalid).is_err(), "{invalid}");
        }
    }

    #[test]
    fn rejects_empty_and_duplicate_sets() {
        assert!(into_sdk_attribute_value(DaxAttributeValue::StringSet { values: vec![] }).is_err());
        assert!(
            into_sdk_attribute_value(DaxAttributeValue::StringSet {
                values: vec!["same".into(), "same".into()]
            })
            .is_err()
        );
    }

    #[test]
    fn rejects_excessive_depth_and_bytes() {
        let mut value = DaxAttributeValue::Null;
        for _ in 0..MAX_ATTRIBUTE_DEPTH {
            value = DaxAttributeValue::ListValue {
                values: vec![value],
            };
        }
        assert!(into_sdk_attribute_value(value).is_err());
        assert!(
            into_sdk_attribute_value(DaxAttributeValue::Binary {
                binary: vec![0; MAX_VALUE_BYTES + 1]
            })
            .is_err()
        );
    }

    proptest! {
        #[test]
        fn scalar_strings_and_binary_round_trip(
            string in "\\PC{0,512}",
            binary in proptest::collection::vec(any::<u8>(), 0..512),
        ) {
            for value in [
                DaxAttributeValue::String { string },
                DaxAttributeValue::Binary { binary },
            ] {
                let sdk = into_sdk_attribute_value(value.clone()).unwrap();
                prop_assert_eq!(from_sdk_attribute_value(sdk).unwrap(), value);
            }
        }

        #[test]
        fn valid_numbers_round_trip(coefficient in 1u64..1_000_000, exponent in -120i32..120) {
            let value = DaxAttributeValue::Number {
                number: format!("{coefficient}e{exponent}"),
            };
            let sdk = into_sdk_attribute_value(value.clone()).unwrap();
            prop_assert_eq!(from_sdk_attribute_value(sdk).unwrap(), value);
        }
    }
}
