use std::collections::HashMap;

use aws_dax::cbor::{
    CborError, decode_attribute_value, decode_item_key, decode_item_non_key_attributes,
    encode_attribute_value, encode_item_key, encode_item_non_key_attributes,
};
use aws_sdk_dynamodb::{
    primitives::Blob,
    types::{AttributeDefinition, AttributeValue, ScalarAttributeType},
};

fn round_trip(value: AttributeValue) {
    let encoded = encode_attribute_value(&value).unwrap();
    assert_eq!(decode_attribute_value(&encoded).unwrap(), value);
}

#[test]
fn encodes_go_golden_vectors_for_primitive_attribute_values() {
    assert_eq!(
        encode_attribute_value(&AttributeValue::S("abc".into())).unwrap(),
        hex("63616263")
    );
    assert_eq!(
        encode_attribute_value(&AttributeValue::B(Blob::new([1, 2, 3]))).unwrap(),
        hex("43010203")
    );
    assert_eq!(
        encode_attribute_value(&AttributeValue::Bool(true)).unwrap(),
        hex("f5")
    );
    assert_eq!(
        encode_attribute_value(&AttributeValue::Bool(false)).unwrap(),
        hex("f4")
    );
    assert_eq!(
        encode_attribute_value(&AttributeValue::Null(true)).unwrap(),
        hex("f6")
    );
    assert_eq!(
        encode_attribute_value(&AttributeValue::N("123".into())).unwrap(),
        hex("187b")
    );
    assert_eq!(
        encode_attribute_value(&AttributeValue::N("-123".into())).unwrap(),
        hex("387a")
    );
    assert_eq!(
        encode_attribute_value(&AttributeValue::N("123456789012345678901234567890".into()))
            .unwrap(),
        hex("c24d018ee90ff6c373e0ee4e3f0ad2")
    );
    assert_eq!(
        encode_attribute_value(&AttributeValue::N("-123456789012345678901234567890".into()))
            .unwrap(),
        hex("c34d018ee90ff6c373e0ee4e3f0ad1")
    );
    assert_eq!(
        encode_attribute_value(&AttributeValue::N("273.15".into())).unwrap(),
        hex("c48221196ab3")
    );
}

#[test]
fn round_trips_nested_non_numeric_attribute_values() {
    let mut map = HashMap::new();
    map.insert("binary".into(), AttributeValue::B(Blob::new([1, 2, 3])));
    map.insert(
        "list".into(),
        AttributeValue::L(vec![
            AttributeValue::S("value".into()),
            AttributeValue::Bool(false),
            AttributeValue::Null(true),
        ]),
    );
    round_trip(AttributeValue::M(map));
    round_trip(AttributeValue::L(Vec::new()));
    round_trip(AttributeValue::M(HashMap::new()));
    round_trip(AttributeValue::S(String::new()));
    round_trip(AttributeValue::B(Blob::new(Vec::<u8>::new())));
}

#[test]
fn encodes_and_decodes_dax_set_tags() {
    let strings = AttributeValue::Ss(vec!["abc".into(), "def".into(), "xyz".into()]);
    assert_eq!(
        encode_attribute_value(&strings).unwrap(),
        hex("d90cf98363616263636465666378797a")
    );
    round_trip(strings);

    let numbers = AttributeValue::Ns(vec!["123".into(), "456".into(), "789".into()]);
    assert_eq!(
        encode_attribute_value(&numbers).unwrap(),
        hex("d90cfa83187b1901c8190315")
    );
    round_trip(numbers);

    let binary = AttributeValue::Bs(vec![Blob::new([1, 2, 3]), Blob::new([4, 5, 6])]);
    assert_eq!(
        encode_attribute_value(&binary).unwrap(),
        hex("d90cfb824301020343040506")
    );
    round_trip(binary);
}

#[test]
fn decodes_go_integer_and_bignum_boundaries() {
    for (wire, value) in [
        ("00", "0"),
        ("20", "-1"),
        ("1b7fffffffffffffff", "9223372036854775807"),
        ("1b8000000000000000", "9223372036854775808"),
        ("3b7fffffffffffffff", "-9223372036854775808"),
        ("3b8000000000000000", "-9223372036854775809"),
        ("1bffffffffffffffff", "18446744073709551615"),
        ("c249010000000000000000", "18446744073709551616"),
        ("c349010000000000000000", "-18446744073709551617"),
    ] {
        assert_eq!(
            decode_attribute_value(&hex(wire)).unwrap(),
            AttributeValue::N(value.into()),
            "wire vector {wire}"
        );
    }
}

#[test]
fn normalizes_decimal_and_scientific_number_round_trips() {
    for (input, expected) in [
        ("273.15", "27315E-2"),
        ("314E-2", "314E-2"),
        ("1234.5E-4", "12345E-5"),
        ("0E+7", "0E7"),
        ("-0", "0"),
    ] {
        let encoded = encode_attribute_value(&AttributeValue::N(input.into())).unwrap();
        assert_eq!(
            decode_attribute_value(&encoded).unwrap(),
            AttributeValue::N(expected.into()),
            "number {input}"
        );
    }
}

#[test]
fn encodes_and_decodes_go_item_key_frames() {
    let single_string = vec![key_definition("hks", ScalarAttributeType::S)];
    let single_number = vec![key_definition("hkn", ScalarAttributeType::N)];
    let composite_string_number = vec![
        key_definition("hks", ScalarAttributeType::S),
        key_definition("rkn", ScalarAttributeType::N),
    ];
    let composite_binary_binary = vec![
        key_definition("hkb", ScalarAttributeType::B),
        key_definition("rkb", ScalarAttributeType::B),
    ];

    let string_item = HashMap::from([("hks".into(), AttributeValue::S("hkv".into()))]);
    assert_eq!(
        encode_item_key(&string_item, &single_string).unwrap(),
        hex("43686b76")
    );
    assert_eq!(
        decode_item_key(&hex("43686b76"), &single_string).unwrap(),
        string_item
    );

    let number_item = HashMap::from([("hkn".into(), AttributeValue::N("5".into()))]);
    assert_eq!(
        encode_item_key(&number_item, &single_number).unwrap(),
        hex("4105")
    );
    assert_eq!(
        decode_item_key(&hex("4105"), &single_number).unwrap(),
        number_item
    );

    let number_range = HashMap::from([
        ("hks".into(), AttributeValue::S("hkv".into())),
        ("rkn".into(), AttributeValue::N("3.14".into())),
    ]);
    assert_eq!(
        encode_item_key(&number_range, &composite_string_number).unwrap(),
        hex("4863686b76c1518020")
    );
    assert_eq!(
        decode_item_key(&hex("4863686b76c1518020"), &composite_string_number).unwrap(),
        HashMap::from([
            ("hks".into(), AttributeValue::S("hkv".into())),
            ("rkn".into(), AttributeValue::N("314E-2".into())),
        ])
    );

    let extended_negative_range = HashMap::from([
        ("hks".into(), AttributeValue::S("hkv".into())),
        (
            "rkn".into(),
            AttributeValue::N("-22234353.464363E-43534545".into()),
        ),
    ]);
    assert_eq!(
        encode_item_key(&extended_negative_range, &composite_string_number).unwrap(),
        hex("5163686b767e829848c8c569c775705f7fe0")
    );
    assert_eq!(
        decode_item_key(
            &hex("5163686b767e829848c8c569c775705f7fe0"),
            &composite_string_number,
        )
        .unwrap(),
        HashMap::from([
            ("hks".into(), AttributeValue::S("hkv".into())),
            (
                "rkn".into(),
                AttributeValue::N("-22234353464363E-43534551".into()),
            ),
        ])
    );

    let binary_item = HashMap::from([
        ("hkb".into(), AttributeValue::B(Blob::new([4, 5, 6]))),
        ("rkb".into(), AttributeValue::B(Blob::new([1, 2, 3]))),
    ]);
    assert_eq!(
        encode_item_key(&binary_item, &composite_binary_binary).unwrap(),
        hex("4743040506010203")
    );
    assert_eq!(
        decode_item_key(&hex("4743040506010203"), &composite_binary_binary).unwrap(),
        binary_item
    );
}

#[test]
fn rejects_invalid_item_key_definitions_and_values() {
    let string_key = vec![key_definition("pk", ScalarAttributeType::S)];
    let item = HashMap::new();
    assert_eq!(
        encode_item_key(&item, &string_key).unwrap_err(),
        CborError::InvalidItemKey("a required key is missing")
    );
    assert_eq!(
        encode_item_key(
            &HashMap::from([("pk".into(), AttributeValue::N("1".into()))]),
            &string_key,
        )
        .unwrap_err(),
        CborError::InvalidItemKey("key value does not match its key definition")
    );
    assert_eq!(
        decode_item_key(&hex("4161"), &[]).unwrap_err(),
        CborError::InvalidItemKey("key definition must contain one or two attributes")
    );
}

#[test]
fn encodes_and_decodes_go_schema_compressed_non_key_attributes() {
    let key_definition = vec![
        key_definition("hks", ScalarAttributeType::S),
        key_definition("rkn", ScalarAttributeType::N),
    ];
    let item = HashMap::from([
        ("hks".into(), AttributeValue::S("hkv".into())),
        ("rkn".into(), AttributeValue::N("123".into())),
        ("av1".into(), AttributeValue::S("avs".into())),
        ("av2".into(), AttributeValue::N("456".into())),
        ("av3".into(), AttributeValue::B(Blob::new([1, 2, 3]))),
    ]);
    let attribute_lists = HashMap::from([(1_i64, vec!["av1".into(), "av2".into(), "av3".into()])]);

    assert_eq!(
        encode_item_non_key_attributes(&item, &key_definition, 1).unwrap(),
        hex("01636176731901c843010203")
    );
    assert_eq!(
        decode_item_non_key_attributes(&hex("01636176731901c843010203"), &attribute_lists).unwrap(),
        HashMap::from([
            ("av1".into(), AttributeValue::S("avs".into())),
            ("av2".into(), AttributeValue::N("456".into())),
            ("av3".into(), AttributeValue::B(Blob::new([1, 2, 3]))),
        ])
    );
    assert_eq!(
        decode_item_non_key_attributes(&hex("01"), &HashMap::new()).unwrap_err(),
        CborError::UnknownAttributeListId(1)
    );
}

#[test]
fn preserves_go_lexdecimal_vectors_and_numeric_ordering() {
    let definition = vec![
        key_definition("pk", ScalarAttributeType::S),
        key_definition("sk", ScalarAttributeType::N),
    ];
    for (number, frame, normalized) in [
        ("3.14", "4762706bc1518020", "314E-2"),
        ("3.141", "4862706bc151870000", "3141E-3"),
        ("3.14E11", "4762706bcc518020", "314E9"),
        ("-123.0", "4862706b3cde3f3ffc", "-1230E-1"),
        (
            "111122223333444445555.66667777",
            "5262706bd51ec863ad59721c98dea6ac70e004",
            "11112222333344444555566667777E-8",
        ),
        (
            "111122223333444445555.66667778",
            "5262706bd51ec863ad59721c98dea6ac718004",
            "11112222333344444555566667778E-8",
        ),
    ] {
        let item = HashMap::from([
            ("pk".into(), AttributeValue::S("pk".into())),
            ("sk".into(), AttributeValue::N(number.into())),
        ]);
        assert_eq!(encode_item_key(&item, &definition).unwrap(), hex(frame));
        assert_eq!(
            decode_item_key(&hex(frame), &definition).unwrap(),
            HashMap::from([
                ("pk".into(), AttributeValue::S("pk".into())),
                ("sk".into(), AttributeValue::N(normalized.into())),
            ])
        );
    }

    let mut previous: Option<Vec<u8>> = None;
    for number in [
        "-3.14",
        "-1",
        "0",
        "0.1",
        "3.14",
        "3.14E11",
        "111122223333444445555.66667777",
        "111122223333444445555.66667778",
    ] {
        let item = HashMap::from([
            ("pk".into(), AttributeValue::S("pk".into())),
            ("sk".into(), AttributeValue::N(number.into())),
        ]);
        let encoded = encode_item_key(&item, &definition).unwrap();
        if let Some(previous) = previous {
            assert!(
                previous.as_slice() < &encoded[4..],
                "lexdecimal order failed for {number}"
            );
        }
        previous = Some(encoded[4..].to_vec());
    }
}

#[test]
fn rejects_invalid_and_not_yet_supported_attribute_values() {
    assert_eq!(
        encode_attribute_value(&AttributeValue::Null(false)).unwrap_err(),
        CborError::InvalidAttributeValue("NULL must be true")
    );
    assert_eq!(
        decode_attribute_value(&hex("c4822119013a")).unwrap(),
        AttributeValue::N("314E-2".into())
    );
    assert_eq!(
        encode_attribute_value(&AttributeValue::N("not-a-number".into())).unwrap_err(),
        CborError::InvalidAttributeValue("N is not a valid decimal")
    );
    assert_eq!(
        decode_attribute_value(&hex("187b")).unwrap(),
        AttributeValue::N("123".into())
    );
    for value in [
        AttributeValue::Ss(Vec::new()),
        AttributeValue::Ns(Vec::new()),
        AttributeValue::Bs(Vec::new()),
    ] {
        assert!(matches!(
            encode_attribute_value(&value),
            Err(CborError::InvalidAttributeValue(_))
        ));
    }
}

#[test]
fn rejects_malformed_input_without_allocating_unbounded_memory() {
    assert_eq!(
        decode_attribute_value(&[]).unwrap_err(),
        CborError::UnexpectedEnd
    );
    assert!(matches!(
        decode_attribute_value(&hex("63ff")),
        Err(CborError::UnexpectedEnd)
    ));
    assert!(matches!(
        decode_attribute_value(&hex("9f")),
        Err(CborError::UnexpectedEnd)
    ));
    assert!(matches!(
        decode_attribute_value(&hex("7a01000001")),
        Err(CborError::ValueTooLarge(_))
    ));
    assert!(matches!(
        decode_attribute_value(&hex("f7")),
        Err(CborError::UnexpectedType { .. })
    ));
    for wire in [
        "58",
        "5900",
        "5a000000",
        "5b00000000000000",
        "c2",
        "c48221",
        "d90cf9",
    ] {
        assert_eq!(
            decode_attribute_value(&hex(wire)).unwrap_err(),
            CborError::UnexpectedEnd,
            "wire vector {wire}"
        );
    }

    let mut nested_large_containers = Vec::new();
    for _ in 0..2 {
        nested_large_containers.extend_from_slice(&hex("9a000186a0"));
    }
    assert_eq!(
        decode_attribute_value(&nested_large_containers).unwrap_err(),
        CborError::UnexpectedEnd
    );
}

#[test]
fn enforces_cbor_container_and_nesting_boundaries() {
    let exact_value = {
        let mut wire = hex("5a01000000");
        wire.resize(5 + 16 * 1024 * 1024, 0);
        wire
    };
    assert!(matches!(
        decode_attribute_value(&exact_value),
        Ok(AttributeValue::B(value)) if value.as_ref().len() == 16 * 1024 * 1024
    ));
    assert_eq!(
        decode_attribute_value(&hex("5a01000001")).unwrap_err(),
        CborError::ValueTooLarge(16 * 1024 * 1024 + 1)
    );

    let exact_container = {
        let mut wire = hex("9a000186a0");
        wire.extend(std::iter::repeat_n(0xf6, 100_000));
        wire
    };
    assert!(matches!(
        decode_attribute_value(&exact_container),
        Ok(AttributeValue::L(values)) if values.len() == 100_000
    ));

    let too_large_container = hex("9a000186a1");
    assert!(matches!(
        decode_attribute_value(&too_large_container),
        Err(CborError::ContainerTooLarge(100_001))
    ));

    let exact_depth = nested_arrays(64);
    assert!(matches!(
        decode_attribute_value(&exact_depth),
        Ok(AttributeValue::L(_))
    ));
    let too_deep = nested_arrays(65);
    assert_eq!(
        decode_attribute_value(&too_deep).unwrap_err(),
        CborError::NestingTooDeep
    );

    let aggregate_budget = nested_string_sets(49_999, 49_999);
    assert!(matches!(
        decode_attribute_value(&aggregate_budget),
        Ok(AttributeValue::L(values)) if values.len() == 2
    ));
    let over_budget = nested_string_sets(49_999, 50_000);
    assert_eq!(
        decode_attribute_value(&over_budget).unwrap_err(),
        CborError::ContainerTooLarge(100_001)
    );
}

fn key_definition(name: &str, attribute_type: ScalarAttributeType) -> AttributeDefinition {
    AttributeDefinition::builder()
        .attribute_name(name)
        .attribute_type(attribute_type)
        .build()
        .expect("test key definition is complete")
}

fn nested_arrays(depth: usize) -> Vec<u8> {
    let mut wire = std::iter::repeat_n(0x81, depth).collect::<Vec<_>>();
    wire.push(0xf6);
    wire
}

fn nested_string_sets(first_length: usize, second_length: usize) -> Vec<u8> {
    let mut wire = vec![0x82];
    for length in [first_length, second_length] {
        wire.extend_from_slice(&hex("d90cf9"));
        wire.push(0x9a);
        wire.extend_from_slice(&(length as u32).to_be_bytes());
        wire.extend(std::iter::repeat_n(0x60, length));
    }
    wire
}

fn hex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16)
                .expect("test vector contains valid hexadecimal")
        })
        .collect()
}
