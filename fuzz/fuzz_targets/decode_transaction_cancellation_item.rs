#![no_main]

use std::collections::HashMap;

use aws_sdk_dynamodb::types::{AttributeDefinition, AttributeValue};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    let key = HashMap::from([(
        "pk".to_owned(),
        AttributeValue::S("fuzz-key".to_owned()),
    )]);
    let key_definition = [AttributeDefinition::builder()
        .attribute_name("pk")
        .attribute_type("S".into())
        .build()
        .expect("fuzz key definition is valid")];

    let _ = aws_dax::cbor::decode_transaction_cancellation_item(
        Some(input),
        &key,
        &key_definition,
        &HashMap::new(),
    );
});
