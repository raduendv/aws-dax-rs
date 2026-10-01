#![no_main]

use aws_sdk_dynamodb::types::AttributeDefinition;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    let key_definition = [AttributeDefinition::builder()
        .attribute_name("pk")
        .attribute_type("S".into())
        .build()
        .expect("fuzz key definition is valid")];

    let _ = aws_dax::cbor::decode_item_key(input, &key_definition);
});
