#![no_main]

use std::collections::HashMap;

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    let attribute_lists = HashMap::from([(
        7_i64,
        vec![
            "first".to_owned(),
            "second".to_owned(),
            "nested".to_owned(),
        ],
    )]);

    let _ = aws_dax::cbor::decode_item_non_key_attributes(input, &attribute_lists);
});
