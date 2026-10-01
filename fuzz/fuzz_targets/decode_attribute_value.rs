#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    let _ = aws_dax::cbor::decode_attribute_value(input);
});
