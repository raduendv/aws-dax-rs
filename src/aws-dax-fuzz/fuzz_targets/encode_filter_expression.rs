#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    if let Ok(expression) = std::str::from_utf8(input) {
        let _ = aws_dax::cbor::fuzz_encode_filter_expression(expression);
    }
});
