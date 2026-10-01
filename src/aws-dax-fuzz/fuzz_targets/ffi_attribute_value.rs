#![no_main]

use std::collections::HashMap;

use aws_dax_ffi::{DaxAttributeValue, fuzz_round_trip_attribute_value};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| {
    let mut cursor = Cursor { input, offset: 0 };
    if let Some(value) = cursor.value(0) {
        let _ = fuzz_round_trip_attribute_value(value);
    }
});

struct Cursor<'a> {
    input: &'a [u8],
    offset: usize,
}

impl Cursor<'_> {
    fn value(&mut self, depth: usize) -> Option<DaxAttributeValue> {
        let tag = self.byte()? % if depth < 40 { 10 } else { 8 };
        match tag {
            0 => Some(DaxAttributeValue::String {
                string: self.string()?,
            }),
            1 => Some(DaxAttributeValue::Number {
                number: self.string()?,
            }),
            2 => Some(DaxAttributeValue::Binary {
                binary: self.bytes()?,
            }),
            3 => Some(DaxAttributeValue::Boolean {
                boolean: self.byte()? & 1 == 1,
            }),
            4 => Some(DaxAttributeValue::Null),
            5 => Some(DaxAttributeValue::StringSet {
                values: self.strings()?,
            }),
            6 => Some(DaxAttributeValue::NumberSet {
                values: self.strings()?,
            }),
            7 => Some(DaxAttributeValue::BinarySet {
                values: self.binary_values()?,
            }),
            8 => {
                let count = usize::from(self.byte()? % 8);
                let values = (0..count)
                    .map(|_| self.value(depth + 1))
                    .collect::<Option<Vec<_>>>()?;
                Some(DaxAttributeValue::ListValue { values })
            }
            _ => {
                let count = usize::from(self.byte()? % 8);
                let values = (0..count)
                    .map(|_| Some((self.string()?, self.value(depth + 1)?)))
                    .collect::<Option<HashMap<_, _>>>()?;
                Some(DaxAttributeValue::MapValue { values })
            }
        }
    }

    fn strings(&mut self) -> Option<Vec<String>> {
        let count = usize::from(self.byte()? % 8);
        (0..count).map(|_| self.string()).collect()
    }

    fn binary_values(&mut self) -> Option<Vec<Vec<u8>>> {
        let count = usize::from(self.byte()? % 8);
        (0..count).map(|_| self.bytes()).collect()
    }

    fn string(&mut self) -> Option<String> {
        Some(String::from_utf8_lossy(&self.bytes()?).into_owned())
    }

    fn bytes(&mut self) -> Option<Vec<u8>> {
        let len = usize::from(self.byte()?);
        let end = self.offset.checked_add(len)?.min(self.input.len());
        let value = self.input.get(self.offset..end)?.to_vec();
        self.offset = end;
        Some(value)
    }

    fn byte(&mut self) -> Option<u8> {
        let value = *self.input.get(self.offset)?;
        self.offset += 1;
        Some(value)
    }
}
