#!/usr/bin/env python3
"""Exercise optimized generated/packaged Python bindings against the real library."""

import argparse
import ctypes
import importlib
import importlib.util
from pathlib import Path
import shutil
import sys
import tempfile


def cases(dax):
    value = dax.DaxAttributeValue
    attributes = [
        value.STRING(""), value.STRING("pk#1"), value.STRING("\u03bb\0value"),
        value.NUMBER("-123.5"), value.BINARY(b"\0\xff" * 128),
        value.BOOLEAN(True), value.BOOLEAN(False), value.NULL(),
        value.STRING_SET(["a", "b"]), value.NUMBER_SET(["1", "2"]),
        value.BINARY_SET([b"a", b"\0"]), value.LIST_VALUE([]),
        value.MAP_VALUE({"nested": value.LIST_VALUE([value.STRING("test"), value.NULL()])}),
        value.STRING("large" * 10000),
    ]
    key = {"pk": value.STRING("pk#1"), "sk": value.STRING("sk#1")}
    item = {**key, "test": value.STRING("test"),
            "created_at": value.STRING("2026-02-05 13:11:01")}
    records = [(dax._UniffiFfiConverterTypeDaxAttributeValue, v) for v in attributes]
    records.extend([
        (dax._UniffiFfiConverterTypeDaxGetItemRequest, dax.DaxGetItemRequest(
            table_name="table", key=key, consistent_read=None,
            projection_expression=None, expression_attribute_names={})),
        (dax._UniffiFfiConverterTypeDaxGetItemRequest, dax.DaxGetItemRequest(
            table_name="table", key=key, consistent_read=True,
            projection_expression="#pk", expression_attribute_names={"#pk": "pk"})),
        (dax._UniffiFfiConverterTypeDaxPutItemRequest, dax.DaxPutItemRequest(
            table_name="table", item=item, return_values=None, condition_expression=None,
            expression_attribute_names={}, expression_attribute_values={})),
        (dax._UniffiFfiConverterTypeDaxGetItemResponse, dax.DaxGetItemResponse(item=item)),
        (dax._UniffiFfiConverterTypeDaxGetItemResponse, dax.DaxGetItemResponse(item=None)),
    ])
    return attributes, records


def encoded(converter, value):
    converter.check_lower(value)
    buffer = converter.lower(value)
    try:
        return ctypes.string_at(buffer.data, buffer.len)
    finally:
        buffer.free()


def validate(dax):
    optimized = dax._UniffiRustBufferBuilder
    if "bytearray" not in optimized.__init__.__code__.co_names:
        raise AssertionError("expected optimized generated builder")
    attributes, records = cases(dax)
    optimized_bytes = [encoded(converter, value) for converter, value in records]
    fixture = (Path(__file__).parent / "fixtures/python_buffer_builder.py").read_text()
    try:
        exec(compile(fixture, "<baseline-builder>", "exec"), dax.__dict__)
        baseline_bytes = [encoded(converter, value) for converter, value in records]
    finally:
        dax._UniffiRustBufferBuilder = optimized
    if baseline_bytes != optimized_bytes:
        raise AssertionError("optimized serialization differs from pinned generator")
    for converter, value in records:
        if converter.lift(converter.lower(value)) != value:
            raise AssertionError("generated converter round trip differs")
    for value in attributes:
        if dax.ffi_round_trip_attribute_value(value) != value:
            raise AssertionError("Rust attribute conversion differs")
    try:
        dax.ffi_round_trip_attribute_value(dax.DaxAttributeValue.NUMBER("invalid"))
    except dax.DaxFfiError.Failure as error:
        if error.details.category != dax.DaxErrorCategory.VALIDATION:
            raise AssertionError("invalid number returned a non-validation error") from error
    else:
        raise AssertionError("invalid number unexpectedly accepted")
    print(f"Python staged writer: {len(records)} byte-identical records, "
          f"{len(attributes)} Rust round trips and structured validation error passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binding", type=Path)
    parser.add_argument("--native-library", type=Path)
    args = parser.parse_args()
    if bool(args.binding) != bool(args.native_library):
        parser.error("--binding and --native-library must be supplied together")
    if not args.binding:
        validate(importlib.import_module("aws_dax_ffi"))
        return
    with tempfile.TemporaryDirectory(prefix="dax-python-writer-") as folder:
        directory = Path(folder)
        shutil.copy2(args.binding, directory / "aws_dax_ffi.py")
        shutil.copy2(args.native_library, directory / args.native_library.name)
        spec = importlib.util.spec_from_file_location("aws_dax_ffi", directory / "aws_dax_ffi.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        validate(module)


if __name__ == "__main__":
    main()
