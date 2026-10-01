# Migration from `aws-dax-go-v2`

This crate is an asynchronous Rust port of the pinned
`aws-dax-go-v2` `v1.0.3` client. It follows AWS SDK for Rust input and output
models where practical while preserving DAX-specific validation, wire
encoding, routing, and lifecycle behavior.

## Basic client construction

Go:

```go
cfg, err := config.LoadDefaultConfig(context.Background())
client, err := dax.NewFromConfig(cfg, "dax://cluster.example.com:8111")
```

Rust:

```rust,no_run
use aws_dax::Client;

# async fn example() -> Result<(), aws_dax::Error> {
let sdk_config = aws_config::load_from_env().await;
let _client = Client::from_sdk_config(&sdk_config, "dax://cluster.example.com:8111")?;
# Ok(())
# }
```

## Operations

Rust builders use the same operation names and DynamoDB model types:

```rust,no_run
use aws_dax::Client;
use aws_sdk_dynamodb::types::AttributeValue;

# async fn example(client: Client) -> Result<(), aws_dax::Error> {
let output = client
    .get_item()
    .table_name("Example")
    .key("id", AttributeValue::S("42".into()))
    .send()
    .await?;
# Ok(())
# }
```

For inputs assembled elsewhere, pass the generated SDK input with
`.input(input)`. Outputs expose the corresponding generated DynamoDB output
model.

## Paginators

Query, Scan, and BatchGet use lazy Rust paginators:

```rust,no_run
use aws_dax::Client;
use aws_sdk_dynamodb::operation::scan::ScanInput;

# async fn example(client: Client) -> Result<(), aws_dax::Error> {
let input = ScanInput::builder().table_name("Example").build()?;
let mut pages = client.scan_paginator(input);
while let Some(page) = pages.next_page().await? {
    for item in page.items().unwrap_or_default() {
        // process item
    }
}
# Ok(())
# }
```

Validation is deferred until the first `next_page().await`, matching the lazy
construction behavior of the Go paginator APIs.

## Compatibility boundaries

The port intentionally documents bounded edges instead of silently emulating
unsupported behavior. Consult the [README](README.md) and
[status.md](status.md) for unsupported TLS verifier overrides, bounded
expression grammar, live-transport fixture coverage, and other release
limitations.
