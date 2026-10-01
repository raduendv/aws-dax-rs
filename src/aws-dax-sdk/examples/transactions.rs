use std::{
    collections::HashMap,
    env,
    time::{SystemTime, UNIX_EPOCH},
};

use aws_config::BehaviorVersion;
use aws_dax::{Client, Config};
use aws_sdk_dynamodb::operation::{
    transact_get_items::TransactGetItemsInput, transact_write_items::TransactWriteItemsInput,
};
use aws_sdk_dynamodb::types::{
    AttributeValue, Delete, Get, Put, TransactGetItem, TransactWriteItem,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = env::var("DAX_ENDPOINT")?;
    let table = env::var("DAX_TABLE_NAME").unwrap_or_else(|_| "DaxParityTable".into());
    let sdk_config = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let mut config = Config::builder().endpoint(&endpoint);
    if let Some(region) = sdk_config.region() {
        config = config.region(region.as_ref());
    }
    if let Some(credentials) = sdk_config.credentials_provider() {
        config = config.credentials_provider(credentials.clone());
    }
    if env::var("DAX_ROUTE_MANAGER").as_deref() == Ok("1") {
        config = config.route_manager_enabled(true);
    }
    if env::var("DAX_TRANSACTION_NO_RETRIES").as_deref() == Ok("1") {
        config = config.retry_max_attempts(0);
    }
    let client = Client::new(config.build()?)?;
    if env::var("DAX_REFRESH_ENDPOINTS").as_deref() == Ok("1") {
        client.refresh_endpoints().await?;
    }

    let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let partition =
        env::var("DAX_TRANSACTION_PK").unwrap_or_else(|_| format!("rust-transaction-{suffix}"));
    let items = ["TX-1", "TX-2"]
        .into_iter()
        .map(|sort| {
            let mut item: HashMap<String, AttributeValue> = HashMap::from([
                ("PK".into(), AttributeValue::S(partition.clone())),
                ("SK".into(), AttributeValue::S(sort.into())),
                (
                    "Name".into(),
                    AttributeValue::S("rust-transaction-example".into()),
                ),
            ]);
            if env::var("DAX_TRANSACTION_NO_ATTRIBUTES").as_deref() == Ok("1") {
                item.remove("Name");
            }
            item
        })
        .collect::<Vec<_>>();

    let use_delete = env::var("DAX_TRANSACTION_DELETE").as_deref() == Ok("1");
    let mut write_builder = TransactWriteItemsInput::builder();
    for item in &items {
        let action = if use_delete {
            let key = item
                .iter()
                .filter(|(name, _)| *name == "PK" || *name == "SK")
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            TransactWriteItem::builder()
                .delete(
                    Delete::builder()
                        .table_name(&table)
                        .set_key(Some(key))
                        .build()?,
                )
                .build()
        } else {
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(&table)
                        .set_item(Some(item.clone()))
                        .build()?,
                )
                .build()
        };
        write_builder = write_builder.transact_items(action);
    }
    if let Ok(token) = env::var("DAX_TRANSACTION_TOKEN") {
        write_builder = write_builder.client_request_token(token);
    }
    let write = write_builder.build()?;
    if env::var("DAX_TRANSACTION_READ_ONLY").as_deref() != Ok("1") {
        client.transact_write_items().input(write).send().await?;
        println!("TransactWriteItems succeeded endpoint={endpoint} pk={partition}");
    } else {
        println!("skipping TransactWriteItems pk={partition}");
    }

    let read = TransactGetItemsInput::builder()
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name(&table)
                        .set_key(Some(key(&items[0])))
                        .build()?,
                )
                .build(),
        )
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name(&table)
                        .set_key(Some(key(&items[1])))
                        .build()?,
                )
                .build(),
        )
        .build()?;
    let output = client.transact_get_items().input(read).send().await?;
    if output.responses().len() != 2 {
        return Err(format!(
            "expected 2 transaction responses, got {}",
            output.responses().len()
        )
        .into());
    }
    println!(
        "TransactGetItems succeeded responses={}",
        output.responses().len()
    );

    if env::var("DAX_TRANSACTION_READ_ONLY").as_deref() != Ok("1") {
        for item in items {
            client
                .delete_item()
                .table_name(&table)
                .key("PK", item["PK"].clone())
                .key("SK", item["SK"].clone())
                .send()
                .await?;
        }
        println!("transaction cleanup succeeded");
    }
    Ok(())
}

fn key(item: &HashMap<String, AttributeValue>) -> HashMap<String, AttributeValue> {
    HashMap::from([
        ("PK".into(), item["PK"].clone()),
        ("SK".into(), item["SK"].clone()),
    ])
}
