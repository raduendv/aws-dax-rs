use aws_config::BehaviorVersion;
use aws_dax::Client;
use aws_sdk_dynamodb::types::AttributeValue;

#[tokio::main]
async fn main() -> Result<(), Box<aws_dax::Error>> {
    let sdk_config = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let client = Client::from_sdk_config(&sdk_config, "dax://example-cluster:8111")?;

    let output = client
        .get_item()
        .table_name("example")
        .key("id", AttributeValue::S("42".into()))
        .send()
        .await?;

    match output.item {
        Some(item) => println!("retrieved {} attributes", item.len()),
        None => println!("item not found"),
    }

    Ok(())
}
