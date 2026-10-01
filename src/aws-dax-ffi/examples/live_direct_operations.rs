use std::{
    collections::HashMap,
    env,
    error::Error,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use aws_dax_ffi::{
    DaxAttributeValue, DaxClientConfig, DaxDeleteItemRequest, DaxGetItemRequest, DaxPutItemRequest,
    DaxQueryRequest, DaxReturnValue, DaxScanRequest, DaxUpdateItemRequest, create_dax_client,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let endpoint = env::var("DAX_ENDPOINT")?;
    let table_name = env::var("DAX_TABLE_NAME").unwrap_or_else(|_| "DaxParityTable".into());
    let region = env::var("AWS_REGION").unwrap_or_else(|_| "eu-west-1".into());
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    let partition = format!("FFI#DIRECT#{nonce}");
    let sort = "ITEM#1".to_owned();
    let key = item([("PK", string(&partition)), ("SK", string(&sort))]);

    let client = create_dax_client(DaxClientConfig {
        endpoints: vec![endpoint],
        region: Some(region),
        ..DaxClientConfig::default()
    })
    .await?;

    client
        .put_item(DaxPutItemRequest {
            table_name: table_name.clone(),
            item: item([
                ("PK", string(&partition)),
                ("SK", string(&sort)),
                ("value", string("created")),
            ]),
            return_values: None,
            condition_expression: None,
            expression_attribute_names: HashMap::new(),
            expression_attribute_values: HashMap::new(),
        })
        .await?;

    let result = async {
        let mut observed = false;
        for _ in 0..20 {
            let output = client
                .get_item(DaxGetItemRequest {
                    table_name: table_name.clone(),
                    key: key.clone(),
                    consistent_read: None,
                    projection_expression: None,
                    expression_attribute_names: HashMap::new(),
                })
                .await?;
            if output.item.is_some() {
                observed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if !observed {
            return Err("GetItem did not observe the inserted item".into());
        }

        let updated = client
            .update_item(DaxUpdateItemRequest {
                table_name: table_name.clone(),
                key: key.clone(),
                update_expression: "SET #value = :value".into(),
                condition_expression: None,
                expression_attribute_names: HashMap::from([("#value".into(), "value".into())]),
                expression_attribute_values: item([(":value", string("updated"))]),
                return_values: Some(DaxReturnValue::AllOld),
            })
            .await?;
        if updated.attributes.is_none() {
            return Err("UpdateItem did not return ALL_OLD attributes".into());
        }

        let query = client
            .query(DaxQueryRequest {
                table_name: table_name.clone(),
                index_name: None,
                key_condition_expression: "PK = :pk".into(),
                filter_expression: None,
                projection_expression: None,
                expression_attribute_names: HashMap::new(),
                expression_attribute_values: item([(":pk", string(&partition))]),
                consistent_read: None,
                limit: Some(10),
                scan_index_forward: None,
                select: None,
                return_consumed_capacity: None,
                exclusive_start_key: None,
            })
            .await?;
        if query.items.is_empty() {
            return Err("Query returned no items".into());
        }

        let scan = client
            .scan(DaxScanRequest {
                table_name: table_name.clone(),
                index_name: None,
                filter_expression: Some("PK = :pk".into()),
                projection_expression: None,
                expression_attribute_names: HashMap::new(),
                expression_attribute_values: item([(":pk", string(&partition))]),
                consistent_read: None,
                limit: None,
                select: None,
                return_consumed_capacity: None,
                exclusive_start_key: None,
            })
            .await?;
        if scan.items.is_empty() {
            return Err("Scan returned no items".into());
        }
        Ok::<(), Box<dyn Error>>(())
    }
    .await;

    let deleted = client
        .delete_item(DaxDeleteItemRequest {
            table_name,
            key,
            return_values: Some(DaxReturnValue::AllOld),
            condition_expression: None,
            expression_attribute_names: HashMap::new(),
            expression_attribute_values: HashMap::new(),
        })
        .await;
    client.shutdown()?;
    result?;
    if deleted?.attributes.is_none() {
        return Err("DeleteItem did not return ALL_OLD attributes".into());
    }
    println!("FFI direct operations passed: PutItem GetItem UpdateItem Query Scan DeleteItem");
    Ok(())
}

fn string(value: impl Into<String>) -> DaxAttributeValue {
    DaxAttributeValue::String {
        string: value.into(),
    }
}

fn item<const N: usize>(
    values: [(&str, DaxAttributeValue); N],
) -> HashMap<String, DaxAttributeValue> {
    values
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}
