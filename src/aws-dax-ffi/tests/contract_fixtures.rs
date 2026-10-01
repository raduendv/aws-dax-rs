use std::collections::HashSet;

use serde_json::Value;

#[test]
fn attribute_value_fixture_covers_every_contract_variant() {
    let values: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/attribute_values.json")).unwrap();
    let kinds = values
        .iter()
        .map(|value| value["kind"].as_str().unwrap())
        .collect::<HashSet<_>>();

    assert_eq!(
        kinds,
        HashSet::from([
            "string",
            "number",
            "binary",
            "boolean",
            "null",
            "string_set",
            "number_set",
            "binary_set",
            "list",
            "map",
        ])
    );
    assert_eq!(values[1]["value"], "1234567890.0123456789");
}

#[test]
fn contract_fixture_covers_all_operations_and_shared_surfaces() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/contract_cases.json")).unwrap();
    let operations = ["direct_operations", "batch_operations", "transactions"]
        .into_iter()
        .flat_map(|section| fixture[section].as_array().unwrap())
        .map(|case| case["operation"].as_str().unwrap())
        .collect::<HashSet<_>>();

    assert_eq!(
        operations,
        HashSet::from([
            "get_item",
            "put_item",
            "delete_item",
            "update_item",
            "query",
            "scan",
            "batch_get_item",
            "batch_write_item",
            "transact_get_items",
            "transact_write_items",
        ])
    );
    assert!(fixture["error"].is_object());
    assert!(fixture["pagination"].is_object());
    assert!(fixture["routing"].is_object());
}

#[test]
fn phase_five_fixture_covers_unprocessed_data_metrics_tokens_and_all_actions() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/contract_cases.json")).unwrap();
    let batch_get = &fixture["batch_operations"][0];
    assert_eq!(batch_get["operation"], "batch_get_item");
    assert!(batch_get["request"]["request_items"]["Example"]["keys"].is_array());
    assert!(batch_get["response"]["responses"].is_object());
    assert!(batch_get["response"]["unprocessed_keys"].is_object());
    assert!(batch_get["response"]["consumed_capacity"].is_array());

    let batch_write = &fixture["batch_operations"][1];
    let batch_actions = batch_write["request"]["request_items"]["Example"]
        .as_array()
        .unwrap()
        .iter()
        .map(|action| action["kind"].as_str().unwrap())
        .collect::<HashSet<_>>();
    assert_eq!(batch_actions, HashSet::from(["put", "delete"]));
    assert_eq!(
        batch_write["request"]["return_item_collection_metrics"],
        "size"
    );
    assert!(batch_write["response"]["unprocessed_items"].is_object());
    assert!(batch_write["response"]["item_collection_metrics"].is_object());

    let transact_get = &fixture["transactions"][0];
    assert_eq!(transact_get["operation"], "transact_get_items");
    assert!(transact_get["response"]["responses"].is_array());
    assert!(transact_get["response"]["consumed_capacity"].is_array());

    let transact_write = &fixture["transactions"][1];
    let transaction_actions = transact_write["request"]["transact_items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|action| action["kind"].as_str().unwrap())
        .collect::<HashSet<_>>();
    assert_eq!(
        transaction_actions,
        HashSet::from(["condition_check", "put", "delete", "update"])
    );
    assert_eq!(
        transact_write["request"]["client_request_token"],
        "fixture-token"
    );
    assert_eq!(
        transact_write["request"]["return_item_collection_metrics"],
        "size"
    );
    assert!(fixture["error"]["cancellation_reasons"].is_array());
}
