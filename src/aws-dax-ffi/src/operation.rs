use std::collections::HashMap;

use aws_sdk_dynamodb::{
    operation::{
        batch_get_item::BatchGetItemInput, batch_write_item::BatchWriteItemInput,
        delete_item::DeleteItemInput, get_item::GetItemInput, put_item::PutItemInput,
        query::QueryInput, scan::ScanInput, transact_get_items::TransactGetItemsInput,
        transact_write_items::TransactWriteItemsInput, update_item::UpdateItemInput,
    },
    types::{
        Capacity, ConditionCheck, ConsumedCapacity, Delete, DeleteRequest, Get,
        ItemCollectionMetrics, KeysAndAttributes, Put, PutRequest, ReturnConsumedCapacity,
        ReturnItemCollectionMetrics, ReturnValue, ReturnValuesOnConditionCheckFailure, Select,
        TransactGetItem, TransactWriteItem, Update, WriteRequest,
    },
};

use crate::{
    DaxBatchGetItemRequest, DaxBatchGetItemResponse, DaxBatchWriteAction, DaxBatchWriteItemRequest,
    DaxBatchWriteItemResponse, DaxCapacity, DaxClient, DaxConsumedCapacity, DaxDeleteItemRequest,
    DaxDeleteItemResponse, DaxFfiError, DaxGetItemRequest, DaxGetItemResponse,
    DaxItemCollectionMetrics, DaxKeysAndAttributes, DaxPutItemRequest, DaxPutItemResponse,
    DaxQueryRequest, DaxQueryResponse, DaxReturnConsumedCapacity, DaxReturnItemCollectionMetrics,
    DaxReturnValue, DaxReturnValuesOnConditionCheckFailure, DaxScanRequest, DaxScanResponse,
    DaxSelect, DaxTransactGetAction, DaxTransactGetItemsRequest, DaxTransactGetItemsResponse,
    DaxTransactGetResponse, DaxTransactWriteAction, DaxTransactWriteItemsRequest,
    DaxTransactWriteItemsResponse, DaxUpdateItemRequest, DaxUpdateItemResponse, runtime,
    value::{ValueConversionError, from_sdk_item, into_sdk_item},
};

#[uniffi::export]
impl DaxClient {
    /// Executes GetItem through the Rust DAX SDK.
    #[allow(
        clippy::result_large_err,
        reason = "UniFFI rich errors cross the ABI by value and cannot be boxed"
    )]
    pub async fn get_item(
        &self,
        request: DaxGetItemRequest,
    ) -> Result<DaxGetItemResponse, DaxFfiError> {
        let client = self.inner.clone();
        runtime::run(async move {
            let input = GetItemInput::builder()
                .table_name(request.table_name)
                .set_key(Some(into_sdk_item(request.key)?))
                .set_consistent_read(request.consistent_read)
                .set_projection_expression(request.projection_expression)
                .set_expression_attribute_names(nonempty(request.expression_attribute_names))
                .build()
                .map_err(validation)?;
            let output = client.get_item().input(input).send().await?;
            Ok(DaxGetItemResponse {
                item: output.item.map(from_sdk_item).transpose()?,
            })
        })
        .await
    }

    /// Executes PutItem through the Rust DAX SDK.
    #[allow(
        clippy::result_large_err,
        reason = "UniFFI rich errors cross the ABI by value and cannot be boxed"
    )]
    pub async fn put_item(
        &self,
        request: DaxPutItemRequest,
    ) -> Result<DaxPutItemResponse, DaxFfiError> {
        let client = self.inner.clone();
        runtime::run(async move {
            let input = PutItemInput::builder()
                .table_name(request.table_name)
                .set_item(Some(into_sdk_item(request.item)?))
                .set_return_values(request.return_values.map(return_value))
                .set_condition_expression(request.condition_expression)
                .set_expression_attribute_names(nonempty(request.expression_attribute_names))
                .set_expression_attribute_values(nonempty_sdk(request.expression_attribute_values)?)
                .build()
                .map_err(validation)?;
            let output = client.put_item().input(input).send().await?;
            Ok(DaxPutItemResponse {
                attributes: output.attributes.map(from_sdk_item).transpose()?,
            })
        })
        .await
    }

    /// Executes DeleteItem through the Rust DAX SDK.
    #[allow(
        clippy::result_large_err,
        reason = "UniFFI rich errors cross the ABI by value and cannot be boxed"
    )]
    pub async fn delete_item(
        &self,
        request: DaxDeleteItemRequest,
    ) -> Result<DaxDeleteItemResponse, DaxFfiError> {
        let client = self.inner.clone();
        runtime::run(async move {
            let input = DeleteItemInput::builder()
                .table_name(request.table_name)
                .set_key(Some(into_sdk_item(request.key)?))
                .set_return_values(request.return_values.map(return_value))
                .set_condition_expression(request.condition_expression)
                .set_expression_attribute_names(nonempty(request.expression_attribute_names))
                .set_expression_attribute_values(nonempty_sdk(request.expression_attribute_values)?)
                .build()
                .map_err(validation)?;
            let output = client.delete_item().input(input).send().await?;
            Ok(DaxDeleteItemResponse {
                attributes: output.attributes.map(from_sdk_item).transpose()?,
            })
        })
        .await
    }

    /// Executes UpdateItem through the Rust DAX SDK.
    #[allow(
        clippy::result_large_err,
        reason = "UniFFI rich errors cross the ABI by value and cannot be boxed"
    )]
    pub async fn update_item(
        &self,
        request: DaxUpdateItemRequest,
    ) -> Result<DaxUpdateItemResponse, DaxFfiError> {
        let client = self.inner.clone();
        runtime::run(async move {
            let input = UpdateItemInput::builder()
                .table_name(request.table_name)
                .set_key(Some(into_sdk_item(request.key)?))
                .update_expression(request.update_expression)
                .set_condition_expression(request.condition_expression)
                .set_expression_attribute_names(nonempty(request.expression_attribute_names))
                .set_expression_attribute_values(nonempty_sdk(request.expression_attribute_values)?)
                .set_return_values(request.return_values.map(return_value))
                .build()
                .map_err(validation)?;
            let output = client.update_item().input(input).send().await?;
            Ok(DaxUpdateItemResponse {
                attributes: output.attributes.map(from_sdk_item).transpose()?,
            })
        })
        .await
    }

    /// Executes Query through the Rust DAX SDK.
    #[allow(
        clippy::result_large_err,
        reason = "UniFFI rich errors cross the ABI by value and cannot be boxed"
    )]
    pub async fn query(&self, request: DaxQueryRequest) -> Result<DaxQueryResponse, DaxFfiError> {
        let client = self.inner.clone();
        runtime::run(async move {
            let input = query_input(request)?;
            let output = client.query().input(input).send().await?;
            query_response(output).map_err(DaxFfiError::from)
        })
        .await
    }

    /// Executes Scan through the Rust DAX SDK.
    #[allow(
        clippy::result_large_err,
        reason = "UniFFI rich errors cross the ABI by value and cannot be boxed"
    )]
    pub async fn scan(&self, request: DaxScanRequest) -> Result<DaxScanResponse, DaxFfiError> {
        let client = self.inner.clone();
        runtime::run(async move {
            let input = scan_input(request)?;
            let output = client.scan().input(input).send().await?;
            scan_response(output).map_err(DaxFfiError::from)
        })
        .await
    }

    /// Executes BatchGetItem through the Rust DAX SDK.
    #[allow(
        clippy::result_large_err,
        reason = "UniFFI rich errors cross the ABI by value and cannot be boxed"
    )]
    pub async fn batch_get_item(
        &self,
        request: DaxBatchGetItemRequest,
    ) -> Result<DaxBatchGetItemResponse, DaxFfiError> {
        let client = self.inner.clone();
        runtime::run(async move {
            let input = batch_get_item_input(request).map_err(DaxFfiError::from)?;
            let output = client.batch_get_item().input(input).send().await?;
            batch_get_item_response(output).map_err(DaxFfiError::from)
        })
        .await
    }

    /// Executes BatchWriteItem through the Rust DAX SDK.
    #[allow(
        clippy::result_large_err,
        reason = "UniFFI rich errors cross the ABI by value and cannot be boxed"
    )]
    pub async fn batch_write_item(
        &self,
        request: DaxBatchWriteItemRequest,
    ) -> Result<DaxBatchWriteItemResponse, DaxFfiError> {
        let client = self.inner.clone();
        runtime::run(async move {
            let input = batch_write_item_input(request).map_err(DaxFfiError::from)?;
            let output = client.batch_write_item().input(input).send().await?;
            batch_write_item_response(output).map_err(DaxFfiError::from)
        })
        .await
    }

    /// Executes TransactGetItems through the Rust DAX SDK.
    #[allow(
        clippy::result_large_err,
        reason = "UniFFI rich errors cross the ABI by value and cannot be boxed"
    )]
    pub async fn transact_get_items(
        &self,
        request: DaxTransactGetItemsRequest,
    ) -> Result<DaxTransactGetItemsResponse, DaxFfiError> {
        let client = self.inner.clone();
        runtime::run(async move {
            let input = transact_get_items_input(request).map_err(DaxFfiError::from)?;
            let output = client.transact_get_items().input(input).send().await?;
            transact_get_items_response(output).map_err(DaxFfiError::from)
        })
        .await
    }

    /// Executes TransactWriteItems through the Rust DAX SDK.
    #[allow(
        clippy::result_large_err,
        reason = "UniFFI rich errors cross the ABI by value and cannot be boxed"
    )]
    pub async fn transact_write_items(
        &self,
        request: DaxTransactWriteItemsRequest,
    ) -> Result<DaxTransactWriteItemsResponse, DaxFfiError> {
        let client = self.inner.clone();
        runtime::run(async move {
            let input = transact_write_items_input(request).map_err(DaxFfiError::from)?;
            let output = client.transact_write_items().input(input).send().await?;
            transact_write_items_response(output).map_err(DaxFfiError::from)
        })
        .await
    }
}

pub(crate) fn query_input(request: DaxQueryRequest) -> Result<QueryInput, ValueConversionError> {
    QueryInput::builder()
        .table_name(request.table_name)
        .set_index_name(request.index_name)
        .key_condition_expression(request.key_condition_expression)
        .set_filter_expression(request.filter_expression)
        .set_projection_expression(request.projection_expression)
        .set_expression_attribute_names(nonempty(request.expression_attribute_names))
        .set_expression_attribute_values(nonempty_sdk(request.expression_attribute_values)?)
        .set_consistent_read(request.consistent_read)
        .set_limit(request.limit)
        .set_scan_index_forward(request.scan_index_forward)
        .set_select(request.select.map(select))
        .set_return_consumed_capacity(
            request
                .return_consumed_capacity
                .map(return_consumed_capacity),
        )
        .set_exclusive_start_key(request.exclusive_start_key.map(into_sdk_item).transpose()?)
        .build()
        .map_err(value_validation)
}

pub(crate) fn scan_input(request: DaxScanRequest) -> Result<ScanInput, ValueConversionError> {
    ScanInput::builder()
        .table_name(request.table_name)
        .set_index_name(request.index_name)
        .set_filter_expression(request.filter_expression)
        .set_projection_expression(request.projection_expression)
        .set_expression_attribute_names(nonempty(request.expression_attribute_names))
        .set_expression_attribute_values(nonempty_sdk(request.expression_attribute_values)?)
        .set_consistent_read(request.consistent_read)
        .set_limit(request.limit)
        .set_select(request.select.map(select))
        .set_return_consumed_capacity(
            request
                .return_consumed_capacity
                .map(return_consumed_capacity),
        )
        .set_exclusive_start_key(request.exclusive_start_key.map(into_sdk_item).transpose()?)
        .build()
        .map_err(value_validation)
}

pub(crate) fn query_response(
    output: aws_sdk_dynamodb::operation::query::QueryOutput,
) -> Result<DaxQueryResponse, ValueConversionError> {
    Ok(DaxQueryResponse {
        items: output
            .items
            .unwrap_or_default()
            .into_iter()
            .map(from_sdk_item)
            .collect::<Result<_, _>>()?,
        count: output.count,
        scanned_count: output.scanned_count,
        last_evaluated_key: output.last_evaluated_key.map(from_sdk_item).transpose()?,
        consumed_capacity: output.consumed_capacity.map(consumed_capacity),
    })
}

pub(crate) fn scan_response(
    output: aws_sdk_dynamodb::operation::scan::ScanOutput,
) -> Result<DaxScanResponse, ValueConversionError> {
    Ok(DaxScanResponse {
        items: output
            .items
            .unwrap_or_default()
            .into_iter()
            .map(from_sdk_item)
            .collect::<Result<_, _>>()?,
        count: output.count,
        scanned_count: output.scanned_count,
        last_evaluated_key: output.last_evaluated_key.map(from_sdk_item).transpose()?,
        consumed_capacity: output.consumed_capacity.map(consumed_capacity),
    })
}

pub(crate) fn batch_get_item_input(
    request: DaxBatchGetItemRequest,
) -> Result<BatchGetItemInput, ValueConversionError> {
    let request_items = request
        .request_items
        .into_iter()
        .map(|(table, value)| Ok((table, into_keys_and_attributes(value)?)))
        .collect::<Result<_, ValueConversionError>>()?;
    BatchGetItemInput::builder()
        .set_request_items(Some(request_items))
        .set_return_consumed_capacity(
            request
                .return_consumed_capacity
                .map(return_consumed_capacity),
        )
        .build()
        .map_err(value_validation)
}

pub(crate) fn batch_get_item_response(
    output: aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemOutput,
) -> Result<DaxBatchGetItemResponse, ValueConversionError> {
    Ok(DaxBatchGetItemResponse {
        responses: output
            .responses
            .unwrap_or_default()
            .into_iter()
            .map(|(table, items)| {
                Ok((
                    table,
                    items
                        .into_iter()
                        .map(from_sdk_item)
                        .collect::<Result<_, _>>()?,
                ))
            })
            .collect::<Result<_, ValueConversionError>>()?,
        unprocessed_keys: output
            .unprocessed_keys
            .unwrap_or_default()
            .into_iter()
            .map(|(table, value)| Ok((table, from_keys_and_attributes(value)?)))
            .collect::<Result<_, ValueConversionError>>()?,
        consumed_capacity: output
            .consumed_capacity
            .unwrap_or_default()
            .into_iter()
            .map(consumed_capacity)
            .collect(),
    })
}

fn batch_write_item_input(
    request: DaxBatchWriteItemRequest,
) -> Result<BatchWriteItemInput, ValueConversionError> {
    let request_items = request
        .request_items
        .into_iter()
        .map(|(table, actions)| {
            Ok((
                table,
                actions
                    .into_iter()
                    .map(into_batch_write_action)
                    .collect::<Result<_, _>>()?,
            ))
        })
        .collect::<Result<_, ValueConversionError>>()?;
    BatchWriteItemInput::builder()
        .set_request_items(Some(request_items))
        .set_return_consumed_capacity(
            request
                .return_consumed_capacity
                .map(return_consumed_capacity),
        )
        .set_return_item_collection_metrics(
            request
                .return_item_collection_metrics
                .map(return_item_collection_metrics),
        )
        .build()
        .map_err(value_validation)
}

fn batch_write_item_response(
    output: aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemOutput,
) -> Result<DaxBatchWriteItemResponse, ValueConversionError> {
    Ok(DaxBatchWriteItemResponse {
        unprocessed_items: output
            .unprocessed_items
            .unwrap_or_default()
            .into_iter()
            .map(|(table, actions)| {
                Ok((
                    table,
                    actions
                        .into_iter()
                        .map(from_batch_write_action)
                        .collect::<Result<_, _>>()?,
                ))
            })
            .collect::<Result<_, ValueConversionError>>()?,
        consumed_capacity: output
            .consumed_capacity
            .unwrap_or_default()
            .into_iter()
            .map(consumed_capacity)
            .collect(),
        item_collection_metrics: from_item_collection_metrics_map(
            output.item_collection_metrics.unwrap_or_default(),
        )?,
    })
}

fn transact_get_items_input(
    request: DaxTransactGetItemsRequest,
) -> Result<TransactGetItemsInput, ValueConversionError> {
    let transact_items = request
        .transact_items
        .into_iter()
        .map(into_transact_get_action)
        .collect::<Result<_, _>>()?;
    TransactGetItemsInput::builder()
        .set_transact_items(Some(transact_items))
        .set_return_consumed_capacity(
            request
                .return_consumed_capacity
                .map(return_consumed_capacity),
        )
        .build()
        .map_err(value_validation)
}

fn transact_get_items_response(
    output: aws_sdk_dynamodb::operation::transact_get_items::TransactGetItemsOutput,
) -> Result<DaxTransactGetItemsResponse, ValueConversionError> {
    Ok(DaxTransactGetItemsResponse {
        responses: output
            .responses
            .unwrap_or_default()
            .into_iter()
            .map(|response| {
                Ok(DaxTransactGetResponse {
                    item: response.item.map(from_sdk_item).transpose()?,
                })
            })
            .collect::<Result<_, ValueConversionError>>()?,
        consumed_capacity: output
            .consumed_capacity
            .unwrap_or_default()
            .into_iter()
            .map(consumed_capacity)
            .collect(),
    })
}

fn transact_write_items_input(
    request: DaxTransactWriteItemsRequest,
) -> Result<TransactWriteItemsInput, ValueConversionError> {
    let transact_items = request
        .transact_items
        .into_iter()
        .map(into_transact_write_action)
        .collect::<Result<_, _>>()?;
    TransactWriteItemsInput::builder()
        .set_transact_items(Some(transact_items))
        .set_client_request_token(request.client_request_token)
        .set_return_consumed_capacity(
            request
                .return_consumed_capacity
                .map(return_consumed_capacity),
        )
        .set_return_item_collection_metrics(
            request
                .return_item_collection_metrics
                .map(return_item_collection_metrics),
        )
        .build()
        .map_err(value_validation)
}

fn transact_write_items_response(
    output: aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsOutput,
) -> Result<DaxTransactWriteItemsResponse, ValueConversionError> {
    Ok(DaxTransactWriteItemsResponse {
        consumed_capacity: output
            .consumed_capacity
            .unwrap_or_default()
            .into_iter()
            .map(consumed_capacity)
            .collect(),
        item_collection_metrics: from_item_collection_metrics_map(
            output.item_collection_metrics.unwrap_or_default(),
        )?,
    })
}

fn into_keys_and_attributes(
    value: DaxKeysAndAttributes,
) -> Result<KeysAndAttributes, ValueConversionError> {
    let keys = value
        .keys
        .into_iter()
        .map(into_sdk_item)
        .collect::<Result<_, _>>()?;
    Ok(KeysAndAttributes::builder()
        .set_keys(Some(keys))
        .set_consistent_read(value.consistent_read)
        .set_projection_expression(value.projection_expression)
        .set_expression_attribute_names(nonempty(value.expression_attribute_names))
        .build()
        .expect("KeysAndAttributes builder has no fallible required fields"))
}

fn from_keys_and_attributes(
    value: KeysAndAttributes,
) -> Result<DaxKeysAndAttributes, ValueConversionError> {
    Ok(DaxKeysAndAttributes {
        keys: value
            .keys
            .into_iter()
            .map(from_sdk_item)
            .collect::<Result<_, _>>()?,
        consistent_read: value.consistent_read,
        projection_expression: value.projection_expression,
        expression_attribute_names: value.expression_attribute_names.unwrap_or_default(),
    })
}

fn into_batch_write_action(
    action: DaxBatchWriteAction,
) -> Result<WriteRequest, ValueConversionError> {
    Ok(match action {
        DaxBatchWriteAction::Put { item } => WriteRequest::builder()
            .put_request(
                PutRequest::builder()
                    .set_item(Some(into_sdk_item(item)?))
                    .build()
                    .expect("PutRequest item is provided"),
            )
            .build(),
        DaxBatchWriteAction::Delete { key } => WriteRequest::builder()
            .delete_request(
                DeleteRequest::builder()
                    .set_key(Some(into_sdk_item(key)?))
                    .build()
                    .expect("DeleteRequest key is provided"),
            )
            .build(),
    })
}

fn from_batch_write_action(
    action: WriteRequest,
) -> Result<DaxBatchWriteAction, ValueConversionError> {
    match (action.put_request, action.delete_request) {
        (Some(put), None) => Ok(DaxBatchWriteAction::Put {
            item: from_sdk_item(put.item)?,
        }),
        (None, Some(delete)) => Ok(DaxBatchWriteAction::Delete {
            key: from_sdk_item(delete.key)?,
        }),
        _ => Err(ValueConversionError(
            "SDK BatchWriteItem response action must contain exactly one write request".into(),
        )),
    }
}

fn into_transact_get_action(
    action: DaxTransactGetAction,
) -> Result<TransactGetItem, ValueConversionError> {
    let get = Get::builder()
        .table_name(action.table_name)
        .set_key(Some(into_sdk_item(action.key)?))
        .set_projection_expression(action.projection_expression)
        .set_expression_attribute_names(nonempty(action.expression_attribute_names))
        .build()
        .expect("TransactGet action table and key are provided");
    Ok(TransactGetItem::builder().get(get).build())
}

fn into_transact_write_action(
    action: DaxTransactWriteAction,
) -> Result<TransactWriteItem, ValueConversionError> {
    Ok(match action {
        DaxTransactWriteAction::ConditionCheck {
            table_name,
            key,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values_on_condition_check_failure,
        } => TransactWriteItem::builder()
            .condition_check(
                ConditionCheck::builder()
                    .table_name(table_name)
                    .set_key(Some(into_sdk_item(key)?))
                    .condition_expression(condition_expression)
                    .set_expression_attribute_names(nonempty(expression_attribute_names))
                    .set_expression_attribute_values(nonempty_sdk(expression_attribute_values)?)
                    .set_return_values_on_condition_check_failure(
                        return_values_on_condition_check_failure
                            .map(sdk_return_values_on_condition_check_failure),
                    )
                    .build()
                    .expect("condition-check table, key, and expression are provided"),
            )
            .build(),
        DaxTransactWriteAction::Put {
            table_name,
            item,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values_on_condition_check_failure,
        } => TransactWriteItem::builder()
            .put(
                Put::builder()
                    .table_name(table_name)
                    .set_item(Some(into_sdk_item(item)?))
                    .set_condition_expression(condition_expression)
                    .set_expression_attribute_names(nonempty(expression_attribute_names))
                    .set_expression_attribute_values(nonempty_sdk(expression_attribute_values)?)
                    .set_return_values_on_condition_check_failure(
                        return_values_on_condition_check_failure
                            .map(sdk_return_values_on_condition_check_failure),
                    )
                    .build()
                    .expect("transaction put table and item are provided"),
            )
            .build(),
        DaxTransactWriteAction::Delete {
            table_name,
            key,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values_on_condition_check_failure,
        } => TransactWriteItem::builder()
            .delete(
                Delete::builder()
                    .table_name(table_name)
                    .set_key(Some(into_sdk_item(key)?))
                    .set_condition_expression(condition_expression)
                    .set_expression_attribute_names(nonempty(expression_attribute_names))
                    .set_expression_attribute_values(nonempty_sdk(expression_attribute_values)?)
                    .set_return_values_on_condition_check_failure(
                        return_values_on_condition_check_failure
                            .map(sdk_return_values_on_condition_check_failure),
                    )
                    .build()
                    .expect("transaction delete table and key are provided"),
            )
            .build(),
        DaxTransactWriteAction::Update {
            table_name,
            key,
            update_expression,
            condition_expression,
            expression_attribute_names,
            expression_attribute_values,
            return_values_on_condition_check_failure,
        } => TransactWriteItem::builder()
            .update(
                Update::builder()
                    .table_name(table_name)
                    .set_key(Some(into_sdk_item(key)?))
                    .update_expression(update_expression)
                    .set_condition_expression(condition_expression)
                    .set_expression_attribute_names(nonempty(expression_attribute_names))
                    .set_expression_attribute_values(nonempty_sdk(expression_attribute_values)?)
                    .set_return_values_on_condition_check_failure(
                        return_values_on_condition_check_failure
                            .map(sdk_return_values_on_condition_check_failure),
                    )
                    .build()
                    .expect("transaction update table, key, and expression are provided"),
            )
            .build(),
    })
}

fn from_item_collection_metrics_map(
    values: HashMap<String, Vec<ItemCollectionMetrics>>,
) -> Result<HashMap<String, Vec<DaxItemCollectionMetrics>>, ValueConversionError> {
    values
        .into_iter()
        .map(|(table, metrics)| {
            Ok((
                table,
                metrics
                    .into_iter()
                    .map(from_item_collection_metrics)
                    .collect::<Result<_, _>>()?,
            ))
        })
        .collect()
}

fn from_item_collection_metrics(
    value: ItemCollectionMetrics,
) -> Result<DaxItemCollectionMetrics, ValueConversionError> {
    let item_collection_key = value.item_collection_key.ok_or_else(|| {
        ValueConversionError("SDK item collection metrics omitted ItemCollectionKey".into())
    })?;
    Ok(DaxItemCollectionMetrics {
        item_collection_key: from_sdk_item(item_collection_key)?,
        size_estimate_range_gb: value.size_estimate_range_gb.unwrap_or_default(),
    })
}

fn nonempty<K, V>(values: HashMap<K, V>) -> Option<HashMap<K, V>> {
    (!values.is_empty()).then_some(values)
}

fn nonempty_sdk(
    values: HashMap<String, crate::DaxAttributeValue>,
) -> Result<Option<HashMap<String, aws_sdk_dynamodb::types::AttributeValue>>, ValueConversionError>
{
    if values.is_empty() {
        Ok(None)
    } else {
        Ok(Some(into_sdk_item(values)?))
    }
}

fn return_value(value: DaxReturnValue) -> ReturnValue {
    match value {
        DaxReturnValue::None => ReturnValue::None,
        DaxReturnValue::AllOld => ReturnValue::AllOld,
    }
}

fn return_consumed_capacity(value: DaxReturnConsumedCapacity) -> ReturnConsumedCapacity {
    match value {
        DaxReturnConsumedCapacity::None => ReturnConsumedCapacity::None,
        DaxReturnConsumedCapacity::Total => ReturnConsumedCapacity::Total,
        DaxReturnConsumedCapacity::Indexes => ReturnConsumedCapacity::Indexes,
    }
}

fn return_item_collection_metrics(
    value: DaxReturnItemCollectionMetrics,
) -> ReturnItemCollectionMetrics {
    match value {
        DaxReturnItemCollectionMetrics::None => ReturnItemCollectionMetrics::None,
        DaxReturnItemCollectionMetrics::Size => ReturnItemCollectionMetrics::Size,
    }
}

fn sdk_return_values_on_condition_check_failure(
    value: DaxReturnValuesOnConditionCheckFailure,
) -> ReturnValuesOnConditionCheckFailure {
    match value {
        DaxReturnValuesOnConditionCheckFailure::None => ReturnValuesOnConditionCheckFailure::None,
        DaxReturnValuesOnConditionCheckFailure::AllOld => {
            ReturnValuesOnConditionCheckFailure::AllOld
        }
    }
}

fn select(value: DaxSelect) -> Select {
    match value {
        DaxSelect::AllAttributes => Select::AllAttributes,
        DaxSelect::AllProjectedAttributes => Select::AllProjectedAttributes,
        DaxSelect::SpecificAttributes => Select::SpecificAttributes,
        DaxSelect::Count => Select::Count,
    }
}

fn consumed_capacity(value: ConsumedCapacity) -> DaxConsumedCapacity {
    DaxConsumedCapacity {
        table_name: value.table_name().map(ToOwned::to_owned),
        capacity_units: value.capacity_units(),
        read_capacity_units: value.read_capacity_units(),
        write_capacity_units: value.write_capacity_units(),
        table: value.table().map(capacity),
        local_secondary_indexes: value
            .local_secondary_indexes()
            .map(capacity_map)
            .unwrap_or_default(),
        global_secondary_indexes: value
            .global_secondary_indexes()
            .map(capacity_map)
            .unwrap_or_default(),
    }
}

fn capacity_map(values: &HashMap<String, Capacity>) -> HashMap<String, DaxCapacity> {
    values
        .iter()
        .map(|(name, value)| (name.clone(), capacity(value)))
        .collect()
}

fn capacity(value: &Capacity) -> DaxCapacity {
    DaxCapacity {
        capacity_units: value.capacity_units(),
        read_capacity_units: value.read_capacity_units(),
        write_capacity_units: value.write_capacity_units(),
    }
}

fn validation(error: impl std::fmt::Display) -> DaxFfiError {
    DaxFfiError::validation(error.to_string())
}

fn value_validation(error: impl std::fmt::Display) -> ValueConversionError {
    ValueConversionError(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DaxAttributeValue, DaxClientConfig, DaxErrorCategory, create_dax_client};

    fn string(value: &str) -> DaxAttributeValue {
        DaxAttributeValue::String {
            string: value.to_owned(),
        }
    }

    fn item(name: &str, value: &str) -> HashMap<String, DaxAttributeValue> {
        HashMap::from([(name.to_owned(), string(value))])
    }

    async fn closed_client() -> std::sync::Arc<DaxClient> {
        let client = create_dax_client(DaxClientConfig {
            endpoints: vec!["dax://cluster.example.com:8111".to_owned()],
            region: Some("us-east-1".to_owned()),
            ..DaxClientConfig::default()
        })
        .await
        .unwrap();
        client.shutdown().unwrap();
        client
    }

    fn assert_closed(error: DaxFfiError) {
        assert_eq!(error.details().category, DaxErrorCategory::Closed);
    }

    #[tokio::test]
    async fn direct_requests_map_all_fields_before_sdk_dispatch() {
        let client = closed_client().await;
        let key = item("PK", "key");

        assert_closed(
            client
                .get_item(DaxGetItemRequest {
                    table_name: "table".into(),
                    key: key.clone(),
                    consistent_read: Some(true),
                    projection_expression: Some("#pk".into()),
                    expression_attribute_names: HashMap::from([("#pk".into(), "PK".into())]),
                })
                .await
                .unwrap_err(),
        );
        assert_closed(
            client
                .put_item(DaxPutItemRequest {
                    table_name: "table".into(),
                    item: key.clone(),
                    return_values: Some(DaxReturnValue::AllOld),
                    condition_expression: Some("attribute_not_exists(PK)".into()),
                    expression_attribute_names: HashMap::new(),
                    expression_attribute_values: HashMap::new(),
                })
                .await
                .unwrap_err(),
        );
        assert_closed(
            client
                .delete_item(DaxDeleteItemRequest {
                    table_name: "table".into(),
                    key: key.clone(),
                    return_values: Some(DaxReturnValue::AllOld),
                    condition_expression: Some("attribute_exists(PK)".into()),
                    expression_attribute_names: HashMap::new(),
                    expression_attribute_values: HashMap::new(),
                })
                .await
                .unwrap_err(),
        );
        assert_closed(
            client
                .update_item(DaxUpdateItemRequest {
                    table_name: "table".into(),
                    key: key.clone(),
                    update_expression: "SET #value = :value".into(),
                    condition_expression: Some("attribute_exists(PK)".into()),
                    expression_attribute_names: HashMap::from([("#value".into(), "value".into())]),
                    expression_attribute_values: item(":value", "updated"),
                    return_values: Some(DaxReturnValue::AllOld),
                })
                .await
                .unwrap_err(),
        );
        assert_closed(
            client
                .query(DaxQueryRequest {
                    table_name: "table".into(),
                    index_name: Some("index".into()),
                    key_condition_expression: "PK = :pk".into(),
                    filter_expression: Some("attribute_exists(SK)".into()),
                    projection_expression: Some("PK, SK".into()),
                    expression_attribute_names: HashMap::new(),
                    expression_attribute_values: item(":pk", "key"),
                    consistent_read: Some(false),
                    limit: Some(1),
                    scan_index_forward: Some(false),
                    select: Some(DaxSelect::AllAttributes),
                    return_consumed_capacity: Some(DaxReturnConsumedCapacity::Total),
                    exclusive_start_key: Some(key.clone()),
                })
                .await
                .unwrap_err(),
        );
        assert_closed(
            client
                .scan(DaxScanRequest {
                    table_name: "table".into(),
                    index_name: Some("index".into()),
                    filter_expression: Some("PK = :pk".into()),
                    projection_expression: Some("PK, SK".into()),
                    expression_attribute_names: HashMap::new(),
                    expression_attribute_values: item(":pk", "key"),
                    consistent_read: Some(false),
                    limit: Some(1),
                    select: Some(DaxSelect::AllAttributes),
                    return_consumed_capacity: Some(DaxReturnConsumedCapacity::Total),
                    exclusive_start_key: Some(key),
                })
                .await
                .unwrap_err(),
        );
    }

    #[test]
    fn phase_five_requests_map_every_field_and_action() {
        let key = item("PK", "key");
        let batch_get = batch_get_item_input(DaxBatchGetItemRequest {
            request_items: HashMap::from([(
                "table".into(),
                DaxKeysAndAttributes {
                    keys: vec![key.clone()],
                    consistent_read: Some(true),
                    projection_expression: Some("#pk".into()),
                    expression_attribute_names: HashMap::from([("#pk".into(), "PK".into())]),
                },
            )]),
            return_consumed_capacity: Some(DaxReturnConsumedCapacity::Indexes),
        })
        .unwrap();
        let batch_get_table = &batch_get.request_items().unwrap()["table"];
        assert_eq!(batch_get_table.keys().len(), 1);
        assert_eq!(batch_get_table.consistent_read(), Some(true));
        assert_eq!(batch_get_table.projection_expression(), Some("#pk"));
        assert_eq!(
            batch_get_table.expression_attribute_names().unwrap()["#pk"],
            "PK"
        );
        assert_eq!(
            batch_get.return_consumed_capacity(),
            Some(&ReturnConsumedCapacity::Indexes)
        );

        let batch_write = batch_write_item_input(DaxBatchWriteItemRequest {
            request_items: HashMap::from([(
                "table".into(),
                vec![
                    DaxBatchWriteAction::Put { item: key.clone() },
                    DaxBatchWriteAction::Delete { key: key.clone() },
                ],
            )]),
            return_consumed_capacity: Some(DaxReturnConsumedCapacity::Total),
            return_item_collection_metrics: Some(DaxReturnItemCollectionMetrics::Size),
        })
        .unwrap();
        let writes = &batch_write.request_items().unwrap()["table"];
        assert!(writes[0].put_request().is_some());
        assert!(writes[1].delete_request().is_some());
        assert_eq!(
            batch_write.return_consumed_capacity(),
            Some(&ReturnConsumedCapacity::Total)
        );
        assert_eq!(
            batch_write.return_item_collection_metrics(),
            Some(&ReturnItemCollectionMetrics::Size)
        );

        let transact_get = transact_get_items_input(DaxTransactGetItemsRequest {
            transact_items: vec![DaxTransactGetAction {
                table_name: "table".into(),
                key: key.clone(),
                projection_expression: Some("#pk".into()),
                expression_attribute_names: HashMap::from([("#pk".into(), "PK".into())]),
            }],
            return_consumed_capacity: Some(DaxReturnConsumedCapacity::Total),
        })
        .unwrap();
        let get = transact_get.transact_items()[0].get().unwrap();
        assert_eq!(get.table_name(), "table");
        assert_eq!(get.projection_expression(), Some("#pk"));
        assert_eq!(get.expression_attribute_names().unwrap()["#pk"], "PK");

        let failure_values = Some(DaxReturnValuesOnConditionCheckFailure::AllOld);
        let names = HashMap::from([("#value".into(), "value".into())]);
        let values = item(":value", "next");
        let transact_write = transact_write_items_input(DaxTransactWriteItemsRequest {
            transact_items: vec![
                DaxTransactWriteAction::ConditionCheck {
                    table_name: "table".into(),
                    key: key.clone(),
                    condition_expression: "#value = :value".into(),
                    expression_attribute_names: names.clone(),
                    expression_attribute_values: values.clone(),
                    return_values_on_condition_check_failure: failure_values,
                },
                DaxTransactWriteAction::Put {
                    table_name: "table".into(),
                    item: key.clone(),
                    condition_expression: Some("attribute_not_exists(PK)".into()),
                    expression_attribute_names: HashMap::new(),
                    expression_attribute_values: HashMap::new(),
                    return_values_on_condition_check_failure: failure_values,
                },
                DaxTransactWriteAction::Delete {
                    table_name: "table".into(),
                    key: key.clone(),
                    condition_expression: Some("attribute_exists(PK)".into()),
                    expression_attribute_names: HashMap::new(),
                    expression_attribute_values: HashMap::new(),
                    return_values_on_condition_check_failure: failure_values,
                },
                DaxTransactWriteAction::Update {
                    table_name: "table".into(),
                    key,
                    update_expression: "SET #value = :value".into(),
                    condition_expression: Some("attribute_exists(PK)".into()),
                    expression_attribute_names: names,
                    expression_attribute_values: values,
                    return_values_on_condition_check_failure: failure_values,
                },
            ],
            client_request_token: Some("token".into()),
            return_consumed_capacity: Some(DaxReturnConsumedCapacity::Indexes),
            return_item_collection_metrics: Some(DaxReturnItemCollectionMetrics::Size),
        })
        .unwrap();
        assert_eq!(transact_write.client_request_token(), Some("token"));
        assert!(
            transact_write.transact_items()[0]
                .condition_check()
                .is_some()
        );
        assert!(transact_write.transact_items()[1].put().is_some());
        assert!(transact_write.transact_items()[2].delete().is_some());
        let update = transact_write.transact_items()[3].update().unwrap();
        assert_eq!(update.update_expression(), "SET #value = :value");
        assert_eq!(
            update.return_values_on_condition_check_failure(),
            Some(&ReturnValuesOnConditionCheckFailure::AllOld)
        );
    }

    #[test]
    fn phase_five_response_fixtures_preserve_nested_outputs() {
        use aws_sdk_dynamodb::types::{AttributeValue, ItemResponse};

        let sdk_item = HashMap::from([("PK".into(), AttributeValue::S("key".into()))]);
        let capacity = ConsumedCapacity::builder()
            .table_name("table")
            .capacity_units(1.5)
            .build();
        let keys = KeysAndAttributes::builder()
            .set_keys(Some(vec![sdk_item.clone()]))
            .set_consistent_read(Some(true))
            .build()
            .unwrap();
        let batch_get = batch_get_item_response(
            aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemOutput::builder()
                .responses("table", vec![sdk_item.clone()])
                .unprocessed_keys("table", keys)
                .consumed_capacity(capacity.clone())
                .build(),
        )
        .unwrap();
        assert_eq!(batch_get.responses["table"][0], item("PK", "key"));
        assert!(batch_get.unprocessed_keys["table"].consistent_read.unwrap());
        assert_eq!(batch_get.consumed_capacity[0].capacity_units, Some(1.5));

        let metric = ItemCollectionMetrics::builder()
            .set_item_collection_key(Some(sdk_item.clone()))
            .set_size_estimate_range_gb(Some(vec![0.25, 0.5]))
            .build();
        let unprocessed = WriteRequest::builder()
            .delete_request(
                DeleteRequest::builder()
                    .set_key(Some(sdk_item.clone()))
                    .build()
                    .unwrap(),
            )
            .build();
        let batch_write = batch_write_item_response(
            aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemOutput::builder()
                .unprocessed_items("table", vec![unprocessed])
                .consumed_capacity(capacity.clone())
                .item_collection_metrics("table", vec![metric.clone()])
                .build(),
        )
        .unwrap();
        assert!(matches!(
            batch_write.unprocessed_items["table"][0],
            DaxBatchWriteAction::Delete { .. }
        ));
        assert_eq!(
            batch_write.item_collection_metrics["table"][0].size_estimate_range_gb,
            vec![0.25, 0.5]
        );

        let transact_get = transact_get_items_response(
            aws_sdk_dynamodb::operation::transact_get_items::TransactGetItemsOutput::builder()
                .responses(ItemResponse::builder().set_item(Some(sdk_item)).build())
                .responses(ItemResponse::builder().build())
                .consumed_capacity(capacity.clone())
                .build(),
        )
        .unwrap();
        assert_eq!(transact_get.responses[0].item, Some(item("PK", "key")));
        assert_eq!(transact_get.responses[1].item, None);

        let transact_write = transact_write_items_response(
            aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsOutput::builder()
                .consumed_capacity(capacity)
                .item_collection_metrics("table", vec![metric])
                .build(),
        )
        .unwrap();
        assert_eq!(
            transact_write.consumed_capacity[0].table_name.as_deref(),
            Some("table")
        );
        assert_eq!(
            transact_write.item_collection_metrics["table"][0].item_collection_key,
            item("PK", "key")
        );
    }

    #[tokio::test]
    async fn phase_five_methods_convert_before_closed_client_dispatch() {
        let client = closed_client().await;
        let key = item("PK", "key");

        assert_closed(
            client
                .batch_get_item(DaxBatchGetItemRequest {
                    request_items: HashMap::from([(
                        "table".into(),
                        DaxKeysAndAttributes {
                            keys: vec![key.clone()],
                            consistent_read: None,
                            projection_expression: None,
                            expression_attribute_names: HashMap::new(),
                        },
                    )]),
                    return_consumed_capacity: None,
                })
                .await
                .unwrap_err(),
        );
        assert_closed(
            client
                .batch_write_item(DaxBatchWriteItemRequest {
                    request_items: HashMap::from([(
                        "table".into(),
                        vec![DaxBatchWriteAction::Put { item: key.clone() }],
                    )]),
                    return_consumed_capacity: None,
                    return_item_collection_metrics: None,
                })
                .await
                .unwrap_err(),
        );
        assert_closed(
            client
                .transact_get_items(DaxTransactGetItemsRequest {
                    transact_items: vec![DaxTransactGetAction {
                        table_name: "table".into(),
                        key: key.clone(),
                        projection_expression: None,
                        expression_attribute_names: HashMap::new(),
                    }],
                    return_consumed_capacity: None,
                })
                .await
                .unwrap_err(),
        );
        assert_closed(
            client
                .transact_write_items(DaxTransactWriteItemsRequest {
                    transact_items: vec![DaxTransactWriteAction::Delete {
                        table_name: "table".into(),
                        key,
                        condition_expression: None,
                        expression_attribute_names: HashMap::new(),
                        expression_attribute_values: HashMap::new(),
                        return_values_on_condition_check_failure: None,
                    }],
                    client_request_token: Some("token".into()),
                    return_consumed_capacity: None,
                    return_item_collection_metrics: None,
                })
                .await
                .unwrap_err(),
        );
    }

    #[tokio::test]
    async fn rejects_invalid_values_before_dispatch() {
        let error = closed_client()
            .await
            .get_item(DaxGetItemRequest {
                table_name: "table".into(),
                key: HashMap::from([(
                    "PK".into(),
                    DaxAttributeValue::Number {
                        number: "NaN".into(),
                    },
                )]),
                consistent_read: None,
                projection_expression: None,
                expression_attribute_names: HashMap::new(),
            })
            .await
            .unwrap_err();
        assert_eq!(error.details().category, DaxErrorCategory::Validation);
    }

    #[tokio::test]
    async fn phase_five_rejects_invalid_nested_values_before_dispatch() {
        let error = closed_client()
            .await
            .transact_write_items(DaxTransactWriteItemsRequest {
                transact_items: vec![DaxTransactWriteAction::Put {
                    table_name: "table".into(),
                    item: HashMap::from([(
                        "PK".into(),
                        DaxAttributeValue::Number {
                            number: "NaN".into(),
                        },
                    )]),
                    condition_expression: None,
                    expression_attribute_names: HashMap::new(),
                    expression_attribute_values: HashMap::new(),
                    return_values_on_condition_check_failure: None,
                }],
                client_request_token: None,
                return_consumed_capacity: None,
                return_item_collection_metrics: None,
            })
            .await
            .unwrap_err();
        assert_eq!(error.details().category, DaxErrorCategory::Validation);
    }
}
