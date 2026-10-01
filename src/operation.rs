//! Typed fluent operation entry points.
//!
//! These builders establish the AWS SDK for Rust model boundary. Their transport
//! implementation arrives after the DAX request/response protocol is ported.

use std::collections::HashMap;

use aws_sdk_dynamodb::{
    operation::batch_get_item::{BatchGetItemInput, BatchGetItemOutput},
    operation::batch_write_item::{BatchWriteItemInput, BatchWriteItemOutput},
    operation::delete_item::{DeleteItemInput, DeleteItemOutput},
    operation::get_item::{GetItemInput, GetItemOutput},
    operation::put_item::{PutItemInput, PutItemOutput},
    operation::query::{QueryInput, QueryOutput},
    operation::scan::{ScanInput, ScanOutput},
    operation::transact_get_items::{TransactGetItemsInput, TransactGetItemsOutput},
    operation::transact_write_items::{TransactWriteItemsInput, TransactWriteItemsOutput},
    operation::update_item::{UpdateItemInput, UpdateItemOutput},
    types::{AttributeValue, ReturnConsumedCapacity, ReturnValue, Select},
};

use crate::{
    Client, DaxErrorKind, Error,
    protocol::{
        cbor::{
            CborError, ResponseEnvelope, decode_batch_get_body, decode_batch_write_body,
            decode_get_item_body, decode_response_envelope, decode_scan_body,
            decode_transact_get_body, decode_transact_write_body,
        },
        request::{
            encode_batch_get_item, encode_batch_write_item, encode_delete_item, encode_get_item,
            encode_put_item, encode_query, encode_scan, encode_update_item,
            non_key_attribute_names, validate_query_input,
        },
    },
};

fn validate_transaction_cancellation_reasons(
    error: Error,
    operation: &'static str,
    expected_items: usize,
) -> Error {
    if let Error::Dax {
        kind: DaxErrorKind::TransactionCanceled,
        cancellation_reasons: Some(reasons),
        ..
    } = &error
        && reasons.len() != expected_items
    {
        return Error::Protocol {
            operation,
            message: format!(
                "transaction cancellation reasons count {} does not match transaction items count {}",
                reasons.len(),
                expected_items
            ),
        };
    }
    error
}

/// Builder for the DAX `PutItem` operation.
#[derive(Debug)]
#[must_use = "operation builders do nothing until sent"]
pub struct PutItemFluentBuilder {
    client: Client,
    input: Option<PutItemInput>,
}

impl PutItemFluentBuilder {
    /// Supplies the AWS SDK for Rust operation input.
    pub fn input(mut self, input: PutItemInput) -> Self {
        self.input = Some(input);
        self
    }

    /// Sets the table name.
    pub fn table_name(mut self, table_name: impl Into<String>) -> Self {
        self.put_input().table_name = Some(table_name.into());
        self
    }

    /// Adds or replaces an item attribute.
    pub fn item(mut self, name: impl Into<String>, value: AttributeValue) -> Self {
        self.put_input()
            .item
            .get_or_insert_with(HashMap::new)
            .insert(name.into(), value);
        self
    }

    /// Sets the requested return-value mode.
    pub fn return_values(mut self, return_values: ReturnValue) -> Self {
        self.put_input().return_values = Some(return_values);
        self
    }

    /// Sets a bounded DAX condition expression.
    pub fn condition_expression(mut self, expression: impl Into<String>) -> Self {
        self.put_input().condition_expression = Some(expression.into());
        self
    }

    /// Adds or replaces an expression attribute name.
    pub fn expression_attribute_name(
        mut self,
        placeholder: impl Into<String>,
        attribute_name: impl Into<String>,
    ) -> Self {
        self.put_input()
            .expression_attribute_names
            .get_or_insert_with(HashMap::new)
            .insert(placeholder.into(), attribute_name.into());
        self
    }

    /// Adds or replaces an expression attribute value.
    pub fn expression_attribute_value(
        mut self,
        placeholder: impl Into<String>,
        value: AttributeValue,
    ) -> Self {
        self.put_input()
            .expression_attribute_values
            .get_or_insert_with(HashMap::new)
            .insert(placeholder.into(), value);
        self
    }

    /// Sends the bounded DAX `PutItem` subset.
    pub async fn send(self) -> Result<PutItemOutput, Error> {
        if self.client.is_closed() {
            return Err(Error::Closed);
        }
        let input = self.input.ok_or_else(|| Error::Validation {
            message: "missing required PutItem input".into(),
        })?;
        reject_unported_put_response_fields(&input)?;
        let table = input.table_name().ok_or_else(|| Error::Validation {
            message: "missing required parameter: TableName".into(),
        })?;
        let item = input.item().ok_or_else(|| Error::Validation {
            message: "missing required parameter: Item".into(),
        })?;
        let key_schema = self.client.key_schema(table).await?;
        let attribute_list_id = self
            .client
            .attribute_list_id(&non_key_attribute_names(item, &key_schema))
            .await?;
        let request =
            encode_put_item(&input, &key_schema, attribute_list_id).map_err(put_request_error)?;
        let response = self.client.execute_protocol("PutItem", request).await?;
        let body = match decode_response_envelope(&response).map_err(put_cbor_error)? {
            ResponseEnvelope::Success(body) => body,
            ResponseEnvelope::Error(error) => return Err(error.into()),
        };
        let attributes =
            decode_put_item_with_loaded_attributes(&self.client, body, item, &key_schema).await?;
        Ok(PutItemOutput::builder().set_attributes(attributes).build())
    }

    fn put_input(&mut self) -> &mut PutItemInput {
        self.input.get_or_insert_with(empty_put_item_input)
    }
}

impl Client {
    /// Starts a typed `PutItem` operation builder.
    pub fn put_item(&self) -> PutItemFluentBuilder {
        PutItemFluentBuilder {
            client: self.clone(),
            input: None,
        }
    }
}
/// Builder for the DAX `DeleteItem` operation.
#[derive(Debug)]
#[must_use = "operation builders do nothing until sent"]
pub struct DeleteItemFluentBuilder {
    client: Client,
    input: Option<DeleteItemInput>,
}

impl DeleteItemFluentBuilder {
    /// Supplies the AWS SDK for Rust operation input.
    pub fn input(mut self, input: DeleteItemInput) -> Self {
        self.input = Some(input);
        self
    }

    /// Sets the table name.
    pub fn table_name(mut self, table_name: impl Into<String>) -> Self {
        self.delete_input().table_name = Some(table_name.into());
        self
    }

    /// Adds or replaces a key attribute.
    pub fn key(mut self, name: impl Into<String>, value: AttributeValue) -> Self {
        self.delete_input()
            .key
            .get_or_insert_with(HashMap::new)
            .insert(name.into(), value);
        self
    }

    /// Sets the requested return-value mode.
    pub fn return_values(mut self, return_values: ReturnValue) -> Self {
        self.delete_input().return_values = Some(return_values);
        self
    }

    /// Sets a bounded DAX condition expression.
    pub fn condition_expression(mut self, expression: impl Into<String>) -> Self {
        self.delete_input().condition_expression = Some(expression.into());
        self
    }

    /// Adds or replaces an expression attribute name.
    pub fn expression_attribute_name(
        mut self,
        placeholder: impl Into<String>,
        attribute_name: impl Into<String>,
    ) -> Self {
        self.delete_input()
            .expression_attribute_names
            .get_or_insert_with(HashMap::new)
            .insert(placeholder.into(), attribute_name.into());
        self
    }

    /// Adds or replaces an expression attribute value.
    pub fn expression_attribute_value(
        mut self,
        placeholder: impl Into<String>,
        value: AttributeValue,
    ) -> Self {
        self.delete_input()
            .expression_attribute_values
            .get_or_insert_with(HashMap::new)
            .insert(placeholder.into(), value);
        self
    }

    /// Sends the bounded DAX `DeleteItem` subset.
    pub async fn send(self) -> Result<DeleteItemOutput, Error> {
        if self.client.is_closed() {
            return Err(Error::Closed);
        }
        let input = self.input.ok_or_else(|| Error::Validation {
            message: "missing required DeleteItem input".into(),
        })?;
        reject_unported_delete_response_fields(&input)?;
        let table = input.table_name().ok_or_else(|| Error::Validation {
            message: "missing required parameter: TableName".into(),
        })?;
        let key = input.key().ok_or_else(|| Error::Validation {
            message: "missing required parameter: Key".into(),
        })?;
        let key_schema = self.client.key_schema(table).await?;
        let request = encode_delete_item(&input, &key_schema).map_err(delete_request_error)?;
        let response = self.client.execute_protocol("DeleteItem", request).await?;
        let body = match decode_response_envelope(&response).map_err(delete_cbor_error)? {
            ResponseEnvelope::Success(body) => body,
            ResponseEnvelope::Error(error) => return Err(error.into()),
        };
        let attributes =
            decode_delete_item_with_loaded_attributes(&self.client, body, key, &key_schema).await?;
        Ok(DeleteItemOutput::builder()
            .set_attributes(attributes)
            .build())
    }

    fn delete_input(&mut self) -> &mut DeleteItemInput {
        self.input.get_or_insert_with(empty_delete_item_input)
    }
}

impl Client {
    /// Starts a typed `DeleteItem` operation builder.
    pub fn delete_item(&self) -> DeleteItemFluentBuilder {
        DeleteItemFluentBuilder {
            client: self.clone(),
            input: None,
        }
    }
}

/// Builder for the bounded DAX `Query` subset.
#[derive(Debug)]
#[must_use = "operation builders do nothing until sent"]
pub struct QueryFluentBuilder {
    client: Client,
    input: Option<QueryInput>,
}

impl QueryFluentBuilder {
    /// Supplies the AWS SDK for Rust operation input.
    pub fn input(mut self, input: QueryInput) -> Self {
        self.input = Some(input);
        self
    }

    /// Sets the table name.
    pub fn table_name(mut self, table_name: impl Into<String>) -> Self {
        self.query_input().table_name = Some(table_name.into());
        self
    }

    /// Sets the partition-key equality and optional sort-key comparison.
    ///
    /// The current Query slice supports `attribute = :placeholder`, optionally
    /// joined with `AND attribute <|<=|=|>=|> :placeholder`,
    /// `AND begins_with(attribute, :placeholder)`, or
    /// `AND attribute BETWEEN :lower AND :upper`.
    pub fn key_condition_expression(mut self, expression: impl Into<String>) -> Self {
        self.query_input().key_condition_expression = Some(expression.into());
        self
    }

    /// Adds or replaces an expression attribute value.
    pub fn expression_attribute_value(
        mut self,
        placeholder: impl Into<String>,
        value: AttributeValue,
    ) -> Self {
        self.query_input()
            .expression_attribute_values
            .get_or_insert_with(HashMap::new)
            .insert(placeholder.into(), value);
        self
    }

    /// Adds or replaces an expression attribute-name alias.
    pub fn expression_attribute_name(
        mut self,
        alias: impl Into<String>,
        attribute_name: impl Into<String>,
    ) -> Self {
        self.query_input()
            .expression_attribute_names
            .get_or_insert_with(HashMap::new)
            .insert(alias.into(), attribute_name.into());
        self
    }

    /// Sets the bounded Query filter expression.
    pub fn filter_expression(mut self, expression: impl Into<String>) -> Self {
        self.query_input().filter_expression = Some(expression.into());
        self
    }

    /// Sets a bounded top-level projection expression.
    pub fn projection_expression(mut self, expression: impl Into<String>) -> Self {
        self.query_input().projection_expression = Some(expression.into());
        self
    }

    /// Sets whether the query is strongly consistent.
    pub fn consistent_read(mut self, consistent_read: bool) -> Self {
        self.query_input().consistent_read = Some(consistent_read);
        self
    }

    /// Limits the number of evaluated items.
    pub fn limit(mut self, limit: i32) -> Self {
        self.query_input().limit = Some(limit);
        self
    }

    /// Sets whether results are returned in ascending sort-key order.
    pub fn scan_index_forward(mut self, scan_index_forward: bool) -> Self {
        self.query_input().scan_index_forward = Some(scan_index_forward);
        self
    }

    /// Sets the requested consumed-capacity detail level.
    pub fn return_consumed_capacity(
        mut self,
        return_consumed_capacity: ReturnConsumedCapacity,
    ) -> Self {
        self.query_input().return_consumed_capacity = Some(return_consumed_capacity);
        self
    }

    /// Sets the returned attribute selection mode.
    ///
    /// The direct DAX slice supports `Select::Count`; `Select::AllAttributes`
    /// is equivalent to the default.
    pub fn select(mut self, select: Select) -> Self {
        self.query_input().select = Some(select);
        self
    }

    /// Adds or replaces an exclusive table-key pagination value.
    pub fn exclusive_start_key(mut self, name: impl Into<String>, value: AttributeValue) -> Self {
        self.query_input()
            .exclusive_start_key
            .get_or_insert_with(HashMap::new)
            .insert(name.into(), value);
        self
    }

    /// Sends the DAX Query subset with a partition-key equality condition.
    pub async fn send(self) -> Result<QueryOutput, Error> {
        if self.client.is_closed() {
            return Err(Error::Closed);
        }
        let input = self.input.ok_or_else(|| Error::Validation {
            message: "missing required Query input".into(),
        })?;
        let table = input.table_name().ok_or_else(|| Error::Validation {
            message: "missing required parameter: TableName".into(),
        })?;
        validate_query_input(&input).map_err(query_request_error)?;
        let key_schema = self.client.key_schema(table).await?;
        let request = encode_query(&input, &key_schema).map_err(query_request_error)?;
        let response = self.client.execute_protocol("Query", request).await?;
        let body = match decode_response_envelope(&response).map_err(query_cbor_error)? {
            ResponseEnvelope::Success(body) => body,
            ResponseEnvelope::Error(error) => return Err(error.into()),
        };
        let response = decode_query_with_loaded_attributes(&self.client, body, &key_schema).await?;
        Ok(QueryOutput::builder()
            .set_items(Some(response.items))
            .set_consumed_capacity(response.consumed_capacity)
            .set_count(response.count)
            .set_scanned_count(response.scanned_count)
            .set_last_evaluated_key(response.last_evaluated_key)
            .build())
    }

    fn query_input(&mut self) -> &mut QueryInput {
        self.input.get_or_insert_with(empty_query_input)
    }
}

fn empty_query_input() -> QueryInput {
    QueryInput::builder()
        .build()
        .expect("the generated Query input builder permits absent required fields")
}

impl Client {
    /// Starts a typed `Query` operation builder.
    pub fn query(&self) -> QueryFluentBuilder {
        QueryFluentBuilder {
            client: self.clone(),
            input: None,
        }
    }
}
/// Builder for the DAX `GetItem` operation.
#[derive(Debug)]
#[must_use = "operation builders do nothing until sent"]
pub struct GetItemFluentBuilder {
    client: Client,
    input: Option<GetItemInput>,
}

impl GetItemFluentBuilder {
    /// Supplies the AWS SDK for Rust operation input.
    pub fn input(mut self, input: GetItemInput) -> Self {
        self.input = Some(input);
        self
    }

    /// Sets the table name.
    pub fn table_name(mut self, table_name: impl Into<String>) -> Self {
        self.get_input().table_name = Some(table_name.into());
        self
    }

    /// Adds or replaces a key attribute.
    pub fn key(mut self, name: impl Into<String>, value: AttributeValue) -> Self {
        self.get_input()
            .key
            .get_or_insert_with(HashMap::new)
            .insert(name.into(), value);
        self
    }

    /// Sets whether the read is strongly consistent.
    pub fn consistent_read(mut self, consistent_read: bool) -> Self {
        self.get_input().consistent_read = Some(consistent_read);
        self
    }

    /// Sets a bounded projection expression.
    pub fn projection_expression(mut self, expression: impl Into<String>) -> Self {
        self.get_input().projection_expression = Some(expression.into());
        self
    }

    /// Adds or replaces an expression attribute name.
    pub fn expression_attribute_name(
        mut self,
        placeholder: impl Into<String>,
        attribute_name: impl Into<String>,
    ) -> Self {
        self.get_input()
            .expression_attribute_names
            .get_or_insert_with(HashMap::new)
            .insert(placeholder.into(), attribute_name.into());
        self
    }

    /// Sends the bounded DAX `GetItem` subset.
    pub async fn send(self) -> Result<GetItemOutput, Error> {
        if self.client.is_closed() {
            return Err(Error::Closed);
        }
        let input = self.input.ok_or_else(|| Error::Validation {
            message: "missing required GetItem input".into(),
        })?;
        reject_unported_get_response_fields(&input)?;
        let table = input.table_name().ok_or_else(|| Error::Validation {
            message: "missing required parameter: TableName".into(),
        })?;
        let key = input.key().ok_or_else(|| Error::Validation {
            message: "missing required parameter: Key".into(),
        })?;
        let key_schema = self.client.key_schema(table).await?;
        let request = encode_get_item(&input, &key_schema).map_err(request_error)?;
        let response = self.client.execute_protocol("GetItem", request).await?;
        let body = match decode_response_envelope(&response)
            .map_err(|error| cbor_error("GetItem", error))?
        {
            ResponseEnvelope::Success(body) => body,
            ResponseEnvelope::Error(error) => return Err(error.into()),
        };

        let item = decode_get_item_with_loaded_attributes(&self.client, body, key).await?;
        Ok(GetItemOutput::builder().set_item(item).build())
    }

    fn get_input(&mut self) -> &mut GetItemInput {
        self.input.get_or_insert_with(empty_get_item_input)
    }
}

fn empty_get_item_input() -> GetItemInput {
    GetItemInput::builder()
        .build()
        .expect("the generated GetItem input builder permits absent required fields")
}

fn empty_put_item_input() -> PutItemInput {
    PutItemInput::builder()
        .build()
        .expect("the generated PutItem input builder permits absent required fields")
}

fn empty_delete_item_input() -> DeleteItemInput {
    DeleteItemInput::builder()
        .build()
        .expect("the generated DeleteItem input builder permits absent required fields")
}

fn empty_scan_input() -> ScanInput {
    ScanInput::builder()
        .build()
        .expect("the generated Scan input builder permits absent required fields")
}

impl Client {
    /// Starts a typed `GetItem` operation builder.
    pub fn get_item(&self) -> GetItemFluentBuilder {
        GetItemFluentBuilder {
            client: self.clone(),
            input: None,
        }
    }
}

async fn decode_get_item_with_loaded_attributes(
    client: &Client,
    body: &[u8],
    key: &HashMap<String, AttributeValue>,
) -> Result<Option<HashMap<String, AttributeValue>>, Error> {
    match decode_get_item_body(body, key, &HashMap::new()) {
        Ok(item) => Ok(item),
        Err(CborError::UnknownAttributeListId(id)) => {
            let attributes = HashMap::from([(id, client.attribute_list(id).await?)]);
            decode_get_item_body(body, key, &attributes)
                .map_err(|error| cbor_error("GetItem", error))
        }
        Err(error) => Err(cbor_error("GetItem", error)),
    }
}

fn cbor_error(operation: &'static str, error: CborError) -> Error {
    Error::Protocol {
        operation,
        message: error.to_string(),
    }
}

fn request_error(error: crate::protocol::request::RequestError) -> Error {
    Error::Validation {
        message: format!("invalid GetItem request: {error:?}"),
    }
}

fn reject_unported_get_response_fields(input: &GetItemInput) -> Result<(), Error> {
    if input
        .return_consumed_capacity()
        .is_some_and(|value| value.as_str() != "NONE")
    {
        return Err(unported_response_field("ReturnConsumedCapacity"));
    }
    Ok(())
}

fn reject_unported_put_response_fields(input: &PutItemInput) -> Result<(), Error> {
    if input
        .return_consumed_capacity()
        .is_some_and(|value| value.as_str() != "NONE")
    {
        return Err(unported_response_field("ReturnConsumedCapacity"));
    }
    if input
        .return_item_collection_metrics()
        .is_some_and(|value| value.as_str() != "NONE")
    {
        return Err(unported_response_field("ReturnItemCollectionMetrics"));
    }
    if input
        .return_values()
        .is_some_and(|value| !matches!(value.as_str(), "NONE" | "ALL_OLD"))
    {
        return Err(unported_response_field("ReturnValues"));
    }
    Ok(())
}

fn reject_unported_delete_response_fields(input: &DeleteItemInput) -> Result<(), Error> {
    if input
        .return_consumed_capacity()
        .is_some_and(|value| value.as_str() != "NONE")
    {
        return Err(unported_response_field("ReturnConsumedCapacity"));
    }
    if input
        .return_item_collection_metrics()
        .is_some_and(|value| value.as_str() != "NONE")
    {
        return Err(unported_response_field("ReturnItemCollectionMetrics"));
    }
    if input
        .return_values()
        .is_some_and(|value| !matches!(value.as_str(), "NONE" | "ALL_OLD"))
    {
        return Err(unported_response_field("ReturnValues"));
    }
    Ok(())
}

fn reject_unported_update_response_fields(input: &UpdateItemInput) -> Result<(), Error> {
    if input
        .return_consumed_capacity()
        .is_some_and(|value| value.as_str() != "NONE")
    {
        return Err(unported_response_field("ReturnConsumedCapacity"));
    }
    if input
        .return_item_collection_metrics()
        .is_some_and(|value| value.as_str() != "NONE")
    {
        return Err(unported_response_field("ReturnItemCollectionMetrics"));
    }
    if input
        .return_values()
        .is_some_and(|value| !matches!(value.as_str(), "NONE" | "ALL_OLD"))
    {
        return Err(unported_response_field("ReturnValues"));
    }
    Ok(())
}

fn unported_response_field(field: &'static str) -> Error {
    Error::Validation {
        message: format!("unsupported response field for this DAX slice: {field}"),
    }
}

async fn decode_delete_item_with_loaded_attributes(
    client: &Client,
    body: &[u8],
    key: &HashMap<String, AttributeValue>,
    key_schema: &[aws_sdk_dynamodb::types::AttributeDefinition],
) -> Result<Option<HashMap<String, AttributeValue>>, Error> {
    match crate::protocol::cbor::decode_delete_item_body(body, key, key_schema, &HashMap::new()) {
        Ok(attributes) => Ok(attributes),
        Err(CborError::UnknownAttributeListId(id)) => {
            let lists = HashMap::from([(id, client.attribute_list(id).await?)]);
            crate::protocol::cbor::decode_delete_item_body(body, key, key_schema, &lists)
                .map_err(delete_cbor_error)
        }
        Err(error) => Err(delete_cbor_error(error)),
    }
}

fn delete_cbor_error(error: CborError) -> Error {
    Error::Protocol {
        operation: "DeleteItem",
        message: error.to_string(),
    }
}

fn delete_request_error(error: crate::protocol::request::RequestError) -> Error {
    Error::Validation {
        message: format!("invalid DeleteItem request: {error:?}"),
    }
}

async fn decode_put_item_with_loaded_attributes(
    client: &Client,
    body: &[u8],
    item: &HashMap<String, AttributeValue>,
    key_schema: &[aws_sdk_dynamodb::types::AttributeDefinition],
) -> Result<Option<HashMap<String, AttributeValue>>, Error> {
    match crate::protocol::cbor::decode_put_item_body(body, item, key_schema, &HashMap::new()) {
        Ok(attributes) => Ok(attributes),
        Err(CborError::UnknownAttributeListId(id)) => {
            let lists = HashMap::from([(id, client.attribute_list(id).await?)]);
            crate::protocol::cbor::decode_put_item_body(body, item, key_schema, &lists)
                .map_err(put_cbor_error)
        }
        Err(error) => Err(put_cbor_error(error)),
    }
}

fn put_cbor_error(error: CborError) -> Error {
    Error::Protocol {
        operation: "PutItem",
        message: error.to_string(),
    }
}

fn put_request_error(error: crate::protocol::request::RequestError) -> Error {
    Error::Validation {
        message: format!("invalid PutItem request: {error:?}"),
    }
}

async fn decode_update_item_with_loaded_attributes(
    client: &Client,
    body: &[u8],
    key: &HashMap<String, AttributeValue>,
    key_schema: &[aws_sdk_dynamodb::types::AttributeDefinition],
) -> Result<Option<HashMap<String, AttributeValue>>, Error> {
    match crate::protocol::cbor::decode_put_item_body(body, key, key_schema, &HashMap::new()) {
        Ok(attributes) => Ok(attributes),
        Err(CborError::UnknownAttributeListId(id)) => {
            let lists = HashMap::from([(id, client.attribute_list(id).await?)]);
            crate::protocol::cbor::decode_put_item_body(body, key, key_schema, &lists)
                .map_err(update_cbor_error)
        }
        Err(error) => Err(update_cbor_error(error)),
    }
}

fn update_cbor_error(error: CborError) -> Error {
    Error::Protocol {
        operation: "UpdateItem",
        message: error.to_string(),
    }
}

fn update_request_error(error: crate::protocol::request::RequestError) -> Error {
    Error::Validation {
        message: format!("invalid UpdateItem request: {error:?}"),
    }
}

async fn decode_scan_with_loaded_attributes(
    client: &Client,
    body: &[u8],
    key_schema: &[aws_sdk_dynamodb::types::AttributeDefinition],
) -> Result<crate::protocol::cbor::ScanResponse, Error> {
    let mut attribute_lists = HashMap::new();
    for _ in 0..1_000 {
        match decode_scan_body(body, key_schema, &attribute_lists) {
            Ok(response) => return Ok(response),
            Err(CborError::UnknownAttributeListId(id)) => {
                if attribute_lists.contains_key(&id) {
                    return Err(scan_cbor_error(CborError::UnknownAttributeListId(id)));
                }
                attribute_lists.insert(id, client.attribute_list(id).await?);
            }
            Err(error) => return Err(scan_cbor_error(error)),
        }
    }
    Err(Error::Protocol {
        operation: "Scan",
        message: "DAX Scan response references too many attribute-name lists".into(),
    })
}

fn scan_cbor_error(error: CborError) -> Error {
    Error::Protocol {
        operation: "Scan",
        message: error.to_string(),
    }
}

async fn decode_query_with_loaded_attributes(
    client: &Client,
    body: &[u8],
    key_schema: &[aws_sdk_dynamodb::types::AttributeDefinition],
) -> Result<crate::protocol::cbor::ScanResponse, Error> {
    let mut attribute_lists = HashMap::new();
    for _ in 0..1_000 {
        match decode_scan_body(body, key_schema, &attribute_lists) {
            Ok(response) => return Ok(response),
            Err(CborError::UnknownAttributeListId(id)) => {
                if attribute_lists.contains_key(&id) {
                    return Err(query_cbor_error(CborError::UnknownAttributeListId(id)));
                }
                attribute_lists.insert(id, client.attribute_list(id).await?);
            }
            Err(error) => return Err(query_cbor_error(error)),
        }
    }
    Err(Error::Protocol {
        operation: "Query",
        message: "DAX Query response references too many attribute-name lists".into(),
    })
}

fn query_cbor_error(error: CborError) -> Error {
    Error::Protocol {
        operation: "Query",
        message: error.to_string(),
    }
}

fn query_request_error(error: crate::protocol::request::RequestError) -> Error {
    Error::Validation {
        message: format!("invalid Query request: {error:?}"),
    }
}

fn scan_request_error(error: crate::protocol::request::RequestError) -> Error {
    Error::Validation {
        message: format!("invalid Scan request: {error:?}"),
    }
}

#[cfg(test)]
mod tests {
    use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
    use aws_sdk_dynamodb::{
        operation::{put_item::PutItemInput, query::QueryInput, scan::ScanInput},
        types::AttributeValue,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::{
        Client, cbor_error, delete_cbor_error, put_cbor_error, query_cbor_error, scan_cbor_error,
        update_cbor_error, validate_transaction_cancellation_reasons,
    };
    use crate::protocol::cbor::{CborError, DaxResponseError};
    use crate::{
        Config, DaxErrorKind, Error,
        protocol::tube::{encode_authorization, encode_tube_preamble},
    };

    #[test]
    fn rejects_transaction_cancellation_reason_count_mismatch() {
        let error = Error::from(DaxResponseError {
            code_sequence: vec![4, 37, 39, 58],
            message: "cancelled".into(),
            request_id: None,
            error_code: None,
            status_code: 400,
            cancellation_reasons: Some(
                vec![crate::protocol::cbor::CancellationReasonMetadata {
                    code: Some("ConditionalCheckFailed".into()),
                    message: None,
                    item_cbor: None,
                }]
                .into_boxed_slice(),
            ),
        });

        assert_eq!(
            validate_transaction_cancellation_reasons(error, "TransactWriteItems", 2),
            Error::Protocol {
                operation: "TransactWriteItems",
                message: "transaction cancellation reasons count 1 does not match transaction items count 2"
                    .into(),
            }
        );
    }

    #[test]
    fn labels_shared_cbor_errors_with_the_calling_operation() {
        for operation in [
            "GetItem",
            "BatchGetItem",
            "BatchWriteItem",
            "TransactGetItems",
            "TransactWriteItems",
        ] {
            assert_eq!(
                cbor_error(operation, CborError::TrailingData),
                Error::Protocol {
                    operation,
                    message: "trailing CBOR data after attribute value".into(),
                }
            );
        }
    }

    #[test]
    fn labels_operation_specific_cbor_errors_with_their_operation() {
        for (operation, map_error) in [
            ("PutItem", put_cbor_error as fn(CborError) -> Error),
            ("DeleteItem", delete_cbor_error as fn(CborError) -> Error),
            ("UpdateItem", update_cbor_error as fn(CborError) -> Error),
            ("Scan", scan_cbor_error as fn(CborError) -> Error),
            ("Query", query_cbor_error as fn(CborError) -> Error),
        ] {
            assert_eq!(
                map_error(CborError::TrailingData),
                Error::Protocol {
                    operation,
                    message: "trailing CBOR data after attribute value".into(),
                }
            );
        }
    }

    #[tokio::test]
    async fn get_item_loads_the_key_schema_then_decodes_a_null_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let credentials = Credentials::new("key", "secret", None, None, "test");
        let server_credentials = credentials.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut preamble = vec![0; encode_tube_preamble().len()];
            stream.read_exact(&mut preamble).await.unwrap();
            let authorization = encode_authorization(
                &server_credentials,
                "us-east-1",
                time::OffsetDateTime::now_utc(),
            )
            .unwrap();
            let mut received = vec![0; authorization.len()];
            stream.read_exact(&mut received).await.unwrap();

            let mut schema_request = [0; 12];
            stream.read_exact(&mut schema_request).await.unwrap();
            assert_eq!(
                schema_request,
                [
                    0x01, 0x3a, 0x2c, 0x43, 0xe2, 0x7e, 0x45, b'T', b'a', b'b', b'l', b'e'
                ]
            );
            stream
                .write_all(&[0x80, 0xa1, 0x62, b'p', b'k', 0x61, b'S'])
                .await
                .unwrap();

            let mut get_request = [0; 16];
            stream.read_exact(&mut get_request).await.unwrap();
            assert_eq!(
                get_request,
                [
                    0x01, 0x1a, 0x0f, 0xb0, 0xcc, 0x6a, 0x45, b'T', b'a', b'b', b'l', b'e', 0x41,
                    b'v', 0xbf, 0xff,
                ]
            );
            stream.write_all(&[0x80, 0xf6]).await.unwrap();
        });
        let client = Client::new(
            Config::builder()
                .endpoint(format!("dax://{address}"))
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(credentials))
                .build()
                .unwrap(),
        )
        .unwrap();
        assert!(
            client
                .get_item()
                .table_name("Table")
                .key("pk", AttributeValue::S("v".into()))
                .send()
                .await
                .unwrap()
                .item()
                .is_none()
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn get_item_maps_dax_error_envelopes_to_structured_errors() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let credentials = Credentials::new("key", "secret", None, None, "test");
        let server_credentials = credentials.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut preamble = vec![0; encode_tube_preamble().len()];
            stream.read_exact(&mut preamble).await.unwrap();
            let authorization = encode_authorization(
                &server_credentials,
                "us-east-1",
                time::OffsetDateTime::now_utc(),
            )
            .unwrap();
            let mut received = vec![0; authorization.len()];
            stream.read_exact(&mut received).await.unwrap();

            let mut schema_request = [0; 12];
            stream.read_exact(&mut schema_request).await.unwrap();
            stream
                .write_all(&[0x80, 0xa1, 0x62, b'p', b'k', 0x61, b'S'])
                .await
                .unwrap();

            let mut get_request = [0; 16];
            stream.read_exact(&mut get_request).await.unwrap();
            stream
                .write_all(&[
                    0x84, 0x04, 0x18, 0x25, 0x18, 0x27, 0x18, 0x2b, 0x64, b'n', b'o', b'p', b'e',
                    0x83, 0x62, b'r', b'1', 0x65, b'C', b'o', b'n', b'd', b'e', 0x19, 0x01, 0x90,
                ])
                .await
                .unwrap();
        });
        let client = Client::new(
            Config::builder()
                .endpoint(format!("dax://{address}"))
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(credentials))
                .build()
                .unwrap(),
        )
        .unwrap();
        let input = aws_sdk_dynamodb::operation::get_item::GetItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("v".into()))
            .build()
            .unwrap();

        assert_eq!(
            client.get_item().input(input).send().await,
            Err(Error::Dax {
                kind: DaxErrorKind::ConditionalCheckFailed,
                message: "nope".into(),
                request_id: Some("r1".into()),
                error_code: Some("Conde".into()),
                status_code: 400,
                code_sequence: vec![4, 37, 39, 43],
                cancellation_reasons: None,
            })
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_unported_response_fields_before_network_io() {
        let credentials = Credentials::new("key", "secret", None, None, "test");
        let client = Client::new(
            Config::builder()
                .endpoint("dax://127.0.0.1:1")
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(credentials))
                .build()
                .unwrap(),
        )
        .unwrap();
        let get = aws_sdk_dynamodb::operation::get_item::GetItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("v".into()))
            .return_consumed_capacity(aws_sdk_dynamodb::types::ReturnConsumedCapacity::Total)
            .build()
            .unwrap();
        let put = PutItemInput::builder()
            .table_name("Table")
            .item("pk", AttributeValue::S("v".into()))
            .return_item_collection_metrics(
                aws_sdk_dynamodb::types::ReturnItemCollectionMetrics::Size,
            )
            .build()
            .unwrap();
        let query = aws_sdk_dynamodb::operation::query::QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("begins_with(pk, :value)")
            .expression_attribute_values(":value", AttributeValue::S("v".into()))
            .build()
            .unwrap();

        assert_eq!(
            client.get_item().input(get).send().await,
            Err(Error::Validation {
                message: "unsupported response field for this DAX slice: ReturnConsumedCapacity"
                    .into(),
            })
        );
        assert_eq!(
            client.put_item().input(put).send().await,
            Err(Error::Validation {
                message:
                    "unsupported response field for this DAX slice: ReturnItemCollectionMetrics"
                        .into(),
            })
        );
        assert_eq!(
            client.query().input(query).send().await,
            Err(Error::Validation {
                message: "invalid Query request: UnsupportedExpression(\"KeyConditionExpression (the first term must use `=`)\")".into(),
            })
        );
    }

    #[tokio::test]
    async fn put_item_loads_schema_and_attribute_list_id_then_decodes_null() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let credentials = Credentials::new("key", "secret", None, None, "test");
        let server_credentials = credentials.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut preamble = vec![0; encode_tube_preamble().len()];
            stream.read_exact(&mut preamble).await.unwrap();
            let authorization = encode_authorization(
                &server_credentials,
                "us-east-1",
                time::OffsetDateTime::now_utc(),
            )
            .unwrap();
            let mut received = vec![0; authorization.len()];
            stream.read_exact(&mut received).await.unwrap();

            let mut schema_request = [0; 12];
            stream.read_exact(&mut schema_request).await.unwrap();
            assert_eq!(
                schema_request,
                [
                    0x01, 0x3a, 0x2c, 0x43, 0xe2, 0x7e, 0x45, b'T', b'a', b'b', b'l', b'e'
                ]
            );
            stream
                .write_all(&[0x80, 0xa1, 0x62, b'p', b'k', 0x61, b'S'])
                .await
                .unwrap();

            let mut list_request = [0; 9];
            stream.read_exact(&mut list_request).await.unwrap();
            assert_eq!(
                list_request,
                [0x01, 0x3a, 0x49, 0x59, 0x27, 0xbb, 0x81, 0x61, b'a']
            );
            stream.write_all(&[0x80, 0x09]).await.unwrap();

            let mut put_request = [0; 20];
            stream.read_exact(&mut put_request).await.unwrap();
            assert_eq!(
                put_request,
                [
                    0x01, 0x3a, 0x7d, 0x8e, 0x7e, 0x56, 0x45, b'T', b'a', b'b', b'l', b'e', 0x41,
                    b'v', 0x43, 0x09, 0x61, b'x', 0xbf, 0xff,
                ]
            );
            stream.write_all(&[0x80, 0xf6]).await.unwrap();
        });
        let client = Client::new(
            Config::builder()
                .endpoint(format!("dax://{address}"))
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(credentials))
                .build()
                .unwrap(),
        )
        .unwrap();
        assert!(
            client
                .put_item()
                .table_name("Table")
                .item("pk", AttributeValue::S("v".into()))
                .item("a", AttributeValue::S("x".into()))
                .send()
                .await
                .unwrap()
                .attributes()
                .is_none()
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn add_update_item_loads_schema_then_decodes_null_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let credentials = Credentials::new("key", "secret", None, None, "test");
        let server_credentials = credentials.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut preamble = vec![0; encode_tube_preamble().len()];
            stream.read_exact(&mut preamble).await.unwrap();
            let authorization = encode_authorization(
                &server_credentials,
                "us-east-1",
                time::OffsetDateTime::now_utc(),
            )
            .unwrap();
            let mut received = vec![0; authorization.len()];
            stream.read_exact(&mut received).await.unwrap();

            let mut schema_request = [0; 12];
            stream.read_exact(&mut schema_request).await.unwrap();
            stream
                .write_all(&[0x80, 0xa1, 0x62, b'p', b'k', 0x61, b'S'])
                .await
                .unwrap();

            let mut update_request = [0; 38];
            stream.read_exact(&mut update_request).await.unwrap();
            assert_eq!(
                update_request,
                [
                    0x01, 0x1a, 0x54, 0xf8, 0x9c, 0x0f, 0x45, b'T', b'a', b'b', b'l', b'e', 0x43,
                    b'k', b'e', b'y', 0xbf, 0x08, 0x52, 0x83, 0x01, 0x81, 0x83, 0x14, 0x82, 0x12,
                    0x65, b'c', b'o', b'u', b'n', b't', 0x82, 0x11, 0x00, 0x81, 0x01, 0xff,
                ]
            );
            stream.write_all(&[0x80, 0xf6]).await.unwrap();
        });
        let client = Client::new(
            Config::builder()
                .endpoint(format!("dax://{address}"))
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(credentials))
                .build()
                .unwrap(),
        )
        .unwrap();
        assert!(
            client
                .update_item()
                .table_name("Table")
                .key("pk", AttributeValue::S("key".into()))
                .update_expression("ADD count :increment")
                .expression_attribute_value(":increment", AttributeValue::N("1".into()))
                .send()
                .await
                .unwrap()
                .attributes()
                .is_none()
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn delete_update_item_loads_schema_then_decodes_null_response() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let credentials = Credentials::new("key", "secret", None, None, "test");
        let server_credentials = credentials.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut preamble = vec![0; encode_tube_preamble().len()];
            stream.read_exact(&mut preamble).await.unwrap();
            let authorization = encode_authorization(
                &server_credentials,
                "us-east-1",
                time::OffsetDateTime::now_utc(),
            )
            .unwrap();
            let mut received = vec![0; authorization.len()];
            stream.read_exact(&mut received).await.unwrap();

            let mut schema_request = [0; 12];
            stream.read_exact(&mut schema_request).await.unwrap();
            stream
                .write_all(&[0x80, 0xa1, 0x62, b'p', b'k', 0x61, b'S'])
                .await
                .unwrap();

            let mut update_request = [0; 45];
            stream.read_exact(&mut update_request).await.unwrap();
            assert_eq!(
                &update_request[..],
                &[
                    0x01, 0x1a, 0x54, 0xf8, 0x9c, 0x0f, 0x45, b'T', b'a', b'b', b'l', b'e', 0x43,
                    b'k', b'e', b'y', 0xbf, 0x08, 0x58, 0x18, 0x83, 0x01, 0x81, 0x83, 0x15, 0x82,
                    0x12, 0x64, b't', b'a', b'g', b's', 0x82, 0x11, 0x00, 0x81, 0xd9, 0x0c, 0xf9,
                    0x81, 0x63, b'o', b'l', b'd', 0xff,
                ][..]
            );
            stream.write_all(&[0x80, 0xf6]).await.unwrap();
        });
        let client = Client::new(
            Config::builder()
                .endpoint(format!("dax://{address}"))
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(credentials))
                .build()
                .unwrap(),
        )
        .unwrap();
        assert!(
            client
                .update_item()
                .table_name("Table")
                .key("pk", AttributeValue::S("key".into()))
                .update_expression("DELETE tags :tag")
                .expression_attribute_value(":tag", AttributeValue::Ss(vec!["old".into()]))
                .send()
                .await
                .unwrap()
                .attributes()
                .is_none()
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn put_item_loads_response_attribute_names_before_decoding_old_attributes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let credentials = Credentials::new("key", "secret", None, None, "test");
        let server_credentials = credentials.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut preamble = vec![0; encode_tube_preamble().len()];
            stream.read_exact(&mut preamble).await.unwrap();
            let authorization = encode_authorization(
                &server_credentials,
                "us-east-1",
                time::OffsetDateTime::now_utc(),
            )
            .unwrap();
            let mut received = vec![0; authorization.len()];
            stream.read_exact(&mut received).await.unwrap();

            let mut schema_request = [0; 12];
            stream.read_exact(&mut schema_request).await.unwrap();
            stream
                .write_all(&[0x80, 0xa1, 0x62, b'p', b'k', 0x61, b'S'])
                .await
                .unwrap();

            let mut id_request = [0; 9];
            stream.read_exact(&mut id_request).await.unwrap();
            stream.write_all(&[0x80, 0x09]).await.unwrap();

            let mut put_request = [0; 22];
            stream.read_exact(&mut put_request).await.unwrap();
            assert_eq!(
                put_request,
                [
                    0x01, 0x3a, 0x7d, 0x8e, 0x7e, 0x56, 0x45, b'T', b'a', b'b', b'l', b'e', 0x41,
                    b'v', 0x43, 0x09, 0x61, b'x', 0xbf, 0x07, 0x02, 0xff,
                ]
            );
            stream
                .write_all(&[0x80, 0xa1, 0x02, 0x45, 0x09, 0x63, b'o', b'l', b'd'])
                .await
                .unwrap();

            let mut names_request = [0; 7];
            stream.read_exact(&mut names_request).await.unwrap();
            assert_eq!(names_request, [0x01, 0x1a, 0x27, 0xf9, 0xbd, 0x71, 0x09]);
            stream.write_all(&[0x80, 0x81, 0x61, b'a']).await.unwrap();
        });
        let client = Client::new(
            Config::builder()
                .endpoint(format!("dax://{address}"))
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(credentials))
                .build()
                .unwrap(),
        )
        .unwrap();
        let input = PutItemInput::builder()
            .table_name("Table")
            .item("pk", AttributeValue::S("v".into()))
            .item("a", AttributeValue::S("x".into()))
            .return_values(aws_sdk_dynamodb::types::ReturnValue::AllOld)
            .build()
            .unwrap();

        assert_eq!(
            client
                .put_item()
                .input(input)
                .send()
                .await
                .unwrap()
                .attributes()
                .and_then(|attributes| attributes.get("a")),
            Some(&AttributeValue::S("old".into()))
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn delete_item_loads_response_attribute_names_before_decoding_old_attributes() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let credentials = Credentials::new("key", "secret", None, None, "test");
        let server_credentials = credentials.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut preamble = vec![0; encode_tube_preamble().len()];
            stream.read_exact(&mut preamble).await.unwrap();
            let authorization = encode_authorization(
                &server_credentials,
                "us-east-1",
                time::OffsetDateTime::now_utc(),
            )
            .unwrap();
            let mut received = vec![0; authorization.len()];
            stream.read_exact(&mut received).await.unwrap();

            let mut schema_request = [0; 12];
            stream.read_exact(&mut schema_request).await.unwrap();
            stream
                .write_all(&[0x80, 0xa1, 0x62, b'p', b'k', 0x61, b'S'])
                .await
                .unwrap();

            let mut delete_request = [0; 18];
            stream.read_exact(&mut delete_request).await.unwrap();
            assert_eq!(
                delete_request,
                [
                    0x01, 0x1a, 0x3c, 0x69, 0x62, 0x21, 0x45, b'T', b'a', b'b', b'l', b'e', 0x41,
                    b'v', 0xbf, 0x07, 0x02, 0xff,
                ]
            );
            stream
                .write_all(&[0x80, 0xa1, 0x02, 0x45, 0x09, 0x63, b'o', b'l', b'd'])
                .await
                .unwrap();

            let mut names_request = [0; 7];
            stream.read_exact(&mut names_request).await.unwrap();
            assert_eq!(names_request, [0x01, 0x1a, 0x27, 0xf9, 0xbd, 0x71, 0x09]);
            stream.write_all(&[0x80, 0x81, 0x61, b'a']).await.unwrap();
        });
        let client = Client::new(
            Config::builder()
                .endpoint(format!("dax://{address}"))
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(credentials))
                .build()
                .unwrap(),
        )
        .unwrap();
        let attributes = client
            .delete_item()
            .table_name("Table")
            .key("pk", AttributeValue::S("v".into()))
            .return_values(aws_sdk_dynamodb::types::ReturnValue::AllOld)
            .send()
            .await
            .unwrap()
            .attributes()
            .cloned()
            .unwrap();
        assert_eq!(attributes.get("pk"), Some(&AttributeValue::S("v".into())));
        assert_eq!(attributes.get("a"), Some(&AttributeValue::S("old".into())));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn scan_loads_schema_and_response_attribute_names_before_decoding_items() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let credentials = Credentials::new("key", "secret", None, None, "test");
        let server_credentials = credentials.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut preamble = vec![0; encode_tube_preamble().len()];
            stream.read_exact(&mut preamble).await.unwrap();
            let authorization = encode_authorization(
                &server_credentials,
                "us-east-1",
                time::OffsetDateTime::now_utc(),
            )
            .unwrap();
            let mut received = vec![0; authorization.len()];
            stream.read_exact(&mut received).await.unwrap();

            let mut schema_request = [0; 12];
            stream.read_exact(&mut schema_request).await.unwrap();
            stream
                .write_all(&[0x80, 0xa1, 0x62, b'p', b'k', 0x61, b'S'])
                .await
                .unwrap();

            let mut scan_request = [0; 23];
            stream.read_exact(&mut scan_request).await.unwrap();
            assert_eq!(
                scan_request,
                [
                    0x01, 0x3a, 0x6f, 0xc8, 0x30, 0x9b, 0x45, b'T', b'a', b'b', b'l', b'e', 0xbf,
                    0x03, 0x02, 0x02, 0x01, 0x09, 0x41, b'v', 0x0d, 0x01, 0xff,
                ]
            );
            stream
                .write_all(&[
                    0x80, 0xa4, 0x01, 0x40, 0x65, b'T', b'a', b'b', b'l', b'e', 0xfb, 0x3f, 0xf8,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf6, 0xf6, 0xf6, 0x07, 0x81, 0x82, 0x41,
                    b'v', 0x45, 0x09, 0x63, b'o', b'l', b'd', 0x08, 0x01, 0x09, 0x41, b'w',
                ])
                .await
                .unwrap();

            let mut names_request = [0; 7];
            stream.read_exact(&mut names_request).await.unwrap();
            assert_eq!(names_request, [0x01, 0x1a, 0x27, 0xf9, 0xbd, 0x71, 0x09]);
            stream.write_all(&[0x80, 0x81, 0x61, b'a']).await.unwrap();
        });
        let client = Client::new(
            Config::builder()
                .endpoint(format!("dax://{address}"))
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(credentials))
                .build()
                .unwrap(),
        )
        .unwrap();

        let output = client
            .scan()
            .table_name("Table")
            .consistent_read(true)
            .return_consumed_capacity(aws_sdk_dynamodb::types::ReturnConsumedCapacity::Indexes)
            .exclusive_start_key("pk", AttributeValue::S("v".into()))
            .limit(1)
            .send()
            .await
            .unwrap();
        assert_eq!(output.count(), 1);
        assert_eq!(output.scanned_count(), 0);
        assert_eq!(
            output
                .consumed_capacity()
                .and_then(|capacity| capacity.capacity_units()),
            Some(1.5)
        );
        assert_eq!(
            output.last_evaluated_key().and_then(|key| key.get("pk")),
            Some(&AttributeValue::S("w".into()))
        );
        let items = output.items().to_vec();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].get("pk"), Some(&AttributeValue::S("v".into())));
        assert_eq!(items[0].get("a"), Some(&AttributeValue::S("old".into())));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn scan_paginator_fetches_continuation_page_on_same_transport() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let credentials = Credentials::new("key", "secret", None, None, "test");
        let server_credentials = credentials.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut preamble = vec![0; encode_tube_preamble().len()];
            stream.read_exact(&mut preamble).await.unwrap();
            let authorization = encode_authorization(
                &server_credentials,
                "us-east-1",
                time::OffsetDateTime::now_utc(),
            )
            .unwrap();
            let mut received = vec![0; authorization.len()];
            stream.read_exact(&mut received).await.unwrap();

            let mut schema_request = [0; 12];
            stream.read_exact(&mut schema_request).await.unwrap();
            stream
                .write_all(&[0x80, 0xa1, 0x62, b'p', b'k', 0x61, b'S'])
                .await
                .unwrap();

            let mut first_request = [0; 23];
            stream.read_exact(&mut first_request).await.unwrap();
            assert_eq!(first_request[19], b'v');
            stream
                .write_all(&[
                    0x80, 0xa4, 0x01, 0x40, 0x65, b'T', b'a', b'b', b'l', b'e', 0xfb, 0x3f, 0xf8,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf6, 0xf6, 0xf6, 0x07, 0x81, 0x82, 0x41,
                    b'v', 0x45, 0x09, 0x63, b'o', b'l', b'd', 0x08, 0x01, 0x09, 0x41, b'w',
                ])
                .await
                .unwrap();

            let mut names_request = [0; 7];
            stream.read_exact(&mut names_request).await.unwrap();
            assert_eq!(names_request, [0x01, 0x1a, 0x27, 0xf9, 0xbd, 0x71, 0x09]);
            stream.write_all(&[0x80, 0x81, 0x61, b'a']).await.unwrap();

            let mut second_request = [0; 23];
            stream.read_exact(&mut second_request).await.unwrap();
            assert_eq!(second_request[19], b'w');
            stream
                .write_all(&[
                    0x80, 0xa4, 0x01, 0x40, 0x65, b'T', b'a', b'b', b'l', b'e', 0xfb, 0x3f, 0xf8,
                    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf6, 0xf6, 0xf6, 0x07, 0x81, 0x82, 0x41,
                    b'w', 0x45, 0x09, 0x63, b'o', b'l', b'd', 0x08, 0x01, 0x09, 0xf6,
                ])
                .await
                .unwrap();
        });

        let client = Client::new(
            Config::builder()
                .endpoint(format!("dax://{address}"))
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(credentials))
                .build()
                .unwrap(),
        )
        .unwrap();
        let input = ScanInput::builder()
            .table_name("Table")
            .exclusive_start_key("pk", AttributeValue::S("v".into()))
            .consistent_read(true)
            .return_consumed_capacity(aws_sdk_dynamodb::types::ReturnConsumedCapacity::Indexes)
            .limit(1)
            .build()
            .unwrap();
        let mut paginator = client.scan_paginator(input);

        let first = paginator.next_page().await.unwrap().unwrap();
        assert_eq!(
            first.last_evaluated_key().and_then(|key| key.get("pk")),
            Some(&AttributeValue::S("w".into()))
        );
        assert!(paginator.has_more_pages());

        let second = paginator.next_page().await.unwrap().unwrap();
        assert!(second.last_evaluated_key().is_none());
        assert!(!paginator.has_more_pages());
        assert!(paginator.next_page().await.unwrap().is_none());

        server.await.unwrap();
    }

    #[tokio::test]
    async fn query_loads_schema_and_response_attribute_names_before_decoding_items() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let credentials = Credentials::new("key", "secret", None, None, "test");
        let server_credentials = credentials.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut preamble = vec![0; encode_tube_preamble().len()];
            stream.read_exact(&mut preamble).await.unwrap();
            let authorization = encode_authorization(
                &server_credentials,
                "us-east-1",
                time::OffsetDateTime::now_utc(),
            )
            .unwrap();
            let mut received = vec![0; authorization.len()];
            stream.read_exact(&mut received).await.unwrap();

            let mut schema_request = [0; 12];
            stream.read_exact(&mut schema_request).await.unwrap();
            stream
                .write_all(&[0x80, 0xa1, 0x62, b'p', b'k', 0x61, b'S'])
                .await
                .unwrap();

            let mut query_request = [0; 30];
            stream.read_exact(&mut query_request).await.unwrap();
            assert_eq!(
                query_request,
                [
                    0x01, 0x3a, 0x37, 0x81, 0xc2, 0xae, 0x45, b'T', b'a', b'b', b'l', b'e', 0x4f,
                    0x83, 0x01, 0x83, 0x00, 0x82, 0x12, 0x62, b'p', b'k', 0x82, 0x11, 0x00, 0x81,
                    0x61, b'v', 0xbf, 0xff,
                ]
            );
            stream
                .write_all(&[
                    0x80, 0xa3, 0x07, 0x81, 0x82, 0x41, b'v', 0x45, 0x09, 0x63, b'o', b'l', b'd',
                    0x08, 0x01, 0x0a, 0x01,
                ])
                .await
                .unwrap();

            let mut names_request = [0; 7];
            stream.read_exact(&mut names_request).await.unwrap();
            assert_eq!(names_request, [0x01, 0x1a, 0x27, 0xf9, 0xbd, 0x71, 0x09]);
            stream.write_all(&[0x80, 0x81, 0x61, b'a']).await.unwrap();
        });
        let client = Client::new(
            Config::builder()
                .endpoint(format!("dax://{address}"))
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(credentials))
                .build()
                .unwrap(),
        )
        .unwrap();

        let output = client
            .query()
            .table_name("Table")
            .key_condition_expression("pk = :value")
            .expression_attribute_value(":value", AttributeValue::S("v".into()))
            .send()
            .await
            .unwrap();
        assert_eq!(output.count(), 1);
        assert_eq!(output.scanned_count(), 1);
        let items = output.items().to_vec();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].get("pk"), Some(&AttributeValue::S("v".into())));
        assert_eq!(items[0].get("a"), Some(&AttributeValue::S("old".into())));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn query_paginator_propagates_continuation_key_on_same_transport() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let credentials = Credentials::new("key", "secret", None, None, "test");
        let server_credentials = credentials.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut preamble = vec![0; encode_tube_preamble().len()];
            stream.read_exact(&mut preamble).await.unwrap();
            let authorization = encode_authorization(
                &server_credentials,
                "us-east-1",
                time::OffsetDateTime::now_utc(),
            )
            .unwrap();
            let mut received = vec![0; authorization.len()];
            stream.read_exact(&mut received).await.unwrap();

            let mut schema_request = [0; 12];
            stream.read_exact(&mut schema_request).await.unwrap();
            stream
                .write_all(&[0x80, 0xa1, 0x62, b'p', b'k', 0x61, b'S'])
                .await
                .unwrap();

            let mut first_request = [0; 30];
            stream.read_exact(&mut first_request).await.unwrap();
            assert_eq!(first_request[6], 0x45);
            assert_eq!(&first_request[7..12], b"Table");
            stream
                .write_all(&[
                    0x80, 0xa4, 0x07, 0x81, 0x82, 0x41, b'v', 0x45, 0x09, 0x63, b'o', b'l', b'd',
                    0x08, 0x01, 0x09, 0x41, b'w', 0x0a, 0x01,
                ])
                .await
                .unwrap();

            let mut names_request = [0; 7];
            stream.read_exact(&mut names_request).await.unwrap();
            assert_eq!(names_request, [0x01, 0x1a, 0x27, 0xf9, 0xbd, 0x71, 0x09]);
            stream.write_all(&[0x80, 0x81, 0x61, b'a']).await.unwrap();

            let mut second_request = [0; 33];
            stream.read_exact(&mut second_request).await.unwrap();
            assert_eq!(&second_request[7..12], b"Table");
            assert!(
                second_request
                    .windows(3)
                    .any(|window| window == [0x09, 0x41, b'w'])
            );
            stream
                .write_all(&[
                    0x80, 0xa3, 0x07, 0x81, 0x82, 0x41, b'w', 0x45, 0x09, 0x63, b'o', b'l', b'd',
                    0x08, 0x01, 0x09, 0xf6,
                ])
                .await
                .unwrap();
        });

        let client = Client::new(
            Config::builder()
                .endpoint(format!("dax://{address}"))
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(credentials))
                .build()
                .unwrap(),
        )
        .unwrap();
        let input = QueryInput::builder()
            .table_name("Table")
            .key_condition_expression("pk = :value")
            .expression_attribute_values(":value", AttributeValue::S("v".into()))
            .build()
            .unwrap();
        let mut paginator = client.query_paginator(input);

        let first = paginator.next_page().await.unwrap().unwrap();
        assert_eq!(
            first.last_evaluated_key().and_then(|key| key.get("pk")),
            Some(&AttributeValue::S("w".into()))
        );
        assert!(paginator.has_more_pages());

        let second = paginator.next_page().await.unwrap().unwrap();
        assert!(second.last_evaluated_key().is_none());
        assert!(!paginator.has_more_pages());
        assert!(paginator.next_page().await.unwrap().is_none());

        server.await.unwrap();
    }

    #[tokio::test]
    async fn get_item_loads_response_attribute_names_before_decoding_item() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let credentials = Credentials::new("key", "secret", None, None, "test");
        let server_credentials = credentials.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut preamble = vec![0; encode_tube_preamble().len()];
            stream.read_exact(&mut preamble).await.unwrap();
            let authorization = encode_authorization(
                &server_credentials,
                "us-east-1",
                time::OffsetDateTime::now_utc(),
            )
            .unwrap();
            let mut received = vec![0; authorization.len()];
            stream.read_exact(&mut received).await.unwrap();

            let mut schema_request = [0; 12];
            stream.read_exact(&mut schema_request).await.unwrap();
            stream
                .write_all(&[0x80, 0xa1, 0x62, b'p', b'k', 0x61, b'S'])
                .await
                .unwrap();

            let mut get_request = [0; 16];
            stream.read_exact(&mut get_request).await.unwrap();
            stream
                .write_all(&[0x80, 0xa1, 0x00, 0x45, 0x09, 0x63, b'o', b'l', b'd'])
                .await
                .unwrap();

            let mut names_request = [0; 7];
            stream.read_exact(&mut names_request).await.unwrap();
            assert_eq!(names_request, [0x01, 0x1a, 0x27, 0xf9, 0xbd, 0x71, 0x09]);
            stream.write_all(&[0x80, 0x81, 0x61, b'a']).await.unwrap();
        });
        let client = Client::new(
            Config::builder()
                .endpoint(format!("dax://{address}"))
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(credentials))
                .build()
                .unwrap(),
        )
        .unwrap();
        let input = aws_sdk_dynamodb::operation::get_item::GetItemInput::builder()
            .table_name("Table")
            .key("pk", AttributeValue::S("v".into()))
            .build()
            .unwrap();

        let item = client
            .get_item()
            .input(input)
            .send()
            .await
            .unwrap()
            .item()
            .cloned();
        assert_eq!(
            item.as_ref().and_then(|item| item.get("a")),
            Some(&AttributeValue::S("old".into()))
        );
        assert_eq!(
            item.as_ref().and_then(|item| item.get("pk")),
            Some(&AttributeValue::S("v".into()))
        );
        server.await.unwrap();
    }
}
/// Builder for the DAX `Scan` operation.
#[derive(Debug)]
#[must_use = "operation builders do nothing until sent"]
pub struct ScanFluentBuilder {
    client: Client,
    input: Option<ScanInput>,
}

impl ScanFluentBuilder {
    /// Supplies the AWS SDK for Rust operation input.
    pub fn input(mut self, input: ScanInput) -> Self {
        self.input = Some(input);
        self
    }

    /// Sets the table name.
    pub fn table_name(mut self, table_name: impl Into<String>) -> Self {
        self.scan_input().table_name = Some(table_name.into());
        self
    }

    /// Adds or replaces an exclusive table-key pagination value.
    pub fn exclusive_start_key(mut self, name: impl Into<String>, value: AttributeValue) -> Self {
        self.scan_input()
            .exclusive_start_key
            .get_or_insert_with(HashMap::new)
            .insert(name.into(), value);
        self
    }

    /// Sets whether the scan is strongly consistent.
    pub fn consistent_read(mut self, consistent_read: bool) -> Self {
        self.scan_input().consistent_read = Some(consistent_read);
        self
    }

    /// Limits the number of evaluated items.
    pub fn limit(mut self, limit: i32) -> Self {
        self.scan_input().limit = Some(limit);
        self
    }

    /// Sets the requested consumed-capacity detail level.
    pub fn return_consumed_capacity(
        mut self,
        return_consumed_capacity: ReturnConsumedCapacity,
    ) -> Self {
        self.scan_input().return_consumed_capacity = Some(return_consumed_capacity);
        self
    }

    /// Sets the returned attribute selection mode.
    ///
    /// The direct DAX slice supports `Select::Count`; `Select::AllAttributes`
    /// is equivalent to the default.
    pub fn select(mut self, select: Select) -> Self {
        self.scan_input().select = Some(select);
        self
    }

    /// Sets the bounded DAX filter expression.
    pub fn filter_expression(mut self, expression: impl Into<String>) -> Self {
        self.scan_input().filter_expression = Some(expression.into());
        self
    }

    /// Sets a bounded top-level projection expression.
    pub fn projection_expression(mut self, expression: impl Into<String>) -> Self {
        self.scan_input().projection_expression = Some(expression.into());
        self
    }

    /// Adds or replaces an expression attribute name.
    pub fn expression_attribute_name(
        mut self,
        placeholder: impl Into<String>,
        attribute_name: impl Into<String>,
    ) -> Self {
        self.scan_input()
            .expression_attribute_names
            .get_or_insert_with(HashMap::new)
            .insert(placeholder.into(), attribute_name.into());
        self
    }

    /// Adds or replaces an expression attribute value.
    pub fn expression_attribute_value(
        mut self,
        placeholder: impl Into<String>,
        value: AttributeValue,
    ) -> Self {
        self.scan_input()
            .expression_attribute_values
            .get_or_insert_with(HashMap::new)
            .insert(placeholder.into(), value);
        self
    }

    /// Sends the bounded DAX `Scan` subset with table-key pagination.
    pub async fn send(self) -> Result<ScanOutput, Error> {
        if self.client.is_closed() {
            return Err(Error::Closed);
        }
        let input = self.input.ok_or_else(|| Error::Validation {
            message: "missing required Scan input".into(),
        })?;
        let table = input.table_name().ok_or_else(|| Error::Validation {
            message: "missing required parameter: TableName".into(),
        })?;
        let key_schema = self.client.key_schema(table).await?;
        let request = encode_scan(&input, &key_schema).map_err(scan_request_error)?;
        let response = self.client.execute_protocol("Scan", request).await?;
        let body = match decode_response_envelope(&response).map_err(scan_cbor_error)? {
            ResponseEnvelope::Success(body) => body,
            ResponseEnvelope::Error(error) => return Err(error.into()),
        };
        let response = decode_scan_with_loaded_attributes(&self.client, body, &key_schema).await?;
        Ok(ScanOutput::builder()
            .set_items(Some(response.items))
            .set_consumed_capacity(response.consumed_capacity)
            .set_count(response.count)
            .set_scanned_count(response.scanned_count)
            .set_last_evaluated_key(response.last_evaluated_key)
            .build())
    }

    fn scan_input(&mut self) -> &mut ScanInput {
        self.input.get_or_insert_with(empty_scan_input)
    }
}

impl Client {
    /// Starts a typed `Scan` operation builder.
    pub fn scan(&self) -> ScanFluentBuilder {
        ScanFluentBuilder {
            client: self.clone(),
            input: None,
        }
    }
}

/// Builder for the bounded DAX `UpdateItem` subset.
#[derive(Debug)]
#[must_use = "operation builders do nothing until sent"]
pub struct UpdateItemFluentBuilder {
    client: Client,
    input: Option<UpdateItemInput>,
}

impl UpdateItemFluentBuilder {
    /// Supplies the AWS SDK for Rust operation input.
    pub fn input(mut self, input: UpdateItemInput) -> Self {
        self.input = Some(input);
        self
    }

    /// Sets the table name.
    pub fn table_name(mut self, table_name: impl Into<String>) -> Self {
        self.update_input().table_name = Some(table_name.into());
        self
    }

    /// Adds or replaces a key attribute.
    pub fn key(mut self, name: impl Into<String>, value: AttributeValue) -> Self {
        self.update_input()
            .key
            .get_or_insert_with(HashMap::new)
            .insert(name.into(), value);
        self
    }

    /// Sets the bounded update expression.
    pub fn update_expression(mut self, expression: impl Into<String>) -> Self {
        self.update_input().update_expression = Some(expression.into());
        self
    }

    /// Sets the bounded condition expression.
    pub fn condition_expression(mut self, expression: impl Into<String>) -> Self {
        self.update_input().condition_expression = Some(expression.into());
        self
    }

    /// Adds or replaces an expression attribute name.
    pub fn expression_attribute_name(
        mut self,
        placeholder: impl Into<String>,
        attribute_name: impl Into<String>,
    ) -> Self {
        self.update_input()
            .expression_attribute_names
            .get_or_insert_with(HashMap::new)
            .insert(placeholder.into(), attribute_name.into());
        self
    }

    /// Adds or replaces an expression attribute value.
    pub fn expression_attribute_value(
        mut self,
        placeholder: impl Into<String>,
        value: AttributeValue,
    ) -> Self {
        self.update_input()
            .expression_attribute_values
            .get_or_insert_with(HashMap::new)
            .insert(placeholder.into(), value);
        self
    }

    /// Sets the requested return-value mode.
    pub fn return_values(mut self, return_values: ReturnValue) -> Self {
        self.update_input().return_values = Some(return_values);
        self
    }

    /// Sends the bounded DAX `UpdateItem` subset.
    pub async fn send(self) -> Result<UpdateItemOutput, Error> {
        if self.client.is_closed() {
            return Err(Error::Closed);
        }
        let input = self.input.ok_or_else(|| Error::Validation {
            message: "missing required UpdateItem input".into(),
        })?;
        reject_unported_update_response_fields(&input)?;
        let table = input.table_name().ok_or_else(|| Error::Validation {
            message: "missing required parameter: TableName".into(),
        })?;
        let key = input.key().ok_or_else(|| Error::Validation {
            message: "missing required parameter: Key".into(),
        })?;
        if input.update_expression().is_none() {
            return Err(Error::Validation {
                message: "missing required parameter: UpdateExpression".into(),
            });
        }
        let key_schema = self.client.key_schema(table).await?;
        let request = encode_update_item(&input, &key_schema).map_err(update_request_error)?;
        let response = self.client.execute_protocol("UpdateItem", request).await?;
        let body = match decode_response_envelope(&response).map_err(update_cbor_error)? {
            ResponseEnvelope::Success(body) => body,
            ResponseEnvelope::Error(error) => return Err(error.into()),
        };
        let attributes =
            decode_update_item_with_loaded_attributes(&self.client, body, key, &key_schema).await?;
        Ok(UpdateItemOutput::builder()
            .set_attributes(attributes)
            .build())
    }

    fn update_input(&mut self) -> &mut UpdateItemInput {
        self.input.get_or_insert_with(empty_update_item_input)
    }
}

fn empty_update_item_input() -> UpdateItemInput {
    UpdateItemInput::builder()
        .build()
        .expect("the generated UpdateItem input builder permits absent required fields")
}

impl Client {
    /// Starts a typed `UpdateItem` operation builder.
    pub fn update_item(&self) -> UpdateItemFluentBuilder {
        UpdateItemFluentBuilder {
            client: self.clone(),
            input: None,
        }
    }
}

/// Builder for the DAX `BatchWriteItem` operation.
#[derive(Debug)]
#[must_use = "operation builders do nothing until sent"]
pub struct BatchWriteItemFluentBuilder {
    client: Client,
    input: Option<BatchWriteItemInput>,
}

impl BatchWriteItemFluentBuilder {
    /// Supplies the AWS SDK for Rust operation input.
    pub fn input(mut self, input: BatchWriteItemInput) -> Self {
        self.input = Some(input);
        self
    }

    /// Sends the bounded DAX `BatchWriteItem` subset.
    pub async fn send(self) -> Result<BatchWriteItemOutput, Error> {
        if self.client.is_closed() {
            return Err(Error::Closed);
        }
        let input = self.input.ok_or_else(|| Error::Validation {
            message: "missing required BatchWriteItem input".into(),
        })?;
        let request_items = input.request_items().ok_or_else(|| Error::Validation {
            message: "missing required parameter: RequestItems".into(),
        })?;
        let total_requests = request_items.values().map(Vec::len).sum::<usize>();
        if total_requests == 0
            || total_requests > 25
            || request_items
                .values()
                .any(|requests| requests.is_empty() || requests.len() > 25)
        {
            return Err(Error::Validation {
                message: "BatchWriteItem request count".into(),
            });
        }
        for requests in request_items.values() {
            for request in requests {
                match (request.put_request(), request.delete_request()) {
                    (Some(put), None) if put.item.is_empty() => {
                        return Err(Error::Validation {
                            message: "missing required parameter: Item".into(),
                        });
                    }
                    (None, Some(delete)) if delete.key.is_empty() => {
                        return Err(Error::Validation {
                            message: "missing required parameter: Key".into(),
                        });
                    }
                    (Some(_), None) | (None, Some(_)) => {}
                    _ => {
                        return Err(Error::Validation {
                            message: "BatchWriteItem write request shape".into(),
                        });
                    }
                }
            }
        }
        let mut schemas = HashMap::new();
        let mut attribute_list_ids = HashMap::new();
        let mut response_attribute_lists = HashMap::new();
        for table in request_items.keys() {
            let schema = self.client.key_schema(table).await?;
            let mut names = Vec::new();
            for request in &request_items[table] {
                if let Some(put) = request.put_request() {
                    names.extend(non_key_attribute_names(&put.item, &schema));
                }
            }
            names.sort_unstable();
            names.dedup();
            let attribute_list_id = self.client.attribute_list_id(&names).await?;
            if attribute_list_id == 1 {
                response_attribute_lists.insert(attribute_list_id, Vec::new());
            } else {
                response_attribute_lists.insert(
                    attribute_list_id,
                    self.client.attribute_list(attribute_list_id).await?,
                );
            }
            schemas.insert(table.clone(), schema);
            attribute_list_ids.insert(table.clone(), attribute_list_id);
        }
        let request = encode_batch_write_item(&input, &schemas, &attribute_list_ids)
            .map_err(request_error)?;
        let response = self
            .client
            .execute_protocol("BatchWriteItem", request)
            .await?;
        let body = match decode_response_envelope(&response)
            .map_err(|error| cbor_error("BatchWriteItem", error))?
        {
            ResponseEnvelope::Success(body) => body,
            ResponseEnvelope::Error(error) => return Err(error.into()),
        };
        let decoded = decode_batch_write_body(body, &schemas, &response_attribute_lists)
            .map_err(|error| cbor_error("BatchWriteItem", error))?;
        Ok(BatchWriteItemOutput::builder()
            .set_unprocessed_items(Some(decoded.unprocessed_items))
            .set_consumed_capacity(decoded.consumed_capacity)
            .set_item_collection_metrics(decoded.item_collection_metrics)
            .build())
    }
}

impl Client {
    /// Starts a typed `BatchWriteItem` operation builder.
    pub fn batch_write_item(&self) -> BatchWriteItemFluentBuilder {
        BatchWriteItemFluentBuilder {
            client: self.clone(),
            input: None,
        }
    }
}

/// Builder for the DAX `BatchGetItem` operation.
#[derive(Debug)]
#[must_use = "operation builders do nothing until sent"]
pub struct BatchGetItemFluentBuilder {
    client: Client,
    input: Option<BatchGetItemInput>,
}

impl BatchGetItemFluentBuilder {
    /// Supplies the AWS SDK for Rust operation input.
    pub fn input(mut self, input: BatchGetItemInput) -> Self {
        self.input = Some(input);
        self
    }

    /// Sends the bounded DAX `BatchGetItem` subset.
    pub async fn send(self) -> Result<BatchGetItemOutput, Error> {
        if self.client.is_closed() {
            return Err(Error::Closed);
        }
        let input = self.input.ok_or_else(|| Error::Validation {
            message: "missing required BatchGetItem input".into(),
        })?;
        let request_items = input.request_items().ok_or_else(|| Error::Validation {
            message: "missing required parameter: RequestItems".into(),
        })?;
        if request_items.is_empty() || request_items.values().any(|item| item.keys().is_empty()) {
            return Err(Error::Validation {
                message: "BatchGetItem request items".into(),
            });
        }
        let total_keys = request_items
            .values()
            .map(|item| item.keys().len())
            .sum::<usize>();
        if total_keys > 100 {
            return Err(Error::Validation {
                message: "BatchGetItem request count".into(),
            });
        }
        let mut schemas = HashMap::new();
        let mut attribute_lists = HashMap::new();
        for (table, keys) in request_items {
            let schema = self.client.key_schema(table).await?;
            let _ = keys;
            schemas.insert(table.clone(), schema);
        }
        let request = encode_batch_get_item(&input, &schemas).map_err(request_error)?;
        let response = self
            .client
            .execute_protocol("BatchGetItem", request)
            .await?;
        let body = match decode_response_envelope(&response)
            .map_err(|error| cbor_error("BatchGetItem", error))?
        {
            ResponseEnvelope::Success(body) => body,
            ResponseEnvelope::Error(error) => return Err(error.into()),
        };
        let decoded = loop {
            match decode_batch_get_body(body, &input, &schemas, &attribute_lists) {
                Ok(decoded) => break decoded,
                Err(crate::protocol::cbor::CborError::UnknownAttributeListId(id)) => {
                    attribute_lists.insert(id, self.client.attribute_list(id).await?);
                }
                Err(error) => return Err(cbor_error("BatchGetItem", error)),
            }
        };
        Ok(BatchGetItemOutput::builder()
            .set_responses(Some(decoded.responses))
            .set_unprocessed_keys(Some(decoded.unprocessed_keys))
            .set_consumed_capacity(decoded.consumed_capacity)
            .build())
    }
}

impl Client {
    /// Starts a typed `BatchGetItem` operation builder.
    pub fn batch_get_item(&self) -> BatchGetItemFluentBuilder {
        BatchGetItemFluentBuilder {
            client: self.clone(),
            input: None,
        }
    }
}
/// Builder for the bounded DAX `TransactWriteItems` operation.
#[derive(Debug)]
#[must_use = "operation builders do nothing until sent"]
pub struct TransactWriteItemsFluentBuilder {
    client: Client,
    input: Option<TransactWriteItemsInput>,
}

impl TransactWriteItemsFluentBuilder {
    /// Supplies the AWS SDK for Rust operation input.
    pub fn input(mut self, input: TransactWriteItemsInput) -> Self {
        self.input = Some(input);
        self
    }

    /// Sends the bounded DAX `TransactWriteItems` subset.
    pub async fn send(self) -> Result<TransactWriteItemsOutput, Error> {
        if self.client.is_closed() {
            return Err(Error::Closed);
        }
        let input = self.input.ok_or_else(|| Error::Validation {
            message: "missing required TransactWriteItems input".into(),
        })?;
        if input.transact_items().is_empty() {
            return Err(Error::Validation {
                message: "missing required parameter: TransactItems".into(),
            });
        }
        if input.transact_items().len() > 100 {
            return Err(Error::Validation {
                message: "TransactWriteItems request count".into(),
            });
        }
        let mut schemas = HashMap::new();
        let mut attribute_list_ids = HashMap::new();
        for item in input.transact_items() {
            let action_count = [
                item.condition_check().is_some(),
                item.put().is_some(),
                item.delete().is_some(),
                item.update().is_some(),
            ]
            .into_iter()
            .filter(|present| *present)
            .count();
            if action_count != 1 {
                return Err(Error::Validation {
                    message: "TransactWriteItems item must contain one action".into(),
                });
            }
            let (table, put_item) = if let Some(check) = item.condition_check() {
                if check.key.is_empty() {
                    return Err(Error::Validation {
                        message: "missing required parameter: Key".into(),
                    });
                }
                if check.table_name.is_empty() {
                    return Err(Error::Validation {
                        message: "missing required parameter: TableName".into(),
                    });
                }
                if check.condition_expression.is_empty() {
                    return Err(Error::Validation {
                        message: "missing required parameter: ConditionExpression".into(),
                    });
                }
                (check.table_name(), None)
            } else if let Some(put) = item.put() {
                if put.item.is_empty() {
                    return Err(Error::Validation {
                        message: "missing required parameter: Item".into(),
                    });
                }
                if put.table_name.is_empty() {
                    return Err(Error::Validation {
                        message: "missing required parameter: TableName".into(),
                    });
                }
                (put.table_name(), Some(put.item()))
            } else if let Some(delete) = item.delete() {
                if delete.key.is_empty() {
                    return Err(Error::Validation {
                        message: "missing required parameter: Key".into(),
                    });
                }
                if delete.table_name.is_empty() {
                    return Err(Error::Validation {
                        message: "missing required parameter: TableName".into(),
                    });
                }
                (delete.table_name(), None)
            } else if let Some(update) = item.update() {
                if update.key.is_empty() {
                    return Err(Error::Validation {
                        message: "missing required parameter: Key".into(),
                    });
                }
                if update.table_name.is_empty() {
                    return Err(Error::Validation {
                        message: "missing required parameter: TableName".into(),
                    });
                }
                if update.update_expression.is_empty() {
                    return Err(Error::Validation {
                        message: "missing required parameter: UpdateExpression".into(),
                    });
                }
                (update.table_name(), None)
            } else {
                return Err(Error::Validation {
                    message: "TransactWriteItems item must contain one action".into(),
                });
            };
            let schema = self.client.key_schema(table).await?;
            if let Some(item) = put_item {
                let names = non_key_attribute_names(item, &schema);
                attribute_list_ids.insert(
                    table.to_owned(),
                    self.client.attribute_list_id(&names).await?,
                );
            }
            schemas.insert(table.to_owned(), schema);
        }
        let request = crate::protocol::request::encode_transact_write_items(
            &input,
            &schemas,
            &attribute_list_ids,
        )
        .map_err(request_error)?;
        let response = self
            .client
            .execute_protocol("TransactWriteItems", request)
            .await?;
        let body = match decode_response_envelope(&response)
            .map_err(|error| cbor_error("TransactWriteItems", error))?
        {
            ResponseEnvelope::Success(body) => body,
            ResponseEnvelope::Error(error) => {
                return Err(validate_transaction_cancellation_reasons(
                    error.into(),
                    "TransactWriteItems",
                    input.transact_items().len(),
                ));
            }
        };
        let (consumed_capacity, item_collection_metrics) =
            decode_transact_write_body(body, &schemas)
                .map_err(|error| cbor_error("TransactWriteItems", error))?;
        Ok(TransactWriteItemsOutput::builder()
            .set_consumed_capacity(consumed_capacity)
            .set_item_collection_metrics(item_collection_metrics)
            .build())
    }
}

impl Client {
    /// Starts a typed `TransactWriteItems` operation builder.
    pub fn transact_write_items(&self) -> TransactWriteItemsFluentBuilder {
        TransactWriteItemsFluentBuilder {
            client: self.clone(),
            input: None,
        }
    }
}
/// Builder for the bounded DAX `TransactGetItems` operation.
#[derive(Debug)]
#[must_use = "operation builders do nothing until sent"]
pub struct TransactGetItemsFluentBuilder {
    client: Client,
    input: Option<TransactGetItemsInput>,
}

impl TransactGetItemsFluentBuilder {
    /// Supplies the AWS SDK for Rust operation input.
    pub fn input(mut self, input: TransactGetItemsInput) -> Self {
        self.input = Some(input);
        self
    }

    /// Sends the bounded DAX `TransactGetItems` subset.
    pub async fn send(self) -> Result<TransactGetItemsOutput, Error> {
        if self.client.is_closed() {
            return Err(Error::Closed);
        }
        let input = self.input.ok_or_else(|| Error::Validation {
            message: "missing required TransactGetItems input".into(),
        })?;
        let items = input.transact_items();
        if items.is_empty() {
            return Err(Error::Validation {
                message: "missing required parameter: TransactItems".into(),
            });
        }
        if items.len() > 100 {
            return Err(Error::Validation {
                message: "TransactGetItems request count".into(),
            });
        }
        let mut schemas = HashMap::new();
        let mut attribute_lists = HashMap::new();
        for item in items {
            let get = item.get().ok_or_else(|| Error::Validation {
                message: "TransactGetItems item must contain Get".into(),
            })?;
            if get.key.is_empty() {
                return Err(Error::Validation {
                    message: "missing required parameter: Key".into(),
                });
            }
            if get.table_name.is_empty() {
                return Err(Error::Validation {
                    message: "missing required parameter: TableName".into(),
                });
            }
            schemas
                .entry(get.table_name().to_owned())
                .or_insert(self.client.key_schema(get.table_name()).await?);
        }
        let request = crate::protocol::request::encode_transact_get_items(&input, &schemas)
            .map_err(request_error)?;
        let response = self
            .client
            .execute_protocol("TransactGetItems", request)
            .await?;
        let body = match decode_response_envelope(&response)
            .map_err(|error| cbor_error("TransactGetItems", error))?
        {
            ResponseEnvelope::Success(body) => body,
            ResponseEnvelope::Error(error) => {
                return Err(validate_transaction_cancellation_reasons(
                    error.into(),
                    "TransactGetItems",
                    items.len(),
                ));
            }
        };
        let (responses, consumed_capacity) = loop {
            match decode_transact_get_body(body, &input, &schemas, &attribute_lists) {
                Ok(decoded) => break decoded,
                Err(CborError::UnknownAttributeListId(id)) => {
                    attribute_lists.insert(id, self.client.attribute_list(id).await?);
                }
                Err(error) => return Err(cbor_error("TransactGetItems", error)),
            }
        };
        Ok(TransactGetItemsOutput::builder()
            .set_responses(Some(responses))
            .set_consumed_capacity(consumed_capacity)
            .build())
    }
}

impl Client {
    /// Starts a typed `TransactGetItems` operation builder.
    pub fn transact_get_items(&self) -> TransactGetItemsFluentBuilder {
        TransactGetItemsFluentBuilder {
            client: self.clone(),
            input: None,
        }
    }
}
