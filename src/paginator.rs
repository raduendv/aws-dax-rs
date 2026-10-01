//! AWS SDK-style paginators for operations with a table-key continuation token.

use std::collections::HashMap;

use aws_sdk_dynamodb::{
    operation::{
        batch_get_item::{BatchGetItemInput, BatchGetItemOutput},
        query::{QueryInput, QueryOutput},
        scan::{ScanInput, ScanOutput},
    },
    types::{AttributeValue, KeysAndAttributes},
};

use crate::{Client, Error};

/// Lazily fetches Query pages until DAX returns no continuation key.
#[derive(Debug)]
#[must_use = "paginators do nothing until next_page is awaited"]
pub struct QueryPaginator {
    client: Client,
    input: QueryInput,
    next_token: Option<HashMap<String, AttributeValue>>,
    first_page: bool,
    finished: bool,
}

impl QueryPaginator {
    pub(crate) fn new(client: Client, input: QueryInput) -> Self {
        let next_token = input.exclusive_start_key().cloned();
        Self {
            client,
            input,
            next_token,
            first_page: true,
            finished: false,
        }
    }

    /// Returns whether another page may be requested.
    pub fn has_more_pages(&self) -> bool {
        !self.finished && (self.first_page || self.next_token.is_some())
    }

    /// Fetches the next page, returning `None` after pagination is exhausted.
    pub async fn next_page(&mut self) -> Result<Option<QueryOutput>, Error> {
        if !self.has_more_pages() {
            return Ok(None);
        }
        let mut input = self.input.clone();
        input.exclusive_start_key = self.next_token.clone();
        let previous_token = self.next_token.clone();
        let output = self.client.query().input(input).send().await?;
        self.first_page = false;
        self.next_token = output.last_evaluated_key().cloned();
        self.finished = self.next_token.is_none() || self.next_token == previous_token;
        Ok(Some(output))
    }
}

/// Lazily fetches Scan pages until DAX returns no continuation key.
#[derive(Debug)]
#[must_use = "paginators do nothing until next_page is awaited"]
pub struct ScanPaginator {
    client: Client,
    input: ScanInput,
    next_token: Option<HashMap<String, AttributeValue>>,
    first_page: bool,
    finished: bool,
}

/// Lazily retries BatchGet unprocessed keys until DAX returns none.
#[derive(Debug)]
#[must_use = "paginators do nothing until next_page is awaited"]
pub struct BatchGetItemPaginator {
    client: Client,
    input: BatchGetItemInput,
    request_items: Option<HashMap<String, KeysAndAttributes>>,
    first_page: bool,
    stop_on_duplicate_token: bool,
    finished: bool,
}

impl BatchGetItemPaginator {
    pub(crate) fn new(client: Client, input: BatchGetItemInput) -> Self {
        Self {
            request_items: input.request_items().cloned(),
            client,
            input,
            first_page: true,
            stop_on_duplicate_token: false,
            finished: false,
        }
    }

    /// Enables or disables stopping when DAX repeats the unprocessed-key token.
    pub fn stop_on_duplicate_token(mut self, enabled: bool) -> Self {
        self.stop_on_duplicate_token = enabled;
        self
    }

    /// Returns whether another page may be requested.
    pub fn has_more_pages(&self) -> bool {
        !self.finished && (self.first_page || self.request_items.is_some())
    }

    /// Fetches the next page, returning `None` after all keys are processed.
    pub async fn next_page(&mut self) -> Result<Option<BatchGetItemOutput>, Error> {
        if !self.has_more_pages() {
            return Ok(None);
        }
        let previous = self.request_items.clone();
        let mut input = self.input.clone();
        input.request_items = self.request_items.clone();
        let output = self.client.batch_get_item().input(input).send().await?;
        self.first_page = false;
        (self.request_items, self.finished) =
            next_batch_get_state(previous.as_ref(), &output, self.stop_on_duplicate_token);
        Ok(Some(output))
    }
}

fn next_batch_get_state(
    previous: Option<&HashMap<String, KeysAndAttributes>>,
    output: &BatchGetItemOutput,
    stop_on_duplicate_token: bool,
) -> (Option<HashMap<String, KeysAndAttributes>>, bool) {
    let next = output
        .unprocessed_keys()
        .filter(|items| !items.is_empty())
        .cloned();
    let duplicate = stop_on_duplicate_token && previous.is_some() && previous == next.as_ref();
    let finished = next.is_none() || duplicate;
    (next, finished)
}

impl ScanPaginator {
    pub(crate) fn new(client: Client, input: ScanInput) -> Self {
        let next_token = input.exclusive_start_key().cloned();
        Self {
            client,
            input,
            next_token,
            first_page: true,
            finished: false,
        }
    }

    /// Returns whether another page may be requested.
    pub fn has_more_pages(&self) -> bool {
        !self.finished && (self.first_page || self.next_token.is_some())
    }

    /// Fetches the next page, returning `None` after pagination is exhausted.
    pub async fn next_page(&mut self) -> Result<Option<ScanOutput>, Error> {
        if !self.has_more_pages() {
            return Ok(None);
        }
        let mut input = self.input.clone();
        input.exclusive_start_key = self.next_token.clone();
        let previous_token = self.next_token.clone();
        let output = self.client.scan().input(input).send().await?;
        self.first_page = false;
        self.next_token = output.last_evaluated_key().cloned();
        self.finished = self.next_token.is_none() || self.next_token == previous_token;
        Ok(Some(output))
    }
}

#[cfg(test)]
mod tests {
    use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};

    use super::*;
    use crate::{Client, Config};

    fn client() -> Client {
        Client::new(
            Config::builder()
                .endpoint("dax://cluster.example.com")
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                    "key", "secret", None, None, "test",
                )))
                .build()
                .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn query_starts_with_one_page_even_without_a_token() {
        let input = QueryInput::builder().table_name("Table").build().unwrap();
        let paginator = client().query_paginator(input);
        assert!(paginator.has_more_pages());
    }

    #[test]
    fn scan_preserves_an_initial_exclusive_start_key() {
        let input = ScanInput::builder()
            .table_name("Table")
            .exclusive_start_key("pk", AttributeValue::S("start".into()))
            .build()
            .unwrap();
        let paginator = client().scan_paginator(input);
        assert!(paginator.has_more_pages());
    }

    #[test]
    fn batch_get_duplicate_token_option_is_explicit() {
        let input = BatchGetItemInput::builder().build().unwrap();
        let paginator = client()
            .batch_get_item_paginator(input)
            .stop_on_duplicate_token(true);
        assert!(paginator.has_more_pages());
    }

    #[test]
    fn batch_get_empty_unprocessed_keys_finish_pagination() {
        let output = BatchGetItemOutput::builder()
            .set_unprocessed_keys(Some(HashMap::new()))
            .build();
        let (next, finished) = next_batch_get_state(None, &output, false);
        assert!(next.is_none());
        assert!(finished);
    }

    #[test]
    fn batch_get_preserves_unprocessed_keys_until_the_next_page() {
        let keys = KeysAndAttributes::builder()
            .set_keys(Some(vec![HashMap::from([(
                "pk".to_owned(),
                AttributeValue::S("v".into()),
            )])]))
            .build()
            .unwrap();
        let pending = HashMap::from([("Table".to_owned(), keys)]);
        let output = BatchGetItemOutput::builder()
            .set_unprocessed_keys(Some(pending.clone()))
            .build();
        let (next, finished) = next_batch_get_state(None, &output, false);
        assert_eq!(next, Some(pending));
        assert!(!finished);
    }

    #[test]
    fn batch_get_duplicate_tokens_stop_only_when_enabled() {
        let keys = KeysAndAttributes::builder()
            .set_keys(Some(vec![HashMap::from([(
                "pk".to_owned(),
                AttributeValue::S("v".into()),
            )])]))
            .build()
            .unwrap();
        let pending = HashMap::from([("Table".to_owned(), keys)]);
        let output = BatchGetItemOutput::builder()
            .set_unprocessed_keys(Some(pending.clone()))
            .build();
        let (_, continues) = next_batch_get_state(Some(&pending), &output, false);
        assert!(!continues);
        let (_, finished) = next_batch_get_state(Some(&pending), &output, true);
        assert!(finished);
    }

    #[test]
    fn batch_get_duplicate_detection_compares_keys_and_request_metadata() {
        let key = HashMap::from([("pk".to_owned(), AttributeValue::S("v".into()))]);
        let previous_keys = KeysAndAttributes::builder()
            .set_keys(Some(vec![key.clone()]))
            .consistent_read(false)
            .projection_expression("pk")
            .build()
            .unwrap();
        let next_keys = KeysAndAttributes::builder()
            .set_keys(Some(vec![key]))
            .consistent_read(true)
            .projection_expression("status")
            .build()
            .unwrap();
        let previous = HashMap::from([("Table".to_owned(), previous_keys)]);
        let output = BatchGetItemOutput::builder()
            .set_unprocessed_keys(Some(HashMap::from([("Table".to_owned(), next_keys)])))
            .build();

        let (next, finished) = next_batch_get_state(Some(&previous), &output, true);

        assert!(next.is_some());
        assert!(!finished);
    }

    #[test]
    fn batch_get_empty_unprocessed_map_ends_after_a_previous_token() {
        let keys = KeysAndAttributes::builder()
            .set_keys(Some(vec![HashMap::from([(
                "pk".to_owned(),
                AttributeValue::S("v".into()),
            )])]))
            .build()
            .unwrap();
        let previous = HashMap::from([("Table".to_owned(), keys)]);
        let output = BatchGetItemOutput::builder()
            .set_unprocessed_keys(Some(HashMap::new()))
            .build();

        let (next, finished) = next_batch_get_state(Some(&previous), &output, false);

        assert!(next.is_none());
        assert!(finished);
    }

    #[test]
    fn batch_get_advances_through_distinct_unprocessed_key_pages() {
        let first_key = HashMap::from([("pk".to_owned(), AttributeValue::S("first".into()))]);
        let second_key = HashMap::from([("pk".to_owned(), AttributeValue::S("second".into()))]);
        let first_pending = HashMap::from([(
            "Table".to_owned(),
            KeysAndAttributes::builder()
                .set_keys(Some(vec![first_key]))
                .build()
                .unwrap(),
        )]);
        let second_pending = HashMap::from([(
            "Table".to_owned(),
            KeysAndAttributes::builder()
                .set_keys(Some(vec![second_key]))
                .build()
                .unwrap(),
        )]);

        let first_output = BatchGetItemOutput::builder()
            .set_unprocessed_keys(Some(first_pending.clone()))
            .build();
        let (next, finished) = next_batch_get_state(None, &first_output, true);
        assert_eq!(next, Some(first_pending.clone()));
        assert!(!finished);

        let second_output = BatchGetItemOutput::builder()
            .set_unprocessed_keys(Some(second_pending.clone()))
            .build();
        let (next, finished) = next_batch_get_state(next.as_ref(), &second_output, true);
        assert_eq!(next, Some(second_pending));
        assert!(!finished);
    }
}
