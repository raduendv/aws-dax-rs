//! Cache-backed DAX control-operation resolution.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeDefinition;

use crate::Error;

use super::{
    cbor::{
        DiscoveredEndpoint, ResponseEnvelope, decode_define_attribute_list_body,
        decode_define_attribute_list_id_body, decode_define_key_schema_body, decode_endpoints_body,
        decode_response_envelope,
    },
    request::{
        encode_define_attribute_list, encode_define_attribute_list_id, encode_define_key_schema,
        encode_endpoints,
    },
    schema::SchemaRegistry,
};

/// Internal boundary used by cache-backed DAX control operations.
///
/// The future connection pool implements this interface. It owns connection
/// acquisition, authentication, retries, deadlines, and cancellation; this
/// layer owns only exact control framing, envelope handling, and schema cache
/// coordination.
pub(crate) trait ControlExecutor: Send + Sync {
    /// Executes one complete DAX control request and returns its raw response.
    async fn execute(&self, operation: &'static str, request: Vec<u8>) -> Result<Vec<u8>, Error>;
}

/// Resolves DAX protocol schema state through its control operations.
#[derive(Debug)]
pub(crate) struct ControlResolver<'a, Executor> {
    executor: &'a Executor,
    schemas: SchemaRegistry,
}

impl<'a, Executor> ControlResolver<'a, Executor>
where
    Executor: ControlExecutor,
{
    /// Creates a resolver backed by the supplied executor and bounded registry.
    pub(crate) fn new(executor: &'a Executor, schemas: SchemaRegistry) -> Self {
        Self { executor, schemas }
    }

    /// Returns the table's key schema, loading it once when absent.
    pub(crate) async fn key_schema(&self, table: &str) -> Result<Vec<AttributeDefinition>, Error> {
        self.schemas
            .load_key_schema(table, || async {
                let response = self
                    .executor
                    .execute("DefineKeySchema", encode_define_key_schema(table))
                    .await?;
                decode_success("DefineKeySchema", &response, decode_define_key_schema_body)
            })
            .await
    }

    /// Returns the DAX ID for canonical non-key attribute names.
    pub(crate) async fn attribute_list_id(&self, names: &[String]) -> Result<i64, Error> {
        self.schemas
            .load_attribute_list_id(names, || async {
                let response = self
                    .executor
                    .execute(
                        "DefineAttributeListId",
                        encode_define_attribute_list_id(names),
                    )
                    .await?;
                decode_success(
                    "DefineAttributeListId",
                    &response,
                    decode_define_attribute_list_id_body,
                )
            })
            .await
    }

    /// Returns the canonical non-key attribute names for a DAX list ID.
    pub(crate) async fn attribute_list(&self, id: i64) -> Result<Vec<String>, Error> {
        self.schemas
            .load_attribute_list(id, || async {
                let response = self
                    .executor
                    .execute("DefineAttributeList", encode_define_attribute_list(id))
                    .await?;
                decode_success(
                    "DefineAttributeList",
                    &response,
                    decode_define_attribute_list_body,
                )
            })
            .await
    }

    /// Returns a snapshot for response decoders requiring list-name mappings.
    pub(crate) async fn attribute_lists(
        &self,
        ids: &[i64],
    ) -> Result<HashMap<i64, Vec<String>>, Error> {
        let mut lists = HashMap::with_capacity(ids.len());
        for id in ids {
            lists.insert(*id, self.attribute_list(*id).await?);
        }
        Ok(lists)
    }

    /// Loads the current endpoint roster from the configured DAX seed.
    pub(crate) async fn endpoints(&self) -> Result<Vec<DiscoveredEndpoint>, Error> {
        let response = self
            .executor
            .execute("Endpoints", encode_endpoints())
            .await?;
        decode_success("Endpoints", &response, decode_endpoints_body)
    }
}

fn decode_success<Output>(
    operation: &'static str,
    response: &[u8],
    decoder: impl FnOnce(&[u8]) -> Result<Output, super::cbor::CborError>,
) -> Result<Output, Error> {
    match decode_response_envelope(response).map_err(|error| protocol_error(operation, error))? {
        ResponseEnvelope::Success(body) => {
            decoder(body).map_err(|error| protocol_error(operation, error))
        }
        ResponseEnvelope::Error(error) => Err(error.into()),
    }
}

fn protocol_error(operation: &'static str, error: super::cbor::CborError) -> Error {
    Error::Protocol {
        operation,
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    use std::{pin::pin, task::Poll};

    use tokio::{
        sync::Notify,
        time::{Duration, sleep},
    };

    use super::{ControlExecutor, ControlResolver};
    use crate::{Error, protocol::schema::SchemaRegistry};

    #[derive(Debug, Default)]
    struct RecordingExecutor {
        calls: Mutex<Vec<(&'static str, Vec<u8>)>>,
        executions: AtomicUsize,
    }

    impl ControlExecutor for RecordingExecutor {
        async fn execute(
            &self,
            operation: &'static str,
            request: Vec<u8>,
        ) -> Result<Vec<u8>, Error> {
            self.executions.fetch_add(1, Ordering::SeqCst);
            self.calls
                .lock()
                .expect("test executor lock is available")
                .push((operation, request));
            sleep(Duration::from_millis(20)).await;
            match operation {
                "DefineKeySchema" => Ok(vec![0x80, 0xa1, 0x62, b'p', b'k', 0x61, b'S']),
                "DefineAttributeListId" => Ok(vec![0x80, 0x09]),
                "DefineAttributeList" => Ok(vec![0x80, 0x82, 0x61, b'a', 0x61, b'z']),
                "Endpoints" => Ok(vec![
                    0x80, 0x81, 0xa5, 0x00, 0x01, 0x01, 0x61, b'n', 0x02, 0x44, 10, 0, 0, 1, 0x03,
                    0x18, 0x91, 0x04, 0x01,
                ]),
                _ => unreachable!("only known control operations are exercised"),
            }
        }
    }

    #[derive(Debug, Default)]
    struct FailingOnceExecutor {
        calls: AtomicUsize,
    }

    impl ControlExecutor for FailingOnceExecutor {
        async fn execute(
            &self,
            operation: &'static str,
            _request: Vec<u8>,
        ) -> Result<Vec<u8>, Error> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(Error::Protocol {
                    operation,
                    message: "synthetic transport failure".into(),
                });
            }

            Ok(vec![0x80, 0x09])
        }
    }

    #[derive(Debug, Default)]
    struct HangingExecutor {
        started: Notify,
    }

    impl ControlExecutor for HangingExecutor {
        async fn execute(
            &self,
            _operation: &'static str,
            _request: Vec<u8>,
        ) -> Result<Vec<u8>, Error> {
            self.started.notify_one();
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn encodes_control_requests_and_reuses_successful_cache_entries() {
        let executor = RecordingExecutor::default();
        let resolver = ControlResolver::new(&executor, SchemaRegistry::default());

        assert_eq!(
            resolver.attribute_list_id(&["a".into(), "z".into()]).await,
            Ok(9)
        );
        assert_eq!(
            resolver.attribute_list_id(&["a".into(), "z".into()]).await,
            Ok(9)
        );
        assert_eq!(
            resolver.attribute_list(9).await,
            Ok(vec!["a".into(), "z".into()])
        );
        assert_eq!(
            resolver.key_schema("Table").await.unwrap()[0].attribute_name(),
            "pk"
        );

        let calls = executor
            .calls
            .lock()
            .expect("test executor lock is available");
        assert_eq!(
            calls.as_slice(),
            [
                (
                    "DefineAttributeListId",
                    vec![
                        0x01, 0x3a, 0x49, 0x59, 0x27, 0xbb, 0x82, 0x61, b'a', 0x61, b'z',
                    ],
                ),
                (
                    "DefineAttributeList",
                    vec![0x01, 0x1a, 0x27, 0xf9, 0xbd, 0x71, 0x09],
                ),
                (
                    "DefineKeySchema",
                    vec![
                        0x01, 0x3a, 0x2c, 0x43, 0xe2, 0x7e, 0x45, b'T', b'a', b'b', b'l', b'e',
                    ],
                ),
            ]
        );
    }

    #[tokio::test]
    async fn resolves_endpoint_roster_through_control_executor() {
        let executor = RecordingExecutor::default();
        let resolver = ControlResolver::new(&executor, SchemaRegistry::default());

        let endpoints = resolver.endpoints().await.expect("endpoint roster");
        assert_eq!(endpoints.len(), 1);
        assert_eq!(endpoints[0].hostname, "n");
        assert_eq!(endpoints[0].port, 145);

        let calls = executor
            .calls
            .lock()
            .expect("test executor lock is available");
        assert_eq!(calls[0].0, "Endpoints");
        assert_eq!(calls[0].1, vec![0x01, 0x1a, 0x1b, 0x2b, 0xcf, 0x02]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shares_one_same_key_load_and_does_not_cache_failures() {
        let executor = Arc::new(RecordingExecutor::default());
        let schemas = SchemaRegistry::default();
        let mut workers = Vec::new();
        for _ in 0..8 {
            let executor = Arc::clone(&executor);
            let schemas = schemas.clone();
            workers.push(tokio::spawn(async move {
                ControlResolver::new(executor.as_ref(), schemas)
                    .attribute_list_id(&["a".into(), "z".into()])
                    .await
            }));
        }
        for worker in workers {
            assert_eq!(worker.await.expect("worker does not panic"), Ok(9));
        }
        assert_eq!(executor.executions.load(Ordering::SeqCst), 1);

        let failing_executor = FailingOnceExecutor::default();
        let resolver = ControlResolver::new(&failing_executor, SchemaRegistry::default());
        assert!(resolver.attribute_list_id(&["a".into()]).await.is_err());
        assert_eq!(resolver.attribute_list_id(&["a".into()]).await, Ok(9));
        assert_eq!(failing_executor.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn bypasses_transport_for_the_reserved_empty_attribute_list() {
        let executor = RecordingExecutor::default();
        let resolver = ControlResolver::new(&executor, SchemaRegistry::default());

        assert_eq!(resolver.attribute_list_id(&[]).await, Ok(1));
        assert_eq!(resolver.attribute_list(1).await, Ok(Vec::<String>::new()));
        assert_eq!(executor.executions.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wakes_same_key_followers_when_the_load_leader_is_cancelled() {
        let executor = Arc::new(HangingExecutor::default());
        let schemas = SchemaRegistry::default();
        let leader_executor = Arc::clone(&executor);
        let leader_schemas = schemas.clone();
        let leader = tokio::spawn(async move {
            ControlResolver::new(leader_executor.as_ref(), leader_schemas)
                .attribute_list_id(&["a".into()])
                .await
        });
        executor.started.notified().await;

        let follower_executor = Arc::clone(&executor);
        let follower_resolver = ControlResolver::new(follower_executor.as_ref(), schemas);
        let follower_names = vec!["a".into()];
        let mut follower = pin!(follower_resolver.attribute_list_id(&follower_names));
        assert!(matches!(
            std::future::poll_fn(|context| Poll::Ready(follower.as_mut().poll(context))).await,
            Poll::Pending
        ));
        leader.abort();

        assert_eq!(
            follower.await,
            Err(Error::Validation {
                message: "schema cache load was cancelled".into(),
            })
        );
    }
}
