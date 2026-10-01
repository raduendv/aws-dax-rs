use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use tokio::sync::Mutex;

use crate::{
    DaxBatchGetItemRequest, DaxBatchGetItemResponse, DaxClient, DaxFfiError, DaxQueryRequest,
    DaxQueryResponse, DaxScanRequest, DaxScanResponse, operation, runtime,
};

// The atomic snapshot keeps synchronous ABI calls nonblocking while a page is
// in flight. Only the serialized SDK paginator decides whether pages remain.
#[derive(Debug)]
struct State<T> {
    paginator: Mutex<T>,
    has_more_pages: AtomicBool,
}

impl<T> State<T> {
    fn new(paginator: T) -> Arc<Self> {
        Arc::new(Self {
            paginator: Mutex::new(paginator),
            has_more_pages: AtomicBool::new(true),
        })
    }
}

macro_rules! paginator {
    ($name:ident, $response:ident, $convert:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, uniffi::Object)]
        pub struct $name {
            state: Arc<State<aws_dax::$name>>,
        }

        #[uniffi::export]
        impl $name {
            /// Returns the last completed SDK page's continuation state.
            pub fn has_more_pages(&self) -> bool {
                self.state.has_more_pages.load(Ordering::Acquire)
            }

            /// Fetches one page, serializing concurrent calls on this paginator.
            #[allow(
                clippy::result_large_err,
                reason = "UniFFI rich errors cross the ABI by value and cannot be boxed"
            )]
            pub async fn next_page(&self) -> Result<Option<$response>, DaxFfiError> {
                let state = Arc::clone(&self.state);
                runtime::run(async move {
                    let mut paginator = state.paginator.lock().await;
                    let output = paginator.next_page().await?;
                    state
                        .has_more_pages
                        .store(paginator.has_more_pages(), Ordering::Release);
                    output
                        .map(operation::$convert)
                        .transpose()
                        .map_err(DaxFfiError::from)
                })
                .await
            }
        }
    };
}

paginator!(
    QueryPaginator,
    DaxQueryResponse,
    query_response,
    "A single stateful Query traversal, retaining its SDK route affinity."
);
paginator!(
    ScanPaginator,
    DaxScanResponse,
    scan_response,
    "A single stateful Scan traversal, retaining its SDK route affinity."
);
paginator!(
    BatchGetItemPaginator,
    DaxBatchGetItemResponse,
    batch_get_item_response,
    "A single stateful traversal of BatchGetItem unprocessed keys."
);

#[uniffi::export]
impl DaxClient {
    /// Creates a lazy Query paginator without network I/O.
    #[allow(
        clippy::result_large_err,
        reason = "UniFFI errors cross the ABI by value"
    )]
    pub fn query_paginator(
        &self,
        request: DaxQueryRequest,
    ) -> Result<Arc<QueryPaginator>, DaxFfiError> {
        Ok(Arc::new(QueryPaginator {
            state: State::new(self.inner.query_paginator(operation::query_input(request)?)),
        }))
    }

    /// Creates a lazy Scan paginator without network I/O.
    #[allow(
        clippy::result_large_err,
        reason = "UniFFI errors cross the ABI by value"
    )]
    pub fn scan_paginator(
        &self,
        request: DaxScanRequest,
    ) -> Result<Arc<ScanPaginator>, DaxFfiError> {
        Ok(Arc::new(ScanPaginator {
            state: State::new(self.inner.scan_paginator(operation::scan_input(request)?)),
        }))
    }

    /// Creates a lazy BatchGetItem paginator with explicit duplicate-token policy.
    #[allow(
        clippy::result_large_err,
        reason = "UniFFI errors cross the ABI by value"
    )]
    pub fn batch_get_item_paginator(
        &self,
        request: DaxBatchGetItemRequest,
        stop_on_duplicate_token: bool,
    ) -> Result<Arc<BatchGetItemPaginator>, DaxFfiError> {
        Ok(Arc::new(BatchGetItemPaginator {
            state: State::new(
                self.inner
                    .batch_get_item_paginator(operation::batch_get_item_input(request)?)
                    .stop_on_duplicate_token(stop_on_duplicate_token),
            ),
        }))
    }
}
