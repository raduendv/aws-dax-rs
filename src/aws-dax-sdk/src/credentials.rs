use std::time::{Duration, SystemTime};

use aws_credential_types::{
    Credentials,
    provider::{
        self, ProvideCredentials, SharedCredentialsProvider, error::CredentialsError, future,
    },
};
use tokio::sync::Mutex;

const MAX_REFRESH_MARGIN: Duration = Duration::from_secs(60);
const NONEXPIRING_REFRESH_INTERVAL: Duration = Duration::from_secs(15 * 60);

#[derive(Debug)]
pub(crate) struct CachedCredentialsProvider {
    source: SharedCredentialsProvider,
    cached: Mutex<Option<CachedCredentials>>,
}

#[derive(Debug)]
struct CachedCredentials {
    credentials: Credentials,
    refresh_at: SystemTime,
}

impl CachedCredentialsProvider {
    pub(crate) fn new(source: SharedCredentialsProvider) -> Self {
        Self {
            source,
            cached: Mutex::new(None),
        }
    }

    async fn credentials(&self, now: impl Fn() -> SystemTime) -> provider::Result {
        // Hold the async lock through refresh so concurrent pools load only once.
        // Cancellation releases the lock without committing a partial result.
        let mut cached = self.cached.lock().await;
        if let Some(entry) = cached.as_ref()
            && now() < entry.refresh_at
        {
            return Ok(entry.credentials.clone());
        }

        let credentials = self.source.provide_credentials().await?;
        let now = now();
        let refresh_after = match credentials.expiry() {
            Some(expiry) => {
                let remaining = expiry
                    .duration_since(now)
                    .ok()
                    .filter(|d| !d.is_zero())
                    .ok_or_else(|| {
                        CredentialsError::provider_error(
                            "credential provider returned expired credentials",
                        )
                    })?;
                remaining - MAX_REFRESH_MARGIN.min(remaining / 10)
            }
            None => NONEXPIRING_REFRESH_INTERVAL,
        };
        *cached = Some(CachedCredentials {
            credentials: credentials.clone(),
            refresh_at: now + refresh_after,
        });
        Ok(credentials)
    }
}

impl ProvideCredentials for CachedCredentialsProvider {
    fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        future::ProvideCredentials::new(self.credentials(SystemTime::now))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            Arc, Mutex as StdMutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use super::*;

    #[derive(Debug)]
    struct Source {
        calls: Arc<AtomicUsize>,
        responses: StdMutex<VecDeque<provider::Result>>,
    }

    impl ProvideCredentials for Source {
        fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
        where
            Self: 'a,
        {
            future::ProvideCredentials::new(async {
                self.calls.fetch_add(1, Ordering::SeqCst);
                tokio::task::yield_now().await;
                self.responses
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("unexpected credential load")
            })
        }
    }

    fn source(responses: Vec<provider::Result>) -> (SharedCredentialsProvider, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            SharedCredentialsProvider::new(Source {
                calls: calls.clone(),
                responses: StdMutex::new(responses.into()),
            }),
            calls,
        )
    }

    fn credentials(key: &str, expiry: Option<SystemTime>) -> provider::Result {
        Ok(Credentials::new(key, "test-secret", None, expiry, "test"))
    }

    #[tokio::test]
    async fn config_clones_share_one_credential_load() {
        let (source, calls) = source((0..100).map(|_| credentials("first", None)).collect());
        let config = crate::Config::builder()
            .endpoint("dax://localhost:8111")
            .region("us-east-1")
            .credentials_provider(source)
            .build()
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..100 {
            let config = config.clone();
            tasks.spawn(async move {
                config
                    .credentials_provider()
                    .provide_credentials()
                    .await
                    .unwrap()
            });
        }
        while let Some(result) = tasks.join_next().await {
            assert_eq!(result.unwrap().access_key_id(), "first");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn concurrent_refresh_is_single_flight_and_margin_is_capped() {
        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        let (source, calls) = source(vec![
            credentials("first", Some(start + Duration::from_secs(3600))),
            credentials("second", Some(start + Duration::from_secs(7200))),
        ]);
        let cache = Arc::new(CachedCredentialsProvider::new(source));
        cache.credentials(|| start).await.unwrap();
        cache
            .credentials(|| start + Duration::from_secs(3539))
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..100 {
            let cache = cache.clone();
            tasks.spawn(async move {
                cache
                    .credentials(|| start + Duration::from_secs(3540))
                    .await
                    .unwrap()
            });
        }
        while let Some(result) = tasks.join_next().await {
            assert_eq!(result.unwrap().access_key_id(), "second");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn separate_configurations_do_not_share_credential_identity() {
        for key in ["first-account", "second-account"] {
            let (source, calls) = source(vec![credentials(key, None)]);
            let config = crate::Config::builder()
                .endpoint("dax://localhost:8111")
                .region("us-east-1")
                .credentials_provider(source)
                .build()
                .unwrap();
            assert_eq!(
                config
                    .credentials_provider()
                    .provide_credentials()
                    .await
                    .unwrap()
                    .access_key_id(),
                key
            );
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn failed_refresh_never_returns_expired_cached_credentials() {
        let now = SystemTime::UNIX_EPOCH;
        let (source, calls) = source(vec![
            credentials("first", Some(now + Duration::from_secs(100))),
            Err(CredentialsError::provider_error("test failure")),
        ]);
        let cache = CachedCredentialsProvider::new(source);
        cache.credentials(|| now).await.unwrap();
        assert!(
            cache
                .credentials(|| now + Duration::from_secs(101))
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn refreshes_before_expiry_and_propagates_refresh_errors() {
        let start = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        let (source, calls) = source(vec![
            credentials("first", Some(start + Duration::from_secs(100))),
            Err(CredentialsError::provider_error("test refresh failure")),
            credentials("second", Some(start + Duration::from_secs(1000))),
        ]);
        let cache = CachedCredentialsProvider::new(source);
        assert_eq!(
            cache.credentials(|| start).await.unwrap().access_key_id(),
            "first"
        );
        assert_eq!(
            cache
                .credentials(|| start + Duration::from_secs(89))
                .await
                .unwrap()
                .access_key_id(),
            "first"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            cache
                .credentials(|| start + Duration::from_secs(90))
                .await
                .is_err()
        );
        assert_eq!(
            cache
                .credentials(|| start + Duration::from_secs(90))
                .await
                .unwrap()
                .access_key_id(),
            "second"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn rejects_expired_credentials_and_recovers() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1000);
        let (source, calls) = source(vec![
            credentials("expired", Some(now)),
            credentials("valid", Some(now + Duration::from_secs(1))),
        ]);
        let cache = CachedCredentialsProvider::new(source);
        assert!(cache.credentials(|| now).await.is_err());
        assert_eq!(
            cache.credentials(|| now).await.unwrap().access_key_id(),
            "valid"
        );
        assert_eq!(
            cache
                .credentials(|| now + Duration::from_millis(899))
                .await
                .unwrap()
                .access_key_id(),
            "valid"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn nonexpiring_credentials_are_not_cached_forever() {
        let now = SystemTime::UNIX_EPOCH;
        let (source, calls) = source(vec![
            credentials("first", None),
            credentials("second", None),
        ]);
        let cache = CachedCredentialsProvider::new(source);
        cache.credentials(|| now).await.unwrap();
        cache
            .credentials(|| now + Duration::from_secs(899))
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            cache
                .credentials(|| now + Duration::from_secs(900))
                .await
                .unwrap()
                .access_key_id(),
            "second"
        );
    }

    #[tokio::test]
    async fn cancelling_refresh_does_not_poison_cache() {
        #[derive(Debug)]
        struct BlockingFirst(AtomicUsize);
        impl ProvideCredentials for BlockingFirst {
            fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
            where
                Self: 'a,
            {
                future::ProvideCredentials::new(async {
                    if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                        std::future::pending::<()>().await;
                    }
                    credentials("recovered", None)
                })
            }
        }
        let cache = CachedCredentialsProvider::new(SharedCredentialsProvider::new(BlockingFirst(
            AtomicUsize::new(0),
        )));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), cache.provide_credentials())
                .await
                .is_err()
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), cache.provide_credentials())
                .await
                .unwrap()
                .unwrap()
                .access_key_id(),
            "recovered"
        );
    }
}
