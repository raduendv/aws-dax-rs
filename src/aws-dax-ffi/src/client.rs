use std::{sync::Arc, time::Duration};

use aws_config::BehaviorVersion;
use aws_types::region::Region;

use crate::{DaxClientConfig, DaxFfiError, DaxIpDiscovery, DaxLogLevel, runtime};

/// Rust-owned DAX client shared safely by generated language objects.
#[derive(Debug, uniffi::Object)]
pub struct DaxClient {
    pub(crate) inner: aws_dax::Client,
}

/// Creates a DAX client using Rust's default AWS region and credential chain.
#[uniffi::export]
#[allow(
    clippy::result_large_err,
    reason = "UniFFI rich errors cross the ABI by value and cannot be boxed"
)]
pub async fn create_dax_client(config: DaxClientConfig) -> Result<Arc<DaxClient>, DaxFfiError> {
    runtime::run(async move {
        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(region) = config.region.as_deref() {
            loader = loader.region(Region::new(region.to_owned()));
        }
        let sdk_config = loader.load().await;

        let mut builder = aws_dax::Config::builder();
        for endpoint in config.endpoints {
            builder = builder.endpoint(endpoint);
        }
        if let Some(region) = sdk_config.region() {
            builder = builder.region(region.as_ref());
        }
        if let Some(credentials) = sdk_config.credentials_provider() {
            builder = builder.credentials_provider(credentials);
        }
        builder = builder
            .request_timeout(Duration::from_millis(config.request_timeout_millis))
            .write_retries(config.write_retries)
            .read_retries(config.read_retries)
            .retry_delay(Duration::from_millis(config.retry_delay_millis))
            .throttle_base_delay(Duration::from_millis(config.throttle_base_delay_millis))
            .throttle_max_backoff(Duration::from_millis(config.throttle_max_backoff_millis))
            .max_pending_connections_per_host(config.max_pending_connections_per_host)
            .cluster_update_interval(Duration::from_millis(config.cluster_update_interval_millis))
            .cluster_update_threshold(Duration::from_millis(
                config.cluster_update_threshold_millis,
            ))
            .idle_connection_reap_delay(Duration::from_millis(
                config.idle_connection_reap_delay_millis,
            ))
            .client_health_check_interval(Duration::from_millis(
                config.client_health_check_interval_millis,
            ))
            .skip_hostname_verification(config.skip_hostname_verification)
            .route_manager_enabled(config.route_manager_enabled)
            .ip_discovery(ip_discovery(config.ip_discovery))
            .log_level(log_level(config.log_level));

        let inner = aws_dax::Client::new(builder.build().map_err(aws_dax::Error::from)?)
            .map_err(DaxFfiError::from)?;
        Ok(Arc::new(DaxClient { inner }))
    })
    .await
}

#[uniffi::export]
impl DaxClient {
    /// Closes transport pools and background work. Repeated calls are safe.
    #[allow(
        clippy::result_large_err,
        reason = "UniFFI rich errors cross the ABI by value and cannot be boxed"
    )]
    pub fn shutdown(&self) -> Result<(), DaxFfiError> {
        self.inner.close().map_err(DaxFfiError::from)
    }

    /// Returns whether this client has been closed.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

fn ip_discovery(value: DaxIpDiscovery) -> aws_dax::IpDiscovery {
    match value {
        DaxIpDiscovery::Default => aws_dax::IpDiscovery::Default,
        DaxIpDiscovery::Ipv4 => aws_dax::IpDiscovery::Ipv4,
        DaxIpDiscovery::Ipv6 => aws_dax::IpDiscovery::Ipv6,
    }
}

fn log_level(value: DaxLogLevel) -> aws_dax::LogLevel {
    match value {
        DaxLogLevel::Off => aws_dax::LogLevel::Off,
        DaxLogLevel::Error => aws_dax::LogLevel::Error,
        DaxLogLevel::Warn => aws_dax::LogLevel::Warn,
        DaxLogLevel::Info => aws_dax::LogLevel::Info,
        DaxLogLevel::Debug => aws_dax::LogLevel::Debug,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> DaxClientConfig {
        DaxClientConfig {
            endpoints: vec!["dax://cluster.example.com:8111".to_owned()],
            region: Some("us-east-1".to_owned()),
            ..DaxClientConfig::default()
        }
    }

    #[tokio::test]
    async fn creates_and_closes_without_network_io() {
        let client = create_dax_client(test_config()).await.unwrap();
        assert!(!client.is_closed());
        client.shutdown().unwrap();
        assert!(client.is_closed());
        client.shutdown().unwrap();
    }

    #[tokio::test]
    async fn concurrent_close_is_idempotent() {
        let client = create_dax_client(test_config()).await.unwrap();
        let callers = (0..16)
            .map(|_| {
                let client = client.clone();
                tokio::spawn(async move { client.shutdown() })
            })
            .collect::<Vec<_>>();
        for caller in callers {
            caller.await.unwrap().unwrap();
        }
        assert!(client.is_closed());
    }

    #[tokio::test]
    async fn repeated_create_and_drop_releases_client_objects() {
        for _ in 0..32 {
            let client = create_dax_client(test_config()).await.unwrap();
            let weak = Arc::downgrade(&client);
            drop(client);
            assert!(weak.upgrade().is_none());
        }
    }

    #[tokio::test]
    async fn concurrent_creation_produces_independent_clients() {
        let callers = (0..16)
            .map(|_| tokio::spawn(create_dax_client(test_config())))
            .collect::<Vec<_>>();
        let mut clients = Vec::with_capacity(callers.len());
        for caller in callers {
            let client = caller.await.unwrap().unwrap();
            assert!(!client.is_closed());
            clients.push(client);
        }

        clients[0].shutdown().unwrap();
        assert!(clients[0].is_closed());
        assert!(clients[1..].iter().all(|client| !client.is_closed()));
    }

    #[tokio::test]
    async fn invalid_config_is_structured() {
        let error = create_dax_client(DaxClientConfig {
            region: Some("us-east-1".to_owned()),
            ..DaxClientConfig::default()
        })
        .await
        .unwrap_err();
        assert_eq!(
            error.details().configuration_kind,
            Some(crate::DaxConfigurationErrorKind::MissingEndpoint)
        );
    }
}
