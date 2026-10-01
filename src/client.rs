use std::collections::HashSet;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

use aws_sdk_dynamodb::types::AttributeDefinition;
use tokio::task::JoinHandle;

use crate::{
    Config, Error, LogLevel,
    protocol::{
        cbor::DiscoveredEndpoint,
        cluster::EndpointRoster,
        cluster::RouteTable,
        cluster::{EndpointRefresh, RouteUnavailable},
        control::ControlResolver,
        request::encode_endpoints,
        schema::SchemaRegistry,
        transport::{RoutedTransportPools, SeedTransport},
    },
};

/// Read-only routing metadata for one discovered DAX node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteSnapshot {
    /// Stable node identifier returned by endpoint discovery.
    pub node_id: i64,
    /// Validated socket address used for direct node dialing.
    pub dial_address: String,
    /// Hostname used as the TLS server name for encrypted routes.
    pub tls_server_name: String,
    /// Whether request-driven route suppression currently allows selection.
    pub healthy: bool,
    /// Whether the periodic health-probe failure threshold currently allows
    /// the route to be considered probe-healthy.
    pub health_probe_healthy: bool,
}

/// A concurrency-safe DAX client.
///
/// Construction validates configuration but intentionally does not start
/// discovery or connect to a DAX cluster until the transport phase of the port.
pub struct Client {
    config: Arc<Config>,
    owners: Arc<AtomicUsize>,
    closed: Arc<AtomicBool>,
    transport: Arc<SeedTransport>,
    schemas: SchemaRegistry,
    route_table: RouteTable,
    routed_pools: RoutedTransportPools,
    route_manager: Arc<Mutex<RouteManagerState>>,
    last_refresh_error: Arc<Mutex<Option<String>>>,
    refresh_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    health_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    reap_task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl Clone for Client {
    fn clone(&self) -> Self {
        self.owners.fetch_add(1, Ordering::Relaxed);
        Self {
            config: self.config.clone(),
            owners: self.owners.clone(),
            closed: self.closed.clone(),
            transport: self.transport.clone(),
            schemas: self.schemas.clone(),
            route_table: self.route_table.clone(),
            routed_pools: self.routed_pools.clone(),
            route_manager: self.route_manager.clone(),
            last_refresh_error: self.last_refresh_error.clone(),
            refresh_task: self.refresh_task.clone(),
            health_task: self.health_task.clone(),
            reap_task: self.reap_task.clone(),
        }
    }
}

#[derive(Debug, Default)]
struct RouteManagerState {
    fail_open_times: Vec<Instant>,
    disabled_until: Option<Instant>,
}

fn scheduler_interval(interval: Duration) -> Duration {
    interval.max(Duration::from_millis(1))
}

impl RouteManagerState {
    fn is_active(&mut self, window: Duration) -> bool {
        let now = Instant::now();
        if self.disabled_until.is_some_and(|until| until > now) {
            return false;
        }
        self.disabled_until = None;
        self.fail_open_times
            .retain(|timestamp| now.duration_since(*timestamp) < window);
        true
    }

    fn record_fail_open(&mut self, window: Duration) {
        let now = Instant::now();
        self.fail_open_times
            .retain(|timestamp| now.duration_since(*timestamp) < window);
        self.fail_open_times.push(now);
        if self.fail_open_times.len() >= 3 {
            self.disabled_until = Some(now + Duration::from_secs(10 * 60));
            self.fail_open_times.clear();
        }
    }
}

impl Client {
    /// Discovers the current DAX cluster node roster from the configured seed.
    pub async fn discover_endpoints(&self) -> Result<Vec<DiscoveredEndpoint>, Error> {
        self.ensure_refresh_task();
        self.ensure_health_task();
        self.ensure_reap_task();
        let endpoints = self.control_resolver()?.endpoints().await?;
        let roster = EndpointRoster::from_discovered(&endpoints, &self.config)?;
        self.route_table.replace_serialized(roster).await?;
        self.routed_pools
            .synchronize_routes(&self.route_table.healthy_route_targets()?);
        self.clear_last_refresh_error();
        Ok(endpoints)
    }

    /// Refreshes the validated endpoint roster and returns its node-level diff.
    ///
    /// A failed discovery or validation leaves the last committed roster
    /// unchanged.
    pub async fn refresh_endpoints(&self) -> Result<EndpointRefresh, Error> {
        self.ensure_refresh_task();
        self.ensure_health_task();
        self.ensure_reap_task();
        let endpoints = self.control_resolver()?.endpoints().await?;
        let roster = EndpointRoster::from_discovered(&endpoints, &self.config)?;
        let initial_ids = roster.node_ids().collect::<Vec<_>>();
        let diff = self
            .route_table
            .replace_serialized(roster)
            .await?
            .map(EndpointRefresh::from)
            .unwrap_or_else(|| EndpointRefresh {
                added: initial_ids,
                removed: Vec::new(),
                retained: Vec::new(),
                changed: Vec::new(),
            });
        self.routed_pools
            .synchronize_routes(&self.route_table.healthy_route_targets()?);
        self.clear_last_refresh_error();
        Ok(diff)
    }

    /// Returns the last successfully validated endpoint roster.
    ///
    /// This accessor never performs network I/O. It returns `None` until
    /// [`Client::discover_endpoints`] has committed its first valid roster.
    pub fn active_endpoints(&self) -> Result<Option<Vec<DiscoveredEndpoint>>, Error> {
        self.route_table.discovered()
    }

    /// Returns the current discovered route targets and suppression state.
    ///
    /// This accessor never performs network I/O and returns an empty vector
    /// until endpoint discovery has committed a roster.
    pub fn route_snapshot(&self) -> Result<Vec<RouteSnapshot>, Error> {
        Ok(self
            .route_table
            .route_snapshots()?
            .into_iter()
            .map(|(target, healthy, health_probe_healthy)| RouteSnapshot {
                node_id: target.node_id,
                dial_address: target.dial_address,
                tls_server_name: target.tls_server_name,
                healthy,
                health_probe_healthy,
            })
            .collect())
    }

    /// Returns the most recent background endpoint-refresh error, if any.
    ///
    /// A successful refresh clears this diagnostic. A failed refresh never
    /// replaces the last successfully validated endpoint roster.
    pub fn last_refresh_error(&self) -> Option<String> {
        self.last_refresh_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn clear_last_refresh_error(&self) {
        *self
            .last_refresh_error
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }

    /// Creates a lazy Query paginator from an SDK-style operation input.
    pub fn query_paginator(
        &self,
        input: aws_sdk_dynamodb::operation::query::QueryInput,
    ) -> crate::QueryPaginator {
        crate::QueryPaginator::new(self.clone(), input)
    }

    /// Creates a lazy Scan paginator from an SDK-style operation input.
    pub fn scan_paginator(
        &self,
        input: aws_sdk_dynamodb::operation::scan::ScanInput,
    ) -> crate::ScanPaginator {
        crate::ScanPaginator::new(self.clone(), input)
    }

    /// Creates a lazy BatchGetItem paginator from an SDK-style operation input.
    pub fn batch_get_item_paginator(
        &self,
        input: aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemInput,
    ) -> crate::BatchGetItemPaginator {
        crate::BatchGetItemPaginator::new(self.clone(), input)
    }

    /// Creates a DAX client from AWS SDK for Rust shared configuration.
    ///
    /// The configured region and credentials provider are retained from
    /// `sdk_config`; `endpoint` supplies the DAX cluster discovery endpoint.
    /// Construction validates local configuration but does not initiate network
    /// I/O.
    pub fn from_sdk_config(
        sdk_config: &aws_config::SdkConfig,
        endpoint: impl Into<String>,
    ) -> Result<Self, Error> {
        let mut config = Config::builder().endpoint(endpoint);
        if let Some(region) = sdk_config.region() {
            config = config.region(region.as_ref());
        }
        if let Some(credentials) = sdk_config.credentials_provider() {
            config = config.credentials_provider(credentials.clone());
        }
        Self::new(config.build()?)
    }

    /// Creates a client after validating DAX configuration and seed endpoints.
    pub fn new(config: Config) -> Result<Self, Error> {
        if let Err(error) = config.endpoints() {
            config.log(
                LogLevel::Error,
                &format!("DAX client initialization failed: {error}"),
            );
            return Err(error.into());
        }
        let transport = SeedTransport::from_config(&config)?;
        let client = Self {
            config: Arc::new(config),
            owners: Arc::new(AtomicUsize::new(1)),
            closed: Arc::new(AtomicBool::new(false)),
            transport: Arc::new(transport),
            schemas: SchemaRegistry::default(),
            route_table: RouteTable::default(),
            routed_pools: RoutedTransportPools::default(),
            route_manager: Arc::new(Mutex::new(RouteManagerState::default())),
            last_refresh_error: Arc::new(Mutex::new(None)),
            refresh_task: Arc::new(Mutex::new(None)),
            health_task: Arc::new(Mutex::new(None)),
            reap_task: Arc::new(Mutex::new(None)),
        };
        client.config.log(
            LogLevel::Info,
            "DAX client created without starting network transport",
        );
        Ok(client)
    }

    /// Returns the client configuration.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Closes the client.
    ///
    /// Close is idempotent so concurrent callers and `Drop` cannot reproduce the
    /// double-close panic found in the Go reference implementation.
    pub fn close(&self) -> Result<(), Error> {
        self.closed.store(true, Ordering::Release);
        self.stop_refresh_task();
        self.stop_health_task();
        self.stop_reap_task();
        self.transport.close();
        self.routed_pools.close_all();
        Ok(())
    }

    /// Returns whether the client has been closed.
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Returns a stable error for a DynamoDB operation unavailable in DAX.
    pub fn unsupported_operation(&self, operation: &'static str) -> Result<(), Error> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        Err(Error::NotImplemented { operation })
    }

    pub(crate) async fn key_schema(&self, table: &str) -> Result<Vec<AttributeDefinition>, Error> {
        self.control_resolver()?.key_schema(table).await
    }

    pub(crate) async fn attribute_list(&self, id: i64) -> Result<Vec<String>, Error> {
        self.control_resolver()?.attribute_list(id).await
    }

    pub(crate) async fn attribute_list_id(&self, names: &[String]) -> Result<i64, Error> {
        self.control_resolver()?.attribute_list_id(names).await
    }

    pub(crate) async fn execute_protocol(
        &self,
        operation: &'static str,
        request: Vec<u8>,
    ) -> Result<Vec<u8>, Error> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        self.ensure_refresh_task();
        self.ensure_health_task();
        self.ensure_reap_task();
        use crate::protocol::control::ControlExecutor;
        let route_manager_active = self.route_manager_active();
        let route = match self.route_table.next_route(None)? {
            Ok(route) => Some(route),
            Err(RouteUnavailable::NoRoster) => None,
            Err(RouteUnavailable::AllNodesUnhealthy) => {
                if !self.config.route_manager_enabled() {
                    return Err(Error::Protocol {
                        operation,
                        message: "all discovered DAX routes are unhealthy".into(),
                    });
                }
                self.route_table.restore_all_routes()?;
                self.routed_pools
                    .synchronize_routes(&self.route_table.route_targets()?);
                self.route_table.next_route(None)?.ok()
            }
        };
        let Some(route) = route else {
            return self.transport.execute(operation, request).await;
        };

        let route_count = self.route_table.route_targets()?.len().max(1);
        let mut attempted = HashSet::with_capacity(route_count);
        let mut current = Some(route);
        let mut last_error = None;
        for _ in 0..route_count {
            let Some(route) = current.take() else {
                break;
            };
            if !attempted.insert(route.node_id) {
                break;
            }
            let executor = self.routed_pools.get_or_create(&route, &self.config)?;
            match executor.execute(operation, request.clone()).await {
                Ok(response) => {
                    self.route_table.record_success(route.node_id)?;
                    return Ok(response);
                }
                Err(Error::Closed) => return Err(Error::Closed),
                Err(error) => {
                    if !is_route_transport_failure(&error) {
                        return Err(error);
                    }
                    if route_manager_active && self.route_table.record_failure(route.node_id)? {
                        let healthy = self.route_table.healthy_route_targets()?;
                        let total = self.route_table.route_targets()?.len();
                        if healthy.len().saturating_mul(3) < total.saturating_mul(2) {
                            self.route_table.restore_all_routes()?;
                            self.routed_pools
                                .synchronize_routes(&self.route_table.route_targets()?);
                            self.record_fail_open();
                        } else {
                            self.routed_pools.synchronize_routes(&healthy);
                        }
                    }
                    last_error = Some(error);
                    current = self.route_table.next_route(Some(route.node_id))?.ok();
                }
            }
        }
        Err(last_error.expect("routed dispatch attempts at least one node"))
    }

    fn route_manager_active(&self) -> bool {
        if !self.config.route_manager_enabled() {
            return false;
        }
        self.route_manager
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_active(self.config.client_health_check_interval().saturating_mul(2))
    }

    fn record_fail_open(&self) {
        self.route_manager
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .record_fail_open(self.config.client_health_check_interval().saturating_mul(2));
    }

    fn ensure_refresh_task(&self) {
        if self.is_closed() {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut task = self
            .refresh_task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.is_closed() {
            return;
        }
        if task.as_ref().is_some_and(JoinHandle::is_finished) {
            task.take();
        }
        if task.is_some() {
            return;
        }
        let interval = self.config.cluster_update_interval();
        let config = self.config.clone();
        let closed = self.closed.clone();
        let transport = self.transport.clone();
        let schemas = self.schemas.clone();
        let route_table = self.route_table.clone();
        let routed_pools = self.routed_pools.clone();
        let last_refresh_error = self.last_refresh_error.clone();
        *task = Some(handle.spawn(async move {
            let mut ticker = tokio::time::interval(scheduler_interval(interval));
            while !closed.load(Ordering::Acquire) {
                if closed.load(Ordering::Acquire) {
                    break;
                }
                let resolver = ControlResolver::new(transport.as_ref(), schemas.clone());
                let refresh_result = async {
                    let endpoints = resolver.endpoints().await?;
                    let roster = EndpointRoster::from_discovered(&endpoints, &config)?;
                    route_table.replace_serialized(roster).await?;
                    let routes = route_table.healthy_route_targets()?;
                    routed_pools.synchronize_routes(&routes);
                    Ok::<(), Error>(())
                }
                .await;
                if let Err(error) = refresh_result {
                    let message = error.to_string();
                    config.log(
                        LogLevel::Warn,
                        &format!("background endpoint refresh failed: {message}"),
                    );
                    *last_refresh_error
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(message);
                } else {
                    *last_refresh_error
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
                }
                ticker.tick().await;
            }
        }));
    }

    fn stop_refresh_task(&self) {
        let mut task = self
            .refresh_task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(task) = task.take() {
            task.abort();
        }
    }

    fn ensure_health_task(&self) {
        if !self.config.route_manager_enabled() || self.is_closed() {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut task = self
            .health_task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.is_closed() {
            return;
        }
        if task.as_ref().is_some_and(JoinHandle::is_finished) {
            task.take();
        }
        if task.is_some() {
            return;
        }
        let interval = self.config.client_health_check_interval();
        let closed = self.closed.clone();
        let config = self.config.clone();
        let route_table = self.route_table.clone();
        let routed_pools = self.routed_pools.clone();
        *task = Some(handle.spawn(async move {
            let mut ticker = tokio::time::interval(scheduler_interval(interval));
            ticker.tick().await;
            while !closed.load(Ordering::Acquire) {
                ticker.tick().await;
                for route in route_table.route_targets().unwrap_or_default() {
                    if closed.load(Ordering::Acquire) {
                        break;
                    }
                    let Ok(executor) = routed_pools.get_or_create(&route, &config) else {
                        continue;
                    };
                    let result = tokio::time::timeout(
                        Duration::from_secs(1),
                        executor.execute_health_check("Endpoints", encode_endpoints()),
                    )
                    .await;
                    match result {
                        Ok(Ok(_)) => {
                            let _ = route_table.record_success(route.node_id);
                            let _ = route_table.record_health_success(route.node_id);
                        }
                        Ok(Err(error)) if is_route_transport_failure(&error) => {
                            if let Err(error) = route_table.record_health_failure(route.node_id) {
                                config.log(
                                    LogLevel::Warn,
                                    &format!("failed to record health-check failure: {error}"),
                                );
                            }
                            let _ = routed_pools.remove(&route);
                        }
                        Err(_) => {
                            if let Err(error) = route_table.record_health_failure(route.node_id) {
                                config.log(
                                    LogLevel::Warn,
                                    &format!("failed to record health-check timeout: {error}"),
                                );
                            }
                            let _ = routed_pools.remove(&route);
                        }
                        Ok(Err(_)) => {}
                    }
                }
            }
        }));
    }

    fn stop_health_task(&self) {
        let mut task = self
            .health_task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(task) = task.take() {
            task.abort();
        }
    }

    fn ensure_reap_task(&self) {
        if self.is_closed() {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let mut task = self
            .reap_task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.is_closed() {
            return;
        }
        if task.as_ref().is_some_and(JoinHandle::is_finished) {
            task.take();
        }
        if task.is_some() {
            return;
        }
        let interval = self.config.idle_connection_reap_delay();
        let closed = self.closed.clone();
        let transport = self.transport.clone();
        let routed_pools = self.routed_pools.clone();
        *task = Some(handle.spawn(async move {
            let mut ticker = tokio::time::interval(scheduler_interval(interval));
            ticker.tick().await;
            while !closed.load(Ordering::Acquire) {
                ticker.tick().await;
                transport.reap_idle_connections();
                routed_pools.reap_idle_connections();
            }
        }));
    }

    fn stop_reap_task(&self) {
        let mut task = self
            .reap_task
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(task) = task.take() {
            task.abort();
        }
    }

    fn control_resolver(&self) -> Result<ControlResolver<'_, SeedTransport>, Error> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        Ok(ControlResolver::new(&self.transport, self.schemas.clone()))
    }
}

impl std::fmt::Debug for Client {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Client")
            .field("config", &self.config)
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        if self.owners.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.closed.store(true, Ordering::Release);
            self.stop_refresh_task();
            self.stop_health_task();
            self.stop_reap_task();
            self.transport.close();
            self.routed_pools.close_all();
        }
    }
}

fn is_route_transport_failure(error: &Error) -> bool {
    matches!(error, Error::Transport { .. })
}

#[cfg(test)]
mod tests {
    use super::{RouteManagerState, is_route_transport_failure};
    use crate::protocol::cluster::EndpointRoster;
    use crate::{Client, Config, Error};
    use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    #[test]
    fn route_manager_disables_after_three_recent_fail_opens() {
        let window = Duration::from_secs(10);
        let mut state = RouteManagerState::default();

        state.record_fail_open(window);
        state.record_fail_open(window);
        assert!(state.is_active(window));
        state.record_fail_open(window);

        assert!(!state.is_active(window));
    }

    #[test]
    fn route_manager_reenables_after_disable_duration() {
        let mut state = RouteManagerState {
            disabled_until: Some(std::time::Instant::now() - Duration::from_secs(1)),
            ..RouteManagerState::default()
        };

        assert!(state.is_active(Duration::from_secs(10)));
        assert!(state.disabled_until.is_none());
    }

    #[test]
    fn health_checks_only_suppress_transport_failures() {
        assert!(is_route_transport_failure(&Error::Transport {
            operation: "Endpoints",
            kind: crate::TransportErrorKind::Io,
            message: "DAX TCP connection failed: refused".into(),
        }));
        assert!(is_route_transport_failure(&Error::Transport {
            operation: "Endpoints",
            kind: crate::TransportErrorKind::Timeout,
            message: "DAX request timed out".into(),
        }));
        assert!(!is_route_transport_failure(&Error::Protocol {
            operation: "Endpoints",
            message: "malformed response envelope".into(),
        }));
        assert!(!is_route_transport_failure(&Error::Dax {
            kind: crate::DaxErrorKind::Validation,
            message: "server rejected request".into(),
            request_id: None,
            error_code: None,
            status_code: 400,
            code_sequence: vec![4],
            cancellation_reasons: None,
        }));
        assert!(!is_route_transport_failure(&Error::Dax {
            kind: crate::DaxErrorKind::NotImplemented,
            message: "operation is not implemented".into(),
            request_id: None,
            error_code: None,
            status_code: 500,
            code_sequence: vec![4, 37, 39, 44],
            cancellation_reasons: None,
        }));
    }

    #[test]
    fn protocol_messages_cannot_trigger_routed_failover() {
        assert!(!is_route_transport_failure(&Error::Protocol {
            operation: "GetItem",
            message: "connection failed while decoding a malformed response".into(),
        }));
        assert!(!is_route_transport_failure(&Error::Protocol {
            operation: "GetItem",
            message: "peer closed a protocol envelope unexpectedly".into(),
        }));
    }

    #[test]
    fn scheduler_intervals_never_pass_zero_to_tokio() {
        assert_eq!(
            super::scheduler_interval(Duration::ZERO),
            Duration::from_millis(1)
        );
        assert_eq!(
            super::scheduler_interval(Duration::from_secs(2)),
            Duration::from_secs(2)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn scheduler_interval_ticks_immediately_then_at_configured_period() {
        let mut ticker = tokio::time::interval(super::scheduler_interval(Duration::from_secs(5)));
        ticker.tick().await;

        let pending = tokio::time::timeout(Duration::ZERO, ticker.tick()).await;
        assert!(pending.is_err());

        tokio::time::advance(Duration::from_secs(5)).await;
        ticker.tick().await;
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_scheduler_executes_endpoint_refreshes_against_controlled_transport() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let credentials = Credentials::new("key", "secret", None, None, "test");
        let server_credentials = credentials.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut preamble = vec![0; crate::protocol::tube::encode_tube_preamble().len()];
            stream.read_exact(&mut preamble).await.unwrap();
            let authorization = crate::protocol::tube::encode_authorization(
                &server_credentials,
                "us-east-1",
                time::OffsetDateTime::now_utc(),
            )
            .unwrap();
            let mut received = vec![0; authorization.len()];
            stream.read_exact(&mut received).await.unwrap();

            for _ in 0..2 {
                let mut request = [0; 6];
                stream.read_exact(&mut request).await.unwrap();
                assert_eq!(request, [0x01, 0x1a, 0x1b, 0x2b, 0xcf, 0x02]);
                stream
                    .write_all(&[
                        0x80, 0x81, 0xa8, 0x00, 0x01, 0x01, 0x63, b'n', b'o', b'd', 0x02, 0x44,
                        127, 0, 0, 1, 0x03, 0x18, 0x91, 0x04, 0x01, 0x05, 0x61, b'a', 0x06, 0x07,
                        0x07, 0x80,
                    ])
                    .await
                    .unwrap();
            }
        });

        let client = Client::new(
            Config::builder()
                .endpoint(format!("dax://{address}"))
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(credentials))
                .cluster_update_interval(Duration::from_secs(5))
                .build()
                .unwrap(),
        )
        .unwrap();
        client.ensure_refresh_task();
        for _ in 0..20 {
            tokio::task::yield_now().await;
            if client.active_endpoints().unwrap().is_some() {
                break;
            }
        }
        assert_eq!(client.active_endpoints().unwrap().unwrap().len(), 1);

        tokio::time::advance(Duration::from_secs(5)).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(client.active_endpoints().unwrap().unwrap().len(), 1);

        client.close().unwrap();
        server.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn health_scheduler_executes_endpoint_probe_against_controlled_route() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let credentials = Credentials::new("key", "secret", None, None, "test");
        let server_credentials = credentials.clone();
        let (probe_seen, probe_received) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut preamble = vec![0; crate::protocol::tube::encode_tube_preamble().len()];
            stream.read_exact(&mut preamble).await.unwrap();
            let authorization = crate::protocol::tube::encode_authorization(
                &server_credentials,
                "us-east-1",
                time::OffsetDateTime::now_utc(),
            )
            .unwrap();
            let mut received = vec![0; authorization.len()];
            stream.read_exact(&mut received).await.unwrap();

            let mut request = [0; 6];
            stream.read_exact(&mut request).await.unwrap();
            assert_eq!(request, [0x01, 0x1a, 0x1b, 0x2b, 0xcf, 0x02]);
            stream
                .write_all(&[
                    0x80, 0x81, 0xa8, 0x00, 0x01, 0x01, 0x63, b'n', b'o', b'd', 0x02, 0x44, 127, 0,
                    0, 1, 0x03, 0x18, 0x91, 0x04, 0x01, 0x05, 0x61, b'a', 0x06, 0x07, 0x07, 0x80,
                ])
                .await
                .unwrap();
            probe_seen.send(()).unwrap();
        });

        let client = Client::new(
            Config::builder()
                .endpoint(format!("dax://{address}"))
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(credentials))
                .route_manager_enabled(true)
                .client_health_check_interval(Duration::from_secs(5))
                .build()
                .unwrap(),
        )
        .unwrap();
        let endpoints = vec![crate::protocol::cbor::DiscoveredEndpoint {
            node_id: 7,
            hostname: "node-7.example.com".into(),
            address: vec![127, 0, 0, 1],
            port: i64::from(address.port()),
            role: 1,
            availability_zone: None,
            leader_session_id: None,
        }];
        let roster = EndpointRoster::from_discovered(&endpoints, &client.config)
            .expect("valid endpoint roster");
        client.route_table.replace_serialized(roster).await.unwrap();
        client.ensure_health_task();
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }

        tokio::time::advance(Duration::from_secs(5)).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        probe_received
            .await
            .expect("health probe server remained available");

        client.close().unwrap();
        server.await.unwrap();
    }

    #[tokio::test]
    async fn route_snapshot_reports_validated_targets_without_network_io() {
        let client = Client::new(
            Config::builder()
                .endpoint("dax://cluster.example.com")
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                    "key", "secret", None, None, "test",
                )))
                .build()
                .expect("test config"),
        )
        .expect("test client");
        let endpoints = vec![crate::protocol::cbor::DiscoveredEndpoint {
            node_id: 7,
            hostname: "node-7.example.com".into(),
            address: vec![127, 0, 0, 1],
            port: 8111,
            role: 1,
            availability_zone: None,
            leader_session_id: None,
        }];
        let roster = EndpointRoster::from_discovered(&endpoints, &client.config)
            .expect("valid endpoint roster");
        client
            .route_table
            .replace_serialized(roster)
            .await
            .expect("roster replacement");

        assert_eq!(
            client.route_snapshot().expect("route snapshot"),
            vec![super::RouteSnapshot {
                node_id: 7,
                dial_address: "127.0.0.1:8111".into(),
                tls_server_name: "node-7.example.com".into(),
                healthy: true,
                health_probe_healthy: true,
            }]
        );

        assert!(!client.route_table.record_failure(7).expect("first failure"));
        assert!(
            !client
                .route_table
                .record_failure(7)
                .expect("second failure")
        );
        assert!(client.route_table.record_failure(7).expect("third failure"));
        assert_eq!(
            client.route_snapshot().expect("suppressed route snapshot"),
            vec![super::RouteSnapshot {
                node_id: 7,
                dial_address: "127.0.0.1:8111".into(),
                tls_server_name: "node-7.example.com".into(),
                healthy: false,
                health_probe_healthy: true,
            }]
        );

        for _ in 0..5 {
            client
                .route_table
                .record_health_failure(7)
                .expect("health failure");
        }
        assert!(!client.route_snapshot().expect("degraded route snapshot")[0].health_probe_healthy);

        let replacement = EndpointRoster::from_discovered(
            &[crate::protocol::cbor::DiscoveredEndpoint {
                node_id: 7,
                hostname: "node-7-replaced.example.com".into(),
                address: vec![127, 0, 0, 2],
                port: 8112,
                role: 1,
                availability_zone: None,
                leader_session_id: None,
            }],
            &client.config,
        )
        .expect("valid replacement roster");
        client
            .route_table
            .replace_serialized(replacement)
            .await
            .expect("replacement roster");
        assert_eq!(
            client.route_snapshot().expect("replaced route snapshot"),
            vec![super::RouteSnapshot {
                node_id: 7,
                dial_address: "127.0.0.2:8112".into(),
                tls_server_name: "node-7-replaced.example.com".into(),
                healthy: false,
                health_probe_healthy: false,
            }]
        );
    }

    #[tokio::test]
    async fn disabled_route_manager_reports_all_routes_unhealthy_without_fail_open() {
        let client = Client::new(
            Config::builder()
                .endpoint("dax://cluster.example.com")
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                    "key", "secret", None, None, "test",
                )))
                .route_manager_enabled(false)
                .build()
                .expect("test config"),
        )
        .expect("test client");
        let endpoints = vec![crate::protocol::cbor::DiscoveredEndpoint {
            node_id: 7,
            hostname: "node-7.example.com".into(),
            address: vec![127, 0, 0, 1],
            port: 8111,
            role: 1,
            availability_zone: None,
            leader_session_id: None,
        }];
        let roster = EndpointRoster::from_discovered(&endpoints, &client.config)
            .expect("valid endpoint roster");
        client
            .route_table
            .replace_serialized(roster)
            .await
            .expect("roster replacement");
        assert!(
            !client
                .route_table
                .record_failure(7)
                .expect("first route failure")
        );
        assert!(
            !client
                .route_table
                .record_failure(7)
                .expect("second route failure")
        );
        assert!(
            client
                .route_table
                .record_failure(7)
                .expect("third route failure")
        );
        assert!(matches!(
            client.execute_protocol("GetItem", Vec::new()).await,
            Err(Error::Protocol { operation: "GetItem", message })
                if message == "all discovered DAX routes are unhealthy"
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn health_scheduler_waits_one_interval_before_first_probe_cycle() {
        let mut ticker = tokio::time::interval(super::scheduler_interval(Duration::from_secs(5)));
        ticker.tick().await;

        let pending = tokio::time::timeout(Duration::ZERO, ticker.tick()).await;
        assert!(pending.is_err());

        tokio::time::advance(Duration::from_secs(5)).await;
        ticker.tick().await;
    }

    #[tokio::test(start_paused = true)]
    async fn reaper_scheduler_waits_one_interval_before_first_reap_cycle() {
        let mut ticker = tokio::time::interval(super::scheduler_interval(Duration::from_secs(30)));
        ticker.tick().await;

        let pending = tokio::time::timeout(Duration::ZERO, ticker.tick()).await;
        assert!(pending.is_err());

        tokio::time::advance(Duration::from_secs(30)).await;
        ticker.tick().await;
    }

    #[tokio::test(start_paused = true)]
    async fn zero_scheduler_duration_uses_one_millisecond_tick() {
        let mut ticker = tokio::time::interval(super::scheduler_interval(Duration::ZERO));
        ticker.tick().await;

        let pending = tokio::time::timeout(Duration::ZERO, ticker.tick()).await;
        assert!(pending.is_err());

        tokio::time::advance(Duration::from_millis(1)).await;
        ticker.tick().await;
    }

    #[tokio::test]
    async fn close_cancels_all_started_lifecycle_tasks() {
        let client = Client::new(
            Config::builder()
                .endpoint("dax://cluster.example.com")
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                    "key", "secret", None, None, "test",
                )))
                .route_manager_enabled(true)
                .build()
                .expect("test config"),
        )
        .expect("test client");

        client.ensure_refresh_task();
        client.ensure_health_task();
        client.ensure_reap_task();
        tokio::task::yield_now().await;

        assert!(client.refresh_task.lock().unwrap().is_some());
        assert!(client.health_task.lock().unwrap().is_some());
        assert!(client.reap_task.lock().unwrap().is_some());

        client.close().expect("close client");

        assert!(client.is_closed());
        assert!(client.refresh_task.lock().unwrap().is_none());
        assert!(client.health_task.lock().unwrap().is_none());
        assert!(client.reap_task.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn final_drop_cancels_all_started_lifecycle_tasks() {
        let client = Client::new(
            Config::builder()
                .endpoint("dax://cluster.example.com")
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                    "key", "secret", None, None, "test",
                )))
                .route_manager_enabled(true)
                .build()
                .expect("test config"),
        )
        .expect("test client");

        client.ensure_refresh_task();
        client.ensure_health_task();
        client.ensure_reap_task();
        tokio::task::yield_now().await;
        assert!(client.refresh_task.lock().unwrap().is_some());
        assert!(client.health_task.lock().unwrap().is_some());
        assert!(client.reap_task.lock().unwrap().is_some());

        let refresh_task = client.refresh_task.clone();
        let health_task = client.health_task.clone();
        let reap_task = client.reap_task.clone();
        drop(client);

        assert!(refresh_task.lock().unwrap().is_none());
        assert!(health_task.lock().unwrap().is_none());
        assert!(reap_task.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn dropping_one_clone_does_not_cancel_shared_lifecycle_tasks() {
        let client = Client::new(
            Config::builder()
                .endpoint("dax://cluster.example.com")
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                    "key", "secret", None, None, "test",
                )))
                .route_manager_enabled(true)
                .build()
                .expect("test config"),
        )
        .expect("test client");
        client.ensure_refresh_task();
        client.ensure_health_task();
        client.ensure_reap_task();
        tokio::task::yield_now().await;

        let remaining = client.clone();
        drop(client);

        assert!(!remaining.is_closed());
        assert!(remaining.refresh_task.lock().unwrap().is_some());
        assert!(remaining.health_task.lock().unwrap().is_some());
        assert!(remaining.reap_task.lock().unwrap().is_some());

        remaining.close().expect("close remaining client");
    }

    #[tokio::test]
    async fn close_cancels_tasks_for_all_clones() {
        let client = Client::new(
            Config::builder()
                .endpoint("dax://cluster.example.com")
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                    "key", "secret", None, None, "test",
                )))
                .route_manager_enabled(true)
                .build()
                .expect("test config"),
        )
        .expect("test client");
        client.ensure_refresh_task();
        client.ensure_health_task();
        client.ensure_reap_task();
        tokio::task::yield_now().await;

        let clone = client.clone();
        client.close().expect("close client");

        assert!(clone.is_closed());
        assert!(clone.refresh_task.lock().unwrap().is_none());
        assert!(clone.health_task.lock().unwrap().is_none());
        assert!(clone.reap_task.lock().unwrap().is_none());
    }

    #[test]
    fn closed_client_does_not_start_new_lifecycle_tasks() {
        let client = Client::new(
            Config::builder()
                .endpoint("dax://cluster.example.com")
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                    "key", "secret", None, None, "test",
                )))
                .route_manager_enabled(true)
                .build()
                .expect("test config"),
        )
        .expect("test client");

        client.close().expect("close client");
        client.ensure_refresh_task();
        client.ensure_health_task();
        client.ensure_reap_task();

        assert!(client.refresh_task.lock().unwrap().is_none());
        assert!(client.health_task.lock().unwrap().is_none());
        assert!(client.reap_task.lock().unwrap().is_none());
    }

    #[tokio::test]
    async fn finished_lifecycle_task_slots_are_replaced() {
        let client = Client::new(
            Config::builder()
                .endpoint("dax://cluster.example.com")
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                    "key", "secret", None, None, "test",
                )))
                .route_manager_enabled(true)
                .build()
                .expect("test config"),
        )
        .expect("test client");

        *client.refresh_task.lock().unwrap() = Some(tokio::spawn(async {}));
        *client.health_task.lock().unwrap() = Some(tokio::spawn(async {}));
        *client.reap_task.lock().unwrap() = Some(tokio::spawn(async {}));
        tokio::task::yield_now().await;

        client.ensure_refresh_task();
        client.ensure_health_task();
        client.ensure_reap_task();

        assert!(client.refresh_task.lock().unwrap().is_some());
        assert!(client.health_task.lock().unwrap().is_some());
        assert!(client.reap_task.lock().unwrap().is_some());
        client.close().expect("close client");
    }

    #[test]
    fn explicit_refresh_recovery_clears_stale_background_error() {
        let client = Client::new(
            Config::builder()
                .endpoint("dax://cluster.example.com")
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                    "key", "secret", None, None, "test",
                )))
                .build()
                .expect("test config"),
        )
        .expect("test client");
        *client.last_refresh_error.lock().unwrap() = Some("stale failure".into());

        client.clear_last_refresh_error();

        assert_eq!(client.last_refresh_error(), None);
    }

    #[test]
    fn clone_owner_counter_tracks_repeated_clone_drop_cycles() {
        let client = Client::new(
            Config::builder()
                .endpoint("dax://cluster.example.com")
                .region("us-east-1")
                .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                    "key", "secret", None, None, "test",
                )))
                .build()
                .expect("test config"),
        )
        .expect("test client");

        for _ in 0..128 {
            let clone = client.clone();
            assert_eq!(client.owners.load(Ordering::Acquire), 2);
            drop(clone);
            assert_eq!(client.owners.load(Ordering::Acquire), 1);
            assert!(!client.is_closed());
        }
    }
}
