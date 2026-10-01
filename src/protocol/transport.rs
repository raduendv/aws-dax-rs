//! Initial direct TCP executor for DAX control operations.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use rustls_native_certs::load_native_certs;
use tokio::{
    net::{TcpStream, lookup_host},
    sync::Semaphore,
    time::{sleep, timeout},
};
use tokio_rustls::{
    TlsConnector,
    rustls::{
        ClientConfig, DigitallySignedStruct, Error as RustlsError, RootCertStore, SignatureScheme,
        client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
        pki_types::{CertificateDer, ServerName, UnixTime},
    },
};

use crate::error::TransportErrorKind;
use crate::{Config, Error, IpDiscovery};

use super::{
    cbor::{ResponseEnvelope, decode_response_envelope},
    cluster::{RouteKey, RouteTarget},
    control::ControlExecutor,
    stream::StreamCborError,
    tube::{ControlTube, TubeAuthentication},
};

const DEFAULT_THROTTLE_BASE_DELAY: Duration = Duration::from_millis(70);
const DEFAULT_THROTTLE_MAX_BACKOFF: Duration = Duration::from_secs(20);

#[derive(Debug)]
struct SkipServerVerification;

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, RustlsError> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ED25519,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
        ]
    }
}

#[derive(Clone)]
pub(crate) struct SeedTransport {
    executors: Arc<Vec<Arc<TcpControlExecutor>>>,
}

impl SeedTransport {
    pub(crate) fn from_config(config: &Config) -> Result<Self, Error> {
        let endpoints = config.endpoints().map_err(Error::from)?;
        let executors = endpoints
            .iter()
            .map(|endpoint| TcpControlExecutor::from_endpoint(config, endpoint).map(Arc::new))
            .collect::<Result<Vec<_>, _>>()?;
        if executors.is_empty() {
            return Err(Error::Validation {
                message: "no DAX endpoint is configured".into(),
            });
        }
        Ok(Self {
            executors: Arc::new(executors),
        })
    }

    pub(crate) fn close(&self) {
        for executor in self.executors.iter() {
            executor.close();
        }
    }

    pub(crate) fn reap_idle_connections(&self) {
        for executor in self.executors.iter() {
            executor.reap_idle_connections();
        }
    }
}

impl ControlExecutor for SeedTransport {
    async fn execute(&self, operation: &'static str, request: Vec<u8>) -> Result<Vec<u8>, Error> {
        let mut last_error = None;
        for executor in self.executors.iter() {
            match executor.execute(operation, request.clone()).await {
                Ok(response) => return Ok(response),
                Err(Error::Closed) => return Err(Error::Closed),
                Err(error) => last_error = Some(error),
            }
        }
        Err(last_error.expect("seed transport is constructed with at least one executor"))
    }
}

/// A single-endpoint, plaintext TCP executor for DAX control operations.
///
/// Idle tubes are stored in LIFO order. TLS uses the configured discovery host
/// as its SNI name. This does not yet provide cluster discovery or routing.
#[derive(Clone)]
pub(crate) struct TcpControlExecutor {
    address: String,
    tls_server_name: Option<String>,
    seed_ip_discovery: Option<IpDiscovery>,
    skip_hostname_verification: bool,
    region: String,
    credentials: SharedCredentialsProvider,
    request_timeout: Duration,
    read_retries: u32,
    write_retries: u32,
    retry_delay: Duration,
    throttle_base_delay: Duration,
    throttle_max_backoff: Duration,
    jitter_source: Arc<dyn JitterSource>,
    idle_tubes: Arc<Mutex<Vec<IdleControlTube>>>,
    idle_connection_reap_delay: Duration,
    pending_connections: Arc<Semaphore>,
    closed: Arc<AtomicBool>,
}

trait JitterSource: Send + Sync {
    fn sample(&self, upper_exclusive: u64) -> u64;
}

#[derive(Debug)]
struct SystemJitter;

impl JitterSource for SystemJitter {
    fn sample(&self, upper_exclusive: u64) -> u64 {
        if upper_exclusive == 0 {
            return 0;
        }
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as u64
            % upper_exclusive
    }
}

/// Node-keyed transport pool boundary for future routed request execution.
#[derive(Clone, Default)]
pub(crate) struct RoutedTransportPools {
    pools: Arc<Mutex<HashMap<RouteKey, Arc<TcpControlExecutor>>>>,
}

impl RoutedTransportPools {
    pub(crate) fn get_or_create(
        &self,
        route: &RouteTarget,
        config: &Config,
    ) -> Result<Arc<TcpControlExecutor>, Error> {
        let key = RouteKey::from(route);
        let mut pools = self
            .pools
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(executor) = pools.get(&key) {
            return Ok(executor.clone());
        }
        let executor = Arc::new(TcpControlExecutor::for_route(config, route)?);
        pools.insert(key, executor.clone());
        Ok(executor)
    }

    pub(crate) fn insert(&self, route: &RouteTarget, executor: Arc<TcpControlExecutor>) {
        self.pools
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(RouteKey::from(route), executor);
    }

    pub(crate) fn get(&self, route: &RouteTarget) -> Option<Arc<TcpControlExecutor>> {
        self.pools
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&RouteKey::from(route))
            .cloned()
    }

    pub(crate) fn remove(&self, route: &RouteTarget) -> Option<Arc<TcpControlExecutor>> {
        let executor = self
            .pools
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&RouteKey::from(route));
        if let Some(executor) = &executor {
            executor.close();
        }
        executor
    }

    pub(crate) fn retain_routes(&self, routes: &[RouteTarget]) {
        let active = routes
            .iter()
            .map(RouteKey::from)
            .collect::<std::collections::HashSet<_>>();
        let mut pools = self
            .pools
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let stale_keys = pools
            .keys()
            .filter(|key| !active.contains(*key))
            .cloned()
            .collect::<Vec<_>>();
        let removed = stale_keys
            .into_iter()
            .filter_map(|key| pools.remove(&key))
            .collect::<Vec<_>>();
        drop(pools);
        for executor in removed {
            executor.close();
        }
    }

    pub(crate) fn synchronize_routes(&self, routes: &[RouteTarget]) {
        self.retain_routes(routes);
    }

    pub(crate) fn route_keys(&self) -> Vec<RouteKey> {
        self.pools
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .keys()
            .cloned()
            .collect()
    }

    pub(crate) fn close_all(&self) {
        let executors = self
            .pools
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .drain()
            .map(|(_, executor)| executor)
            .collect::<Vec<_>>();
        for executor in executors {
            executor.close();
        }
    }

    pub(crate) fn reap_idle_connections(&self) {
        let executors = self
            .pools
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for executor in executors {
            executor.reap_idle_connections();
        }
    }
}

pub(crate) trait AsyncStream:
    tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send
{
}
impl<Stream> AsyncStream for Stream where
    Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send
{
}

pub(crate) type BoxedStream = Box<dyn AsyncStream>;
type DirectControlTube =
    ControlTube<tokio::io::ReadHalf<BoxedStream>, tokio::io::WriteHalf<BoxedStream>>;

struct ConnectedControlTube {
    tube: DirectControlTube,
    authentication: Option<TubeAuthentication>,
}

struct IdleControlTube {
    connected: ConnectedControlTube,
    idle_since: std::time::Instant,
}

impl TcpControlExecutor {
    /// Builds a direct executor from the sole configured plaintext endpoint.
    pub(crate) fn from_config(config: &Config) -> Result<Self, Error> {
        let endpoints = config.endpoints().map_err(Error::from)?;
        let endpoint = endpoints.first().ok_or_else(|| Error::Validation {
            message: "no DAX endpoint is configured".into(),
        })?;
        Self::from_endpoint(config, endpoint)
    }

    pub(crate) fn from_endpoint(
        config: &Config,
        endpoint: &crate::config::Endpoint,
    ) -> Result<Self, Error> {
        Self::with_connection_target(
            config,
            format!("{}:{}", endpoint.host(), endpoint.port()),
            endpoint
                .scheme()
                .is_encrypted()
                .then(|| endpoint.host().into()),
            Some(config.ip_discovery()),
        )
    }

    pub(crate) fn for_route(config: &Config, route: &RouteTarget) -> Result<Self, Error> {
        let endpoints = config.endpoints().map_err(Error::from)?;
        let encrypted = endpoints
            .first()
            .is_some_and(|endpoint| endpoint.scheme().is_encrypted());
        Self::with_connection_target(
            config,
            route.dial_address.clone(),
            encrypted.then(|| route.tls_server_name.clone()),
            None,
        )
    }

    fn with_connection_target(
        config: &Config,
        address: String,
        tls_server_name: Option<String>,
        seed_ip_discovery: Option<IpDiscovery>,
    ) -> Result<Self, Error> {
        Ok(Self {
            address,
            tls_server_name,
            seed_ip_discovery,
            skip_hostname_verification: config.skip_hostname_verification(),
            region: config.region().into(),
            credentials: config.credentials_provider().clone(),
            request_timeout: config.request_timeout(),
            read_retries: config.read_retries(),
            write_retries: config.write_retries(),
            retry_delay: config.retry_delay(),
            throttle_base_delay: nonzero_or(
                config.throttle_base_delay(),
                DEFAULT_THROTTLE_BASE_DELAY,
            ),
            throttle_max_backoff: nonzero_or(
                config.throttle_max_backoff(),
                DEFAULT_THROTTLE_MAX_BACKOFF,
            ),
            jitter_source: Arc::new(SystemJitter),
            idle_tubes: Arc::new(Mutex::new(Vec::new())),
            idle_connection_reap_delay: config.idle_connection_reap_delay(),
            pending_connections: Arc::new(Semaphore::new(connection_attempt_limit(
                config.max_pending_connections_per_host(),
            ))),
            closed: Arc::new(AtomicBool::new(false)),
        })
    }

    #[cfg(test)]
    fn new(
        address: String,
        region: impl Into<String>,
        credentials: SharedCredentialsProvider,
        request_timeout: Duration,
    ) -> Self {
        Self {
            address,
            tls_server_name: None,
            seed_ip_discovery: None,
            skip_hostname_verification: false,
            region: region.into(),
            credentials,
            request_timeout,
            read_retries: 0,
            write_retries: 0,
            retry_delay: Duration::ZERO,
            throttle_base_delay: DEFAULT_THROTTLE_BASE_DELAY,
            throttle_max_backoff: DEFAULT_THROTTLE_MAX_BACKOFF,
            jitter_source: Arc::new(SystemJitter),
            idle_tubes: Arc::new(Mutex::new(Vec::new())),
            idle_connection_reap_delay: Duration::from_secs(30),
            pending_connections: Arc::new(Semaphore::new(10)),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    #[cfg(test)]
    fn with_idle_connection_reap_delay(mut self, delay: Duration) -> Self {
        self.idle_connection_reap_delay = delay;
        self
    }

    #[cfg(test)]
    fn with_retry_policy(mut self, read_retries: u32, write_retries: u32, delay: Duration) -> Self {
        self.read_retries = read_retries;
        self.write_retries = write_retries;
        self.retry_delay = delay;
        self
    }

    #[cfg(test)]
    fn with_jitter_source(mut self, jitter_source: Arc<dyn JitterSource>) -> Self {
        self.jitter_source = jitter_source;
        self
    }

    /// Closes idle tubes and prevents future connection attempts.
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.pending_connections.close();
        self.idle_tubes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }

    pub(crate) fn reap_idle_connections(&self) {
        let reap_before = std::time::Instant::now()
            .checked_sub(self.idle_connection_reap_delay)
            .unwrap_or(std::time::Instant::now());
        self.idle_tubes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|idle| idle.idle_since > reap_before);
    }
}

impl ControlExecutor for TcpControlExecutor {
    async fn execute(&self, operation: &'static str, request: Vec<u8>) -> Result<Vec<u8>, Error> {
        timeout(
            self.request_timeout,
            self.execute_with_retries_budget(operation, request, self.retry_budget(operation)),
        )
        .await
        .map_err(|_| Error::Transport {
            operation,
            kind: TransportErrorKind::Timeout,
            message: "DAX request timed out".into(),
        })?
    }
}

impl TcpControlExecutor {
    pub(crate) async fn execute_health_check(
        &self,
        operation: &'static str,
        request: Vec<u8>,
    ) -> Result<Vec<u8>, Error> {
        timeout(
            self.request_timeout,
            self.execute_with_retries_budget(operation, request, 3),
        )
        .await
        .map_err(|_| Error::Transport {
            operation,
            kind: TransportErrorKind::Timeout,
            message: "DAX request timed out".into(),
        })?
    }

    async fn execute_with_retries_budget(
        &self,
        operation: &'static str,
        request: Vec<u8>,
        attempts: u32,
    ) -> Result<Vec<u8>, Error> {
        let mut last_error = None;
        for attempt in 0..=attempts {
            match self.execute_inner(operation, request.clone()).await {
                Ok(response) if attempt < attempts && retryable_response(&response) => {
                    let delay = if throttled_response(&response) {
                        throttle_backoff(
                            attempt,
                            self.throttle_base_delay,
                            self.throttle_max_backoff,
                            &*self.jitter_source,
                        )
                    } else {
                        self.retry_delay
                    };
                    if !delay.is_zero() {
                        sleep(delay).await;
                    }
                }
                Ok(response) => return Ok(response),
                Err(Error::Closed) => return Err(Error::Closed),
                Err(error) if attempt < attempts => {
                    last_error = Some(error);
                    if !self.retry_delay.is_zero() {
                        sleep(self.retry_delay).await;
                    }
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.expect("retry loop returns during its final attempt"))
    }

    async fn execute_inner(
        &self,
        operation: &'static str,
        request: Vec<u8>,
    ) -> Result<Vec<u8>, Error> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }
        let credentials = self
            .credentials
            .provide_credentials()
            .await
            .map_err(|error| Error::Protocol {
                operation,
                message: format!("failed to load DAX credentials: {error}"),
            })?;
        let mut checked_out = self.take_idle_tube();
        if checked_out.is_none() {
            let _permit = if is_high_priority(operation) {
                None
            } else {
                Some(
                    self.pending_connections
                        .acquire()
                        .await
                        .map_err(|_| Error::Closed)?,
                )
            };
            if self.closed.load(Ordering::Acquire) {
                return Err(Error::Closed);
            }
            let stream = self.connect(operation).await?;
            let tube = ControlTube::from_stream(stream)
                .await
                .map_err(|error| stream_error(operation, error))?;
            checked_out = Some(ConnectedControlTube {
                tube,
                authentication: None,
            });
        }

        let mut checked_out = checked_out.expect("a connected tube was just created");
        let now = time::OffsetDateTime::now_utc();
        if checked_out
            .authentication
            .as_ref()
            .is_none_or(|authentication| !authentication.is_current(&credentials, now))
        {
            checked_out
                .tube
                .authorize(&credentials, &self.region, now)
                .await
                .map_err(|error| Error::Protocol {
                    operation,
                    message: error.to_string(),
                })?;
            checked_out.authentication = Some(TubeAuthentication::new(&credentials, now));
        }

        match checked_out.tube.execute(operation, &request).await {
            Ok(response) => {
                if authentication_required(&response) {
                    if let Some(authentication) = &mut checked_out.authentication {
                        authentication.expire(time::OffsetDateTime::now_utc());
                    }
                }
                if !self.closed.load(Ordering::Acquire) {
                    self.idle_tubes
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .push(IdleControlTube {
                            connected: checked_out,
                            idle_since: std::time::Instant::now(),
                        });
                }
                Ok(response)
            }
            Err(error) => Err(stream_error(operation, error)),
        }
    }

    async fn connect(&self, operation: &'static str) -> Result<BoxedStream, Error> {
        self.connect_to(operation, &self.address, self.tls_server_name.as_deref())
            .await
    }

    pub(crate) async fn connect_route(
        &self,
        operation: &'static str,
        route: &RouteTarget,
    ) -> Result<BoxedStream, Error> {
        self.connect_to(operation, &route.dial_address, Some(&route.tls_server_name))
            .await
    }

    async fn connect_to(
        &self,
        operation: &'static str,
        address: &str,
        tls_server_name: Option<&str>,
    ) -> Result<BoxedStream, Error> {
        let stream = if let Some(policy) = self.seed_ip_discovery {
            let resolved = lookup_host(address)
                .await
                .map_err(|error| io_error(operation, error))?
                .collect::<Vec<_>>();
            let addresses = select_seed_addresses(policy, &resolved)?;
            let mut last_error = None;
            let mut stream = None;
            for address in addresses {
                match TcpStream::connect(address).await {
                    Ok(connected) => {
                        stream = Some(connected);
                        break;
                    }
                    Err(error) => last_error = Some(error),
                }
            }
            stream.ok_or_else(|| {
                io_error(
                    operation,
                    last_error.expect("seed resolution produced at least one address"),
                )
            })?
        } else {
            TcpStream::connect(address)
                .await
                .map_err(|error| io_error(operation, error))?
        };
        let Some(server_name) = tls_server_name else {
            return Ok(Box::new(stream));
        };
        let configuration = if self.skip_hostname_verification {
            ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
                .with_no_client_auth()
        } else {
            let roots = load_native_certs();
            if !roots.errors.is_empty() {
                return Err(Error::Validation {
                    message: "failed to load native TLS certificate roots".into(),
                });
            }
            let mut root_store = RootCertStore::empty();
            root_store.add_parsable_certificates(roots.certs);
            ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth()
        };
        let server_name =
            ServerName::try_from(server_name.to_owned()).map_err(|_| Error::Validation {
                message: "invalid dax TLS server name".into(),
            })?;
        TlsConnector::from(Arc::new(configuration))
            .connect(server_name, stream)
            .await
            .map(|stream| Box::new(stream) as BoxedStream)
            .map_err(|error| Error::Transport {
                operation,
                kind: TransportErrorKind::Io,
                message: format!("DAX TLS connection failed: {error}"),
            })
    }

    fn take_idle_tube(&self) -> Option<ConnectedControlTube> {
        let mut idle_tubes = self
            .idle_tubes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let reap_before = std::time::Instant::now()
            .checked_sub(self.idle_connection_reap_delay)
            .unwrap_or(std::time::Instant::now());
        idle_tubes.retain(|idle| idle.idle_since > reap_before);
        idle_tubes.pop().map(|idle| idle.connected)
    }

    fn retry_budget(&self, operation: &str) -> u32 {
        if is_write_operation(operation) {
            self.write_retries
        } else {
            self.read_retries
        }
    }
}

fn select_seed_addresses(
    policy: IpDiscovery,
    addresses: &[SocketAddr],
) -> Result<Vec<SocketAddr>, Error> {
    let selected_ips =
        policy.select_addresses(&addresses.iter().map(SocketAddr::ip).collect::<Vec<_>>())?;
    let selected = addresses
        .iter()
        .filter(|address| selected_ips.contains(&address.ip()))
        .copied()
        .collect::<Vec<_>>();
    if selected.is_empty() {
        return Err(Error::Validation {
            message: "DAX seed hostname resolved to no addresses".into(),
        });
    }
    Ok(selected)
}

fn connection_attempt_limit(configured: i32) -> usize {
    usize::try_from(configured)
        .ok()
        .filter(|limit| *limit > 0)
        .unwrap_or(10)
}

fn is_high_priority(operation: &str) -> bool {
    matches!(
        operation,
        "DefineKeySchema" | "DefineAttributeListId" | "DefineAttributeList"
    )
}

fn throttled_response(response: &[u8]) -> bool {
    matches!(
        decode_response_envelope(response),
        Ok(ResponseEnvelope::Error(error))
            if matches!(
                crate::error::dax_error_kind_for_retry(&error.code_sequence),
                Some(crate::error::DaxErrorKind::ProvisionedThroughputExceeded)
                    | Some(crate::error::DaxErrorKind::Throttling)
            )
    )
}

fn throttle_backoff(
    attempt: u32,
    base_delay: Duration,
    max_backoff: Duration,
    jitter_source: &dyn JitterSource,
) -> Duration {
    let base_nanos = base_delay.as_nanos();
    let max_nanos = max_backoff.as_nanos();
    let multiplier = 1u128.checked_shl(attempt.min(127)).unwrap_or(u128::MAX);
    let minimum = base_nanos.saturating_mul(multiplier).min(max_nanos);
    let half = minimum / 2;
    let jitter_range = half.saturating_add(1);
    let jitter = jitter_source.sample(jitter_range.min(u64::MAX as u128) as u64) as u128;
    Duration::from_nanos((half + jitter).min(u64::MAX as u128) as u64)
}

const fn nonzero_or(value: Duration, default: Duration) -> Duration {
    if value.is_zero() { default } else { value }
}

fn is_write_operation(operation: &str) -> bool {
    matches!(
        operation,
        "PutItem" | "DeleteItem" | "UpdateItem" | "BatchWriteItem" | "TransactWriteItems"
    )
}

fn retryable_response(response: &[u8]) -> bool {
    matches!(
        decode_response_envelope(response),
        Ok(ResponseEnvelope::Error(error))
            if matches!(error.code_sequence.first(), Some(1 | 2))
                || matches!(
                    crate::error::dax_error_kind_for_retry(&error.code_sequence),
                    Some(crate::error::DaxErrorKind::ProvisionedThroughputExceeded)
                        | Some(crate::error::DaxErrorKind::Throttling)
                )
                || error.code_sequence == [4, 23, 31, 33]
    )
}

fn authentication_required(response: &[u8]) -> bool {
    matches!(
        decode_response_envelope(response),
        Ok(ResponseEnvelope::Error(error))
            if error
                .code_sequence
                .windows(3)
                .any(|codes| matches!(codes, [23, 31, 32..=34]))
    )
}

fn io_error(operation: &'static str, error: std::io::Error) -> Error {
    let kind = if error.kind() == std::io::ErrorKind::UnexpectedEof {
        TransportErrorKind::UnexpectedEof
    } else {
        TransportErrorKind::Io
    };
    Error::Transport {
        operation,
        kind,
        message: format!("DAX TCP connection failed: {error}"),
    }
}

fn stream_error(operation: &'static str, error: StreamCborError) -> Error {
    let message = error.to_string();
    let kind = match &error {
        StreamCborError::Io(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
            TransportErrorKind::UnexpectedEof
        }
        StreamCborError::Io(_) => TransportErrorKind::Io,
        _ => return Error::Protocol { operation, message },
    };
    Error::Transport {
        operation,
        kind,
        message,
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::{
        ControlExecutor, DEFAULT_THROTTLE_BASE_DELAY, DEFAULT_THROTTLE_MAX_BACKOFF,
        RoutedTransportPools, TcpControlExecutor, authentication_required, nonzero_or,
        select_seed_addresses, throttle_backoff,
    };
    use crate::protocol::cluster::RouteTarget;
    use crate::{
        Config, Error, IpDiscovery,
        protocol::tube::{encode_authorization, encode_tube_preamble},
    };

    struct FixedJitter(u64);

    impl super::JitterSource for FixedJitter {
        fn sample(&self, upper_exclusive: u64) -> u64 {
            self.0.min(upper_exclusive.saturating_sub(1))
        }
    }

    #[test]
    fn throttle_backoff_matches_go_equal_jitter_bounds() {
        assert_eq!(
            throttle_backoff(
                0,
                Duration::from_millis(70),
                Duration::from_secs(20),
                &FixedJitter(0),
            ),
            Duration::from_millis(35)
        );
        assert_eq!(
            throttle_backoff(
                0,
                Duration::from_millis(70),
                Duration::from_secs(20),
                &FixedJitter(u64::MAX),
            ),
            Duration::from_millis(70)
        );
        assert_eq!(
            throttle_backoff(
                10,
                Duration::from_millis(70),
                Duration::from_secs(20),
                &FixedJitter(0),
            ),
            Duration::from_secs(10)
        );
        assert_eq!(
            throttle_backoff(
                10,
                Duration::from_millis(70),
                Duration::from_secs(20),
                &FixedJitter(u64::MAX),
            ),
            Duration::from_secs(20)
        );
    }

    #[test]
    fn zero_throttle_bounds_restore_go_defaults() {
        assert_eq!(
            nonzero_or(Duration::ZERO, DEFAULT_THROTTLE_BASE_DELAY),
            DEFAULT_THROTTLE_BASE_DELAY
        );
        assert_eq!(
            nonzero_or(Duration::ZERO, DEFAULT_THROTTLE_MAX_BACKOFF),
            DEFAULT_THROTTLE_MAX_BACKOFF
        );
    }

    #[test]
    fn throttle_backoff_honors_configured_base_and_cap() {
        assert_eq!(
            throttle_backoff(
                0,
                Duration::from_millis(10),
                Duration::from_secs(1),
                &FixedJitter(0),
            ),
            Duration::from_millis(5)
        );
        assert_eq!(
            throttle_backoff(
                10,
                Duration::from_millis(10),
                Duration::from_millis(30),
                &FixedJitter(u64::MAX),
            ),
            Duration::from_millis(30)
        );
    }

    #[test]
    fn throttle_backoff_applies_cap_before_jitter_when_max_is_below_base() {
        assert_eq!(
            throttle_backoff(
                0,
                Duration::from_millis(70),
                Duration::from_millis(10),
                &FixedJitter(u64::MAX),
            ),
            Duration::from_millis(10)
        );
    }

    #[test]
    fn recognizes_direct_and_intermediate_throttling_envelopes() {
        let direct = [
            0x84, 0x04, 0x18, 0x25, 0x18, 0x27, 0x18, 0x32, 0x61, b'x', 0xf6,
        ];
        let with_intermediate_code = [
            0x85, 0x04, 0x18, 0x25, 0x18, 0x26, 0x18, 0x27, 0x18, 0x32, 0x61, b'x', 0xf6,
        ];

        assert!(super::throttled_response(&direct));
        assert!(super::throttled_response(&with_intermediate_code));
    }

    #[test]
    fn seed_address_selection_preserves_ports_and_family_order() {
        let addresses = [
            "127.0.0.1:8111".parse().unwrap(),
            "[::1]:9111".parse().unwrap(),
            "127.0.0.2:8222".parse().unwrap(),
        ];

        assert_eq!(
            select_seed_addresses(IpDiscovery::Default, &addresses).unwrap(),
            vec![
                "127.0.0.1:8111".parse().unwrap(),
                "127.0.0.2:8222".parse().unwrap()
            ]
        );
        assert_eq!(
            select_seed_addresses(IpDiscovery::Ipv6, &addresses).unwrap(),
            vec!["[::1]:9111".parse().unwrap()]
        );
    }

    #[test]
    fn seed_address_selection_rejects_an_unmatched_family() {
        let addresses = ["127.0.0.1:8111".parse().unwrap()];
        assert!(select_seed_addresses(IpDiscovery::Ipv6, &addresses).is_err());
    }

    #[tokio::test]
    async fn sends_preamble_authorization_and_control_request_over_tcp() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let credentials = Credentials::new("key", "secret", None, None, "test");
        let server_credentials = credentials.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut preamble = vec![0; encode_tube_preamble().len()];
            stream.read_exact(&mut preamble).await.unwrap();
            assert_eq!(preamble, encode_tube_preamble());

            let authorization = encode_authorization(
                &server_credentials,
                "us-east-1",
                time::OffsetDateTime::now_utc(),
            )
            .unwrap();
            let mut received = vec![0; authorization.len()];
            stream.read_exact(&mut received).await.unwrap();
            assert_eq!(&received[..6], &authorization[..6]);
            assert_eq!(received.last(), authorization.last());

            let mut request = [0; 2];
            stream.read_exact(&mut request).await.unwrap();
            assert_eq!(request, [0x01, 0x02]);
            stream.write_all(&[0x80, 0x09]).await.unwrap();
        });
        let executor = TcpControlExecutor::new(
            address.to_string(),
            "us-east-1",
            SharedCredentialsProvider::new(credentials),
            Duration::from_secs(1),
        );

        assert_eq!(
            executor.execute("Test", vec![0x01, 0x02]).await.unwrap(),
            vec![0x80, 0x09]
        );
        server.await.unwrap();
    }

    #[test]
    fn retaining_routes_closes_and_removes_departed_node_pools() {
        let pools = RoutedTransportPools::default();
        let first = RouteTarget {
            node_id: 1,
            dial_address: "127.0.0.1:8111".into(),
            tls_server_name: "node-1.example".into(),
        };
        let second = RouteTarget {
            node_id: 2,
            dial_address: "127.0.0.1:8112".into(),
            tls_server_name: "node-2.example".into(),
        };
        let credentials =
            SharedCredentialsProvider::new(Credentials::new("key", "secret", None, None, "test"));
        pools.insert(
            &first,
            Arc::new(TcpControlExecutor::new(
                first.dial_address.clone(),
                "us-east-1",
                credentials.clone(),
                Duration::from_secs(1),
            )),
        );
        pools.insert(
            &second,
            Arc::new(TcpControlExecutor::new(
                second.dial_address.clone(),
                "us-east-1",
                credentials,
                Duration::from_secs(1),
            )),
        );
        pools.retain_routes(std::slice::from_ref(&first));
        assert!(pools.get(&first).is_some());
        assert!(pools.get(&second).is_none());
    }

    #[test]
    fn synchronizing_routes_does_not_reuse_a_pool_after_address_change() {
        let pools = RoutedTransportPools::default();
        let old = RouteTarget {
            node_id: 1,
            dial_address: "127.0.0.1:8111".into(),
            tls_server_name: "node.example".into(),
        };
        let updated = RouteTarget {
            node_id: 1,
            dial_address: "127.0.0.2:8111".into(),
            tls_server_name: "node.example".into(),
        };
        let credentials =
            SharedCredentialsProvider::new(Credentials::new("key", "secret", None, None, "test"));
        pools.insert(
            &old,
            Arc::new(TcpControlExecutor::new(
                old.dial_address.clone(),
                "us-east-1",
                credentials,
                Duration::from_secs(1),
            )),
        );
        pools.synchronize_routes(std::slice::from_ref(&updated));
        assert!(pools.get(&old).is_none());
        assert!(pools.get(&updated).is_none());
    }

    #[tokio::test]
    async fn reuses_a_clean_tube_without_repeating_its_preamble_or_authorization() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let credentials = Credentials::new("key", "secret", None, None, "test");
        let server_credentials = credentials.clone();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut preamble = vec![0; encode_tube_preamble().len()];
            stream.read_exact(&mut preamble).await.unwrap();
            assert_eq!(preamble, encode_tube_preamble());

            let authorization = encode_authorization(
                &server_credentials,
                "us-east-1",
                time::OffsetDateTime::now_utc(),
            )
            .unwrap();
            let mut received = vec![0; authorization.len()];
            stream.read_exact(&mut received).await.unwrap();

            for expected in [[0x01, 0x02], [0x03, 0x04]] {
                let mut request = [0; 2];
                stream.read_exact(&mut request).await.unwrap();
                assert_eq!(request, expected);
                stream.write_all(&[0x80, 0x09]).await.unwrap();
            }
        });
        let executor = TcpControlExecutor::new(
            address.to_string(),
            "us-east-1",
            SharedCredentialsProvider::new(credentials),
            Duration::from_secs(1),
        );

        assert_eq!(
            executor.execute("Test", vec![0x01, 0x02]).await.unwrap(),
            vec![0x80, 0x09]
        );
        assert_eq!(
            executor.execute("Test", vec![0x03, 0x04]).await.unwrap(),
            vec![0x80, 0x09]
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn reaps_expired_idle_tubes_before_reuse() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let credentials = Credentials::new("key", "secret", None, None, "test");
        let server_credentials = credentials.clone();
        let server = tokio::spawn(async move {
            for expected_request in [[0x01, 0x02], [0x03, 0x04]] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut preamble = vec![0; encode_tube_preamble().len()];
                stream.read_exact(&mut preamble).await.unwrap();
                assert_eq!(preamble, encode_tube_preamble());

                let authorization = encode_authorization(
                    &server_credentials,
                    "us-east-1",
                    time::OffsetDateTime::now_utc(),
                )
                .unwrap();
                let mut received = vec![0; authorization.len()];
                stream.read_exact(&mut received).await.unwrap();

                let mut request = [0; 2];
                stream.read_exact(&mut request).await.unwrap();
                assert_eq!(request, expected_request);
                stream.write_all(&[0x80, 0x09]).await.unwrap();
            }
        });
        let executor = TcpControlExecutor::new(
            address.to_string(),
            "us-east-1",
            SharedCredentialsProvider::new(credentials),
            Duration::from_secs(1),
        )
        .with_idle_connection_reap_delay(Duration::ZERO);

        assert_eq!(
            executor.execute("Test", vec![0x01, 0x02]).await.unwrap(),
            vec![0x80, 0x09]
        );
        assert_eq!(
            executor.execute("Test", vec![0x03, 0x04]).await.unwrap(),
            vec![0x80, 0x09]
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_a_failed_read_attempt_using_the_read_budget() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let credentials = Credentials::new("key", "secret", None, None, "test");
        let server_credentials = credentials.clone();
        let server = tokio::spawn(async move {
            for (expected_request, response) in
                [([0x01, 0x02], None), ([0x01, 0x02], Some([0x80, 0x09]))]
            {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut preamble = vec![0; encode_tube_preamble().len()];
                stream.read_exact(&mut preamble).await.unwrap();
                assert_eq!(preamble, encode_tube_preamble());

                let authorization = encode_authorization(
                    &server_credentials,
                    "us-east-1",
                    time::OffsetDateTime::now_utc(),
                )
                .unwrap();
                let mut received = vec![0; authorization.len()];
                stream.read_exact(&mut received).await.unwrap();

                let mut request = [0; 2];
                stream.read_exact(&mut request).await.unwrap();
                assert_eq!(request, expected_request);
                if let Some(response) = response {
                    stream.write_all(&response).await.unwrap();
                }
            }
        });
        let executor = TcpControlExecutor::new(
            address.to_string(),
            "us-east-1",
            SharedCredentialsProvider::new(credentials),
            Duration::from_secs(1),
        )
        .with_retry_policy(1, 0, Duration::ZERO);

        assert_eq!(
            executor.execute("GetItem", vec![0x01, 0x02]).await.unwrap(),
            vec![0x80, 0x09]
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn retries_a_failed_write_attempt_using_the_write_budget() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let credentials = Credentials::new("key", "secret", None, None, "test");
        let server_credentials = credentials.clone();
        let server = tokio::spawn(async move {
            for response in [None, Some([0x80, 0x09])] {
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
                let mut request = [0; 2];
                stream.read_exact(&mut request).await.unwrap();
                assert_eq!(request, [0x01, 0x02]);
                if let Some(response) = response {
                    stream.write_all(&response).await.unwrap();
                }
            }
        });
        let executor = TcpControlExecutor::new(
            address.to_string(),
            "us-east-1",
            SharedCredentialsProvider::new(credentials),
            Duration::from_secs(1),
        )
        .with_retry_policy(0, 1, Duration::ZERO);

        assert_eq!(
            executor.execute("PutItem", vec![0x01, 0x02]).await.unwrap(),
            vec![0x80, 0x09]
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn returns_a_non_retryable_response_without_using_remaining_budget() {
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
            let mut request = [0; 2];
            stream.read_exact(&mut request).await.unwrap();
            stream
                .write_all(&[0x81, 0x04, 0x63, b'b', b'a', b'd', 0xf6])
                .await
                .unwrap();
        });
        let executor = TcpControlExecutor::new(
            address.to_string(),
            "us-east-1",
            SharedCredentialsProvider::new(credentials),
            Duration::from_secs(1),
        )
        .with_retry_policy(3, 3, Duration::ZERO);

        let response = executor.execute("GetItem", vec![0x01, 0x02]).await.unwrap();
        assert_eq!(response, [0x81, 0x04, 0x63, b'b', b'a', b'd', 0xf6]);
        server.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn waits_for_configured_delay_before_retrying_a_retryable_response() {
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

            let mut request = [0; 2];
            stream.read_exact(&mut request).await.unwrap();
            assert_eq!(request, [0x01, 0x02]);
            stream
                .write_all(&[0x81, 0x01, 0x61, b'r', 0xf6])
                .await
                .unwrap();

            stream.read_exact(&mut request).await.unwrap();
            assert_eq!(request, [0x01, 0x02]);
            stream.write_all(&[0x80, 0x09]).await.unwrap();
        });
        let retry_delay = Duration::from_secs(5);
        let executor = TcpControlExecutor::new(
            address.to_string(),
            "us-east-1",
            SharedCredentialsProvider::new(credentials),
            Duration::MAX,
        )
        .with_retry_policy(1, 0, retry_delay);

        let execution =
            tokio::spawn(async move { executor.execute("GetItem", vec![0x01, 0x02]).await });
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;
        assert!(!execution.is_finished());

        tokio::time::advance(retry_delay - Duration::from_millis(1)).await;
        tokio::task::yield_now().await;
        assert!(!execution.is_finished());

        tokio::time::advance(Duration::from_millis(1)).await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert_eq!(execution.await.unwrap().unwrap(), [0x80, 0x09]);
        server.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn uses_equal_jitter_for_throttled_responses() {
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

            let mut request = [0; 2];
            stream.read_exact(&mut request).await.unwrap();
            stream
                .write_all(&[
                    0x84, 0x04, 0x18, 0x25, 0x18, 0x27, 0x18, 0x32, 0x61, b'x', 0xf6,
                ])
                .await
                .unwrap();
            stream.read_exact(&mut request).await.unwrap();
            stream.write_all(&[0x80, 0x09]).await.unwrap();
        });
        let executor = TcpControlExecutor::new(
            address.to_string(),
            "us-east-1",
            SharedCredentialsProvider::new(credentials),
            Duration::MAX,
        )
        .with_retry_policy(1, 0, Duration::from_secs(5))
        .with_jitter_source(Arc::new(FixedJitter(0)));

        let execution =
            tokio::spawn(async move { executor.execute("GetItem", vec![0x01, 0x02]).await });
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }
        assert!(!execution.is_finished());

        tokio::time::advance(Duration::from_millis(34)).await;
        tokio::task::yield_now().await;
        assert!(!execution.is_finished());

        tokio::time::advance(Duration::from_millis(1)).await;
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert_eq!(execution.await.unwrap().unwrap(), [0x80, 0x09]);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn does_not_wait_after_the_final_retryable_attempt() {
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
            let mut request = [0; 2];
            stream.read_exact(&mut request).await.unwrap();
            stream
                .write_all(&[0x81, 0x01, 0x61, b'r', 0xf6])
                .await
                .unwrap();
        });
        let executor = TcpControlExecutor::new(
            address.to_string(),
            "us-east-1",
            SharedCredentialsProvider::new(credentials),
            Duration::from_secs(30),
        )
        .with_retry_policy(0, 0, Duration::from_secs(60));

        let response = executor.execute("GetItem", vec![0x01, 0x02]).await.unwrap();
        assert_eq!(response, [0x81, 0x01, 0x61, b'r', 0xf6]);
        server.await.unwrap();
    }

    #[test]
    fn recognizes_a_reusable_authentication_required_response() {
        assert!(authentication_required(&[
            0x84, 0x04, 0x17, 0x18, 0x1f, 0x18, 0x21, 0x6b, b'a', b'u', b't', b'h', b' ', b'n',
            b'e', b'e', b'd', b'e', b'd', 0xf6,
        ]));
        assert!(!authentication_required(&[
            0x81, 0x04, 0x63, b'b', b'a', b'd', 0xf6
        ]));
    }

    #[test]
    fn preserves_reference_retry_classification_and_read_write_budgets() {
        let retryable_server = [0x81, 0x01, 0x61, b'x', 0xf6];
        let retryable_recoverable = [0x81, 0x02, 0x61, b'x', 0xf6];
        let retryable_throughput = [
            0x84, 0x04, 0x18, 0x25, 0x18, 0x27, 0x18, 0x28, 0x61, b'x', 0xf6,
        ];
        let retryable_throughput_with_intermediate_code = [
            0x85, 0x04, 0x18, 0x25, 0x18, 0x26, 0x18, 0x27, 0x18, 0x28, 0x61, b'x', 0xf6,
        ];
        let retryable_throttling = [
            0x84, 0x04, 0x18, 0x25, 0x18, 0x27, 0x18, 0x32, 0x61, b'x', 0xf6,
        ];
        let retryable_throttling_with_intermediate_code = [
            0x85, 0x04, 0x18, 0x25, 0x18, 0x26, 0x18, 0x27, 0x18, 0x32, 0x61, b'x', 0xf6,
        ];
        let retryable_authentication = [
            0x84, 0x04, 0x17, 0x18, 0x1f, 0x18, 0x21, 0x6b, b'a', b'u', b't', b'h', b' ', b'n',
            b'e', b'e', b'd', b'e', b'd', 0xf6,
        ];
        let non_retryable_validation_with_intermediate_code = [
            0x85, 0x04, 0x18, 0x25, 0x18, 0x26, 0x18, 0x27, 0x18, 0x2e, 0x61, b'x', 0xf6,
        ];
        let non_retryable_not_implemented_with_intermediate_code = [
            0x84, 0x04, 0x18, 0x25, 0x18, 0x26, 0x18, 0x2c, 0x61, b'x', 0xf6,
        ];
        let non_retryable_truncated_service_code =
            [0x83, 0x04, 0x18, 0x25, 0x18, 0x27, 0x61, b'x', 0xf6];
        let non_retryable_unknown_intermediate_code = [
            0x85, 0x04, 0x18, 0x25, 0x18, 0x28, 0x18, 0x27, 0x18, 0x32, 0x61, b'x', 0xf6,
        ];
        assert!(super::retryable_response(&retryable_server));
        assert!(super::retryable_response(&retryable_recoverable));
        assert!(super::retryable_response(&retryable_throughput));
        assert!(super::retryable_response(
            &retryable_throughput_with_intermediate_code
        ));
        assert!(super::retryable_response(&retryable_throttling));
        assert!(super::retryable_response(
            &retryable_throttling_with_intermediate_code
        ));
        assert!(super::retryable_response(&retryable_authentication));
        assert!(!super::retryable_response(&[
            0x81, 0x04, 0x63, b'b', b'a', b'd', 0xf6
        ]));
        assert!(!super::retryable_response(&[
            0x85, 0x04, 0x17, 0x18, 0x1f, 0x18, 0x21, 0x01, 0x61, b'x', 0xf6
        ]));
        assert!(!super::retryable_response(
            &non_retryable_validation_with_intermediate_code
        ));
        assert!(!super::retryable_response(
            &non_retryable_not_implemented_with_intermediate_code
        ));
        assert!(!super::retryable_response(
            &non_retryable_truncated_service_code
        ));
        assert!(!super::retryable_response(
            &non_retryable_unknown_intermediate_code
        ));
        assert!(super::is_write_operation("PutItem"));
        assert!(super::is_write_operation("DeleteItem"));
        assert!(!super::is_write_operation("GetItem"));
    }

    #[test]
    fn preserves_reference_connection_attempt_defaults_and_priorities() {
        assert_eq!(super::connection_attempt_limit(0), 10);
        assert_eq!(super::connection_attempt_limit(3), 3);
        assert!(super::is_high_priority("DefineKeySchema"));
        assert!(super::is_high_priority("DefineAttributeListId"));
        assert!(super::is_high_priority("DefineAttributeList"));
        assert!(!super::is_high_priority("GetItem"));
    }

    #[test]
    fn routes_each_operation_to_the_reference_retry_budget() {
        let executor = super::TcpControlExecutor::new(
            "127.0.0.1:8111".into(),
            "us-east-1",
            SharedCredentialsProvider::new(Credentials::new("key", "secret", None, None, "test")),
            Duration::from_secs(1),
        )
        .with_retry_policy(2, 5, Duration::ZERO);

        for operation in [
            "PutItem",
            "DeleteItem",
            "UpdateItem",
            "BatchWriteItem",
            "TransactWriteItems",
        ] {
            assert_eq!(executor.retry_budget(operation), 5, "{operation}");
        }
        for operation in [
            "GetItem",
            "Query",
            "Scan",
            "BatchGetItem",
            "TransactGetItems",
            "DefineKeySchema",
            "DefineAttributeListId",
            "DefineAttributeList",
            "Endpoints",
        ] {
            assert_eq!(executor.retry_budget(operation), 2, "{operation}");
        }
    }

    #[test]
    fn routed_transport_pools_are_keyed_and_closed_per_node() {
        let pools = super::RoutedTransportPools::default();
        let route = super::RouteTarget {
            node_id: 1,
            dial_address: "127.0.0.1:8111".into(),
            tls_server_name: "node.example".into(),
        };
        assert!(pools.get(&route).is_none());
        pools.close_all();
        assert!(pools.remove(&route).is_none());
    }

    #[tokio::test]
    async fn removing_a_route_pool_forces_health_probe_recreation() {
        let config = Config::builder()
            .endpoint("dax://cluster.example.com")
            .region("us-east-1")
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "key", "secret", None, None, "test",
            )))
            .build()
            .unwrap();
        let route = super::RouteTarget {
            node_id: 1,
            dial_address: "127.0.0.1:8111".into(),
            tls_server_name: "node.example".into(),
        };
        let pools = super::RoutedTransportPools::default();
        let first = pools.get_or_create(&route, &config).unwrap();
        let removed = pools.remove(&route).unwrap();
        assert!(removed.execute("Endpoints", vec![]).await.is_err());
        let second = pools.get_or_create(&route, &config).unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn close_invalidates_idle_tubes_and_refuses_new_execution() {
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
            let mut request = [0; 2];
            stream.read_exact(&mut request).await.unwrap();
            stream.write_all(&[0x80, 0x09]).await.unwrap();
            let mut eof_probe = [0; 1];
            assert_eq!(stream.read(&mut eof_probe).await.unwrap(), 0);
        });
        let executor = TcpControlExecutor::new(
            address.to_string(),
            "us-east-1",
            SharedCredentialsProvider::new(credentials),
            Duration::from_secs(1),
        );

        assert_eq!(
            executor.execute("Test", vec![0x01, 0x02]).await.unwrap(),
            vec![0x80, 0x09]
        );
        executor.close();
        assert_eq!(
            executor.execute("Test", vec![0x03]).await,
            Err(Error::Closed)
        );
        server.await.unwrap();
    }

    #[test]
    fn accepts_verified_daxs_configuration_without_starting_a_connection() {
        let config = Config::builder()
            .endpoint("daxs://cluster.example.com")
            .region("us-east-1")
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "key", "secret", None, None, "test",
            )))
            .build()
            .unwrap();

        let executor = TcpControlExecutor::from_config(&config).unwrap();
        assert_eq!(
            executor.tls_server_name.as_deref(),
            Some("cluster.example.com")
        );
        assert!(!executor.skip_hostname_verification);
    }

    #[test]
    fn accepts_explicitly_unverified_daxs_configuration() {
        let config = Config::builder()
            .endpoint("daxs://cluster.example.com")
            .region("us-east-1")
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "key", "secret", None, None, "test",
            )))
            .skip_hostname_verification(true)
            .build()
            .unwrap();

        let executor = TcpControlExecutor::from_config(&config).unwrap();
        assert_eq!(
            executor.tls_server_name.as_deref(),
            Some("cluster.example.com")
        );
        assert!(executor.skip_hostname_verification);
    }

    #[test]
    fn retains_ordered_unencrypted_seed_executors() {
        let config = Config::builder()
            .endpoint("dax://seed-a.example.com")
            .endpoint("dax://seed-b.example.com:8112")
            .region("us-east-1")
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "key", "secret", None, None, "test",
            )))
            .build()
            .unwrap();

        let transport = super::SeedTransport::from_config(&config).unwrap();

        assert_eq!(transport.executors.len(), 2);
        assert_eq!(transport.executors[0].address, "seed-a.example.com:8111");
        assert_eq!(transport.executors[1].address, "seed-b.example.com:8112");
    }

    #[test]
    fn creates_plaintext_route_executors_with_discovered_address() {
        let config = Config::builder()
            .endpoint("dax://cluster.example.com")
            .region("us-east-1")
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "key", "secret", None, None, "test",
            )))
            .build()
            .unwrap();
        let route = RouteTarget {
            node_id: 7,
            dial_address: "192.0.2.7:8111".into(),
            tls_server_name: "node-7.example.com".into(),
        };

        let executor = TcpControlExecutor::for_route(&config, &route).unwrap();

        assert_eq!(executor.address, "192.0.2.7:8111");
        assert!(executor.tls_server_name.is_none());
    }
}
