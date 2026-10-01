use std::{fmt, net::IpAddr, sync::Arc, time::Duration};

use aws_credential_types::provider::SharedCredentialsProvider;
use url::Url;

use crate::{ConfigError, Error, LogLevel, Logger, logging::SharedLogger};

const DAX_DEFAULT_PORT: u16 = 8111;
const DAXS_DEFAULT_PORT: u16 = 9111;

/// Controls which address family is used for DAX endpoint discovery.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum IpDiscovery {
    /// Prefer IPv4 when a cluster has both address families.
    #[default]
    Default,
    /// Select only IPv4 endpoints.
    Ipv4,
    /// Select only IPv6 endpoints.
    Ipv6,
}

impl IpDiscovery {
    /// Parses the case-insensitive values accepted by the Go client.
    pub fn parse(value: &str) -> Result<Self, ConfigError> {
        if value.is_empty() {
            Ok(Self::Default)
        } else if value.eq_ignore_ascii_case("ipv4") {
            Ok(Self::Ipv4)
        } else if value.eq_ignore_ascii_case("ipv6") {
            Ok(Self::Ipv6)
        } else {
            Err(ConfigError::InvalidIpDiscovery(value.into()))
        }
    }

    /// Selects matching addresses, preserving their order within a family.
    ///
    /// With [`IpDiscovery::Default`], IPv4 is preferred for a dual-stack result.
    pub fn select_addresses(&self, addresses: &[IpAddr]) -> Result<Vec<IpAddr>, Error> {
        let ipv4 = addresses
            .iter()
            .copied()
            .filter(IpAddr::is_ipv4)
            .collect::<Vec<_>>();
        let ipv6 = addresses
            .iter()
            .copied()
            .filter(IpAddr::is_ipv6)
            .collect::<Vec<_>>();

        match self {
            Self::Default if !ipv4.is_empty() => Ok(ipv4),
            Self::Default => Ok(ipv6),
            Self::Ipv4 if !ipv4.is_empty() => Ok(ipv4),
            Self::Ipv6 if !ipv6.is_empty() => Ok(ipv6),
            Self::Ipv4 => Err(Error::Validation {
                message: "ipDiscovery ipv4 does not match the SupportedNetworkType ipv6.".into(),
            }),
            Self::Ipv6 => Err(Error::Validation {
                message: "ipDiscovery ipv6 does not match the SupportedNetworkType ipv4.".into(),
            }),
        }
    }
}

/// DAX endpoint transport scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointScheme {
    /// Unencrypted DAX transport.
    Dax,
    /// TLS-encrypted DAX transport.
    Daxs,
}

impl EndpointScheme {
    /// The port used when an endpoint omits a port.
    pub const fn default_port(self) -> u16 {
        match self {
            Self::Dax => DAX_DEFAULT_PORT,
            Self::Daxs => DAXS_DEFAULT_PORT,
        }
    }

    /// Whether this scheme uses TLS.
    pub const fn is_encrypted(self) -> bool {
        matches!(self, Self::Daxs)
    }
}

/// A normalized DAX cluster discovery endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    scheme: EndpointScheme,
    host: String,
    port: u16,
}

impl Endpoint {
    /// Parses a DAX endpoint.
    ///
    /// Scheme-less host-and-port values use the `dax` scheme. `dax` and `daxs`
    /// endpoints may omit their port, in which case they use ports 8111 and
    /// 9111 respectively.
    pub fn parse(endpoint: &str) -> Result<Self, ConfigError> {
        let uri = if endpoint.contains("://") {
            endpoint.into()
        } else if endpoint.contains(':') {
            format!("dax://{endpoint}")
        } else {
            return Err(ConfigError::InvalidEndpoint);
        };

        let (scheme, rest) = uri.split_once("://").ok_or(ConfigError::InvalidEndpoint)?;
        let scheme = match scheme {
            "dax" => EndpointScheme::Dax,
            "daxs" => EndpointScheme::Daxs,
            _ => return Err(ConfigError::UnsupportedEndpointScheme),
        };

        // The Go parser falls back to the scheme default when the port cannot
        // be parsed, so preserve that observable behavior here.
        let parsed = Url::parse(&uri).or_else(|_| parse_with_default_port(&uri, scheme))?;
        let host = parsed
            .host_str()
            .filter(|host| !host.is_empty())
            .ok_or(ConfigError::InvalidEndpoint)?;
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(ConfigError::InvalidEndpoint);
        }
        if rest.is_empty() {
            return Err(ConfigError::InvalidEndpoint);
        }

        Ok(Self {
            scheme,
            host: host.trim_matches(['[', ']']).into(),
            port: parsed.port().unwrap_or_else(|| scheme.default_port()),
        })
    }

    /// Returns the endpoint transport scheme.
    pub const fn scheme(&self) -> EndpointScheme {
        self.scheme
    }

    /// Returns the DNS name or IP literal without IPv6 brackets.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Returns the normalized endpoint port.
    pub const fn port(&self) -> u16 {
        self.port
    }
}

fn parse_with_default_port(uri: &str, scheme: EndpointScheme) -> Result<Url, ConfigError> {
    let (prefix, authority_and_path) = uri.split_once("://").ok_or(ConfigError::InvalidEndpoint)?;
    let authority = authority_and_path
        .split(['/', '?', '#'])
        .next()
        .filter(|authority| !authority.is_empty())
        .ok_or(ConfigError::InvalidEndpoint)?;
    let host = authority
        .strip_prefix('[')
        .and_then(|authority| {
            authority
                .find(']')
                .map(|closing_bracket| &authority[..closing_bracket])
        })
        .map(|host| format!("[{host}]"))
        .or_else(|| authority.rsplit_once(':').map(|(host, _)| host.into()))
        .filter(|host: &String| !host.is_empty())
        .ok_or(ConfigError::InvalidEndpoint)?;
    Url::parse(&format!("{prefix}://{host}:{}", scheme.default_port()))
        .map_err(|_| ConfigError::InvalidEndpoint)
}

/// Configuration for a DAX client.
#[derive(Clone)]
pub struct Config {
    endpoints: Vec<String>,
    region: Option<String>,
    credentials_provider: Option<SharedCredentialsProvider>,
    request_timeout: Duration,
    write_retries: u32,
    read_retries: u32,
    retry_delay: Duration,
    throttle_base_delay: Duration,
    throttle_max_backoff: Duration,
    max_pending_connections_per_host: i32,
    cluster_update_interval: Duration,
    cluster_update_threshold: Duration,
    idle_connection_reap_delay: Duration,
    client_health_check_interval: Duration,
    skip_hostname_verification: bool,
    route_manager_enabled: bool,
    ip_discovery: IpDiscovery,
    logger: Option<SharedLogger>,
    log_level: LogLevel,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            endpoints: Vec::new(),
            region: None,
            credentials_provider: None,
            request_timeout: Duration::from_secs(60),
            write_retries: 2,
            read_retries: 2,
            retry_delay: Duration::ZERO,
            throttle_base_delay: Duration::from_millis(70),
            throttle_max_backoff: Duration::from_secs(20),
            max_pending_connections_per_host: 10,
            cluster_update_interval: Duration::from_secs(4),
            cluster_update_threshold: Duration::from_millis(125),
            idle_connection_reap_delay: Duration::from_secs(30),
            client_health_check_interval: Duration::from_secs(5),
            skip_hostname_verification: false,
            route_manager_enabled: false,
            ip_discovery: IpDiscovery::Default,
            logger: None,
            log_level: LogLevel::Off,
        }
    }
}

impl Config {
    /// Creates a configuration builder with DAX-compatible defaults.
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder {
            config: Self::default(),
        }
    }

    /// Validates the configuration and normalizes all configured endpoints.
    pub fn endpoints(&self) -> Result<Vec<Endpoint>, ConfigError> {
        self.validate()?;
        let endpoints = self
            .endpoints
            .iter()
            .map(|endpoint| Endpoint::parse(endpoint))
            .collect::<Result<Vec<_>, _>>()?;
        validate_endpoint_schemes(&endpoints)?;
        Ok(endpoints)
    }

    /// Returns the configured request timeout.
    pub const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// Returns the configured write retry count.
    pub const fn write_retries(&self) -> u32 {
        self.write_retries
    }

    /// Returns the configured read retry count.
    pub const fn read_retries(&self) -> u32 {
        self.read_retries
    }

    /// Returns the configured retry delay.
    pub const fn retry_delay(&self) -> Duration {
        self.retry_delay
    }

    /// Returns the base delay used by throttling equal-jitter backoff.
    pub const fn throttle_base_delay(&self) -> Duration {
        self.throttle_base_delay
    }

    /// Returns the maximum throttling backoff.
    pub const fn throttle_max_backoff(&self) -> Duration {
        self.throttle_max_backoff
    }

    /// Returns the maximum simultaneous connection attempts per DAX node.
    pub const fn max_pending_connections_per_host(&self) -> i32 {
        self.max_pending_connections_per_host
    }

    /// Returns the interval between cluster endpoint refresh attempts.
    pub const fn cluster_update_interval(&self) -> Duration {
        self.cluster_update_interval
    }

    /// Returns the time budget for an individual cluster update.
    pub const fn cluster_update_threshold(&self) -> Duration {
        self.cluster_update_threshold
    }

    /// Returns the delay before idle connections are reaped.
    pub const fn idle_connection_reap_delay(&self) -> Duration {
        self.idle_connection_reap_delay
    }

    /// Returns the interval between node health checks.
    pub const fn client_health_check_interval(&self) -> Duration {
        self.client_health_check_interval
    }

    /// Returns whether TLS hostname verification is disabled.
    pub const fn skip_hostname_verification(&self) -> bool {
        self.skip_hostname_verification
    }

    /// Returns whether route-manager removal behavior is enabled.
    pub const fn route_manager_enabled(&self) -> bool {
        self.route_manager_enabled
    }

    /// Returns the configured IP discovery policy.
    pub const fn ip_discovery(&self) -> IpDiscovery {
        self.ip_discovery
    }

    /// Returns the minimum severity emitted to the configured logger.
    pub const fn log_level(&self) -> LogLevel {
        self.log_level
    }

    pub(crate) fn region(&self) -> &str {
        self.region
            .as_deref()
            .expect("validated DAX configuration has a region")
    }

    pub(crate) fn credentials_provider(&self) -> &SharedCredentialsProvider {
        self.credentials_provider
            .as_ref()
            .expect("validated DAX configuration has a credentials provider")
    }

    pub(crate) fn log(&self, level: LogLevel, message: &str) {
        if self.log_level >= level {
            if let Some(logger) = &self.logger {
                logger.log(level, message);
            }
        }
    }

    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        if self.endpoints.is_empty() {
            return Err(ConfigError::MissingEndpoint);
        }
        if self.region.as_deref().is_none_or(str::is_empty) {
            return Err(ConfigError::MissingRegion);
        }
        if self.credentials_provider.is_none() {
            return Err(ConfigError::MissingCredentials);
        }
        if self.max_pending_connections_per_host < 0 {
            return Err(ConfigError::NegativeMaxPendingConnections);
        }
        Ok(())
    }
}

impl fmt::Debug for Config {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Config")
            .field("endpoints", &"<redacted>")
            .field("region", &self.region)
            .field(
                "credentials_provider",
                &self.credentials_provider.as_ref().map(|_| "<redacted>"),
            )
            .field("request_timeout", &self.request_timeout)
            .field("write_retries", &self.write_retries)
            .field("read_retries", &self.read_retries)
            .field("retry_delay", &self.retry_delay)
            .field("throttle_base_delay", &self.throttle_base_delay)
            .field("throttle_max_backoff", &self.throttle_max_backoff)
            .field(
                "max_pending_connections_per_host",
                &self.max_pending_connections_per_host,
            )
            .field("cluster_update_interval", &self.cluster_update_interval)
            .field("cluster_update_threshold", &self.cluster_update_threshold)
            .field(
                "idle_connection_reap_delay",
                &self.idle_connection_reap_delay,
            )
            .field(
                "client_health_check_interval",
                &self.client_health_check_interval,
            )
            .field(
                "skip_hostname_verification",
                &self.skip_hostname_verification,
            )
            .field("route_manager_enabled", &self.route_manager_enabled)
            .field("ip_discovery", &self.ip_discovery)
            .field("logger", &self.logger.as_ref().map(|_| "<redacted>"))
            .field("log_level", &self.log_level)
            .finish()
    }
}

/// Builder for [`Config`].
#[derive(Debug)]
pub struct ConfigBuilder {
    config: Config,
}

impl ConfigBuilder {
    /// Adds a cluster discovery endpoint.
    pub fn endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.config.endpoints.push(endpoint.into());
        self
    }

    /// Sets the AWS region.
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.config.region = Some(region.into());
        self
    }

    /// Sets the AWS credentials provider used for DAX authorization.
    pub fn credentials_provider(mut self, provider: SharedCredentialsProvider) -> Self {
        self.config.credentials_provider = Some(provider);
        self
    }

    /// Sets the number of additional attempts for both DAX reads and writes.
    pub fn retry_max_attempts(mut self, attempts: u32) -> Self {
        self.config.read_retries = attempts;
        self.config.write_retries = attempts;
        self
    }

    /// Sets the number of additional attempts for DAX write operations.
    pub const fn write_retries(mut self, retries: u32) -> Self {
        self.config.write_retries = retries;
        self
    }

    /// Sets the number of additional attempts for DAX read operations.
    pub const fn read_retries(mut self, retries: u32) -> Self {
        self.config.read_retries = retries;
        self
    }

    /// Sets the timeout used when a caller does not supply a deadline.
    pub const fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Sets the fixed delay added before retrying non-throttle failures.
    ///
    /// Throttled responses use the Go-compatible equal-jitter backoff,
    /// independent of this value.
    pub const fn retry_delay(mut self, delay: Duration) -> Self {
        self.config.retry_delay = delay;
        self
    }

    /// Sets the base delay used by throttling equal-jitter backoff.
    ///
    /// A zero duration restores the Go-compatible default of 70 milliseconds.
    pub const fn throttle_base_delay(mut self, delay: Duration) -> Self {
        self.config.throttle_base_delay = delay;
        self
    }

    /// Sets the maximum throttling backoff.
    ///
    /// A zero duration restores the Go-compatible default of 20 seconds.
    pub const fn throttle_max_backoff(mut self, delay: Duration) -> Self {
        self.config.throttle_max_backoff = delay;
        self
    }

    /// Sets the IP address family policy for endpoint discovery.
    pub const fn ip_discovery(mut self, policy: IpDiscovery) -> Self {
        self.config.ip_discovery = policy;
        self
    }

    /// Sets the maximum simultaneous connection attempts per DAX node.
    pub const fn max_pending_connections_per_host(mut self, maximum: i32) -> Self {
        self.config.max_pending_connections_per_host = maximum;
        self
    }

    /// Sets the interval between cluster endpoint refresh attempts.
    pub const fn cluster_update_interval(mut self, interval: Duration) -> Self {
        self.config.cluster_update_interval = interval;
        self
    }

    /// Sets the time budget for an individual cluster update.
    pub const fn cluster_update_threshold(mut self, threshold: Duration) -> Self {
        self.config.cluster_update_threshold = threshold;
        self
    }

    /// Sets the delay before idle connections are reaped.
    pub const fn idle_connection_reap_delay(mut self, delay: Duration) -> Self {
        self.config.idle_connection_reap_delay = delay;
        self
    }

    /// Sets the interval between node health checks.
    pub const fn client_health_check_interval(mut self, interval: Duration) -> Self {
        self.config.client_health_check_interval = interval;
        self
    }

    /// Controls TLS hostname verification for encrypted endpoints.
    ///
    /// Disabling verification weakens endpoint authentication and should be used
    /// only when an application has an explicit compensating control.
    pub const fn skip_hostname_verification(mut self, skip: bool) -> Self {
        self.config.skip_hostname_verification = skip;
        self
    }

    /// Enables temporary removal of routes that encounter network errors.
    pub const fn route_manager_enabled(mut self, enabled: bool) -> Self {
        self.config.route_manager_enabled = enabled;
        self
    }

    /// Sets the diagnostic logger used by this client.
    pub fn logger(mut self, logger: Arc<dyn Logger>) -> Self {
        self.config.logger = Some(logger);
        self
    }

    /// Sets the minimum diagnostic severity sent to the configured logger.
    pub const fn log_level(mut self, level: LogLevel) -> Self {
        self.config.log_level = level;
        self
    }

    /// Finalizes the configuration after validating its deterministic fields.
    pub fn build(self) -> Result<Config, ConfigError> {
        self.config.endpoints()?;
        Ok(self.config)
    }
}

fn validate_endpoint_schemes(endpoints: &[Endpoint]) -> Result<(), ConfigError> {
    let Some(first) = endpoints.first() else {
        return Err(ConfigError::MissingEndpoint);
    };
    if endpoints
        .iter()
        .any(|endpoint| endpoint.scheme != first.scheme)
    {
        return Err(ConfigError::InconsistentEndpointSchemes);
    }
    if first.scheme.is_encrypted() && endpoints.len() > 1 {
        return Err(ConfigError::MultipleEncryptedEndpoints);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Config, Endpoint, EndpointScheme};
    use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
    use std::time::Duration;

    #[test]
    fn uses_go_compatible_default_retry_budgets() {
        let config = Config::builder()
            .endpoint("dax://cluster.example.com")
            .region("us-east-1")
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "key", "secret", None, None, "test",
            )))
            .build()
            .expect("valid default configuration");

        assert_eq!(config.read_retries(), 2);
        assert_eq!(config.write_retries(), 2);
        assert_eq!(config.retry_delay(), Duration::ZERO);
        assert_eq!(config.throttle_base_delay(), Duration::from_millis(70));
        assert_eq!(config.throttle_max_backoff(), Duration::from_secs(20));
    }

    #[test]
    fn uses_go_compatible_default_lifecycle_settings() {
        let config = Config::builder()
            .endpoint("dax://cluster.example.com")
            .region("us-east-1")
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "key", "secret", None, None, "test",
            )))
            .build()
            .expect("valid default configuration");

        assert_eq!(config.max_pending_connections_per_host(), 10);
        assert_eq!(config.cluster_update_interval(), Duration::from_secs(4));
        assert_eq!(
            config.cluster_update_threshold(),
            Duration::from_millis(125)
        );
        assert_eq!(config.idle_connection_reap_delay(), Duration::from_secs(30));
        assert_eq!(
            config.client_health_check_interval(),
            Duration::from_secs(5)
        );
        assert!(!config.route_manager_enabled());
        assert!(!config.skip_hostname_verification());
    }

    #[test]
    fn preserves_independent_read_and_write_retry_budgets() {
        let config = Config::builder()
            .endpoint("dax://cluster.example.com")
            .region("us-east-1")
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "key", "secret", None, None, "test",
            )))
            .read_retries(1)
            .write_retries(3)
            .build()
            .expect("valid retry configuration");

        assert_eq!(config.read_retries(), 1);
        assert_eq!(config.write_retries(), 3);
    }

    #[test]
    fn preserves_configured_retry_delay() {
        let config = Config::builder()
            .endpoint("dax://cluster.example.com")
            .region("us-east-1")
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "key", "secret", None, None, "test",
            )))
            .retry_delay(Duration::from_millis(70))
            .build()
            .expect("valid configuration");

        assert_eq!(config.retry_delay(), Duration::from_millis(70));
    }

    #[test]
    fn preserves_configured_throttle_backoff_bounds() {
        let config = Config::builder()
            .endpoint("dax://cluster.example.com")
            .region("us-east-1")
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "key", "secret", None, None, "test",
            )))
            .throttle_base_delay(Duration::from_millis(5))
            .throttle_max_backoff(Duration::from_secs(3))
            .build()
            .expect("valid throttle configuration");

        assert_eq!(config.throttle_base_delay(), Duration::from_millis(5));
        assert_eq!(config.throttle_max_backoff(), Duration::from_secs(3));
    }

    #[test]
    fn rejects_missing_region_before_transport_configuration() {
        let error = Config::builder()
            .endpoint("dax://cluster.example.com")
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "key", "secret", None, None, "test",
            )))
            .build()
            .expect_err("missing region must be rejected");

        assert_eq!(error, super::ConfigError::MissingRegion);
    }

    #[test]
    fn rejects_missing_credentials_before_transport_configuration() {
        let error = Config::builder()
            .endpoint("dax://cluster.example.com")
            .region("us-east-1")
            .build()
            .expect_err("missing credentials must be rejected");

        assert_eq!(error, super::ConfigError::MissingCredentials);
    }

    #[test]
    fn applies_scheme_specific_default_ports() {
        let dax = Endpoint::parse("dax://cluster.example.com").expect("valid DAX endpoint");
        let daxs = Endpoint::parse("daxs://cluster.example.com").expect("valid TLS endpoint");

        assert_eq!(dax.scheme(), EndpointScheme::Dax);
        assert_eq!(dax.port(), 8111);
        assert_eq!(daxs.scheme(), EndpointScheme::Daxs);
        assert_eq!(daxs.port(), 9111);
    }

    #[test]
    fn rejects_mixed_and_multiple_encrypted_endpoints() {
        let mixed = Config::builder()
            .endpoint("dax://cluster.example.com")
            .endpoint("daxs://other.example.com")
            .region("us-east-1")
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "key", "secret", None, None, "test",
            )))
            .build()
            .expect_err("mixed endpoint schemes must be rejected");
        assert_eq!(mixed, super::ConfigError::InconsistentEndpointSchemes);

        let encrypted = Config::builder()
            .endpoint("daxs://cluster.example.com")
            .endpoint("daxs://other.example.com")
            .region("us-east-1")
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "key", "secret", None, None, "test",
            )))
            .build()
            .expect_err("multiple encrypted endpoints must be rejected");
        assert_eq!(encrypted, super::ConfigError::MultipleEncryptedEndpoints);
    }

    #[test]
    fn rejects_unsupported_endpoint_scheme() {
        assert_eq!(
            Endpoint::parse("https://cluster.example.com").unwrap_err(),
            super::ConfigError::UnsupportedEndpointScheme
        );
    }

    #[test]
    fn parses_scheme_less_and_bracketed_ipv6_endpoints() {
        let scheme_less =
            Endpoint::parse("cluster.example.com:8123").expect("valid scheme-less endpoint");
        assert_eq!(scheme_less.scheme(), EndpointScheme::Dax);
        assert_eq!(scheme_less.host(), "cluster.example.com");
        assert_eq!(scheme_less.port(), 8123);

        let ipv6 = Endpoint::parse("dax://[2001:db8::1]:8123").expect("valid IPv6 endpoint");
        assert_eq!(ipv6.host(), "2001:db8::1");
        assert_eq!(ipv6.port(), 8123);
    }

    #[test]
    fn rejects_bare_hostname_without_scheme_or_port() {
        assert_eq!(
            Endpoint::parse("cluster.example.com").unwrap_err(),
            super::ConfigError::InvalidEndpoint
        );
    }
}
