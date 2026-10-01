use crate::{
    DaxClient, DaxDiscoveredEndpoint, DaxEndpointRefresh, DaxFfiError, DaxRouteSnapshot, runtime,
};

#[uniffi::export]
impl DaxClient {
    /// Discovers and commits the validated cluster roster through the SDK.
    #[allow(
        clippy::result_large_err,
        reason = "UniFFI rich errors cross the ABI by value and cannot be boxed"
    )]
    pub async fn discover_endpoints(&self) -> Result<Vec<DaxDiscoveredEndpoint>, DaxFfiError> {
        let client = self.inner.clone();
        runtime::run(async move {
            Ok(client
                .discover_endpoints()
                .await?
                .into_iter()
                .map(Into::into)
                .collect())
        })
        .await
    }

    /// Refreshes the validated cluster roster and returns its node-level diff.
    #[allow(
        clippy::result_large_err,
        reason = "UniFFI rich errors cross the ABI by value and cannot be boxed"
    )]
    pub async fn refresh_endpoints(&self) -> Result<DaxEndpointRefresh, DaxFfiError> {
        let client = self.inner.clone();
        runtime::run(async move { Ok(client.refresh_endpoints().await?.into()) }).await
    }

    /// Returns the last validated roster without network I/O.
    #[allow(
        clippy::result_large_err,
        reason = "UniFFI errors cross the ABI by value"
    )]
    pub fn active_endpoints(&self) -> Result<Option<Vec<DaxDiscoveredEndpoint>>, DaxFfiError> {
        Ok(self
            .inner
            .active_endpoints()?
            .map(|endpoints| endpoints.into_iter().map(Into::into).collect()))
    }

    /// Returns validated route targets and health state without network I/O.
    #[allow(
        clippy::result_large_err,
        reason = "UniFFI errors cross the ABI by value"
    )]
    pub fn route_snapshot(&self) -> Result<Vec<DaxRouteSnapshot>, DaxFfiError> {
        Ok(self
            .inner
            .route_snapshot()?
            .into_iter()
            .map(Into::into)
            .collect())
    }

    /// Returns the SDK's most recent background refresh diagnostic.
    pub fn last_refresh_error(&self) -> Option<String> {
        self.inner.last_refresh_error()
    }
}

impl From<aws_dax::DiscoveredEndpoint> for DaxDiscoveredEndpoint {
    fn from(value: aws_dax::DiscoveredEndpoint) -> Self {
        Self {
            node_id: value.node_id,
            hostname: value.hostname,
            address: value.address,
            port: value.port,
            role: value.role,
            availability_zone: value.availability_zone,
            leader_session_id: value.leader_session_id,
        }
    }
}

impl From<aws_dax::EndpointRefresh> for DaxEndpointRefresh {
    fn from(value: aws_dax::EndpointRefresh) -> Self {
        Self {
            added: value.added,
            removed: value.removed,
            retained: value.retained,
            changed: value.changed,
        }
    }
}

impl From<aws_dax::RouteSnapshot> for DaxRouteSnapshot {
    fn from(value: aws_dax::RouteSnapshot) -> Self {
        Self {
            node_id: value.node_id,
            dial_address: value.dial_address,
            tls_server_name: value.tls_server_name,
            healthy: value.healthy,
            health_probe_healthy: value.health_probe_healthy,
        }
    }
}
