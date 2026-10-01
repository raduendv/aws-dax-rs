//! Cluster endpoint normalization used by discovery and future routing.

use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{
        Arc, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::{Config, Error};

use super::cbor::DiscoveredEndpoint;

/// A discovered endpoint after address-family filtering and socket validation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedEndpoint {
    pub(crate) node_id: i64,
    pub(crate) hostname: String,
    pub(crate) address: IpAddr,
    pub(crate) port: u16,
    pub(crate) role: i64,
    pub(crate) availability_zone: Option<String>,
    pub(crate) leader_session_id: Option<i64>,
}

impl ResolvedEndpoint {
    pub(crate) fn socket_addr(&self) -> SocketAddr {
        SocketAddr::new(self.address, self.port)
    }

    pub(crate) fn dial_address(&self) -> String {
        self.socket_addr().to_string()
    }

    pub(crate) fn tls_server_name(&self) -> &str {
        &self.hostname
    }
}

/// Normalizes a complete DAX endpoint roster without mutating active routes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EndpointRoster {
    endpoints: Vec<ResolvedEndpoint>,
}

/// Thread-safe active roster that only commits complete validated snapshots.
#[derive(Debug, Clone, Default)]
pub(crate) struct EndpointRosterStore {
    active: Arc<RwLock<Option<EndpointRoster>>>,
}

/// Deterministic node selection over the currently active endpoint roster.
#[derive(Debug, Clone, Default)]
pub(crate) struct RouteTable {
    roster: EndpointRosterStore,
    cursor: Arc<AtomicUsize>,
    refresh: Arc<tokio::sync::Mutex<()>>,
    failures: Arc<RwLock<HashMap<i64, u32>>>,
    health_failures: Arc<RwLock<HashMap<i64, u32>>>,
}

const ROUTE_FAILURE_THRESHOLD: u32 = 3;
const HEALTH_FAILURE_THRESHOLD: u32 = 5;

/// Reason route selection did not produce a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RouteUnavailable {
    /// Discovery has not committed an endpoint roster yet.
    NoRoster,
    /// Every node in the committed roster is currently unhealthy.
    AllNodesUnhealthy,
}

/// Internal transport-ready route metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RouteTarget {
    pub(crate) node_id: i64,
    pub(crate) dial_address: String,
    pub(crate) tls_server_name: String,
}

/// A node-scoped connection-pool key used by the future routed executor.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct RouteKey {
    pub(crate) node_id: i64,
    pub(crate) dial_address: String,
}

impl From<&RouteTarget> for RouteKey {
    fn from(target: &RouteTarget) -> Self {
        Self {
            node_id: target.node_id,
            dial_address: target.dial_address.clone(),
        }
    }
}

impl From<ResolvedEndpoint> for RouteTarget {
    fn from(endpoint: ResolvedEndpoint) -> Self {
        Self {
            node_id: endpoint.node_id,
            dial_address: endpoint.dial_address(),
            tls_server_name: endpoint.tls_server_name().into(),
        }
    }
}

impl RouteTable {
    pub(crate) fn replace(&self, next: EndpointRoster) -> Result<Option<RosterDiff>, Error> {
        let diff = self.roster.replace(next)?;
        self.cursor.store(0, Ordering::Release);
        let active_ids = self
            .roster
            .snapshot()?
            .map(|roster| {
                roster
                    .node_ids()
                    .map(|node_id| (node_id, ()))
                    .collect::<HashMap<_, _>>()
            })
            .unwrap_or_default();
        self.failures
            .write()
            .map_err(|_| Error::Protocol {
                operation: "Endpoints",
                message: "route health lock is poisoned".into(),
            })?
            .retain(|node_id, _| active_ids.contains_key(node_id));
        self.health_failures
            .write()
            .map_err(|_| Error::Protocol {
                operation: "Endpoints",
                message: "route health lock is poisoned".into(),
            })?
            .retain(|node_id, _| active_ids.contains_key(node_id));
        Ok(diff)
    }

    pub(crate) async fn replace_serialized(
        &self,
        next: EndpointRoster,
    ) -> Result<Option<RosterDiff>, Error> {
        let _guard = self.refresh.lock().await;
        self.replace(next)
    }

    pub(crate) fn next(&self) -> Result<Option<ResolvedEndpoint>, Error> {
        self.next_after(None)
    }

    pub(crate) fn next_after(
        &self,
        previous_node_id: Option<i64>,
    ) -> Result<Option<ResolvedEndpoint>, Error> {
        let snapshot = self.roster.snapshot()?;
        let Some(roster) = snapshot else {
            return Ok(None);
        };
        let endpoints = roster.endpoints();
        if endpoints.is_empty() {
            return Ok(None);
        }

        let failures = self.failures.read().map_err(|_| Error::Protocol {
            operation: "RouteSelection",
            message: "route health lock is poisoned".into(),
        })?;
        for _ in 0..endpoints.len() {
            let index = self.cursor.fetch_add(1, Ordering::AcqRel) % endpoints.len();
            if failures
                .get(&endpoints[index].node_id)
                .is_none_or(|failures| *failures < ROUTE_FAILURE_THRESHOLD)
                && previous_node_id != Some(endpoints[index].node_id)
            {
                return Ok(Some(endpoints[index].clone()));
            }
        }
        if previous_node_id.is_some() {
            return self.next_after(None);
        }
        Ok(None)
    }

    pub(crate) fn next_route(
        &self,
        previous_node_id: Option<i64>,
    ) -> Result<Result<RouteTarget, RouteUnavailable>, Error> {
        if let Some(route) = self.next_after(previous_node_id)? {
            return Ok(Ok(route.into()));
        }
        if self.roster.snapshot()?.is_none() {
            Ok(Err(RouteUnavailable::NoRoster))
        } else {
            Ok(Err(RouteUnavailable::AllNodesUnhealthy))
        }
    }

    pub(crate) fn next_with_reason(
        &self,
    ) -> Result<Result<ResolvedEndpoint, RouteUnavailable>, Error> {
        let snapshot = self.roster.snapshot()?;
        let Some(roster) = snapshot else {
            return Ok(Err(RouteUnavailable::NoRoster));
        };
        let endpoints = roster.endpoints();
        if endpoints.is_empty() {
            return Ok(Err(RouteUnavailable::NoRoster));
        }
        let failures = self.failures.read().map_err(|_| Error::Protocol {
            operation: "RouteSelection",
            message: "route health lock is poisoned".into(),
        })?;
        for _ in 0..endpoints.len() {
            let index = self.cursor.fetch_add(1, Ordering::AcqRel) % endpoints.len();
            if failures
                .get(&endpoints[index].node_id)
                .is_none_or(|failures| *failures < ROUTE_FAILURE_THRESHOLD)
            {
                return Ok(Ok(endpoints[index].clone()));
            }
        }
        Ok(Err(RouteUnavailable::AllNodesUnhealthy))
    }

    pub(crate) fn discovered(&self) -> Result<Option<Vec<DiscoveredEndpoint>>, Error> {
        self.roster.discovered()
    }

    pub(crate) fn route_targets(&self) -> Result<Vec<RouteTarget>, Error> {
        Ok(self
            .roster
            .snapshot()?
            .map(|roster| {
                roster
                    .endpoints()
                    .iter()
                    .cloned()
                    .map(RouteTarget::from)
                    .collect()
            })
            .unwrap_or_default())
    }

    pub(crate) fn healthy_route_targets(&self) -> Result<Vec<RouteTarget>, Error> {
        let snapshot = self.roster.snapshot()?;
        let Some(roster) = snapshot else {
            return Ok(Vec::new());
        };
        let failures = self.failures.read().map_err(|_| Error::Protocol {
            operation: "RouteSelection",
            message: "route health lock is poisoned".into(),
        })?;
        Ok(roster
            .endpoints()
            .iter()
            .filter(|endpoint| {
                failures
                    .get(&endpoint.node_id)
                    .is_none_or(|failures| *failures < ROUTE_FAILURE_THRESHOLD)
            })
            .cloned()
            .map(RouteTarget::from)
            .collect())
    }

    pub(crate) fn route_snapshots(&self) -> Result<Vec<(RouteTarget, bool, bool)>, Error> {
        let targets = self.route_targets()?;
        let healthy = self
            .healthy_route_targets()?
            .into_iter()
            .map(|target| RouteKey::from(&target))
            .collect::<std::collections::HashSet<_>>();
        let health_failures = self.health_failures.read().map_err(|_| Error::Protocol {
            operation: "HealthCheck",
            message: "route health lock is poisoned".into(),
        })?;
        Ok(targets
            .into_iter()
            .map(|target| {
                let is_healthy = healthy.contains(&RouteKey::from(&target));
                let health_probe_healthy = health_failures
                    .get(&target.node_id)
                    .is_none_or(|failures| *failures < HEALTH_FAILURE_THRESHOLD);
                (target, is_healthy, health_probe_healthy)
            })
            .collect())
    }

    pub(crate) fn mark_unhealthy(&self, node_id: i64) -> Result<bool, Error> {
        let known = self
            .roster
            .snapshot()?
            .is_some_and(|roster| roster.node_ids().any(|id| id == node_id));
        if !known {
            return Ok(false);
        }
        self.failures
            .write()
            .map_err(|_| Error::Protocol {
                operation: "RouteSelection",
                message: "route health lock is poisoned".into(),
            })?
            .insert(node_id, ROUTE_FAILURE_THRESHOLD);
        Ok(true)
    }

    pub(crate) fn mark_healthy(&self, node_id: i64) -> Result<bool, Error> {
        Ok(self
            .failures
            .write()
            .map_err(|_| Error::Protocol {
                operation: "RouteSelection",
                message: "route health lock is poisoned".into(),
            })?
            .remove(&node_id)
            .is_some())
    }

    pub(crate) fn record_failure(&self, node_id: i64) -> Result<bool, Error> {
        let known = self
            .roster
            .snapshot()?
            .is_some_and(|roster| roster.node_ids().any(|id| id == node_id));
        if !known {
            return Ok(false);
        }
        let mut failures = self.failures.write().map_err(|_| Error::Protocol {
            operation: "RouteSelection",
            message: "route health lock is poisoned".into(),
        })?;
        let count = failures.entry(node_id).or_insert(0);
        *count = count.saturating_add(1);
        Ok(*count >= ROUTE_FAILURE_THRESHOLD)
    }

    pub(crate) fn record_success(&self, node_id: i64) -> Result<bool, Error> {
        self.mark_healthy(node_id)
    }

    pub(crate) fn record_health_failure(&self, node_id: i64) -> Result<bool, Error> {
        let known = self
            .roster
            .snapshot()?
            .is_some_and(|roster| roster.node_ids().any(|id| id == node_id));
        if !known {
            return Ok(false);
        }
        let mut failures = self.health_failures.write().map_err(|_| Error::Protocol {
            operation: "HealthCheck",
            message: "route health lock is poisoned".into(),
        })?;
        let count = failures.entry(node_id).or_insert(0);
        *count = count.saturating_add(1);
        Ok(*count >= HEALTH_FAILURE_THRESHOLD)
    }

    pub(crate) fn record_health_success(&self, node_id: i64) -> Result<bool, Error> {
        Ok(self
            .health_failures
            .write()
            .map_err(|_| Error::Protocol {
                operation: "HealthCheck",
                message: "route health lock is poisoned".into(),
            })?
            .remove(&node_id)
            .is_some())
    }

    pub(crate) fn restore_all_routes(&self) -> Result<(), Error> {
        self.failures
            .write()
            .map_err(|_| Error::Protocol {
                operation: "RouteSelection",
                message: "route health lock is poisoned".into(),
            })?
            .clear();
        self.health_failures
            .write()
            .map_err(|_| Error::Protocol {
                operation: "RouteSelection",
                message: "route health lock is poisoned".into(),
            })?
            .clear();
        Ok(())
    }
}

impl EndpointRosterStore {
    pub(crate) fn replace(&self, next: EndpointRoster) -> Result<Option<RosterDiff>, Error> {
        let mut active = self.active.write().map_err(|_| Error::Protocol {
            operation: "Endpoints",
            message: "endpoint roster lock is poisoned".into(),
        })?;
        let diff = active.as_ref().map(|current| current.diff(&next));
        *active = Some(next);
        Ok(diff)
    }

    pub(crate) fn snapshot(&self) -> Result<Option<EndpointRoster>, Error> {
        self.active
            .read()
            .map(|active| active.clone())
            .map_err(|_| Error::Protocol {
                operation: "Endpoints",
                message: "endpoint roster lock is poisoned".into(),
            })
    }

    pub(crate) fn discovered(&self) -> Result<Option<Vec<DiscoveredEndpoint>>, Error> {
        self.snapshot()
            .map(|roster| roster.map(|roster| roster.discovered()))
    }
}

/// The node-level effect of replacing one validated roster with another.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RosterDiff {
    pub(crate) added: Vec<i64>,
    pub(crate) removed: Vec<i64>,
    pub(crate) retained: Vec<i64>,
    pub(crate) changed: Vec<i64>,
}

/// Public summary of one committed endpoint-roster refresh.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointRefresh {
    /// Node IDs newly present in the committed roster.
    pub added: Vec<i64>,
    /// Node IDs no longer present in the committed roster.
    pub removed: Vec<i64>,
    /// Node IDs whose endpoint metadata did not change.
    pub retained: Vec<i64>,
    /// Node IDs retained but with changed endpoint metadata.
    pub changed: Vec<i64>,
}

impl From<RosterDiff> for EndpointRefresh {
    fn from(diff: RosterDiff) -> Self {
        Self {
            added: diff.added,
            removed: diff.removed,
            retained: diff.retained,
            changed: diff.changed,
        }
    }
}

impl EndpointRoster {
    /// Builds a roster using the configured address-family policy.
    pub(crate) fn from_discovered(
        discovered: &[DiscoveredEndpoint],
        config: &Config,
    ) -> Result<Self, Error> {
        let addresses = discovered
            .iter()
            .map(endpoint_ip)
            .collect::<Result<Vec<_>, _>>()?;
        let selected = config.ip_discovery().select_addresses(&addresses)?;
        let mut endpoints = Vec::with_capacity(selected.len());
        let mut node_ids = HashMap::with_capacity(selected.len());

        for (endpoint, address) in discovered.iter().zip(addresses) {
            if !selected.contains(&address) {
                continue;
            }
            let port = u16::try_from(endpoint.port).map_err(|_| Error::Validation {
                message: format!(
                    "DAX endpoint {} has invalid port {}",
                    endpoint.hostname, endpoint.port
                ),
            })?;
            if node_ids.insert(endpoint.node_id, ()).is_some() {
                return Err(Error::Validation {
                    message: format!(
                        "DAX endpoint discovery returned duplicate node ID {}",
                        endpoint.node_id
                    ),
                });
            }
            endpoints.push(ResolvedEndpoint {
                node_id: endpoint.node_id,
                hostname: if endpoint.hostname.is_empty() {
                    config
                        .endpoints()?
                        .first()
                        .map(|seed| seed.host().to_owned())
                        .unwrap_or_default()
                } else {
                    endpoint.hostname.clone()
                },
                address,
                port,
                role: endpoint.role,
                availability_zone: endpoint.availability_zone.clone(),
                leader_session_id: endpoint.leader_session_id,
            });
        }

        if endpoints.is_empty() {
            return Err(Error::Validation {
                message: "DAX endpoint discovery returned no usable endpoints".into(),
            });
        }
        Ok(Self { endpoints })
    }

    pub(crate) fn endpoints(&self) -> &[ResolvedEndpoint] {
        &self.endpoints
    }

    pub(crate) fn node_ids(&self) -> impl Iterator<Item = i64> + '_ {
        self.endpoints.iter().map(|endpoint| endpoint.node_id)
    }

    fn discovered(&self) -> Vec<DiscoveredEndpoint> {
        self.endpoints
            .iter()
            .map(|endpoint| DiscoveredEndpoint {
                node_id: endpoint.node_id,
                hostname: endpoint.hostname.clone(),
                address: match endpoint.address {
                    IpAddr::V4(address) => address.octets().to_vec(),
                    IpAddr::V6(address) => address.octets().to_vec(),
                },
                port: i64::from(endpoint.port),
                role: endpoint.role,
                availability_zone: endpoint.availability_zone.clone(),
                leader_session_id: endpoint.leader_session_id,
            })
            .collect()
    }

    /// Compares two complete rosters by stable node ID.
    pub(crate) fn diff(&self, next: &Self) -> RosterDiff {
        let current = self
            .endpoints
            .iter()
            .map(|endpoint| (endpoint.node_id, endpoint))
            .collect::<HashMap<_, _>>();
        let replacement = next
            .endpoints
            .iter()
            .map(|endpoint| (endpoint.node_id, endpoint))
            .collect::<HashMap<_, _>>();
        let mut diff = RosterDiff {
            added: Vec::new(),
            removed: Vec::new(),
            retained: Vec::new(),
            changed: Vec::new(),
        };
        for endpoint in &next.endpoints {
            match current.get(&endpoint.node_id) {
                None => diff.added.push(endpoint.node_id),
                Some(previous) if *previous == endpoint => diff.retained.push(endpoint.node_id),
                Some(_) => diff.changed.push(endpoint.node_id),
            }
        }
        for endpoint in &self.endpoints {
            if !replacement.contains_key(&endpoint.node_id) {
                diff.removed.push(endpoint.node_id);
            }
        }
        diff
    }

    /// Replaces this roster only after the replacement has already validated.
    pub(crate) fn replace_with(&mut self, next: Self) -> RosterDiff {
        let diff = self.diff(&next);
        *self = next;
        diff
    }
}

fn endpoint_ip(endpoint: &DiscoveredEndpoint) -> Result<IpAddr, Error> {
    match endpoint.address.as_slice() {
        [a, b, c, d] => Ok(IpAddr::V4(Ipv4Addr::new(*a, *b, *c, *d))),
        [a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p] => Ok(IpAddr::V6(Ipv6Addr::from([
            *a, *b, *c, *d, *e, *f, *g, *h, *i, *j, *k, *l, *m, *n, *o, *p,
        ]))),
        _ => Err(Error::Validation {
            message: format!(
                "DAX endpoint {} has an invalid IP address length {}",
                endpoint.hostname,
                endpoint.address.len()
            ),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::{EndpointRoster, ResolvedEndpoint};
    use crate::{Config, IpDiscovery};
    use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};

    fn config(policy: IpDiscovery) -> Config {
        Config::builder()
            .endpoint("dax://seed.example:8111")
            .region("us-east-1")
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "a", "b", None, None, "test",
            )))
            .ip_discovery(policy)
            .build()
            .expect("valid test config")
    }

    fn endpoint(address: &[u8]) -> crate::DiscoveredEndpoint {
        crate::DiscoveredEndpoint {
            node_id: 1,
            hostname: "node.example".into(),
            address: address.into(),
            port: 8111,
            role: 1,
            availability_zone: None,
            leader_session_id: None,
        }
    }

    #[test]
    fn default_policy_prefers_ipv4_and_preserves_order() {
        let roster = EndpointRoster::from_discovered(
            &[
                endpoint(&[0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1]),
                endpoint(&[127, 0, 0, 1]),
            ],
            &config(IpDiscovery::Default),
        )
        .expect("dual-stack roster");
        assert_eq!(
            roster.endpoints()[0].socket_addr().to_string(),
            "127.0.0.1:8111"
        );
    }

    #[test]
    fn ipv6_policy_rejects_ipv4_only_roster() {
        let error = EndpointRoster::from_discovered(
            &[endpoint(&[127, 0, 0, 1])],
            &config(IpDiscovery::Ipv6),
        )
        .expect_err("family mismatch");
        assert!(error.to_string().contains("does not match"));
    }

    #[test]
    fn malformed_address_is_explicitly_rejected() {
        let error =
            EndpointRoster::from_discovered(&[endpoint(&[1, 2, 3])], &config(IpDiscovery::Default))
                .expect_err("invalid address");
        assert!(error.to_string().contains("invalid IP address length"));
    }

    #[test]
    fn socket_addr_preserves_node_metadata() {
        let endpoint = ResolvedEndpoint {
            node_id: 3,
            hostname: "node".into(),
            address: "192.0.2.1".parse().unwrap(),
            port: 8121,
            role: 2,
            availability_zone: Some("az".into()),
            leader_session_id: Some(9),
        };
        assert_eq!(endpoint.socket_addr().to_string(), "192.0.2.1:8121");
        assert_eq!(endpoint.dial_address(), "192.0.2.1:8121");
        assert_eq!(endpoint.role, 2);
    }

    #[test]
    fn dial_address_brackets_ipv6_literals() {
        let endpoint = ResolvedEndpoint {
            node_id: 4,
            hostname: "node".into(),
            address: "2001:db8::1".parse().unwrap(),
            port: 9111,
            role: 1,
            availability_zone: None,
            leader_session_id: None,
        };
        assert_eq!(endpoint.dial_address(), "[2001:db8::1]:9111");
        assert_eq!(endpoint.tls_server_name(), "node");
    }

    #[test]
    fn route_target_contains_transport_metadata_without_connecting() {
        let table = super::RouteTable::default();
        let roster = EndpointRoster::from_discovered(
            &[endpoint(&[127, 0, 0, 1])],
            &config(IpDiscovery::Ipv4),
        )
        .expect("route roster");
        table.replace(roster).expect("route replacement");
        let target = table
            .next_route(None)
            .expect("route selection")
            .expect("route target");
        assert_eq!(target.node_id, 1);
        assert_eq!(target.dial_address, "127.0.0.1:8111");
        assert_eq!(target.tls_server_name, "node.example");
        assert_eq!(
            super::RouteKey::from(&target),
            super::RouteKey {
                node_id: 1,
                dial_address: "127.0.0.1:8111".into(),
            }
        );
    }

    #[test]
    fn route_targets_follow_the_committed_roster_order() {
        let table = super::RouteTable::default();
        let roster = EndpointRoster::from_discovered(
            &[endpoint(&[127, 0, 0, 1]), {
                let mut endpoint = endpoint(&[127, 0, 0, 2]);
                endpoint.node_id = 2;
                endpoint
            }],
            &config(IpDiscovery::Ipv4),
        )
        .expect("route roster");
        table.replace(roster).expect("route replacement");
        let targets = table.route_targets().expect("route targets");
        assert_eq!(
            targets
                .iter()
                .map(|target| target.node_id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn healthy_route_targets_exclude_thresholded_nodes() {
        let table = super::RouteTable::default();
        let roster = EndpointRoster::from_discovered(
            &[endpoint(&[127, 0, 0, 1]), {
                let mut endpoint = endpoint(&[127, 0, 0, 2]);
                endpoint.node_id = 2;
                endpoint
            }],
            &config(IpDiscovery::Ipv4),
        )
        .expect("route roster");
        table.replace(roster).expect("route replacement");
        table.record_failure(1).expect("failure");
        table.record_failure(1).expect("failure");
        table.record_failure(1).expect("failure");
        let targets = table.healthy_route_targets().expect("healthy routes");
        assert_eq!(
            targets
                .iter()
                .map(|target| target.node_id)
                .collect::<Vec<_>>(),
            vec![2]
        );
    }

    #[test]
    fn roster_diff_tracks_added_removed_and_changed_nodes() {
        let current = EndpointRoster::from_discovered(
            &[endpoint(&[127, 0, 0, 1]), {
                let mut endpoint = endpoint(&[127, 0, 0, 2]);
                endpoint.node_id = 2;
                endpoint
            }],
            &config(IpDiscovery::Ipv4),
        )
        .expect("current roster");
        let next = EndpointRoster::from_discovered(
            &[
                {
                    let mut endpoint = endpoint(&[127, 0, 0, 3]);
                    endpoint.node_id = 1;
                    endpoint
                },
                {
                    let mut endpoint = endpoint(&[127, 0, 0, 4]);
                    endpoint.node_id = 3;
                    endpoint
                },
            ],
            &config(IpDiscovery::Ipv4),
        )
        .expect("replacement roster");

        assert_eq!(
            current.diff(&next),
            super::RosterDiff {
                added: vec![3],
                removed: vec![2],
                retained: Vec::new(),
                changed: vec![1],
            }
        );
    }

    #[test]
    fn duplicate_node_ids_are_rejected_before_roster_creation() {
        let mut duplicate = endpoint(&[127, 0, 0, 2]);
        duplicate.node_id = 1;
        let error = EndpointRoster::from_discovered(
            &[endpoint(&[127, 0, 0, 1]), duplicate],
            &config(IpDiscovery::Ipv4),
        )
        .expect_err("duplicate node");
        assert!(error.to_string().contains("duplicate node ID"));
    }

    #[test]
    fn roster_store_keeps_previous_snapshot_until_replacement_validates() {
        let store = super::EndpointRosterStore::default();
        let first = EndpointRoster::from_discovered(
            &[endpoint(&[127, 0, 0, 1])],
            &config(IpDiscovery::Ipv4),
        )
        .expect("first roster");
        assert_eq!(store.replace(first).expect("store write"), None);

        let invalid =
            EndpointRoster::from_discovered(&[endpoint(&[1, 2, 3])], &config(IpDiscovery::Ipv4))
                .expect_err("invalid replacement");
        assert!(invalid.to_string().contains("invalid IP address length"));

        let snapshot = store
            .snapshot()
            .expect("store read")
            .expect("previous snapshot");
        assert_eq!(
            snapshot.endpoints()[0].socket_addr().to_string(),
            "127.0.0.1:8111"
        );
    }

    #[test]
    fn route_table_selects_nodes_round_robin_and_resets_after_replace() {
        let table = super::RouteTable::default();
        let roster = EndpointRoster::from_discovered(
            &[endpoint(&[127, 0, 0, 1]), {
                let mut endpoint = endpoint(&[127, 0, 0, 2]);
                endpoint.node_id = 2;
                endpoint
            }],
            &config(IpDiscovery::Ipv4),
        )
        .expect("route roster");
        table.replace(roster).expect("route replacement");
        assert_eq!(table.next().expect("first route").unwrap().node_id, 1);
        assert_eq!(table.next().expect("second route").unwrap().node_id, 2);
        assert_eq!(table.next().expect("wrapped route").unwrap().node_id, 1);

        let replacement = EndpointRoster::from_discovered(
            &[{
                let mut endpoint = endpoint(&[127, 0, 0, 3]);
                endpoint.node_id = 3;
                endpoint
            }],
            &config(IpDiscovery::Ipv4),
        )
        .expect("replacement route roster");
        table.replace(replacement).expect("route replacement");
        assert_eq!(table.next().expect("reset route").unwrap().node_id, 3);
    }

    #[test]
    fn route_table_skips_unhealthy_nodes_and_releases_them() {
        let table = super::RouteTable::default();
        let roster = EndpointRoster::from_discovered(
            &[endpoint(&[127, 0, 0, 1]), {
                let mut endpoint = endpoint(&[127, 0, 0, 2]);
                endpoint.node_id = 2;
                endpoint
            }],
            &config(IpDiscovery::Ipv4),
        )
        .expect("route roster");
        table.replace(roster).expect("route replacement");
        assert!(table.mark_unhealthy(1).expect("mark unhealthy"));
        assert!(!table.mark_unhealthy(99).expect("unknown node"));
        assert_eq!(table.next().expect("healthy route").unwrap().node_id, 2);
        assert!(table.mark_healthy(1).expect("mark healthy"));
        assert_eq!(table.next().expect("recovered route").unwrap().node_id, 1);
    }

    #[test]
    fn route_table_returns_no_route_when_all_nodes_are_unhealthy() {
        let table = super::RouteTable::default();
        let roster = EndpointRoster::from_discovered(
            &[endpoint(&[127, 0, 0, 1])],
            &config(IpDiscovery::Ipv4),
        )
        .expect("route roster");
        table.replace(roster).expect("route replacement");
        assert!(table.mark_unhealthy(1).expect("mark unhealthy"));
        assert!(table.next().expect("route selection").is_none());
        assert_eq!(
            table.next_with_reason().expect("route reason"),
            Err(super::RouteUnavailable::AllNodesUnhealthy)
        );
    }

    #[test]
    fn route_table_can_restore_all_routes_after_fail_open() {
        let table = super::RouteTable::default();
        let roster = EndpointRoster::from_discovered(
            &[endpoint(&[127, 0, 0, 1])],
            &config(IpDiscovery::Ipv4),
        )
        .expect("route roster");
        table.replace(roster).expect("route replacement");
        assert!(table.mark_unhealthy(1).expect("mark unhealthy"));
        assert!(table.next().expect("route selection").is_none());
        table.restore_all_routes().expect("restore routes");
        assert_eq!(table.next().expect("restored route").unwrap().node_id, 1);
    }

    #[test]
    fn route_manager_fail_open_threshold_matches_two_thirds_rule() {
        let table = super::RouteTable::default();
        let roster = EndpointRoster::from_discovered(
            &[
                endpoint(&[127, 0, 0, 1]),
                {
                    let mut endpoint = endpoint(&[127, 0, 0, 2]);
                    endpoint.node_id = 2;
                    endpoint
                },
                {
                    let mut endpoint = endpoint(&[127, 0, 0, 3]);
                    endpoint.node_id = 3;
                    endpoint
                },
            ],
            &config(IpDiscovery::Ipv4),
        )
        .expect("route roster");
        table.replace(roster).expect("route replacement");
        table.mark_unhealthy(1).expect("first route removal");
        table.mark_unhealthy(2).expect("second route removal");
        let healthy = table.healthy_route_targets().expect("healthy routes");
        assert_eq!(healthy.len(), 1);
        assert!(healthy.len() * 3 < table.route_targets().expect("all routes").len() * 2);
    }

    #[test]
    fn route_table_removes_a_node_only_after_three_failures() {
        let table = super::RouteTable::default();
        let roster = EndpointRoster::from_discovered(
            &[endpoint(&[127, 0, 0, 1]), {
                let mut endpoint = endpoint(&[127, 0, 0, 2]);
                endpoint.node_id = 2;
                endpoint
            }],
            &config(IpDiscovery::Ipv4),
        )
        .expect("route roster");
        table.replace(roster).expect("route replacement");
        assert!(!table.record_failure(1).expect("first failure"));
        assert!(!table.record_failure(1).expect("second failure"));
        assert!(table.record_failure(1).expect("third failure"));
        assert_eq!(table.next().expect("healthy route").unwrap().node_id, 2);
        assert!(table.record_success(1).expect("recovery"));
        assert_eq!(table.next().expect("recovered route").unwrap().node_id, 1);
    }

    #[test]
    fn health_failures_use_a_distinct_five_failure_threshold() {
        let table = super::RouteTable::default();
        let roster = EndpointRoster::from_discovered(
            &[endpoint(&[127, 0, 0, 1])],
            &config(IpDiscovery::Ipv4),
        )
        .expect("route roster");
        table.replace(roster).expect("route replacement");

        for _ in 0..4 {
            assert!(!table.record_health_failure(1).expect("health failure"));
        }
        assert!(
            table
                .record_health_failure(1)
                .expect("fifth health failure")
        );
        assert!(table.record_health_success(1).expect("health recovery"));
        assert!(
            !table
                .record_health_success(1)
                .expect("health recovery is idempotent")
        );
    }

    #[test]
    fn successful_health_probe_recovers_a_suppressed_route() {
        let table = super::RouteTable::default();
        let roster = EndpointRoster::from_discovered(
            &[endpoint(&[127, 0, 0, 1]), {
                let mut endpoint = endpoint(&[127, 0, 0, 2]);
                endpoint.node_id = 2;
                endpoint
            }],
            &config(IpDiscovery::Ipv4),
        )
        .expect("route roster");
        table.replace(roster).expect("route replacement");
        assert!(!table.record_failure(1).expect("first failure"));
        assert!(!table.record_failure(1).expect("second failure"));
        assert!(table.record_failure(1).expect("third failure"));
        assert!(
            !table
                .healthy_route_targets()
                .expect("suppressed routes")
                .iter()
                .any(|route| route.node_id == 1)
        );
        assert!(table.record_success(1).expect("health probe recovery"));
        assert!(
            table
                .healthy_route_targets()
                .expect("recovered routes")
                .iter()
                .any(|route| route.node_id == 1)
        );
    }

    #[test]
    fn successful_health_probe_clears_both_failure_domains() {
        let table = super::RouteTable::default();
        let roster = EndpointRoster::from_discovered(
            &[endpoint(&[127, 0, 0, 1]), {
                let mut endpoint = endpoint(&[127, 0, 0, 2]);
                endpoint.node_id = 2;
                endpoint
            }],
            &config(IpDiscovery::Ipv4),
        )
        .expect("route roster");
        table.replace(roster).expect("route replacement");

        for _ in 0..3 {
            table.record_failure(1).expect("request failure");
        }
        for _ in 0..4 {
            table.record_health_failure(1).expect("health failure");
        }
        assert!(
            !table
                .healthy_route_targets()
                .expect("suppressed routes")
                .iter()
                .any(|route| route.node_id == 1)
        );

        assert!(table.record_success(1).expect("request recovery"));
        assert!(table.record_health_success(1).expect("health recovery"));
        assert!(
            table
                .healthy_route_targets()
                .expect("recovered routes")
                .iter()
                .any(|route| route.node_id == 1)
        );
        assert!(!table.record_health_failure(1).expect("new health failure"));
    }

    #[test]
    fn route_table_prefers_a_different_node_for_retry() {
        let table = super::RouteTable::default();
        let roster = EndpointRoster::from_discovered(
            &[endpoint(&[127, 0, 0, 1]), {
                let mut endpoint = endpoint(&[127, 0, 0, 2]);
                endpoint.node_id = 2;
                endpoint
            }],
            &config(IpDiscovery::Ipv4),
        )
        .expect("route roster");
        table.replace(roster).expect("route replacement");
        assert_eq!(
            table
                .next_after(Some(1))
                .expect("retry route")
                .expect("alternate route")
                .node_id,
            2
        );
    }

    #[test]
    fn route_table_reports_missing_roster() {
        let table = super::RouteTable::default();
        assert_eq!(
            table.next_with_reason().expect("route reason"),
            Err(super::RouteUnavailable::NoRoster)
        );
    }

    #[test]
    fn roster_replacement_clears_health_for_removed_and_readded_nodes() {
        let table = super::RouteTable::default();
        let first = EndpointRoster::from_discovered(
            &[endpoint(&[127, 0, 0, 1])],
            &config(IpDiscovery::Ipv4),
        )
        .expect("first roster");
        table.replace(first).expect("first replacement");
        assert!(table.mark_unhealthy(1).expect("mark unhealthy"));

        let emptying = EndpointRoster::from_discovered(
            &[{
                let mut endpoint = endpoint(&[127, 0, 0, 2]);
                endpoint.node_id = 2;
                endpoint
            }],
            &config(IpDiscovery::Ipv4),
        )
        .expect("second roster");
        table.replace(emptying).expect("second replacement");

        let readded = EndpointRoster::from_discovered(
            &[endpoint(&[127, 0, 0, 1])],
            &config(IpDiscovery::Ipv4),
        )
        .expect("readded roster");
        table.replace(readded).expect("readded replacement");
        assert_eq!(table.next().expect("readded route").unwrap().node_id, 1);
    }

    #[tokio::test]
    async fn serialized_replacement_preserves_one_complete_commit_per_refresh() {
        let table = super::RouteTable::default();
        let first = EndpointRoster::from_discovered(
            &[endpoint(&[127, 0, 0, 1])],
            &config(IpDiscovery::Ipv4),
        )
        .expect("first roster");
        let second = EndpointRoster::from_discovered(
            &[{
                let mut endpoint = endpoint(&[127, 0, 0, 2]);
                endpoint.node_id = 2;
                endpoint
            }],
            &config(IpDiscovery::Ipv4),
        )
        .expect("second roster");
        let third = EndpointRoster::from_discovered(
            &[{
                let mut endpoint = endpoint(&[127, 0, 0, 3]);
                endpoint.node_id = 3;
                endpoint
            }],
            &config(IpDiscovery::Ipv4),
        )
        .expect("third roster");

        let first_result = table.replace_serialized(first).await.expect("first commit");
        assert!(first_result.is_none());
        let (second_result, third_result) = tokio::join!(
            table.replace_serialized(second),
            table.replace_serialized(third)
        );
        assert!(second_result.expect("second commit").is_some());
        assert!(third_result.expect("third commit").is_some());
        assert_eq!(table.next().expect("route").unwrap().node_id, 3);
    }

    #[test]
    fn committed_roster_can_be_read_without_network_io() {
        let store = super::EndpointRosterStore::default();
        let roster = EndpointRoster::from_discovered(
            &[endpoint(&[127, 0, 0, 1])],
            &config(IpDiscovery::Ipv4),
        )
        .expect("roster");
        store.replace(roster).expect("store replacement");
        let discovered = store
            .discovered()
            .expect("store read")
            .expect("committed roster");
        assert_eq!(discovered[0].address, vec![127, 0, 0, 1]);
        assert_eq!(discovered[0].port, 8111);
    }
}
