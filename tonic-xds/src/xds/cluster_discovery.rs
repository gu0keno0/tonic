//! xDS-backed [`ClusterDiscovery`] implementation and default connector.

use std::sync::Arc;

use tonic::transport::{Channel, Endpoint};

use crate::client::endpoint::{Connector, EndpointAddress};
use crate::client::lb::{BoxDiscover, ClusterDiscovery};
use crate::client::loadbalance::channel::LbChannel;
use crate::common::async_util::BoxFuture;
use crate::xds::cache::XdsCache;
use crate::xds::endpoint_manager::EndpointManager;

/// xDS-backed cluster discovery that resolves cluster names into endpoint
/// change streams by watching the [`XdsCache`].
pub(crate) struct XdsClusterDiscovery {
    cache: Arc<XdsCache>,
    endpoint_manager: EndpointManager,
}

impl XdsClusterDiscovery {
    pub(crate) fn new(cache: Arc<XdsCache>) -> Self {
        Self {
            cache,
            endpoint_manager: EndpointManager::new(),
        }
    }
}

impl ClusterDiscovery for XdsClusterDiscovery {
    fn discover_cluster(&self, cluster_name: &str) -> BoxDiscover {
        let watch = self.cache.watch_endpoints(cluster_name);
        self.endpoint_manager.discover_endpoints(watch)
    }
}

/// Default connector that creates lazily-connected [`LbChannel<Channel>`]
/// for each endpoint address. Uses plaintext HTTP (no TLS).
///
/// Uses insecure (plaintext) connections.
// TODO(PR2/A29): Replace this with a TLS-aware connector that receives the
// CertProviderRegistry and per-cluster UpstreamTlsContext (from ClusterResource).
// When a cluster has transport_socket configured, the connector should:
//   1. Look up root + identity cert provider instances from the registry
//   2. Build ClientTlsConfig with the fetched CertificateData
//   3. Apply SAN matching for server authorization
//   4. Use connect() instead of connect_lazy() for TLS handshake
pub(crate) struct DefaultConnector;

impl Connector for DefaultConnector {
    type Service = LbChannel<Channel>;

    fn connect(&self, addr: &EndpointAddress) -> BoxFuture<Self::Service> {
        let uri = format!("http://{addr}");
        let channel = Endpoint::from_shared(uri)
            .expect("EndpointAddress Display guarantees valid URI")
            .connect_lazy();
        Box::pin(std::future::ready(LbChannel::new(addr.clone(), channel)))
    }
}
