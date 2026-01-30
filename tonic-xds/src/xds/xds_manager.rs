use crate::client::lb::EndpointDiscover;
use crate::common::async_util::BoxFuture;
use std::task::{Context, Poll};
use tower::BoxError;

use crate::xds::route::{RouteDecision, RouteInput};

/// A boxed endpoint discover that can be used as a trait object.
pub(crate) struct BoxEndpointDiscover<K, S> {
    inner: Box<dyn ErasedEndpointDiscover<K, S> + Send>,
}

impl<K, S> BoxEndpointDiscover<K, S> {
    /// Creates a new boxed endpoint discover from any type implementing `EndpointDiscover`.
    pub(crate) fn new<D>(discover: D) -> Self
    where
        D: EndpointDiscover<K, S> + Send + 'static,
    {
        Self {
            inner: Box::new(discover),
        }
    }
}

/// Internal trait for type erasure of EndpointDiscover.
trait ErasedEndpointDiscover<K, S> {
    fn poll_discover(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<crate::client::lb::EndpointUpdate<K, S>, BoxError>>>;
}

impl<K, S, D> ErasedEndpointDiscover<K, S> for D
where
    D: EndpointDiscover<K, S>,
{
    fn poll_discover(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<crate::client::lb::EndpointUpdate<K, S>, BoxError>>> {
        EndpointDiscover::poll_discover(self, cx)
    }
}

impl<K, S> EndpointDiscover<K, S> for BoxEndpointDiscover<K, S> {
    fn poll_discover(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<crate::client::lb::EndpointUpdate<K, S>, BoxError>>> {
        self.inner.poll_discover(cx)
    }
}

/// Trait for routing requests to clusters based on xDS routing configurations.
pub(crate) trait XdsRouter: Send + Sync + 'static {
    fn route(&self, input: &RouteInput<'_>) -> BoxFuture<RouteDecision>;
}

/// Trait for discovering cluster endpoints based on xDS cluster configurations.
pub(crate) trait XdsClusterDiscovery<Endpoint, S>: Send + Sync + 'static {
    fn discover_cluster(&self, cluster_name: &str) -> BoxEndpointDiscover<Endpoint, S>;
}

/// Combined trait for xDS management (routing + load balancing).
/// Automatically implemented for any type that implements both `XdsRouter` and `XdsClusterDiscovery`.
#[allow(dead_code)]
pub(crate) trait XdsManager<Endpoint, S>: XdsRouter + XdsClusterDiscovery<Endpoint, S> {}

impl<T, Endpoint, S> XdsManager<Endpoint, S> for T where
    T: XdsRouter + XdsClusterDiscovery<Endpoint, S>
{
}
