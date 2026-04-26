use crate::client::cluster::ClusterClientRegistry;
use crate::client::endpoint::{Connector, EndpointAddress};
use crate::client::loadbalance::channel_state::IdleChannel;
use crate::client::route::RouteDecision;
use crate::common::async_util::BoxFuture;
use http::Request;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::ServiceExt;
use tower::{BoxError, Service, discover::Change, load::Load};

/// A pinned, boxed stream of endpoint changes for Tower's `Discover`-based
/// load balancers. Now yields `IdleChannel` (just addresses) instead of
/// connected services.
pub(crate) type BoxDiscover =
    Pin<Box<dyn futures_core::Stream<Item = Result<Change<EndpointAddress, IdleChannel>, BoxError>> + Send>>;

/// Trait for discovering cluster endpoints.
///
/// Implementations resolve a cluster name into a stream of endpoint changes
/// (`Change::Insert` / `Change::Remove`). Yields `IdleChannel`s — the
/// `LoadBalancer` uses a `Connector` to establish actual connections.
pub(crate) trait ClusterDiscovery: Send + Sync + 'static {
    fn discover_cluster(&self, cluster_name: &str) -> BoxDiscover;
}

/// Errors that can occur during load balancing.
#[derive(Debug, Clone, thiserror::Error)]
pub(crate) enum LoadBalancingError {
    #[error("No routing decision extension from the routing layer available")]
    NoRoutingDecision,
}

/// A Tower Service that performs load balancing based on routing decisions.
///
/// Type parameters:
/// - `C`: Connector that produces services from endpoint addresses.
pub(crate) struct XdsLbService<Req, C: Connector>
where
    Req: Send + 'static,
    C::Service: Service<Req>,
    <C::Service as Service<Req>>::Response: Send + 'static,
{
    cluster_registry: Arc<ClusterClientRegistry<Req, <C::Service as Service<Req>>::Response>>,
    cluster_discovery: Arc<dyn ClusterDiscovery>,
    connector: Arc<C>,
}

impl<Req, C: Connector> XdsLbService<Req, C>
where
    Req: Send + 'static,
    C::Service: Service<Req>,
    <C::Service as Service<Req>>::Response: Send + 'static,
{
    pub(crate) fn new(
        cluster_registry: Arc<ClusterClientRegistry<Req, <C::Service as Service<Req>>::Response>>,
        cluster_discovery: Arc<dyn ClusterDiscovery>,
        connector: Arc<C>,
    ) -> Self {
        Self {
            cluster_registry,
            cluster_discovery,
            connector,
        }
    }
}

impl<Req, C: Connector> Clone for XdsLbService<Req, C>
where
    Req: Send + 'static,
    C::Service: Service<Req>,
    <C::Service as Service<Req>>::Response: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            cluster_registry: self.cluster_registry.clone(),
            cluster_discovery: self.cluster_discovery.clone(),
            connector: self.connector.clone(),
        }
    }
}

impl<B, C> Service<Request<B>> for XdsLbService<Request<B>, C>
where
    Request<B>: Send + 'static,
    C: Connector + Send + Sync + 'static,
    C::Service: Service<Request<B>> + Load + Clone + Send + 'static,
    <C::Service as Service<Request<B>>>::Response: Send + 'static,
    <C::Service as Service<Request<B>>>::Error: Into<BoxError>,
    <C::Service as Service<Request<B>>>::Future: Send + 'static,
    <C::Service as Load>::Metric: PartialOrd,
{
    type Response = <C::Service as Service<Request<B>>>::Response;
    type Error = BoxError;
    type Future = BoxFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        let Some(routing_decision) = request.extensions().get::<RouteDecision>().cloned() else {
            return Box::pin(async move { Err(LoadBalancingError::NoRoutingDecision.into()) });
        };

        let cluster_client = self.cluster_registry.get_cluster(
            &routing_decision.cluster,
            || self.cluster_discovery.discover_cluster(&routing_decision.cluster),
            self.connector.clone(),
        );

        let mut channel = cluster_client.channel();

        Box::pin(async move {
            channel.ready().await?;
            channel.call(request).await
        })
    }
}
