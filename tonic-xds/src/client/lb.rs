use crate::client::cluster::ClusterClientRegistry;
use crate::client::endpoint::{EndpointAddress, MakeConnector};
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
/// load balancers. Yields `IdleChannel` (just addresses) — the `LoadBalancer`
/// uses a `Connector` to establish actual connections.
pub(crate) type BoxDiscover =
    Pin<Box<dyn futures_core::Stream<Item = Result<Change<EndpointAddress, IdleChannel>, BoxError>> + Send>>;

/// Trait for discovering cluster endpoints.
///
/// Implementations resolve a cluster name into a stream of endpoint changes
/// (`Change::Insert` / `Change::Remove`).
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
/// - `MC`: MakeConnector that produces per-cluster connectors.
pub(crate) struct XdsLbService<Req, MC: MakeConnector>
where
    Req: Send + 'static,
    MC::Service: Service<Req>,
    <MC::Service as Service<Req>>::Response: Send + 'static,
{
    cluster_registry: Arc<ClusterClientRegistry<Req, <MC::Service as Service<Req>>::Response>>,
    cluster_discovery: Arc<dyn ClusterDiscovery>,
    make_connector: Arc<MC>,
}

impl<Req, MC: MakeConnector> XdsLbService<Req, MC>
where
    Req: Send + 'static,
    MC::Service: Service<Req>,
    <MC::Service as Service<Req>>::Response: Send + 'static,
{
    pub(crate) fn new(
        cluster_registry: Arc<ClusterClientRegistry<Req, <MC::Service as Service<Req>>::Response>>,
        cluster_discovery: Arc<dyn ClusterDiscovery>,
        make_connector: Arc<MC>,
    ) -> Self {
        Self {
            cluster_registry,
            cluster_discovery,
            make_connector,
        }
    }
}

impl<Req, MC: MakeConnector> Clone for XdsLbService<Req, MC>
where
    Req: Send + 'static,
    MC::Service: Service<Req>,
    <MC::Service as Service<Req>>::Response: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            cluster_registry: self.cluster_registry.clone(),
            cluster_discovery: self.cluster_discovery.clone(),
            make_connector: self.make_connector.clone(),
        }
    }
}

impl<B, MC> Service<Request<B>> for XdsLbService<Request<B>, MC>
where
    Request<B>: Send + 'static,
    MC: MakeConnector,
    MC::Connector: Send + Sync + 'static,
    MC::Service: Service<Request<B>> + Load + Clone + Send + 'static,
    <MC::Service as Service<Request<B>>>::Response: Send + 'static,
    <MC::Service as Service<Request<B>>>::Error: Into<BoxError>,
    <MC::Service as Service<Request<B>>>::Future: Send + 'static,
    <MC::Service as Load>::Metric: PartialOrd,
{
    type Response = <MC::Service as Service<Request<B>>>::Response;
    type Error = BoxError;
    type Future = BoxFuture<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        let Some(routing_decision) = request.extensions().get::<RouteDecision>().cloned() else {
            return Box::pin(async move { Err(LoadBalancingError::NoRoutingDecision.into()) });
        };

        let connector = self.make_connector.make_connector(&routing_decision.cluster);

        let cluster_client = self.cluster_registry.get_cluster(
            &routing_decision.cluster,
            || self.cluster_discovery.discover_cluster(&routing_decision.cluster),
            connector,
        );

        let mut channel = cluster_client.channel();

        Box::pin(async move {
            channel.ready().await?;
            channel.call(request).await
        })
    }
}
