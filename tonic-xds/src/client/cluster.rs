use crate::client::endpoint::{Connector, EndpointAddress};
use crate::client::loadbalance::channel_state::IdleChannel;
use crate::client::loadbalance::errors::LbError;
use crate::client::loadbalance::loadbalancer::LoadBalancer;
use crate::client::loadbalance::pickers::p2c::P2cPicker;
use crate::client::loadbalance::pickers::ChannelPicker;
use crate::common::async_util::BoxFuture;
use dashmap::DashMap;
use http::{Request, Response};
use std::fmt::Debug;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tonic::body::Body as TonicBody;
use tower::{BoxError, Service, buffer::Buffer, discover::Discover, load::Load};

type RespFut<Resp> = BoxFuture<Result<Resp, BoxError>>;

const DEFAULT_BUFFER_CAPACITY: usize = 1024;

/// `ClusterChannel` is similar to `tonic::transport::Channel`, but is for load-balancing across all
/// the channels for a xDS Cluster.
/// `ClusterChannel` should be cloned to be used in multi-threaded environment. It leverages a `tower::Buffer` to
/// queue requests from multiple callers and behind the queue, it load-balances the requests across all
/// available channels by leveraging the inner `ClusterBalancer` object.
pub(crate) struct ClusterChannel<Req, Resp>
where
    Req: Send + 'static,
    Resp: 'static,
{
    // The mpsc channel between callers and the actual pool of channels.
    svc: Buffer<Req, BoxFuture<Result<Resp, BoxError>>>,
}

impl<Req, Resp> Clone for ClusterChannel<Req, Resp>
where
    Req: Send + 'static,
    Resp: 'static,
{
    fn clone(&self) -> Self {
        Self {
            svc: self.svc.clone(),
        }
    }
}

impl<Req, Resp> ClusterChannel<Req, Resp>
where
    Req: Send + 'static,
    Resp: 'static,
{
    /// Creates a new `ClusterChannel` from a load-balancing service.
    pub(crate) fn from_balancer<B>(balancer: B, buffer_cap: usize) -> Self
    where
        B: Service<Req, Response = Resp, Error = BoxError, Future = RespFut<Resp>> + Send + 'static,
    {
        let svc = Buffer::new(balancer, buffer_cap);
        Self { svc }
    }
}

impl<Req, Resp> Service<Req> for ClusterChannel<Req, Resp>
where
    Req: Send + 'static,
    Resp: 'static,
{
    type Response = Resp;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Service::poll_ready(&mut self.svc, cx).map_err(BoxError::from)
    }

    fn call(&mut self, request: Req) -> Self::Future {
        Box::pin(self.svc.call(request))
    }
}

/// `ClusterClient` manages channels that load-balance for a xDS cluster.
pub(crate) struct ClusterClient<Req, Resp>
where
    Req: Send + 'static,
    Resp: 'static,
{
    name: String,
    channel: ClusterChannel<Req, Resp>,
}

impl Debug for ClusterClient<(), ()> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterClient")
            .field("name", &self.name)
            .finish()
    }
}

impl<Req, Resp> ClusterClient<Req, Resp>
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    /// Creates a new `ClusterClient` with the given discovery stream and connector.
    /// Uses [`LoadBalancer`] with P2C load balancing internally.
    pub(crate) fn new<D, C>(name: String, discover: D, connector: Arc<C>) -> Self
    where
        D: Discover<Key = EndpointAddress, Service = IdleChannel> + Unpin + Send + 'static,
        D::Error: std::fmt::Debug,
        C: Connector + Send + Sync + 'static,
        C::Service: Service<Req, Response = Resp> + Load + Clone + Send + 'static,
        <C::Service as Service<Req>>::Error: Into<BoxError>,
        <C::Service as Service<Req>>::Future: Send + 'static,
        <C::Service as Load>::Metric: PartialOrd,
        P2cPicker: ChannelPicker<C::Service, Req>,
    {
        use crate::client::loadbalance::channel_state::ReadyChannel;
        let picker: Arc<dyn ChannelPicker<ReadyChannel<C::Service>, Req> + Send + Sync> =
            Arc::new(P2cPicker);
        let lb = LoadBalancer::new(discover, connector, picker);
        // Map LbError → BoxError and box the future to match ClusterChannel's expectations.
        let mapped = tower::util::MapErr::new(lb, |e: LbError| -> BoxError { Box::new(e) });
        let boxed = tower::util::MapFuture::new(mapped, |fut| {
            Box::pin(fut) as RespFut<Resp>
        });
        let channel = ClusterChannel::from_balancer(boxed, DEFAULT_BUFFER_CAPACITY);
        Self { name, channel }
    }

    /// Returns a channel that can be used to send RPCs to the cluster.
    pub(crate) fn channel(&self) -> ClusterChannel<Req, Resp> {
        self.channel.clone()
    }

    /// Returns the name of the cluster.
    #[allow(dead_code)]
    pub(crate) fn name(&self) -> &str {
        &self.name
    }
}

/// `ClusterRegistry` is the client registry for all xDS clusters.
/// The xDS Tower service implementations uses this to get the client for a specific cluster.
pub(crate) struct ClusterClientRegistry<Req, Resp>
where
    Req: Send + 'static,
    Resp: 'static,
{
    registry: DashMap<String, Arc<ClusterClient<Req, Resp>>>,
}

impl<Req, Resp> ClusterClientRegistry<Req, Resp>
where
    Req: Send + 'static,
    Resp: Send + 'static,
    Resp: 'static,
{
    /// Creates a new `ClusterClientRegistry`.
    pub(crate) fn new() -> Self {
        Self {
            registry: DashMap::new(),
        }
    }
    /// Get the client of a cluster with lazy discovery.
    pub(crate) fn get_cluster<F, D, C>(
        &self,
        key: &str,
        discover_fn: F,
        connector: Arc<C>,
    ) -> Arc<ClusterClient<Req, Resp>>
    where
        F: FnOnce() -> D,
        D: Discover<Key = EndpointAddress, Service = IdleChannel> + Unpin + Send + 'static,
        D::Error: std::fmt::Debug,
        C: Connector + Send + Sync + 'static,
        C::Service: Service<Req, Response = Resp> + Load + Clone + Send + 'static,
        <C::Service as Service<Req>>::Error: Into<BoxError>,
        <C::Service as Service<Req>>::Future: Send + 'static,
        <C::Service as Load>::Metric: PartialOrd,
        P2cPicker: ChannelPicker<C::Service, Req>,
    {
        self.registry
            .entry(key.to_string())
            .or_insert_with(|| {
                let name = key.to_string();
                let discover = discover_fn();
                Arc::new(ClusterClient::new(name, discover, connector))
            })
            .clone()
    }
}

impl<Req, Resp> Default for ClusterClientRegistry<Req, Resp>
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

/// A type erased registry for Tonic clients.
/// This will be used by the xDS Tower Service implementations to get the client for a specific Tonic xDS cluster.
#[allow(dead_code)]
pub(crate) type ClusterClientRegistryGrpc =
    ClusterClientRegistry<Request<TonicBody>, Response<TonicBody>>;
