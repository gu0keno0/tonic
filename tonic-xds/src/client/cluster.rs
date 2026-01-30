use crate::client::lb::{
    BalancerRequest, BalancerResponse, EndpointDiscover, LbPicker, LoadBalancer, P2cPicker,
    PollDiscoverResponse,
};
use crate::common::async_util::BoxFuture;
use dashmap::DashMap;
use http::{Request, Response};
use std::fmt::Debug;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tonic::body::Body as TonicBody;
use tower::{buffer::Buffer, load::Load, BoxError, Service};

type BalancerRespFut<Resp> = BoxFuture<Result<BalancerResponse<Resp>, BoxError>>;

const DEFAULT_BUFFER_CAPACITY: usize = 1024;

/// `ClusterBalancer` is responsible for managing load balancing requests across multiple channels.
/// It uses our custom `LoadBalancer` with pluggable `LbPicker` for load balancing strategies.
pub(crate) struct ClusterBalancer<K, S, D, P, Req>
where
    K: Hash + Eq,
{
    balancer: LoadBalancer<K, S, D, P, Req>,
}

impl<K, S, D, Req> ClusterBalancer<K, S, D, P2cPicker, Req>
where
    K: Hash + Eq + Clone,
    S: Service<Req> + Load,
    <S as Load>::Metric: PartialOrd,
    D: EndpointDiscover<K, S>,
{
    /// Creates a new `ClusterBalancer` with provided endpoint discovery and P2C picker.
    pub(crate) fn new(discover: D) -> Self {
        Self {
            balancer: LoadBalancer::new(discover, P2cPicker::from_entropy()),
        }
    }
}

impl<K, S, D, P, Req> ClusterBalancer<K, S, D, P, Req>
where
    K: Hash + Eq + Clone,
    S: Service<Req>,
    D: EndpointDiscover<K, S>,
    P: LbPicker<K, S, Req>,
{
    /// Creates a new `ClusterBalancer` with provided endpoint discovery and custom picker.
    #[allow(dead_code)]
    pub(crate) fn with_picker(discover: D, picker: P) -> Self {
        Self {
            balancer: LoadBalancer::new(discover, picker),
        }
    }

    /// Returns the number of ready endpoints currently tracked by the balancer.
    #[allow(dead_code)]
    pub(crate) fn ready_len(&self) -> usize {
        self.balancer.ready_len()
    }

    /// Returns the number of pending endpoints currently tracked by the balancer.
    #[allow(dead_code)]
    pub(crate) fn pending_len(&self) -> usize {
        self.balancer.pending_len()
    }
}

impl<K, S, D, P, Req> Service<BalancerRequest<Req>> for ClusterBalancer<K, S, D, P, Req>
where
    K: Hash + Eq + Clone,
    S: Service<Req> + Load,
    S::Error: Into<BoxError>,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    <S as Load>::Metric: PartialOrd,
    D: EndpointDiscover<K, S>,
    P: LbPicker<K, S, Req>,
{
    type Response = BalancerResponse<S::Response>;
    type Error = BoxError;
    type Future = BalancerRespFut<S::Response>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.balancer.poll_ready(cx)
    }

    fn call(&mut self, req: BalancerRequest<Req>) -> Self::Future {
        self.balancer.call(req)
    }
}

/// `ClusterChannel` is similar to `tonic::transport::Channel`, but is for load-balancing across all
/// the channels for a xDS Cluster.
/// `ClusterChannel` should be cloned to be used in multi-threaded environment. It leverages a `tower::Buffer` to
/// queue requests from multiple callers and behind the queue, it load-balances the requests across all
/// available channels by leveraging the inner `ClusterBalancer` object.
///
/// The channel accepts `BalancerRequest<Req>` internally, allowing both real requests
/// and discovery polling to flow through the same buffer.
pub(crate) struct ClusterChannel<Req, Resp>
where
    Req: Send + 'static,
    Resp: 'static,
{
    // The mpsc channel between callers and the actual pool of channels.
    svc: Buffer<BalancerRequest<Req>, BalancerRespFut<Resp>>,
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
    Resp: Send + 'static,
{
    /// Creates a new `ClusterChannel` with the given balancer.
    pub(crate) fn from_balancer<B>(balancer: B, buffer_cap: usize) -> Self
    where
        B: Service<BalancerRequest<Req>, Response = BalancerResponse<Resp>, Error = BoxError, Future = BalancerRespFut<Resp>>
            + Send
            + 'static,
    {
        let svc = Buffer::new(balancer, buffer_cap);
        Self { svc }
    }

    /// Triggers discovery polling on the load balancer.
    ///
    /// This sends a `PollDiscover` request through the buffer, which causes
    /// the balancer to poll the endpoint discover and apply any pending updates.
    #[allow(dead_code)]
    pub(crate) async fn poll_discover(&mut self) -> Result<PollDiscoverResponse, BoxError> {
        // Ensure the service is ready
        std::future::poll_fn(|cx| Service::poll_ready(&mut self.svc, cx))
            .await
            .map_err(BoxError::from)?;

        let response = self.svc.call(BalancerRequest::PollDiscover).await?;
        match response {
            BalancerResponse::PollDiscover(r) => Ok(r),
            BalancerResponse::Call(_) => unreachable!("PollDiscover should return PollDiscover response"),
        }
    }
}

impl<Req, Resp> Service<Req> for ClusterChannel<Req, Resp>
where
    Req: Send + 'static,
    Resp: Send + 'static,
{
    type Response = Resp;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Service::poll_ready(&mut self.svc, cx).map_err(BoxError::from)
    }

    fn call(&mut self, request: Req) -> Self::Future {
        let fut = self.svc.call(BalancerRequest::Call(request));
        Box::pin(async move {
            let response = fut.await?;
            match response {
                BalancerResponse::Call(r) => Ok(r),
                BalancerResponse::PollDiscover(_) => {
                    unreachable!("Call should return Call response")
                }
            }
        })
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
    /// Creates a new `ClusterClient` with the given cluster name and endpoint discovery implementation.
    pub(crate) fn new<K, S, D>(name: String, discover: D) -> Self
    where
        K: Hash + Eq + Clone + Send + 'static,
        S: Service<Req, Response = Resp> + Load + Send + 'static,
        S::Error: Into<BoxError>,
        S::Future: Send + 'static,
        <S as Load>::Metric: PartialOrd + Send,
        D: EndpointDiscover<K, S> + Send + 'static,
    {
        let balancer = ClusterBalancer::new(discover);
        let channel = ClusterChannel::from_balancer(balancer, DEFAULT_BUFFER_CAPACITY);
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
{
    /// Creates a new `ClusterClientRegistry`.
    pub(crate) fn new() -> Self {
        Self {
            registry: DashMap::new(),
        }
    }

    /// Get the client of a cluster with lazy discovery.
    pub(crate) fn get_cluster<K, S, F, D>(
        &self,
        key: &str,
        discover_fn: F,
    ) -> Arc<ClusterClient<Req, Resp>>
    where
        F: FnOnce() -> D,
        K: Hash + Eq + Clone + Send + 'static,
        S: Service<Req, Response = Resp> + Load + Send + 'static,
        S::Error: Into<BoxError>,
        S::Future: Send + 'static,
        <S as Load>::Metric: PartialOrd + Send,
        D: EndpointDiscover<K, S> + Send + 'static,
    {
        let client = self
            .registry
            .entry(key.to_string())
            .or_insert_with(|| {
                let name = key.to_string();
                let discover = discover_fn();
                Arc::new(ClusterClient::new(name, discover))
            })
            .clone();
        client
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
