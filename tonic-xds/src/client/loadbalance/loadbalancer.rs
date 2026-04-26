//! Load balancer tower service.
//!
//! Receives endpoint updates via [`tower::discover::Discover`] (yielding
//! [`IdleChannel`]s), manages the connection lifecycle via the channel state
//! machine, and routes requests to ready endpoints via a [`ChannelPicker`].

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use indexmap::IndexMap;
use pin_project_lite::pin_project;
use tower::discover::{Change, Discover};
use tower::Service;

use crate::client::endpoint::{Connector, EndpointAddress};
use crate::client::loadbalance::channel_state::{IdleChannel, ReadyChannel};
use crate::client::loadbalance::errors::LbError;
use crate::client::loadbalance::keyed_futures::KeyedFutures;
use crate::client::loadbalance::pickers::ChannelPicker;

pin_project! {
    /// Future returned by [`LoadBalancer::call`].
    ///
    /// Either resolves immediately with an [`LbError`] or delegates to the
    /// inner service future, mapping its error to [`LbError::ServiceFailure`].
    #[project = LbFutureProj]
    pub(crate) enum LbFuture<F> {
        Error { error: Option<LbError> },
        Pending { #[pin] inner: F },
    }
}

impl<F, T, E> Future for LbFuture<F>
where
    F: Future<Output = Result<T, E>>,
    E: Into<tower::BoxError>,
{
    type Output = Result<T, LbError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            LbFutureProj::Error { error } => match error.take() {
                Some(e) => Poll::Ready(Err(e)),
                None => Poll::Ready(Err(LbError::Precondition("LbFuture::Error polled twice"))),
            },
            LbFutureProj::Pending { inner } => {
                inner.poll(cx).map_err(|e| LbError::ServiceFailure(e.into()))
            }
        }
    }
}

/// A load-balancing tower [`Service`] that manages endpoint lifecycle and
/// distributes requests across ready endpoints.
///
/// Type parameters:
/// - `D`: Discovery stream yielding `Change<EndpointAddress, IdleChannel>`
/// - `C`: Connector that produces services from endpoint addresses.
///   `C::Service` is the underlying service type held in ready channels.
/// - `Req`: The request type.
pub(crate) struct LoadBalancer<D, C: Connector, Req> {
    /// Discovery stream providing endpoint additions/removals.
    discovery: D,
    /// Connector for creating connections from idle channels.
    connector: Arc<C>,
    /// In-flight connection attempts, keyed by endpoint address.
    connecting: KeyedFutures<EndpointAddress, ReadyChannel<C::Service>>,
    /// Ready-to-serve channels, keyed by endpoint address.
    ready: IndexMap<EndpointAddress, ReadyChannel<C::Service>>,
    /// Channel picker for load balancing.
    picker: Arc<dyn ChannelPicker<C::Service, Req>>,
}

impl<D, C, Req> LoadBalancer<D, C, Req>
where
    D: Discover<Key = EndpointAddress, Service = IdleChannel> + Unpin,
    D::Error: std::fmt::Debug,
    C: Connector + Send + Sync + 'static,
    C::Service: Send + 'static,
{
    /// Create a new load balancer with the given picker.
    pub(crate) fn new(
        discovery: D,
        connector: Arc<C>,
        picker: Arc<dyn ChannelPicker<C::Service, Req>>,
    ) -> Self {
        Self {
            discovery,
            connector,
            connecting: KeyedFutures::new(),
            ready: IndexMap::new(),
            picker,
        }
    }

    /// Poll the discovery stream for endpoint changes.
    fn poll_discover(&mut self, cx: &mut Context<'_>) {
        loop {
            match Pin::new(&mut self.discovery).poll_discover(cx) {
                Poll::Ready(Some(Ok(change))) => match change {
                    Change::Insert(addr, idle) => {
                        let _ = self.connecting.cancel(&addr);
                        self.ready.remove(&addr);
                        let connecting = idle.connect(self.connector.clone());
                        let _ = self.connecting.add(addr, connecting);
                    }
                    Change::Remove(addr) => {
                        let _ = self.connecting.cancel(&addr);
                        self.ready.remove(&addr);
                    }
                },
                Poll::Ready(Some(Err(e))) => {
                    tracing::warn!("discovery error: {:?}", e);
                }
                Poll::Ready(None) | Poll::Pending => break,
            }
        }
    }

    /// Drain completed connection futures into the ready set.
    fn poll_connecting(&mut self, cx: &mut Context<'_>) {
        while let Poll::Ready(Some((addr, ready))) = self.connecting.poll_next(cx) {
            self.ready.insert(addr, ready);
        }
    }
}

impl<D, C, Req> Service<Req> for LoadBalancer<D, C, Req>
where
    D: Discover<Key = EndpointAddress, Service = IdleChannel> + Unpin,
    D::Error: std::fmt::Debug,
    C: Connector + Send + Sync + 'static,
    C::Service: Service<Req> + Send + 'static,
    <C::Service as Service<Req>>::Error: Into<tower::BoxError>,
    Req: Send + 'static,
{
    type Response = <C::Service as Service<Req>>::Response;
    type Error = LbError;
    type Future = LbFuture<<C::Service as Service<Req>>::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_discover(cx);
        self.poll_connecting(cx);
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let Some(idx) = self.picker.pick(&req, &self.ready) else {
            return LbFuture::Error {
                error: Some(LbError::Unavailable),
            };
        };
        let Some((_, svc)) = self.ready.get_index_mut(idx) else {
            return LbFuture::Error {
                error: Some(LbError::Precondition("picker returned invalid index")),
            };
        };
        LbFuture::Pending {
            inner: svc.call(req),
        }
    }
}
