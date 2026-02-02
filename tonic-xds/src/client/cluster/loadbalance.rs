use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::task::{Context, Poll};

use tower::discover::{Change, Discover};
use tower::{BoxError, Service, ServiceExt};

use super::picker::{EndpointChange, Picker};

/// A load balancer that uses a pluggable picker for endpoint selection.
///
/// Unlike `tower::balance::p2c::Balance`, this load balancer is optimistic about
/// readiness: it returns `Ready` in `poll_ready` as long as there are endpoints
/// available, then awaits the actual service readiness inside `call`.
///
/// This design allows for:
/// - Pluggable picking strategies via the `Picker` trait
/// - Outlier detection through status reporting (future work)
/// - More flexible readiness semantics
pub struct ClusterLoadBalancer<D, P, Req>
where
    D: Discover,
{
    discover: D,
    picker: P,
    services: HashMap<D::Key, D::Service>,
    _marker: std::marker::PhantomData<fn(Req)>,
}

impl<D, P, Req> ClusterLoadBalancer<D, P, Req>
where
    D: Discover,
    D::Key: Hash + Eq,
{
    /// Creates a new `ClusterLoadBalancer` with the given discovery stream and picker.
    pub fn new(discover: D, picker: P) -> Self {
        Self {
            discover,
            picker,
            services: HashMap::new(),
            _marker: std::marker::PhantomData,
        }
    }

    /// Returns the number of endpoints currently tracked by the balancer.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.services.len()
    }

    /// Returns true if there are no endpoints.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.services.is_empty()
    }
}

impl<D, P, Req> Service<Req> for ClusterLoadBalancer<D, P, Req>
where
    D: Discover + Unpin,
    D::Key: Hash + Eq + Clone + Send + 'static,
    D::Error: Into<BoxError>,
    D::Service: Service<Req> + Clone + Send + 'static,
    <D::Service as Service<Req>>::Response: Send,
    <D::Service as Service<Req>>::Error: Into<BoxError> + Send,
    <D::Service as Service<Req>>::Future: Send,
    P: Picker<D::Service, Key = D::Key>,
    Req: Send + 'static,
{
    type Response = <D::Service as Service<Req>>::Response;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // Poll discovery for changes
        loop {
            match Pin::new(&mut self.discover).poll_discover(cx) {
                Poll::Ready(Some(Ok(change))) => match change {
                    Change::Insert(key, svc) => {
                        self.services.insert(key.clone(), svc);
                        self.picker.update(EndpointChange::Insert(key));
                    }
                    Change::Remove(key) => {
                        self.services.remove(&key);
                        self.picker.update(EndpointChange::Remove(key));
                    }
                },
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Err(e.into()));
                }
                Poll::Ready(None) => {
                    // Discovery stream ended
                    break;
                }
                Poll::Pending => {
                    // No more changes available right now
                    break;
                }
            }
        }

        // Return Ready if we have endpoints, Pending otherwise
        if self.services.is_empty() {
            // The discovery poll above registered the waker,
            // so we'll be woken when new endpoints arrive
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn call(&mut self, req: Req) -> Self::Future {
        // Pick an endpoint
        let key = match self.picker.pick(&self.services) {
            Some(k) => k,
            None => {
                return Box::pin(async { Err(BoxError::from("no endpoints available")) });
            }
        };

        // Get and clone the service
        let mut svc = match self.services.get(&key).cloned() {
            Some(s) => s,
            None => {
                return Box::pin(async { Err(BoxError::from("endpoint not found")) });
            }
        };

        // Return future that awaits ready and calls the service
        Box::pin(async move {
            // Await ready
            svc.ready().await.map_err(Into::into)?;

            // Call the service
            svc.call(req).await.map_err(Into::into)
        })
    }
}
