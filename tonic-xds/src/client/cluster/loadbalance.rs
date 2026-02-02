use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;
use std::pin::Pin;
use std::task::{Context, Poll};

use tower::discover::{Change, Discover};
use tower::{BoxError, Service, ServiceExt};

use crate::client::endpoint::OutlierDetectionStats;

use super::outlier::{CallOutcome, NoOutlierDetector, OutlierChange, OutlierDetector};
use super::picker::{EndpointChange, Picker};

/// A load balancer that uses a pluggable picker for endpoint selection.
///
/// Unlike `tower::balance::p2c::Balance`, this load balancer is optimistic about
/// readiness: it returns `Ready` in `poll_ready` as long as there are endpoints
/// available, then awaits the actual service readiness inside `call`.
///
/// This design allows for:
/// - Pluggable picking strategies via the `Picker` trait
/// - Outlier detection for ejecting unhealthy endpoints
/// - More flexible readiness semantics
pub struct ClusterLoadBalancer<D, P, Req, O>
where
    D: Discover,
{
    discover: D,
    picker: P,
    services: HashMap<D::Key, D::Service>,
    ejected: HashSet<D::Key>,
    outlier_detector: O,
    _marker: std::marker::PhantomData<fn(Req)>,
}

impl<D, P, Req> ClusterLoadBalancer<D, P, Req, NoOutlierDetector<D::Key, Result<<D::Service as Service<Req>>::Response, BoxError>>>
where
    D: Discover,
    D::Key: Hash + Eq + Clone + Send + 'static,
    D::Service: Service<Req>,
    <D::Service as Service<Req>>::Response: 'static,
{
    /// Creates a new `ClusterLoadBalancer` with the given discovery stream and picker.
    /// No outlier detection is enabled.
    pub fn new(discover: D, picker: P) -> Self {
        Self {
            discover,
            picker,
            services: HashMap::new(),
            ejected: HashSet::new(),
            outlier_detector: NoOutlierDetector::new(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<D, P, Req, O> ClusterLoadBalancer<D, P, Req, O>
where
    D: Discover,
    D::Key: Hash + Eq,
{
    /// Creates a new `ClusterLoadBalancer` with a custom outlier detector.
    pub fn with_outlier_detector(discover: D, picker: P, outlier_detector: O) -> Self {
        Self {
            discover,
            picker,
            services: HashMap::new(),
            ejected: HashSet::new(),
            outlier_detector,
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

impl<D, P, Req, O> Service<Req> for ClusterLoadBalancer<D, P, Req, O>
where
    D: Discover + Unpin,
    D::Key: Hash + Eq + Clone + Send + 'static,
    D::Error: Into<BoxError>,
    D::Service: Service<Req> + OutlierDetectionStats + Clone + Send + 'static,
    <D::Service as Service<Req>>::Response: Send + 'static,
    <D::Service as Service<Req>>::Error: Into<BoxError> + Send,
    <D::Service as Service<Req>>::Future: Send,
    P: Picker<D::Service, Key = D::Key>,
    Req: Send + 'static,
    O: OutlierDetector<Key = D::Key, Result = Result<<D::Service as Service<Req>>::Response, BoxError>>
        + Send
        + 'static,
{
    type Response = <D::Service as Service<Req>>::Response;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // 1. Poll outlier detector for ejection/unejection changes FIRST
        for change in self.outlier_detector.poll_changes() {
            match change {
                OutlierChange::Ejected(key) => {
                    self.ejected.insert(key.clone());
                    self.picker.update(EndpointChange::Eject(key));
                }
                OutlierChange::Unejected(key) => {
                    self.ejected.remove(&key);
                    // Reset stats on the service when unejected
                    if let Some(svc) = self.services.get(&key) {
                        svc.reset_stats();
                    }
                    self.picker.update(EndpointChange::Uneject(key));
                }
            }
        }

        // 2. Poll discovery for endpoint changes
        loop {
            match Pin::new(&mut self.discover).poll_discover(cx) {
                Poll::Ready(Some(Ok(change))) => match change {
                    Change::Insert(key, svc) => {
                        self.services.insert(key.clone(), svc);
                        self.picker.update(EndpointChange::Insert(key.clone()));
                        self.outlier_detector.on_endpoint_added(key);
                    }
                    Change::Remove(key) => {
                        self.services.remove(&key);
                        self.ejected.remove(&key);
                        self.picker.update(EndpointChange::Remove(key.clone()));
                        self.outlier_detector.on_endpoint_removed(&key);
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

        // 3. Return Ready if we have non-ejected endpoints
        let available = self.services.len().saturating_sub(self.ejected.len());
        if available == 0 {
            // The discovery poll above registered the waker,
            // so we'll be woken when new endpoints arrive
            Poll::Pending
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn call(&mut self, req: Req) -> Self::Future {
        // Pick an endpoint (excluding ejected ones)
        let key = match self.picker.pick(&self.services, &self.ejected) {
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

        // Get ejection checker to pass into async block
        let checker = self.outlier_detector.checker();

        // Return future that awaits ready, calls, and reports outcome
        Box::pin(async move {
            // Await ready
            svc.ready().await.map_err(Into::into)?;

            // Call the service
            let result = svc.call(req).await.map_err(Into::into);

            // Classify the result and record outcome
            match checker.classify(&result) {
                CallOutcome::Success => svc.record_success(),
                CallOutcome::Failure => svc.record_failure(),
            }

            // Check if ejection threshold is crossed
            checker.check(
                &key,
                svc.failure_rate(),
                svc.request_volume(),
                svc.consecutive_failures(),
            );

            result
        })
    }
}
