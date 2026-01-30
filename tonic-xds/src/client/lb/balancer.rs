//! Load balancer service and picker traits.
//!
//! This module provides:
//! - [`LbPicker`]: Trait for endpoint selection algorithms
//! - [`P2cPicker`]: Power-of-two-choices picker implementation
//! - [`LoadBalancer`]: Tower service that balances requests across endpoints

use super::discover::{EndpointDiscover, EndpointUpdateCache};
use crate::common::async_util::BoxFuture;
use rand::Rng;
use rand::SeedableRng;
use std::hash::Hash;
use std::sync::Arc;
use std::task::{Context, Poll};
use tower::ready_cache::ReadyCache;
use tower::{load::Load, BoxError, Service};

/// The type of endpoint change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EndpointChangeType {
    /// An endpoint was inserted or updated.
    Insert,
    /// An endpoint was removed.
    Remove,
}

/// Trait for load balancing picker algorithms.
///
/// Pickers select an endpoint from the ready cache for each request.
/// Different implementations provide different algorithms (P2C, round-robin, sticky, etc.).
///
/// Pickers are notified of endpoint changes via [`on_endpoint_change`](Self::on_endpoint_change),
/// allowing them to maintain internal state (e.g., consistent hashing rings).
pub(crate) trait LbPicker<K, S, Req>
where
    K: Hash + Eq,
{
    /// Called when an endpoint is inserted or removed.
    ///
    /// Pickers can use this to maintain internal state like hash rings.
    /// This is called before the change is applied to the ready cache.
    fn on_endpoint_change(&mut self, key: &K, change: EndpointChangeType);

    /// Pick an endpoint index for the given request.
    ///
    /// # Arguments
    ///
    /// - `ready`: The ready cache containing available endpoints
    /// - `request`: The incoming request (can be used for sticky routing)
    ///
    /// # Returns
    ///
    /// The selected endpoint index, or `None` if no endpoint is available.
    fn pick(&mut self, ready: &ReadyCache<K, S, Req>, request: &Req) -> Option<usize>;
}

/// Power-of-two-choices (P2C) picker.
///
/// P2C randomly selects two endpoints and picks the one with lower load.
/// This provides good load distribution with O(1) selection time.
pub(crate) struct P2cPicker {
    rng: rand::rngs::SmallRng,
}

impl P2cPicker {
    /// Creates a new P2C picker with the given random seed.
    #[allow(dead_code)]
    pub(crate) fn new(seed: u64) -> Self {
        Self {
            rng: rand::rngs::SmallRng::seed_from_u64(seed),
        }
    }

    /// Creates a new P2C picker with a random seed from OS entropy.
    #[allow(dead_code)]
    pub(crate) fn from_entropy() -> Self {
        Self {
            rng: rand::rngs::SmallRng::from_os_rng(),
        }
    }
}

impl<K, S, Req> LbPicker<K, S, Req> for P2cPicker
where
    K: Hash + Eq + Clone,
    S: Service<Req> + Load,
    <S as Load>::Metric: PartialOrd,
{
    fn on_endpoint_change(&mut self, _key: &K, _change: EndpointChangeType) {
        // P2C doesn't need to track endpoint changes - it picks randomly from ready cache
    }

    fn pick(&mut self, ready: &ReadyCache<K, S, Req>, _request: &Req) -> Option<usize> {
        let len = ready.ready_len();

        match len {
            0 => None,
            1 => Some(0),
            _ => {
                // Pick two random indices
                let idx1 = self.rng.random_range(0..len);
                let mut idx2 = self.rng.random_range(0..len - 1);
                if idx2 >= idx1 {
                    idx2 += 1;
                }

                // Get services at those indices and compare load
                let (_, svc1) = ready.get_ready_index(idx1)?;
                let (_, svc2) = ready.get_ready_index(idx2)?;

                let load1 = svc1.load();
                let load2 = svc2.load();

                if load2 < load1 {
                    Some(idx2)
                } else {
                    Some(idx1)
                }
            }
        }
    }
}

/// A Tower Service that load-balances requests across multiple endpoints.
///
/// `LoadBalancer` consumes batched updates from an [`EndpointUpdateCache`],
/// maintains a [`ReadyCache`] of available services, and uses an [`LbPicker`]
/// to select endpoints for incoming requests.
///
/// The cache spawns a background task that eagerly polls the discover,
/// so updates are accumulated asynchronously and consumed in `poll_ready`.
pub(crate) struct LoadBalancer<K, S, P, Req>
where
    K: Hash + Eq,
{
    cache: EndpointUpdateCache<K, S>,
    ready_cache: ReadyCache<K, S, Req>,
    picker: P,
}

impl<K, S, P, Req> LoadBalancer<K, S, P, Req>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    S: Service<Req> + Send + Sync + 'static,
    P: LbPicker<K, S, Req>,
{
    /// Creates a new `LoadBalancer` with the given discover and picker.
    ///
    /// This spawns a background task that eagerly polls the discover for updates.
    pub(crate) fn new<D>(discover: D, picker: P) -> Self
    where
        D: EndpointDiscover<K, S> + Send + 'static,
    {
        Self {
            cache: EndpointUpdateCache::spawn(discover),
            ready_cache: ReadyCache::default(),
            picker,
        }
    }

    /// Returns the number of ready endpoints.
    #[allow(dead_code)]
    pub(crate) fn ready_len(&self) -> usize {
        self.ready_cache.ready_len()
    }

    /// Returns the number of pending endpoints.
    #[allow(dead_code)]
    pub(crate) fn pending_len(&self) -> usize {
        self.ready_cache.pending_len()
    }
}

impl<K, S, P, Req> Service<Req> for LoadBalancer<K, S, P, Req>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    S: Service<Req> + Load + Send + Sync + 'static,
    S::Error: Into<BoxError>,
    S::Future: Send + 'static,
    S::Response: Send + 'static,
    <S as Load>::Metric: PartialOrd,
    P: LbPicker<K, S, Req>,
    Req: Send,
{
    type Response = S::Response;
    type Error = BoxError;
    type Future = BoxFuture<Result<S::Response, BoxError>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // 1. Consume batched updates from the background polling task
        let updates = self.cache.consume();
        // Unwrap the Arc to take ownership - consume() should be the only holder
        if let Some(updates) = Arc::into_inner(updates) {
            for (key, value) in updates.into_iter() {
            match value {
                Some(service) => {
                    // Notify picker of insert
                    self.picker
                        .on_endpoint_change(&key, EndpointChangeType::Insert);
                    // Insert or update endpoint (evict first if exists)
                    self.ready_cache.evict(&key);
                    self.ready_cache.push(key, service);
                }
                None => {
                    // Notify picker of remove
                    self.picker
                        .on_endpoint_change(&key, EndpointChangeType::Remove);
                    // Remove endpoint
                    self.ready_cache.evict(&key);
                }
            }
            }
        } else {
            // This shouldn't happen - consume() should return exclusively owned Arc
            // Log warning and proceed without applying updates
            tracing::warn!("EndpointUpdateCache::consume() returned shared Arc, skipping updates");
        }

        // 3. Drive pending services to ready
        let _ = self.ready_cache.poll_pending(cx);

        // 4. Check if we have any ready endpoints
        if self.ready_cache.ready_len() > 0 {
            Poll::Ready(Ok(()))
        } else if self.ready_cache.pending_len() > 0 {
            Poll::Pending
        } else {
            // No endpoints at all - keep polling for new ones
            Poll::Pending
        }
    }

    fn call(&mut self, request: Req) -> Self::Future {
        // Use picker to select an endpoint index
        let idx = self
            .picker
            .pick(&self.ready_cache, &request)
            .expect("poll_ready should ensure at least one ready endpoint");

        // Call the selected endpoint by index
        let fut = self.ready_cache.call_ready_index(idx, request);

        Box::pin(async move { fut.await.map_err(Into::into) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tower::load::Load;

    /// A mock service that tracks load via in-flight count.
    struct MockService {
        in_flight: Arc<AtomicUsize>,
    }

    impl MockService {
        fn new() -> Self {
            Self {
                in_flight: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn with_load(load: usize) -> Self {
            Self {
                in_flight: Arc::new(AtomicUsize::new(load)),
            }
        }
    }

    impl Service<()> for MockService {
        type Response = ();
        type Error = BoxError;
        type Future = std::future::Ready<Result<(), BoxError>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: ()) -> Self::Future {
            std::future::ready(Ok(()))
        }
    }

    impl Load for MockService {
        type Metric = usize;

        fn load(&self) -> Self::Metric {
            self.in_flight.load(Ordering::SeqCst)
        }
    }

    #[test]
    fn test_p2c_picker_empty_cache() {
        let mut picker = P2cPicker::new(42);
        let ready: ReadyCache<&str, MockService, ()> = ReadyCache::default();

        let result = picker.pick(&ready, &());
        assert!(result.is_none());
    }

    #[test]
    fn test_p2c_picker_single_endpoint() {
        let mut picker = P2cPicker::new(42);
        let mut ready: ReadyCache<&str, MockService, ()> = ReadyCache::default();
        ready.push("addr1", MockService::new());

        // Drive to ready
        let waker = std::task::Waker::noop().clone();
        let mut cx = Context::from_waker(&waker);
        let _ = ready.poll_pending(&mut cx);

        let result = picker.pick(&ready, &());
        assert_eq!(result, Some(0));
    }

    #[test]
    fn test_p2c_picker_prefers_lower_load() {
        let mut picker = P2cPicker::new(42);
        let mut ready: ReadyCache<&str, MockService, ()> = ReadyCache::default();
        ready.push("low", MockService::with_load(1));
        ready.push("high", MockService::with_load(100));

        // Drive to ready
        let waker = std::task::Waker::noop().clone();
        let mut cx = Context::from_waker(&waker);
        let _ = ready.poll_pending(&mut cx);

        // With only 2 endpoints, P2C always compares both
        // The one with lower load should always be picked
        let mut low_idx_count = 0;
        for _ in 0..100 {
            let idx = picker.pick(&ready, &()).unwrap();
            // Check which service is at the picked index
            let (key, _) = ready.get_ready_index(idx).unwrap();
            if *key == "low" {
                low_idx_count += 1;
            }
        }

        assert_eq!(low_idx_count, 100);
    }

    #[test]
    fn test_p2c_picker_on_endpoint_change() {
        let mut picker = P2cPicker::new(42);

        // P2C should ignore endpoint changes (no-op)
        // Need to specify types for the trait method
        <P2cPicker as LbPicker<&str, MockService, ()>>::on_endpoint_change(
            &mut picker,
            &"addr1",
            EndpointChangeType::Insert,
        );
        <P2cPicker as LbPicker<&str, MockService, ()>>::on_endpoint_change(
            &mut picker,
            &"addr1",
            EndpointChangeType::Remove,
        );

        // Should still work after changes
        let ready: ReadyCache<&str, MockService, ()> = ReadyCache::default();
        assert!(picker.pick(&ready, &()).is_none());
    }
}
