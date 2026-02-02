use crate::common::async_util::BoxFuture;
use std::net::SocketAddr;
use std::sync::{atomic::AtomicU64, atomic::Ordering, Arc};
use std::task::{Context, Poll};
use tower::{load::Load, Service};

/// Represents the host part of an endpoint address
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum EndpointHost {
    Ipv4(std::net::Ipv4Addr),
    Ipv6(std::net::Ipv6Addr),
    #[allow(dead_code)]
    Hostname(String),
}

/// Represents a validated endpoint address extracted from xDS
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct EndpointAddress {
    /// The IP address or hostname
    host: EndpointHost,
    /// The port number
    port: u16,
}

impl From<SocketAddr> for EndpointAddress {
    fn from(addr: SocketAddr) -> Self {
        match addr {
            SocketAddr::V4(v4_addr) => Self {
                host: EndpointHost::Ipv4(*v4_addr.ip()),
                port: v4_addr.port(),
            },
            SocketAddr::V6(v6_addr) => Self {
                host: EndpointHost::Ipv6(*v6_addr.ip()),
                port: v6_addr.port(),
            },
        }
    }
}

/// Trait for tracking call outcomes for outlier detection.
///
/// Implemented by endpoint services to track success/failure counts.
/// Similar to the `Load` trait but for outlier detection purposes.
pub trait OutlierDetectionStats: Send + Sync {
    /// Record a successful call.
    fn record_success(&self);

    /// Record a failed call.
    fn record_failure(&self);

    /// Get the current failure rate (0.0 to 1.0).
    fn failure_rate(&self) -> f64;

    /// Get the total number of requests tracked.
    fn request_volume(&self) -> u64;

    /// Get the current consecutive failure count.
    fn consecutive_failures(&self) -> u64;

    /// Reset the stats (called when unejected).
    fn reset_stats(&self);
}

/// RAII tracker for in-flight requests.
/// This is mainly used to implement endpoint load reporting for load balancing purposes.
#[derive(Clone, Debug, Default)]
struct InFlightTracker {
    in_flight: Arc<AtomicU64>,
}

impl InFlightTracker {
    fn new(in_flight: Arc<AtomicU64>) -> Self {
        in_flight.fetch_add(1, Ordering::Relaxed);
        Self { in_flight }
    }
}

impl Drop for InFlightTracker {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// An endpoint channel for communicating with a single gRPC endpoint,
/// with load reporting and outlier detection stats support.
pub(crate) struct EndpointChannel<S> {
    inner: S,
    in_flight: Arc<AtomicU64>,
    // Outlier detection stats
    successes: Arc<AtomicU64>,
    failures: Arc<AtomicU64>,
    consecutive_failures: Arc<AtomicU64>,
}

impl<S> EndpointChannel<S> {
    /// Creates a new `EndpointChannel`.
    /// This should be used by xDS implementations to construct channels to individual endpoints.
    #[allow(dead_code)]
    pub(crate) fn new(inner: S) -> Self {
        Self {
            inner,
            in_flight: Arc::new(AtomicU64::new(0)),
            successes: Arc::new(AtomicU64::new(0)),
            failures: Arc::new(AtomicU64::new(0)),
            consecutive_failures: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl<S> Clone for EndpointChannel<S>
where
    S: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            in_flight: self.in_flight.clone(),
            successes: self.successes.clone(),
            failures: self.failures.clone(),
            consecutive_failures: self.consecutive_failures.clone(),
        }
    }
}

impl<S, Req> Service<Req> for EndpointChannel<S>
where
    S: Service<Req> + Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<Result<S::Response, S::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let in_flight = InFlightTracker::new(self.in_flight.clone());
        let fut = self.inner.call(req);

        // -1 when the inner future completes
        Box::pin(async move {
            let _in_flight_guard = in_flight;
            let res = fut.await;
            res
        })
    }
}

impl<S> Load for EndpointChannel<S> {
    type Metric = u64;
    fn load(&self) -> Self::Metric {
        self.in_flight.load(Ordering::Relaxed)
    }
}

impl<S> OutlierDetectionStats for EndpointChannel<S>
where
    S: Send + Sync,
{
    fn record_success(&self) {
        self.successes.fetch_add(1, Ordering::Relaxed);
        // Reset consecutive failures on success
        self.consecutive_failures.store(0, Ordering::Relaxed);
    }

    fn record_failure(&self) {
        self.failures.fetch_add(1, Ordering::Relaxed);
        self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
    }

    fn failure_rate(&self) -> f64 {
        let s = self.successes.load(Ordering::Relaxed);
        let f = self.failures.load(Ordering::Relaxed);
        let total = s + f;
        if total == 0 {
            0.0
        } else {
            f as f64 / total as f64
        }
    }

    fn request_volume(&self) -> u64 {
        self.successes.load(Ordering::Relaxed) + self.failures.load(Ordering::Relaxed)
    }

    fn consecutive_failures(&self) -> u64 {
        self.consecutive_failures.load(Ordering::Relaxed)
    }

    fn reset_stats(&self) {
        self.successes.store(0, Ordering::Relaxed);
        self.failures.store(0, Ordering::Relaxed);
        self.consecutive_failures.store(0, Ordering::Relaxed);
    }
}
