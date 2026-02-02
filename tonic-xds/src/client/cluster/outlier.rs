use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::hash::Hash;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use tonic::Code;
use tower::BoxError;

/// Outcome of a call for outlier detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallOutcome {
    Success,
    Failure,
}

/// Changes in outlier detection state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutlierChange<K> {
    /// Endpoint was ejected due to high failure rate.
    Ejected(K),
    /// Endpoint was restored after ejection period expired.
    Unejected(K),
}

/// Configuration for outlier detection.
#[derive(Debug, Clone)]
pub struct OutlierDetectionConfig {
    /// Time between ejection analysis sweeps (not used in current impl).
    pub interval: Duration,
    /// Base duration for ejection.
    pub base_ejection_time: Duration,
    /// Maximum ejection duration.
    pub max_ejection_time: Duration,
    /// Maximum percentage of addresses that can be ejected.
    pub max_ejection_percent: u32,
    /// Consecutive failures threshold for ejection (0 = disabled).
    pub consecutive_failures_threshold: u64,
    /// Failure percentage threshold for ejection (0-100).
    pub failure_percentage_threshold: u32,
    /// Minimum request volume before failure percentage ejection is considered.
    pub failure_percentage_request_volume: u64,
}

impl Default for OutlierDetectionConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(10),
            base_ejection_time: Duration::from_secs(30),
            max_ejection_time: Duration::from_secs(300),
            max_ejection_percent: 10,
            consecutive_failures_threshold: 5,
            failure_percentage_threshold: 50,
            failure_percentage_request_volume: 100,
        }
    }
}

/// A cloneable handle for checking ejection conditions and enqueuing ejections.
///
/// This is designed to be cloned and passed into async blocks after each call.
/// It only holds the mpsc sender, threshold config, and classifier function.
pub struct EjectionChecker<K, R> {
    tx: mpsc::UnboundedSender<K>,
    // Consecutive failures config
    consecutive_failures_threshold: u64,
    // Failure percentage config
    failure_percentage_threshold: f64,
    failure_percentage_min_volume: u64,
    // Classifier function
    classify: fn(&R) -> CallOutcome,
}

impl<K, R> Clone for EjectionChecker<K, R> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            consecutive_failures_threshold: self.consecutive_failures_threshold,
            failure_percentage_threshold: self.failure_percentage_threshold,
            failure_percentage_min_volume: self.failure_percentage_min_volume,
            classify: self.classify,
        }
    }
}

impl<K: Clone, R> EjectionChecker<K, R> {
    /// Classify the result and return the outcome.
    pub fn classify(&self, result: &R) -> CallOutcome {
        (self.classify)(result)
    }

    /// Check if the endpoint should be ejected based on its stats.
    /// If any threshold is crossed, enqueues the key for ejection.
    pub fn check(
        &self,
        key: &K,
        failure_rate: f64,
        request_volume: u64,
        consecutive_failures: u64,
    ) {
        let should_eject =
            // Consecutive failures check (if enabled)
            (self.consecutive_failures_threshold > 0
                && consecutive_failures >= self.consecutive_failures_threshold)
            // Failure percentage check (if volume threshold met)
            || (request_volume >= self.failure_percentage_min_volume
                && failure_rate > self.failure_percentage_threshold);

        if should_eject {
            let _ = self.tx.send(key.clone());
        }
    }
}

/// Trait for outlier detection.
///
/// Implementations handle ejection/unejection of endpoints based on failure rates.
/// The actual stats tracking is done on the endpoint services via `OutlierDetectionStats`.
pub trait OutlierDetector: Send {
    type Key: Clone;
    type Result;

    /// Returns the current fleet size (number of active endpoints).
    fn fleet_size(&self) -> usize;

    /// Returns a cloneable checker for use in async blocks.
    /// The checker includes the classifier for determining success/failure.
    fn checker(&self) -> EjectionChecker<Self::Key, Self::Result>;

    /// Poll for ejection/unejection changes (non-blocking).
    /// Called from poll_ready.
    fn poll_changes(&mut self) -> Vec<OutlierChange<Self::Key>>;

    /// Called when an endpoint is added to topology.
    fn on_endpoint_added(&mut self, key: Self::Key);

    /// Called when an endpoint is removed from topology.
    fn on_endpoint_removed(&mut self, key: &Self::Key);
}

/// Classifies call results into success/failure for outlier detection.
pub trait ResultClassifier<R>: Send {
    fn classify(&self, result: &R) -> CallOutcome;
}

/// Entry in the ejection heap for tracking unejection time.
#[derive(Debug, Clone, PartialEq, Eq)]
struct EjectionEntry<K> {
    key: K,
    uneject_at: Instant,
}

impl<K: Eq> PartialOrd for EjectionEntry<K> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<K: Eq> Ord for EjectionEntry<K> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.uneject_at.cmp(&other.uneject_at)
    }
}

/// gRPC-specific outlier detector.
///
/// Manages ejection based on failure rates reported by endpoint services.
/// Classifies gRPC status codes to determine success/failure.
///
/// # Type Parameters
/// * `K` - The endpoint key type
/// * `B` - The HTTP body type (e.g., `tonic::body::Body`)
pub struct GrpcOutlierDetector<K, B> {
    config: OutlierDetectionConfig,

    // Channel for ejection notifications from check -> poll_changes
    ejection_tx: mpsc::UnboundedSender<K>,
    ejection_rx: mpsc::UnboundedReceiver<K>,

    // Heap for time-based unejection (min-heap by uneject_at)
    ejection_heap: BinaryHeap<Reverse<EjectionEntry<K>>>,

    // Track active endpoints for lazy cleanup
    active_endpoints: HashSet<K>,

    // Track currently ejected endpoints to avoid duplicate ejections
    ejected: HashSet<K>,

    _marker: std::marker::PhantomData<B>,
}

impl<K, B> GrpcOutlierDetector<K, B>
where
    K: Hash + Eq + Clone,
{
    /// Creates a new gRPC outlier detector with the given configuration.
    pub fn new(config: OutlierDetectionConfig) -> Self {
        let (ejection_tx, ejection_rx) = mpsc::unbounded_channel();
        Self {
            config,
            ejection_tx,
            ejection_rx,
            ejection_heap: BinaryHeap::new(),
            active_endpoints: HashSet::new(),
            ejected: HashSet::new(),
            _marker: std::marker::PhantomData,
        }
    }
}

/// Classify a gRPC HTTP response for outlier detection.
/// gRPC errors are HTTP 200 with grpc-status in headers (Trailers-Only format).
fn classify_grpc_http_result<B>(
    result: &Result<http::Response<B>, BoxError>,
) -> CallOutcome {
    match result {
        Ok(response) => {
            // Check grpc-status header for gRPC-level errors
            // For "Trailers-Only" responses (errors), grpc-status is in headers
            let grpc_status = response
                .headers()
                .get("grpc-status")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<i32>().ok())
                .unwrap_or(0); // 0 = OK, missing = OK (will be in trailers)

            if grpc_status == 0 {
                CallOutcome::Success
            } else {
                // Map grpc-status code to outcome
                match grpc_status {
                    // Server-side failures that indicate endpoint health issues
                    14 | 13 | 8 | 2 | 4 => CallOutcome::Failure, // Unavailable, Internal, ResourceExhausted, Unknown, DeadlineExceeded
                    // Client errors or OK - don't count against server
                    _ => CallOutcome::Success,
                }
            }
        }
        Err(e) => {
            // HTTP-level error (connection error, etc.)
            // Try to extract tonic::Status from the error
            if let Some(status) = e.downcast_ref::<tonic::Status>() {
                classify_grpc_status(status)
            } else {
                // Non-gRPC error, treat as failure
                CallOutcome::Failure
            }
        }
    }
}

impl<K, B> OutlierDetector for GrpcOutlierDetector<K, B>
where
    K: Hash + Eq + Clone + Send + 'static,
    B: Send + 'static,
{
    type Key = K;
    type Result = Result<http::Response<B>, BoxError>;

    fn fleet_size(&self) -> usize {
        self.active_endpoints.len()
    }

    fn checker(&self) -> EjectionChecker<Self::Key, Self::Result> {
        EjectionChecker {
            tx: self.ejection_tx.clone(),
            consecutive_failures_threshold: self.config.consecutive_failures_threshold,
            failure_percentage_threshold: self.config.failure_percentage_threshold as f64 / 100.0,
            failure_percentage_min_volume: self.config.failure_percentage_request_volume,
            classify: classify_grpc_http_result::<B>,
        }
    }

    fn poll_changes(&mut self) -> Vec<OutlierChange<Self::Key>> {
        let mut changes = Vec::new();

        // 1. Drain ejection queue (from checker calls)
        while let Ok(key) = self.ejection_rx.try_recv() {
            // Skip if endpoint was removed from topology
            if !self.active_endpoints.contains(&key) {
                continue;
            }
            // Skip if already ejected
            if self.ejected.contains(&key) {
                continue;
            }

            // TODO: Check max_ejection_percent using fleet_size()

            // Add to ejected set and heap
            self.ejected.insert(key.clone());
            let uneject_at = Instant::now() + self.config.base_ejection_time;
            self.ejection_heap.push(Reverse(EjectionEntry {
                key: key.clone(),
                uneject_at,
            }));
            changes.push(OutlierChange::Ejected(key));
        }

        // 2. Check heap for time-based unejections
        let now = Instant::now();
        while let Some(Reverse(entry)) = self.ejection_heap.peek() {
            if entry.uneject_at <= now {
                let entry = self.ejection_heap.pop().unwrap().0;
                // Skip if endpoint was removed from topology (lazy cleanup)
                if self.active_endpoints.contains(&entry.key) {
                    self.ejected.remove(&entry.key);
                    changes.push(OutlierChange::Unejected(entry.key));
                }
            } else {
                // Heap is sorted, no more expired entries
                break;
            }
        }

        changes
    }

    fn on_endpoint_added(&mut self, key: Self::Key) {
        self.active_endpoints.insert(key);
    }

    fn on_endpoint_removed(&mut self, key: &Self::Key) {
        self.active_endpoints.remove(key);
        self.ejected.remove(key);
        // Heap entries cleaned lazily in poll_changes
    }
}

/// gRPC result classifier that classifies based on status codes.
pub struct GrpcResultClassifier;

impl<Resp> ResultClassifier<Result<Resp, BoxError>> for GrpcResultClassifier {
    fn classify(&self, result: &Result<Resp, BoxError>) -> CallOutcome {
        match result {
            Ok(_) => CallOutcome::Success,
            Err(e) => {
                // Try to extract tonic::Status from the error
                if let Some(status) = e.downcast_ref::<tonic::Status>() {
                    classify_grpc_status(status)
                } else {
                    // Non-gRPC error, treat as failure
                    CallOutcome::Failure
                }
            }
        }
    }
}

/// No-op outlier detector for when outlier detection is disabled.
pub struct NoOutlierDetector<K, R> {
    // Dummy sender that's never read from
    tx: mpsc::UnboundedSender<K>,
    _marker: std::marker::PhantomData<R>,
}

impl<K, R> NoOutlierDetector<K, R> {
    /// Creates a new no-op outlier detector.
    pub fn new() -> Self {
        let (tx, _rx) = mpsc::unbounded_channel();
        Self {
            tx,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<K, R> Default for NoOutlierDetector<K, R> {
    fn default() -> Self {
        Self::new()
    }
}

/// No-op classifier that always returns Success.
fn noop_classify<R>(_result: &R) -> CallOutcome {
    CallOutcome::Success
}

impl<K, R> OutlierDetector for NoOutlierDetector<K, R>
where
    K: Clone + Send + 'static,
    R: Send + 'static,
{
    type Key = K;
    type Result = R;

    fn fleet_size(&self) -> usize {
        0
    }

    fn checker(&self) -> EjectionChecker<Self::Key, Self::Result> {
        EjectionChecker {
            tx: self.tx.clone(),
            // Thresholds that will never trigger
            consecutive_failures_threshold: 0, // disabled
            failure_percentage_threshold: f64::MAX,
            failure_percentage_min_volume: u64::MAX,
            classify: noop_classify::<R>,
        }
    }

    fn poll_changes(&mut self) -> Vec<OutlierChange<Self::Key>> {
        Vec::new()
    }

    fn on_endpoint_added(&mut self, _key: Self::Key) {
        // No-op
    }

    fn on_endpoint_removed(&mut self, _key: &Self::Key) {
        // No-op
    }
}

/// Classify gRPC status code for outlier detection.
fn classify_grpc_status(status: &tonic::Status) -> CallOutcome {
    match status.code() {
        // Server-side failures that indicate endpoint health issues
        Code::Unavailable | Code::Internal | Code::ResourceExhausted | Code::Unknown => {
            CallOutcome::Failure
        }
        // DeadlineExceeded could be server or network issue
        Code::DeadlineExceeded => CallOutcome::Failure,
        // Client errors or OK - don't count against server
        Code::Ok
        | Code::Cancelled
        | Code::InvalidArgument
        | Code::NotFound
        | Code::AlreadyExists
        | Code::PermissionDenied
        | Code::FailedPrecondition
        | Code::Aborted
        | Code::OutOfRange
        | Code::Unimplemented
        | Code::DataLoss
        | Code::Unauthenticated => CallOutcome::Success,
    }
}
