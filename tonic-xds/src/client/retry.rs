//! Retry service for xDS channels.
//!
//! Provides retry logic for transient connection failures.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::BodyExt;
use tonic::body::Body as TonicBody;
use tower::{BoxError, Service};

/// Configuration for connection retry.
#[derive(Clone)]
pub struct ConnectionRetryConfig {
    /// Maximum number of retries per request.
    pub max_retries: u32,
    /// Counter for tracking retries (for testing).
    pub retry_counter: Arc<AtomicU64>,
}

impl ConnectionRetryConfig {
    /// Creates a new retry config with the given maximum number of retries.
    pub fn new(max_retries: u32) -> Self {
        Self {
            max_retries,
            retry_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Creates a retry config with a shared counter for tracking retries (for testing).
    pub fn with_counter(max_retries: u32, counter: Arc<AtomicU64>) -> Self {
        Self {
            max_retries,
            retry_counter: counter,
        }
    }
}

/// A retry service that retries on connection errors.
///
/// This service will retry requests that fail due to connection-level errors
/// (e.g., connection refused, connection reset) but not gRPC-level errors.
/// It buffers the request body to allow retries.
#[derive(Clone)]
pub struct ConnectionRetryService<S> {
    inner: S,
    config: ConnectionRetryConfig,
}

impl<S> ConnectionRetryService<S> {
    /// Creates a new retry service wrapping the given inner service.
    pub fn new(inner: S, config: ConnectionRetryConfig) -> Self {
        Self { inner, config }
    }
}

/// Check if an error is a connection error worth retrying.
fn is_connection_error(e: &BoxError) -> bool {
    let error_str = format!("{:?}", e);
    error_str.contains("connection")
        || error_str.contains("Connection")
        || error_str.contains("connect error")  // "tcp connect error"
        || error_str.contains("refused")
        || error_str.contains("reset")
        || error_str.contains("broken pipe")
        || error_str.contains("os error 111") // ECONNREFUSED on Linux
        || error_str.contains("os error 61") // ECONNREFUSED on macOS
}

impl<S> Service<Request<TonicBody>> for ConnectionRetryService<S>
where
    S: Service<Request<TonicBody>, Response = Response<TonicBody>, Error = BoxError>
        + Clone
        + Send
        + 'static,
    S::Future: Send,
{
    type Response = Response<TonicBody>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<TonicBody>) -> Self::Future {
        let mut inner = self.inner.clone();
        let max_retries = self.config.max_retries;
        let retry_counter = self.config.retry_counter.clone();

        Box::pin(async move {
            // Buffer the request body so we can retry
            let (parts, body) = request.into_parts();
            let body_bytes = body
                .collect()
                .await
                .map_err(|e| BoxError::from(format!("failed to buffer body: {}", e)))?
                .to_bytes();

            let mut last_error: Option<BoxError> = None;
            let mut attempts = 0;

            while attempts <= max_retries {
                // Reconstruct request with cloned body
                let req = Request::from_parts(
                    parts.clone(),
                    TonicBody::new(http_body_util::Full::new(body_bytes.clone())),
                );

                match inner.call(req).await {
                    Ok(response) => return Ok(response),
                    Err(e) => {
                        if is_connection_error(&e) && attempts < max_retries {
                            retry_counter.fetch_add(1, Ordering::Relaxed);
                            attempts += 1;
                            last_error = Some(e);
                            // Continue to retry
                        } else {
                            return Err(e);
                        }
                    }
                }
            }

            Err(last_error.unwrap_or_else(|| BoxError::from("retry exhausted")))
        })
    }
}

/// Layer for adding connection retry to a service.
#[derive(Clone)]
pub struct ConnectionRetryLayer {
    config: ConnectionRetryConfig,
}

impl ConnectionRetryLayer {
    /// Creates a new retry layer with the given config.
    pub fn new(config: ConnectionRetryConfig) -> Self {
        Self { config }
    }
}

impl<S> tower::Layer<S> for ConnectionRetryLayer {
    type Service = ConnectionRetryService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        ConnectionRetryService::new(inner, self.config.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_connection_error() {
        assert!(is_connection_error(&BoxError::from("connection refused")));
        assert!(is_connection_error(&BoxError::from("Connection reset")));
        assert!(is_connection_error(&BoxError::from("os error 61")));
        assert!(!is_connection_error(&BoxError::from("grpc status: unavailable")));
        assert!(!is_connection_error(&BoxError::from("timeout")));
    }
}
