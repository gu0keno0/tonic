//! Errors for the load balancer.

/// Errors produced by the load balancer.
#[derive(Debug, thiserror::Error)]
pub(crate) enum LbError {
    /// No ready endpoints available to serve the request.
    #[error("no ready endpoints available")]
    Unavailable,

    /// The underlying service returned an error.
    #[error("service error: {0}")]
    ServiceFailure(tower::BoxError),

    /// Internal precondition violation (bug).
    #[error("internal error: {0}")]
    Precondition(&'static str),
}

impl From<LbError> for tonic::Status {
    fn from(err: LbError) -> Self {
        match err {
            LbError::Unavailable => tonic::Status::unavailable("no ready endpoints available"),
            LbError::Precondition(msg) => tonic::Status::internal(msg),
            LbError::ServiceFailure(source) => match source.downcast::<tonic::Status>() {
                Ok(status) => *status,
                Err(source) => tonic::Status::unknown(format!("service error: {source}")),
            },
        }
    }
}
