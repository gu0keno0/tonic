//! Errors for the load balancer.

/// Errors produced by the load balancer.
#[derive(Debug, thiserror::Error)]
pub(crate) enum LbError {
    /// No ready endpoints available to serve the request.
    #[error("no ready endpoints available")]
    Unavailable,

    /// The selected lb channel was not ready.
    #[error("lb channel not ready")]
    LbChannelNotReady,

    /// The selected lb channel returned an error.
    #[error("lb channel error: {0}")]
    LbChannelError(tower::BoxError),

    /// Internal precondition violation (bug).
    #[error("internal error: {0}")]
    Precondition(&'static str),
}

impl From<LbError> for tonic::Status {
    fn from(err: LbError) -> Self {
        match err {
            LbError::Unavailable => tonic::Status::unavailable("no ready endpoints available"),
            LbError::LbChannelNotReady => tonic::Status::unavailable("lb channel not ready"),
            LbError::Precondition(msg) => tonic::Status::internal(msg),
            LbError::LbChannelError(source) => match source.downcast::<tonic::Status>() {
                Ok(status) => *status,
                Err(source) => tonic::Status::unknown(format!("lb channel error: {source}")),
            },
        }
    }
}
