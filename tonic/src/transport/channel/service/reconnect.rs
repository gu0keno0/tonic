use super::super::state::{ChannelState, SharedStateTracker};
use pin_project::pin_project;
use std::fmt;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tower::make::MakeService;
use tower_service::Service;
use tracing::trace;

pub(crate) struct Reconnect<M, Target>
where
    M: Service<Target>,
    M::Error: Into<crate::BoxError>,
{
    mk_service: M,
    state: State<M::Future, M::Response>,
    target: Target,
    error: Option<crate::BoxError>,
    has_been_connected: bool,
    is_lazy: bool,
    state_tracker: Option<SharedStateTracker>,
}

#[derive(Debug)]
enum State<F, S> {
    Idle,
    Connecting(F),
    Connected(S),
}

impl<F, S> State<F, S> {
    /// Returns the ChannelState corresponding to this internal state.
    fn kind(&self) -> ChannelState {
        match self {
            State::Idle => ChannelState::Idle,
            State::Connecting(_) => ChannelState::Connecting,
            State::Connected(_) => ChannelState::Connected,
        }
    }
}

impl<M, Target> Reconnect<M, Target>
where
    M: Service<Target>,
    M::Error: Into<crate::BoxError>,
{
    pub(crate) fn new(
        mk_service: M,
        target: Target,
        is_lazy: bool,
        state_tracker: Option<SharedStateTracker>,
    ) -> Self {
        Reconnect {
            mk_service,
            state: State::Idle,
            target,
            error: None,
            has_been_connected: false,
            is_lazy,
            state_tracker,
        }
    }

    /// Sets the internal state and notifies the tracker.
    /// All state transitions MUST go through this method to ensure
    /// the ChannelStateTracker is notified of state changes.
    fn set_state(&mut self, new_state: State<M::Future, M::Response>) {
        self.state = new_state;
        if let Some(ref tracker) = self.state_tracker {
            tracker.set(self.state.kind());
        }
    }
}

impl<M, Target, S, Request> Service<Request> for Reconnect<M, Target>
where
    M: Service<Target, Response = S>,
    S: Service<Request>,
    M::Future: Unpin,
    crate::BoxError: From<M::Error> + From<S::Error>,
    Target: Clone,
    <M as tower_service::Service<Target>>::Error: Into<crate::BoxError>,
{
    type Response = S::Response;
    type Error = crate::BoxError;
    type Future = ResponseFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut state;

        if self.error.is_some() {
            return Poll::Ready(Ok(()));
        }

        loop {
            match self.state {
                State::Idle => {
                    trace!("poll_ready; idle");
                    match self.mk_service.poll_ready(cx) {
                        Poll::Ready(r) => r?,
                        Poll::Pending => {
                            trace!("poll_ready; MakeService not ready");
                            return Poll::Pending;
                        }
                    }

                    let fut = self.mk_service.make_service(self.target.clone());
                    // Use set_state to notify tracker
                    self.set_state(State::Connecting(fut));
                    continue;
                }
                State::Connecting(ref mut f) => {
                    trace!("poll_ready; connecting");
                    match Pin::new(f).poll(cx) {
                        Poll::Ready(Ok(service)) => {
                            state = State::Connected(service);
                        }
                        Poll::Pending => {
                            trace!("poll_ready; not ready");
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(e)) => {
                            trace!("poll_ready; error");

                            state = State::Idle;

                            if !(self.has_been_connected || self.is_lazy) {
                                // Use set_state to notify tracker before returning error
                                self.set_state(state);
                                return Poll::Ready(Err(e.into()));
                            } else {
                                let error = e.into();
                                tracing::debug!("reconnect::poll_ready: {:?}", error);
                                self.error = Some(error);
                                break;
                            }
                        }
                    }
                }
                State::Connected(ref mut inner) => {
                    trace!("poll_ready; connected");

                    self.has_been_connected = true;

                    match inner.poll_ready(cx) {
                        Poll::Ready(Ok(())) => {
                            trace!("poll_ready; ready");
                            return Poll::Ready(Ok(()));
                        }
                        Poll::Pending => {
                            trace!("poll_ready; not ready");
                            return Poll::Pending;
                        }
                        Poll::Ready(Err(_)) => {
                            trace!("poll_ready; error");
                            state = State::Idle;
                        }
                    }
                }
            }

            // Use set_state to notify tracker
            self.set_state(state);
        }

        // Use set_state to notify tracker
        self.set_state(state);
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request) -> Self::Future {
        tracing::trace!("Reconnect::call");
        if let Some(error) = self.error.take() {
            tracing::debug!("error: {}", error);
            return ResponseFuture::error(error);
        }

        let State::Connected(service) = &mut self.state else {
            panic!("service not ready; poll_ready must be called first");
        };

        let fut = service.call(request);
        ResponseFuture::new(fut)
    }
}

impl<M, Target> fmt::Debug for Reconnect<M, Target>
where
    M: Service<Target> + fmt::Debug,
    M::Future: fmt::Debug,
    M::Response: fmt::Debug,
    Target: fmt::Debug,
    <M as tower_service::Service<Target>>::Error: Into<crate::BoxError>,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Reconnect")
            .field("mk_service", &self.mk_service)
            .field("state", &self.state)
            .field("target", &self.target)
            .finish()
    }
}

/// Future that resolves to the response or failure to connect.
#[pin_project]
#[derive(Debug)]
pub(crate) struct ResponseFuture<F> {
    #[pin]
    inner: Inner<F>,
}

#[pin_project(project = InnerProj)]
#[derive(Debug)]
enum Inner<F> {
    Future(#[pin] F),
    Error(Option<crate::BoxError>),
}

impl<F> ResponseFuture<F> {
    pub(crate) fn new(inner: F) -> Self {
        ResponseFuture {
            inner: Inner::Future(inner),
        }
    }

    pub(crate) fn error(error: crate::BoxError) -> Self {
        ResponseFuture {
            inner: Inner::Error(Some(error)),
        }
    }
}

impl<F, T, E> Future for ResponseFuture<F>
where
    F: Future<Output = Result<T, E>>,
    E: Into<crate::BoxError>,
{
    type Output = Result<T, crate::BoxError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        //self.project().inner.poll(cx).map_err(Into::into)
        let me = self.project();
        match me.inner.project() {
            InnerProj::Future(fut) => fut.poll(cx).map_err(Into::into),
            InnerProj::Error(e) => {
                let e = e.take().expect("Polled after ready.");
                Poll::Ready(Err(e))
            }
        }
    }
}
