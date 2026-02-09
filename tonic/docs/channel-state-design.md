# Channel State Tracking Design

## Goal

Add per-channel connection state tracking to Tonic's `Channel` with a watch/notify mechanism,
allowing tower layers and users to observe real connection readiness.

## Connection States

Rather than conforming to gRPC's 5-state model, we directly mirror tonic's internal
`Reconnect::State` for simplicity and 1:1 mapping:

```rust
/// Channel connectivity state, mirrors `Reconnect::State` internally.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelState {
    /// Channel is idle and not attempting to connect.
    /// This is the initial state for lazy channels, or after a connection failure.
    Idle,

    /// Channel is attempting to establish a connection.
    /// Includes TCP connect, TLS handshake, and HTTP/2 handshake.
    Connecting,

    /// Channel has successfully established a connection and is ready for RPCs.
    Connected,
}
```

**Why not gRPC's states?**

| gRPC State | Tonic Equivalent | Notes |
|------------|------------------|-------|
| IDLE | `Idle` | Same |
| CONNECTING | `Connecting` | Same |
| READY | `Connected` | Same meaning, different name |
| TRANSIENT_FAILURE | `Idle` + error stored | Tonic doesn't distinguish; just goes back to Idle |
| SHUTDOWN | N/A | Tonic doesn't have explicit shutdown |

Using tonic's internal naming keeps the implementation simple - direct 1:1 mapping with
`Reconnect::State<F, S>` without needing to interpret additional fields.

## Architecture

### Why `tokio::sync::watch`?

We use `watch` because:
1. **Single producer, multiple consumers** - The `Reconnect` service updates state, multiple observers can watch
2. **Always has a value** - `borrow()` gives immediate synchronous access to current state
3. **Efficient for "latest value" semantics** - Observers always see the most recent state
4. **Works in `poll_ready()`** - Can be converted to `WatchStream` for polling in tower Services

### State Storage

```rust
// In tonic/src/transport/channel/service/reconnect.rs

/// Channel connectivity state, mirrors `Reconnect::State` internally.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelState {
    Idle,
    Connecting,
    Connected,
}

// Existing internal state with generic data
enum State<F, S> {
    Idle,
    Connecting(F),
    Connected(S),
}

// Helper to extract state kind (strips generic data)
impl<F, S> State<F, S> {
    fn kind(&self) -> ChannelState {
        match self {
            State::Idle => ChannelState::Idle,
            State::Connecting(_) => ChannelState::Connecting,
            State::Connected(_) => ChannelState::Connected,
        }
    }
}
```

```rust
// In tonic/src/transport/channel/state.rs (new file)
use std::sync::Arc;
use tokio::sync::watch;

/// Internal tracker that updates state (held by Reconnect service and connection task)
pub(crate) struct ChannelStateTracker {
    sender: watch::Sender<ChannelState>,
}

impl ChannelStateTracker {
    pub(crate) fn new(initial: ChannelState) -> (Self, watch::Receiver<ChannelState>) {
        let (sender, receiver) = watch::channel(initial);
        (Self { sender }, receiver)
    }

    pub(crate) fn set(&self, state: ChannelState) {
        self.sender.send_if_modified(|current| {
            if *current != state {
                *current = state;
                true
            } else {
                false
            }
        });
    }
}

pub(crate) type SharedStateTracker = Arc<ChannelStateTracker>;
```

### Integration Points

1. **Reconnect service** (`reconnect.rs`) - State transitions happen here
   - `Idle` → `Connecting`: When `make_service` is called
   - `Connecting` → `Connected`: When connection future resolves successfully
   - `Connecting` → `Idle`: When connection fails (will retry on next poll_ready)
   - `Connected` → `Idle`: When inner service fails `poll_ready`

2. **Connection task** (`connection.rs`) - Proactive notification
   - When `conn.await` completes → set `Idle` (connection died)

3. **Channel struct** (`channel/mod.rs`) - Expose the receiver
   - Add method to get `watch::Receiver<ChannelState>`

### State Transition Diagram

```
    ┌──────────────────────────────────────┐
    │               IDLE                    │◄─────────────────────┐
    │  (not connected, initial or failed)  │                      │
    └───────────────┬──────────────────────┘                      │
                    │ poll_ready() called                         │
                    ▼                                             │
    ┌──────────────────────────────────────┐                      │
    │           CONNECTING                  │                      │
    │  (TCP/TLS/HTTP2 handshake in progress)│──────────────────────┤
    └───────────────┬──────────────────────┘  handshake failed    │
                    │ handshake success                           │
                    ▼                                             │
    ┌──────────────────────────────────────┐                      │
    │            CONNECTED                  │                      │
    │  (ready to send requests)            │──────────────────────┘
    └──────────────────────────────────────┘  connection lost
                                              (detected by poll_ready
                                               or connection task)
```

Simple 3-state model that directly mirrors `Reconnect::State<F, S>`.

### Mapping to Tonic's Reconnect States

The `Reconnect` service in `reconnect.rs` has internal state:

```rust
enum State<F, S> {
    Idle,           // → ChannelState::Idle
    Connecting(F),  // → ChannelState::Connecting
    Connected(S),   // → ChannelState::Connected
}
```

**Direct 1:1 mapping** - no interpretation of additional fields needed:

| `Reconnect::State<F, S>` | `ChannelState` |
|--------------------------|----------------|
| `State::Idle` | `ChannelState::Idle` |
| `State::Connecting(_)` | `ChannelState::Connecting` |
| `State::Connected(_)` | `ChannelState::Connected` |

**Transition points in `poll_ready()`:**

```rust
// In Reconnect::poll_ready()
State::Idle => {
    // → ChannelState::Connecting (before make_service)
    let fut = self.mk_service.make_service(self.target.clone());
    self.state = State::Connecting(fut);
}
State::Connecting(ref mut f) => {
    match Pin::new(f).poll(cx) {
        Poll::Ready(Ok(service)) => {
            // → ChannelState::Connected
            self.state = State::Connected(service);
        }
        Poll::Ready(Err(_)) => {
            // → ChannelState::Idle (will retry)
            self.state = State::Idle;
        }
    }
}
State::Connected(ref mut inner) => {
    match inner.poll_ready(cx) {
        Poll::Ready(Err(_)) => {
            // → ChannelState::Idle (connection lost)
            self.state = State::Idle;
        }
    }
}
```

## Tonic Channel to Hyper Connection Architecture

This section explains how tonic's `Channel` connects all the way down to hyper's HTTP/2
connection and how connection state is managed.

### Complete State Machine Diagram with Code Pointers

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         TONIC CONNECTION STATE MACHINE                               │
│                                                                                      │
│  Files:                                                                              │
│    - channel/mod.rs           (Channel struct)                                       │
│    - channel/endpoint.rs      (Endpoint, connect/connect_lazy)                       │
│    - channel/service/connection.rs  (Connection, MakeSendRequestService, SendRequest)│
│    - channel/service/reconnect.rs   (Reconnect, State enum)                          │
└─────────────────────────────────────────────────────────────────────────────────────┘

                              INITIALIZATION
                              ══════════════

┌─────────────────────────────────────────────────────────────────────────────────────┐
│  endpoint.connect_lazy()  [endpoint.rs:486-493]                                      │
│      │                                                                               │
│      └─► Channel::new(connector, endpoint)  [channel/mod.rs]                        │
│            │                                                                         │
│            └─► Connection::new(connector, endpoint, is_lazy=true)  [connection.rs:28]│
│                  │                                                                   │
│                  └─► Reconnect::new(mk_service, uri, is_lazy=true)  [reconnect.rs:37]│
│                        │                                                             │
│                        └─► state: State::Idle  [reconnect.rs:40]                    │
│                            is_lazy: true                                             │
│                            has_been_connected: false                                 │
└─────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────────┐
│  endpoint.connect().await  [endpoint.rs:473-480]                                     │
│      │                                                                               │
│      └─► Channel::connect(connector, endpoint)  [channel/mod.rs]                    │
│            │                                                                         │
│            └─► Connection::connect(connector, endpoint)  [connection.rs:80-91]      │
│                  │                                                                   │
│                  └─► Connection::new(..., is_lazy=false).ready_oneshot().await      │
│                        │                                                             │
│                        └─► Polls until Connected, then returns                       │
└─────────────────────────────────────────────────────────────────────────────────────┘


                         RECONNECT STATE MACHINE
                         ══════════════════════

                    reconnect.rs:62-138  (poll_ready)

     ┌────────────────────────────────────────────────────────────────────┐
     │                                                                    │
     │                         ┌─────────┐                                │
     │            ┌───────────►│  IDLE   │◄────────────────┐              │
     │            │            └────┬────┘                 │              │
     │            │                 │                      │              │
     │            │                 │ [line 71-84]         │              │
     │            │                 │ mk_service.poll_ready(cx)           │
     │            │                 │ mk_service.make_service(target)     │
     │            │                 │                      │              │
     │            │                 ▼                      │              │
     │            │          ┌────────────┐                │              │
     │            │          │ CONNECTING │                │              │
     │            │          │    (F)     │                │              │
     │            │          └─────┬──────┘                │              │
     │            │                │                       │              │
     │            │    [line 85-109]                       │              │
     │            │    Pin::new(f).poll(cx)                │              │
     │            │         │      │      │                │              │
     │            │         │      │      │                │              │
     │   Err + lazy/        │      │      │                │              │
     │   has_connected      │      │      │         Err [line 125-128]    │
     │   [line 95-108]      │      │      │         inner.poll_ready()    │
     │   error stored       │      │      │         returns Err           │
     │            │         │      │      │                │              │
     │            │    Pending    Ok   Err + first         │              │
     │            │   [line 91]  [88]   connect            │              │
     │            │      │        │    [line 100-101]      │              │
     │            │      │        │    return Err          │              │
     │            │      │        │         │              │              │
     │            │      │        ▼         │              │              │
     │            │      │   ┌──────────┐   │              │              │
     │            └──────┼───│CONNECTED │───┼──────────────┘              │
     │                   │   │   (S)    │   │                             │
     │                   │   └────┬─────┘   │                             │
     │                   │        │         │                             │
     │              return    [line 111-130]│                             │
     │              Pending   inner.poll_ready(cx)                        │
     │                        │         │                                 │
     │                       Ok      Pending                              │
     │                   [line 117]  [line 121]                           │
     │                       │         │                                  │
     │                   return     return                                │
     │                   Ready(Ok)  Pending                               │
     │                                                                    │
     └────────────────────────────────────────────────────────────────────┘


                         DETAILED STATE TRANSITIONS
                         ═════════════════════════

┌─────────────────────────────────────────────────────────────────────────────────────┐
│ TRANSITION 1: Idle → Connecting                                                      │
│ Location: reconnect.rs:71-84                                                         │
│                                                                                      │
│   State::Idle => {                                                                   │
│       match self.mk_service.poll_ready(cx) {        // Check connector ready         │
│           Poll::Ready(r) => r?,                                                      │
│           Poll::Pending => return Poll::Pending,                                     │
│       }                                                                              │
│       let fut = self.mk_service.make_service(...);  // Start TCP+TLS+HTTP2          │
│       self.state = State::Connecting(fut);          // ← STATE CHANGE               │
│       continue;                                                                      │
│   }                                                                                  │
│                                                                                      │
│ What happens in make_service:                                                        │
│   → MakeSendRequestService::call()  [connection.rs:189-209]                         │
│     → connector.call(uri)           // TCP + TLS                                     │
│     → builder.handshake(io)         // HTTP/2 handshake                             │
│     → spawns conn task              // Background I/O driver                         │
│     → returns SendRequest                                                            │
└─────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────────┐
│ TRANSITION 2: Connecting → Connected (success)                                       │
│ Location: reconnect.rs:85-90                                                         │
│                                                                                      │
│   State::Connecting(ref mut f) => {                                                  │
│       match Pin::new(f).poll(cx) {                                                   │
│           Poll::Ready(Ok(service)) => {                                              │
│               state = State::Connected(service);    // ← STATE CHANGE               │
│           }                                                                          │
│       }                                                                              │
│   }                                                                                  │
│   self.state = state;  // [line 133]                                                │
│                                                                                      │
│ The 'service' is:                                                                    │
│   → SendRequest  [connection.rs:132-156]                                            │
│     → wraps hyper::client::conn::http2::SendRequest<Body>                           │
└─────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────────┐
│ TRANSITION 3: Connecting → Idle (connection failed, will retry)                      │
│ Location: reconnect.rs:95-108                                                        │
│                                                                                      │
│   State::Connecting(ref mut f) => {                                                  │
│       match Pin::new(f).poll(cx) {                                                   │
│           Poll::Ready(Err(e)) => {                                                   │
│               state = State::Idle;                  // ← STATE CHANGE               │
│                                                                                      │
│               if !(self.has_been_connected || self.is_lazy) {                       │
│                   return Poll::Ready(Err(e.into())); // First connect fails: error  │
│               } else {                                                               │
│                   self.error = Some(error);          // Store error for call()      │
│                   break;                             // poll_ready returns Ok(())   │
│               }                                                                      │
│           }                                                                          │
│       }                                                                              │
│   }                                                                                  │
│                                                                                      │
│ Note: If error stored, next call() returns error immediately [line 142-144]         │
└─────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────────┐
│ TRANSITION 4: Connected → Idle (connection lost)                                     │
│ Location: reconnect.rs:111-130                                                       │
│                                                                                      │
│   State::Connected(ref mut inner) => {                                               │
│       self.has_been_connected = true;               // Mark as previously connected │
│                                                                                      │
│       match inner.poll_ready(cx) {                  // Check hyper SendRequest      │
│           Poll::Ready(Ok(())) => return Poll::Ready(Ok(())),  // Still alive        │
│           Poll::Pending => return Poll::Pending,                                     │
│           Poll::Ready(Err(_)) => {                                                   │
│               state = State::Idle;                  // ← STATE CHANGE               │
│           }                                                                          │
│       }                                                                              │
│   }                                                                                  │
│   self.state = state;  // [line 133]                                                │
│   // Loop continues → Idle branch → starts reconnecting                             │
│                                                                                      │
│ What causes inner.poll_ready() to return Err:                                        │
│   → SendRequest::poll_ready()  [connection.rs:147-148]                              │
│     → self.inner.poll_ready()  (hyper's SendRequest)                                │
│       → checks dispatch.is_closed()                                                  │
│         → true when background conn task completed/died                              │
└─────────────────────────────────────────────────────────────────────────────────────┘


                    BACKGROUND CONNECTION TASK
                    ═════════════════════════

┌─────────────────────────────────────────────────────────────────────────────────────┐
│ Location: connection.rs:198-205                                                      │
│                                                                                      │
│   let (send_request, conn) = builder.handshake(io).await?;                          │
│                                                                                      │
│   Executor::execute(&executor, Box::pin(async move {                                │
│       if let Err(e) = conn.await {           // ← Runs until connection dies        │
│           tracing::debug!("connection task error: {:?}", e);                        │
│       }                                                                              │
│   }));                                                                               │
│                                                                                      │
│   // When conn.await completes:                                                      │
│   // 1. Connection is dead (network error, GOAWAY, server closed, etc.)             │
│   // 2. Dispatch channel receiver is dropped                                         │
│   // 3. send_request.dispatch.is_closed() returns true                              │
│   // 4. Next SendRequest::poll_ready() returns Err                                  │
│                                                                                      │
│ THIS IS WHERE PROACTIVE STATE NOTIFICATION SHOULD GO:                               │
│                                                                                      │
│   let state_tracker = state_tracker.clone();                                        │
│   Executor::execute(&executor, Box::pin(async move {                                │
│       let result = conn.await;                                                       │
│       if let Some(tracker) = &state_tracker {                                       │
│           tracker.set(ChannelState::Idle);   // ← Immediate notification!           │
│       }                                                                              │
│       if let Err(e) = result {                                                       │
│           tracing::debug!("connection task error: {:?}", e);                        │
│       }                                                                              │
│   }));                                                                               │
└─────────────────────────────────────────────────────────────────────────────────────┘


                         HYPER's SendRequest
                         ═══════════════════

┌─────────────────────────────────────────────────────────────────────────────────────┐
│ Tonic's wrapper: connection.rs:132-156                                               │
│                                                                                      │
│   struct SendRequest {                                                               │
│       inner: hyper::client::conn::http2::SendRequest<Body>,                         │
│   }                                                                                  │
│                                                                                      │
│   impl Service<Request<Body>> for SendRequest {                                      │
│       fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {   │
│           self.inner.poll_ready(cx).map_err(Into::into)                             │
│       }                                                                              │
│   }                                                                                  │
│                                                                                      │
│ Hyper's implementation (hyper/src/client/conn/http2.rs):                            │
│                                                                                      │
│   pub fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<()>> {         │
│       if self.is_closed() {                                                          │
│           Poll::Ready(Err(Error::new_closed()))     // ← Connection dead            │
│       } else {                                                                       │
│           Poll::Ready(Ok(()))                                                        │
│       }                                                                              │
│   }                                                                                  │
│                                                                                      │
│   pub fn is_closed(&self) -> bool {                                                  │
│       self.dispatch.is_closed()  // mpsc channel to conn task                       │
│   }                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────────┘


                         FULL SERVICE STACK
                         ══════════════════

┌─────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                      │
│   Channel  [channel/mod.rs]                                                          │
│     │                                                                                │
│     └─► Buffer  [tower::buffer]                                                     │
│           │                                                                          │
│           └─► (mpsc channel to worker)                                              │
│                 │                                                                    │
│                 └─► Worker Task (owns everything below)                             │
│                       │                                                              │
│                       └─► AddOrigin  [service/add_origin.rs]                        │
│                             │                                                        │
│                             └─► UserAgent  [service/user_agent.rs]                  │
│                                   │                                                  │
│                                   └─► GrpcTimeout  [transport/service/grpc_timeout] │
│                                         │                                            │
│                                         └─► Reconnect  [service/reconnect.rs]       │
│                                               │                                      │
│                                               │ State::Idle: no inner service       │
│                                               │ State::Connecting: has Future       │
│                                               │ State::Connected: has SendRequest   │
│                                               │                                      │
│                                               └─► SendRequest  [service/connection] │
│                                                     │                                │
│                                                     └─► hyper::http2::SendRequest   │
│                                                           │                          │
│                                                           └─► dispatch channel       │
│                                                                 │                    │
│                                                                 └─► conn task       │
│                                                                       (I/O driver)  │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────┘
```

### Service Stack Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Channel                                                                     │
│  └─► Buffer<Request<Body>, BoxFuture<...>>                                  │
│       └─► [Worker Task]                                                      │
│            └─► Connection                                                    │
│                 └─► BoxService (layer stack)                                │
│                      └─► AddOrigin                                          │
│                           └─► UserAgent                                     │
│                                └─► GrpcTimeout                              │
│                                     └─► Reconnect<MakeSendRequestService>   │
│                                          └─► SendRequest (tonic wrapper)    │
│                                               └─► hyper::client::conn::     │
│                                                    http2::SendRequest<Body> │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Components

#### 1. Channel (`channel/mod.rs`)
```rust
pub struct Channel {
    svc: Buffer<Request<Body>, BoxFuture<'static, Result<Response<Body>, BoxError>>>,
}
```
- Public API entry point
- Wraps everything in `tower::buffer::Buffer`
- `Buffer` spawns a **worker task** that owns the inner service

#### 2. Connection (`service/connection.rs`)
```rust
pub(crate) struct Connection {
    inner: BoxService<Request<Body>, Response<Body>, crate::BoxError>,
}
```
- Constructs the service layer stack
- Creates `Reconnect` with `MakeSendRequestService`

#### 3. Reconnect (`service/reconnect.rs`)
```rust
pub(crate) struct Reconnect<M, Target> {
    mk_service: M,                              // MakeSendRequestService
    state: State<M::Future, M::Response>,       // Idle/Connecting/Connected
    target: Target,                             // URI
    error: Option<BoxError>,
    has_been_connected: bool,
    is_lazy: bool,
}

enum State<F, S> {
    Idle,
    Connecting(F),   // F = Future that resolves to SendRequest
    Connected(S),    // S = SendRequest
}
```

#### 4. MakeSendRequestService (`service/connection.rs`)
```rust
struct MakeSendRequestService<C> {
    connector: C,                    // TCP/TLS connector
    executor: SharedExec,
    settings: Builder<SharedExec>,   // HTTP/2 settings
}
```
- Implements `tower::Service<Uri>` with `Response = SendRequest`
- Does TCP connect + TLS handshake + HTTP/2 handshake

#### 5. SendRequest (`service/connection.rs:132-156`)
```rust
struct SendRequest {
    inner: hyper::client::conn::http2::SendRequest<Body>,
}

impl From<hyper::client::conn::http2::SendRequest<Body>> for SendRequest {
    fn from(inner: hyper::client::conn::http2::SendRequest<Body>) -> Self {
        Self { inner }
    }
}

impl tower::Service<Request<Body>> for SendRequest {
    type Response = Response<Body>;
    type Error = crate::BoxError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)  // ← delegates to hyper
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let fut = self.inner.send_request(req);
        Box::pin(async move { fut.await.map_err(Into::into).map(|res| res.map(Body::new)) })
    }
}
```
- Thin wrapper around hyper's `SendRequest`
- Implements `tower::Service<Request<Body>>`
- `poll_ready` delegates to `hyper::SendRequest::poll_ready` (connection liveness check)
- `call` delegates to `hyper::SendRequest::send_request` (sends HTTP/2 request)

### How MakeService Links to Service::call

Tower's `MakeService` trait has a blanket implementation for any `Service<Target>`:

```rust
// In tower/src/make/make_service.rs
impl<M, S, Target, Request> MakeService<Target, Request> for M
where
    M: Service<Target, Response = S>,
    S: Service<Request>,
{
    fn make_service(&mut self, target: Target) -> Self::Future {
        Service::call(self, target)  // ← Delegates directly to Service::call
    }
}
```

**Important:** The caller must call `poll_ready()` before `make_service()`. This is the
Tower Service contract. `Reconnect` follows this correctly:

```rust
// In Reconnect::poll_ready()
State::Idle => {
    // Step 1: Call poll_ready on MakeSendRequestService
    match self.mk_service.poll_ready(cx) {
        Poll::Ready(r) => r?,
        Poll::Pending => return Poll::Pending,
    }

    // Step 2: Only after poll_ready succeeds, call make_service (→ Service::call)
    let fut = self.mk_service.make_service(self.target.clone());
    self.state = State::Connecting(fut);
    continue;
}
```

### Connection Creation Flow

When `Reconnect` transitions from `Idle` to `Connecting`:

```
Reconnect::poll_ready() [State::Idle]
    │
    ├─► mk_service.poll_ready(cx)              // Check connector ready
    │
    └─► mk_service.make_service(uri)           // MakeService trait method
        │
        └─► MakeSendRequestService::call(uri)  // Service::call (blanket impl)
            │
            ├─► connector.call(uri)            // TCP + TLS connection
            │   └─► returns: BoxedIo (TCP stream)
            │
            └─► builder.handshake(io).await    // HTTP/2 handshake
                │
                └─► returns: (SendRequest, Connection)
                    │
                    ├─► SendRequest: handle for sending requests
                    │   (has mpsc Sender to connection task)
                    │
                    └─► Connection: future that drives HTTP/2 I/O
                        (spawned as background task)
```

### Hyper's Connection Architecture

When `builder.handshake(io)` completes, it returns two parts:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│   handshake(io) returns:                                                    │
│                                                                             │
│   ┌──────────────────────┐         mpsc channel        ┌─────────────────┐ │
│   │     SendRequest      │◄───────────────────────────►│   Connection    │ │
│   │                      │    (dispatch channel)       │    (future)     │ │
│   │  - send_request()    │                             │                 │ │
│   │  - poll_ready()      │                             │  - Drives I/O   │ │
│   │  - is_closed()       │                             │  - Handles      │ │
│   │                      │                             │    GOAWAY       │ │
│   └──────────────────────┘                             │  - Manages      │ │
│          ▲                                             │    streams      │ │
│          │                                             └────────┬────────┘ │
│          │                                                      │          │
│   Returned to Reconnect                              Spawned as background │
│   as State::Connected(S)                             task by tonic         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

**In tonic** (`connection.rs:194-207`):
```rust
Box::pin(async move {
    let io = fut.await.map_err(Into::into)?;
    let (send_request, conn) = builder.handshake(io).await?;

    // Spawn connection as background task
    Executor::execute(&executor, Box::pin(async move {
        if let Err(e) = conn.await {
            tracing::debug!("connection task error: {:?}", e);
        }
    }));

    Ok(SendRequest::from(send_request))
})
```

### How Hyper Detects Connection Closure

**In hyper's `SendRequest::poll_ready`:**
```rust
pub fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<crate::Result<()>> {
    if self.is_closed() {
        Poll::Ready(Err(crate::Error::new_closed()))
    } else {
        Poll::Ready(Ok(()))
    }
}

pub fn is_closed(&self) -> bool {
    self.dispatch.is_closed()  // Checks if mpsc channel is closed
}
```

**Connection death detection flow (passive - current behavior):**

```
1. Connection dies (network error, server closes, GOAWAY, etc.)
   │
   ▼
2. Background connection task completes/errors
   │
   ▼
3. Connection task drops its end of the dispatch channel (mpsc Receiver)
   │
   ▼
4. SendRequest.dispatch.is_closed() returns true
   │
   ▼
5. Next SendRequest::poll_ready() returns Err(closed)  ← DELAYED until next poll!
   │
   ▼
6. Reconnect sees error in State::Connected branch:
   │
   │  State::Connected(ref mut inner) => {
   │      match inner.poll_ready(cx) {
   │          Poll::Ready(Err(_)) => {
   │              state = State::Idle;  // ← Triggers reconnection
   │          }
   │      }
   │  }
   │
   ▼
7. Loop continues: Idle → Connecting (new handshake)
```

### Why Tonic Spawns the Connection Task

The `conn` future returned from `builder.handshake(io)` **drives the HTTP/2 connection I/O**.
It must be continuously polled to:
- Read/write HTTP/2 frames on the TCP socket
- Handle control frames (PING, GOAWAY, WINDOW_UPDATE, SETTINGS)
- Multiplex streams
- Process keep-alive

If `conn` isn't polled, **no data flows** - the connection is dead. So it must run as a
background task:

```rust
// In MakeSendRequestService::call() - connection.rs:194-207
Box::pin(async move {
    let io = fut.await.map_err(Into::into)?;
    let (send_request, conn) = builder.handshake(io).await?;

    // conn MUST be spawned - it drives all HTTP/2 I/O
    Executor::execute(&executor, Box::pin(async move {
        if let Err(e) = conn.await {
            tracing::debug!("connection task error: {:?}", e);
        }
    }));

    Ok(SendRequest::from(send_request))
})
```

**Architecture:**
```
┌─────────────────────────────────────────────────────────────┐
│  SendRequest                     conn (background task)     │
│  ┌─────────────┐                ┌─────────────────────────┐ │
│  │ send_request├───requests────►│                         │ │
│  │             │◄──responses────┤  Drives actual I/O:     │ │
│  │ poll_ready  │                │  - TCP read/write       │ │
│  │             │   dispatch     │  - HTTP/2 framing       │ │
│  │ is_closed   │◄──channel─────►│  - Stream multiplexing  │ │
│  └─────────────┘                │  - Keep-alive/PING      │ │
│                                 └─────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Proactive Connection State Notification (IMPORTANT)

**Problem with passive detection:** The current implementation only detects connection
closure when `poll_ready()` is called. This means there's a delay between the connection
dying and observers being notified - they only learn about it on the next RPC attempt.

**Solution:** Update the state tracker directly from the spawned connection task when
`conn.await` completes:

```rust
// IMPROVED: Proactive notification when connection dies
let state_tracker = state_tracker.clone();
Executor::execute(&executor, Box::pin(async move {
    let result = conn.await;

    // Connection died - notify immediately!
    if let Some(tracker) = &state_tracker {
        tracker.set(ChannelState::Idle);  // Back to Idle, will reconnect on next poll_ready
    }

    if let Err(e) = result {
        tracing::debug!("connection task error: {:?}", e);
    }
}));
```

**Benefits of proactive notification:**

| Aspect | Passive (current) | Proactive (with state tracker) |
|--------|-------------------|-------------------------------|
| When notified | Next `poll_ready()` call | Immediately when connection dies |
| Delay | Depends on when next RPC happens | Zero - instant notification |
| LB behavior | May route requests to dead channel | Can immediately use other channels |
| User experience | First RPC after failure may fail | Seamless failover |

**Flow with proactive notification:**

```
1. Connection dies (network error, server closes, GOAWAY, etc.)
   │
   ▼
2. Background conn task completes
   │
   ├─► state_tracker.set(Idle)  ← IMMEDIATE notification!
   │   │
   │   ▼
   │   watch channel notifies all observers
   │   │
   │   ▼
   │   LB's StreamMap wakes up, updates connected_count
   │
   └─► dispatch channel receiver dropped
       │
       ▼
       SendRequest.is_closed() = true (for next poll_ready)
```

This is a **critical improvement** for load balancing - observers can react to connection
failures immediately rather than discovering them on the next request.

### Buffer Worker and poll_ready

**Important:** `Channel` wraps everything in `tower::buffer::Buffer`, which spawns a
background worker task:

```
┌─────────────────────────────────────────────────────────────────┐
│  Caller's task                                                  │
│                                                                 │
│  channel.poll_ready() → checks mpsc channel capacity            │
│  channel.call(req)    → sends req to worker via mpsc            │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼ mpsc channel
┌─────────────────────────────────────────────────────────────────┐
│  Buffer Worker task (spawned background task)                   │
│                                                                 │
│  loop {                                                         │
│      recv request from channel                                  │
│      inner_service.poll_ready().await  ← Reconnect::poll_ready  │
│      inner_service.call(req).await                              │
│      send response back                                         │
│  }                                                              │
└─────────────────────────────────────────────────────────────────┘
```

This means:
- `Reconnect::poll_ready()` is called by the **Buffer worker task**, not the caller
- State changes in `Reconnect` happen in the worker task
- The `watch` channel bridges state from worker task to external observers

### Summary: Where State Tracking Hooks In

```
Channel::poll_ready()
    │
    └─► Buffer worker task
        │
        └─► Reconnect::poll_ready()  ← STATE TRACKING (via state.kind())
            │
            ├─► State::Idle
            │   └─► set(Connecting), then make_service()
            │
            ├─► State::Connecting
            │   ├─► Success: set(Connected), state = Connected
            │   └─► Failure: set(Idle), state = Idle
            │
            └─► State::Connected
                └─► inner.poll_ready() fails: set(Idle), state = Idle


Connection task (spawned)  ← PROACTIVE NOTIFICATION
    │
    └─► conn.await completes
        └─► set(Idle)  ← Immediate notification, no poll_ready needed
```

The `ChannelStateTracker` will be:
1. Added to `Reconnect` and updated at each state transition via `self.state.kind()`
2. Passed to the spawned connection task for proactive notification when connection dies

## Implementation Steps

### Step 1: Add ChannelState enum to reconnect.rs
- Add `#[doc(hidden)] pub enum ChannelState { Idle, Connecting, Connected }`
- Add `impl State<F, S> { fn kind(&self) -> ChannelState }` helper
- Re-export from `channel/mod.rs`

### Step 2: Create state tracker module
- Add `tonic/src/transport/channel/state.rs`
- Define `ChannelStateTracker` struct (crate-internal)
- Add unit tests

### Step 3: Modify Reconnect
- Add `Option<SharedStateTracker>` field to `Reconnect` struct
- Add `with_state_tracker()` method or modify `new()`
- Update state on transitions in `poll_ready()`:
  ```rust
  // In poll_ready(), at each state transition:
  if let Some(ref tracker) = self.state_tracker {
      tracker.set(self.state.kind());
  }
  ```

### Step 4: Thread state through Connection
- Modify `Connection::new()` to create `ChannelStateTracker`
- Store the `watch::Receiver` for later retrieval
- Pass `SharedStateTracker` to `Reconnect`
- Pass `SharedStateTracker` to spawned connection task for proactive notification

### Step 5: Thread state through Channel
- Add `state_rx: watch::Receiver<ChannelState>` field to `Channel`
- Add public method:
  ```rust
  #[doc(hidden)]
  pub fn state(&self) -> watch::Receiver<ChannelState> {
      self.state_rx.clone()
  }
  ```

### Step 6: Update Endpoint
- `connect()` and `connect_lazy()` already call `Channel::new()` / `Channel::connect()`
- The state creation happens in `Connection`, so Endpoint changes may be minimal

### Step 7: Add integration tests
- Test that state transitions correctly during connect/disconnect cycles
- Test lazy vs eager connection initial states
- Test proactive notification when connection task dies

## API Usage Examples

### Example 1: Background monitoring task

```rust
let endpoint = Endpoint::from_static("http://localhost:50051");
let channel = endpoint.connect_lazy();

// Get a receiver to watch for state changes
let mut state_rx = channel.state();

// Monitor in background
tokio::spawn(async move {
    loop {
        let state = *state_rx.borrow_and_update();
        println!("Channel state: {:?}", state);

        if state_rx.changed().await.is_err() {
            break; // Channel dropped
        }
    }
});
```

### Example 2: Efficient `poll_ready()` with StreamMap (O(changed) complexity)

For load balancer layers that need to poll multiple channels' states efficiently.

**Key insight:** `StreamMap` internally only polls streams that have been woken (similar to
`FuturesUnordered`), giving O(changed) complexity instead of O(total_channels).

```rust
use tokio_stream::{StreamMap, StreamExt, wrappers::WatchStream};
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};

struct LoadBalancerService {
    channels: HashMap<ChannelId, Channel>,
    // StreamMap efficiently polls only streams with pending changes
    state_streams: StreamMap<ChannelId, WatchStream<ChannelState>>,
    // Track ready count for O(1) readiness check
    connected_count: usize,
    states: HashMap<ChannelId, ChannelState>,
}

impl LoadBalancerService {
    fn new() -> Self {
        Self {
            channels: HashMap::new(),
            state_streams: StreamMap::new(),
            connected_count: 0,
            states: HashMap::new(),
        }
    }

    fn add_channel(&mut self, id: ChannelId, channel: Channel) {
        let stream = WatchStream::new(channel.state());
        self.state_streams.insert(id.clone(), stream);
        self.states.insert(id.clone(), ChannelState::Idle);
        self.channels.insert(id, channel);
    }

    fn remove_channel(&mut self, id: &ChannelId) {
        if let Some(old_state) = self.states.remove(id) {
            if old_state == ChannelState::Connected {
                self.connected_count -= 1;
            }
        }
        self.state_streams.remove(id);
        self.channels.remove(id);
    }

    fn update_state(&mut self, id: ChannelId, new_state: ChannelState) {
        let old_state = self.states.insert(id, new_state);

        // Update connected_count for O(1) readiness check
        match (old_state, new_state) {
            (Some(ChannelState::Connected), s) if s != ChannelState::Connected => {
                self.connected_count -= 1;
            }
            (Some(s), ChannelState::Connected) if s != ChannelState::Connected => {
                self.connected_count += 1;
            }
            (None, ChannelState::Connected) => {
                self.connected_count += 1;
            }
            _ => {}
        }
    }
}

impl<B> Service<Request<B>> for LoadBalancerService {
    type Response = Response<Body>;
    type Error = BoxError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        // O(changed) - StreamMap only polls streams that have pending notifications
        while let Poll::Ready(Some((id, state))) = Pin::new(&mut self.state_streams).poll_next(cx) {
            self.update_state(id, state);
        }

        // O(1) readiness check
        if self.connected_count > 0 {
            Poll::Ready(Ok(()))
        } else {
            // Waker already registered by poll_next
            Poll::Pending
        }
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        // Select a Ready channel
        let ready_id = self.states
            .iter()
            .find(|(_, &s)| s == ChannelState::Connected)
            .map(|(id, _)| id.clone())
            .expect("poll_ready must be called first");

        let channel = self.channels.get(&ready_id).unwrap().clone();
        // ... forward to channel
    }
}
```

**Complexity:**
- `poll_ready`: O(changed) for processing state changes + O(1) for readiness check
- Only channels whose state actually changed are processed
- No O(n) iteration over all channels on every poll

### Example 3: Synchronous state check (no async)

```rust
let channel = endpoint.connect_lazy();
let state_rx = channel.state();

// Immediate synchronous check - no await needed
let current_state = *state_rx.borrow();
if current_state == ChannelState::Connected {
    println!("Channel is ready!");
}
```

## Files to Modify

1. **`tonic/src/transport/channel/service/reconnect.rs`**
   - Add `#[doc(hidden)] pub enum ChannelState { Idle, Connecting, Connected }`
   - Add `impl State<F, S> { fn kind(&self) -> ChannelState }` helper
   - Add `state_tracker: Option<SharedStateTracker>` field to `Reconnect`
   - Update state at each transition point in `poll_ready()`

2. **`tonic/src/transport/channel/state.rs`** (NEW)
   - `ChannelStateTracker` struct (crate-internal)
   - `SharedStateTracker` type alias

3. **`tonic/src/transport/channel/mod.rs`**
   - Add `mod state;`
   - Re-export `ChannelState` with `#[doc(hidden)]`
   - Add `state_rx` field to `Channel`
   - Add `#[doc(hidden)] fn state()` method

4. **`tonic/src/transport/channel/service/mod.rs`**
   - Re-export state types for internal use

5. **`tonic/src/transport/channel/service/connection.rs`**
   - Create `ChannelStateTracker` in `Connection::new()`
   - Pass tracker to `Reconnect`
   - Pass tracker to spawned connection task (proactive notification)
   - Store receiver for `Channel` to retrieve

6. **`tonic/src/transport/channel/endpoint.rs`**
   - Minimal changes - state flows through `Connection`

## Dependencies

### tonic/Cargo.toml changes

```toml
# Update tokio-stream to include sync feature for WatchStream
tokio-stream = { version = "0.1.16", default-features = false, features = ["sync"] }
```

Or, add it conditionally under the `channel` feature:
```toml
[features]
channel = [
  # ... existing deps
  "tokio-stream/sync",  # for WatchStream support
]
```

### tonic-xds/Cargo.toml

```toml
# tokio-stream with sync feature for StreamMap + WatchStream
tokio-stream = { version = "0.1", features = ["sync"] }
```

### Re-exports from tonic (optional)

To make it convenient for users, tonic could re-export:
```rust
// In tonic/src/transport/channel/mod.rs
pub use tokio_stream::wrappers::WatchStream;
```

## Testing Plan

1. **Unit tests** (`tonic/src/transport/channel/state.rs`)
   - State tracker sends/receives correctly
   - No notification on same-state set
   - Multiple receivers get updates

2. **Integration tests** (`tonic/tests/`)
   - Lazy channel starts in `Idle`
   - Eager channel transitions `Connecting` → `Ready`
   - Connection failure → `Idle`
   - Reconnection after failure
   - State receiver works after channel clone

3. **Load balancer test** (`tonic-xds`)
   - Multiple channels with `StreamMap` + `WatchStream` in `poll_ready()`
   - Verify O(changed) behavior: only changed channels trigger processing
   - Verify `connected_count` tracking is correct across state transitions
   - Test add/remove channel dynamically
   - Test that waker is properly registered when all channels are non-Ready

## Summary

**tonic provides:**
- `#[doc(hidden)] ChannelState` enum (mirrors `Reconnect::State`)
- `#[doc(hidden)] Channel::state() -> watch::Receiver<ChannelState>`
- State tracking in `Reconnect` service via `state.kind()`
- Proactive notification from spawned connection task
- `tokio-stream/sync` feature for `WatchStream`

**tonic-xds consumes:**
- `StreamMap<ChannelId, WatchStream<ChannelState>>` for efficient O(changed) polling
- `connected_count` for O(1) readiness check
- Dynamic channel add/remove support
- Immediate failover when connections die (proactive notification)

---

## Implementation Summary

### Files Modified in tonic

#### 1. `src/transport/channel/state.rs` (NEW)

Created the channel state tracking module with:

```rust
/// Channel connectivity state, mirrors `Reconnect::State` internally.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChannelState {
    Idle,
    Connecting,
    Connected,
}

/// Internal tracker that updates channel connectivity state.
pub(crate) struct ChannelStateTracker {
    sender: watch::Sender<ChannelState>,
}

pub(crate) type SharedStateTracker = Arc<ChannelStateTracker>;
```

Includes unit tests for:
- State display formatting
- Basic state transitions
- No duplicate notifications for same state
- Multiple receivers receiving updates

#### 2. `src/transport/channel/service/reconnect.rs`

Added state tracking to the `Reconnect` service:

- Added `state_tracker: Option<SharedStateTracker>` field
- Added `State::kind()` method to convert internal state to `ChannelState`:
  ```rust
  impl<F, S> State<F, S> {
      fn kind(&self) -> ChannelState {
          match self {
              State::Idle => ChannelState::Idle,
              State::Connecting(_) => ChannelState::Connecting,
              State::Connected(_) => ChannelState::Connected,
          }
      }
  }
  ```
- Added `set_state()` method to ensure all state transitions notify the tracker:
  ```rust
  fn set_state(&mut self, new_state: State<M::Future, M::Response>) {
      self.state = new_state;
      if let Some(ref tracker) = self.state_tracker {
          tracker.set(self.state.kind());
      }
  }
  ```
- Modified `poll_ready()` to use `set_state()` for all state transitions

#### 3. `src/transport/channel/service/connection.rs`

Integrated state tracking into the connection lifecycle:

- Added `state_rx: watch::Receiver<ChannelState>` field to `Connection`
- Modified `Connection::new()` to:
  - Create `ChannelStateTracker` with appropriate initial state (Idle for lazy, Connecting for eager)
  - Pass `SharedStateTracker` to both `Reconnect` and `MakeSendRequestService`
- Added `Connection::state()` method returning `watch::Receiver<ChannelState>`
- Modified `MakeSendRequestService` to hold `SharedStateTracker`
- **Proactive notification:** Modified spawned connection task to notify `Idle` when connection dies:
  ```rust
  let task_state_tracker = state_tracker.clone();
  Executor::execute(&executor, Box::pin(async move {
      let result = conn.await;
      // Connection died - notify immediately!
      task_state_tracker.set(ChannelState::Idle);
      if let Err(e) = result {
          tracing::debug!("connection task error: {:?}", e);
      }
  }));
  ```

#### 4. `src/transport/channel/mod.rs`

Exposed state tracking through the public API:

- Added `mod state;` and `pub use state::ChannelState;`
- Added `state_rx: Option<watch::Receiver<ChannelState>>` field to `Channel`
  - `Some` for single-endpoint channels
  - `None` for balanced channels (which have multiple underlying connections)
- Modified `Channel::new()` and `Channel::connect()` to extract `state_rx` from `Connection`
- Added public method:
  ```rust
  #[doc(hidden)]
  pub fn state(&self) -> Option<watch::Receiver<ChannelState>> {
      self.state_rx.clone()
  }
  ```

#### 5. `src/transport/mod.rs`

- Added `ChannelState` to the public exports

### Files Modified in tonic-xds

#### 1. `Cargo.toml`

- Changed tonic dependency to use local path: `tonic = { path = "../tonic", features = ["channel"] }`
- Added `sync` feature to tokio-stream: `tokio-stream = { version = "0.1", features = ["sync"] }`
- Updated tonic-prost and tonic-prost-build to use local paths

#### 2. `src/client/channel_state_test.rs` (NEW)

Created comprehensive tests demonstrating StreamMap usage with channel states:

1. **`test_stream_map_channel_state_tracking`**
   - Basic StreamMap usage for tracking multiple channel states
   - Demonstrates draining initial values from WatchStream
   - Verifies state change notifications are received correctly

2. **`test_stream_map_with_select`**
   - Non-blocking state monitoring using `tokio::select!`
   - Demonstrates concurrent state updates from multiple channels

3. **`test_stream_map_dynamic_membership`**
   - Adding and removing channels from StreamMap dynamically
   - Verifies only active channels' updates are received

4. **`test_count_ready_channels`**
   - Demonstrates efficient counting of connected channels
   - Pattern for O(1) readiness checking in load balancers

#### 3. `src/client/mod.rs`

- Added `#[cfg(test)] mod channel_state_test;`

### Test Results

All 4 StreamMap tests pass:
```
running 4 tests
test client::channel_state_test::tests::test_count_ready_channels ... ok
test client::channel_state_test::tests::test_stream_map_channel_state_tracking ... ok
test client::channel_state_test::tests::test_stream_map_dynamic_membership ... ok
test client::channel_state_test::tests::test_stream_map_with_select ... ok

test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 1 filtered out
```

### Key Design Decisions

1. **`Option<watch::Receiver>` for balanced channels**: Balanced channels don't have a single
   connection state, so `Channel::state()` returns `None` for them.

2. **Proactive notification**: The spawned connection task notifies `Idle` immediately when
   the connection dies, enabling load balancers to react without waiting for the next RPC.

3. **`set_state()` method**: All state transitions in `Reconnect` go through `set_state()` to
   ensure the tracker is always notified, preventing missed state changes.

4. **`#[doc(hidden)]`**: The API is marked as hidden since it's primarily for internal use
   by tonic-xds and may change in future versions.
