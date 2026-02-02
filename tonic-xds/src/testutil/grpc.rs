//! Test utilities for gRPC servers and clients.
use std::error::Error;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::{net::TcpListener, sync::oneshot};
use tonic::server::NamedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint, Server, ServerTlsConfig};
use tonic::{Code, Request, Response, Status};

pub(crate) use crate::testutil::proto::helloworld::{
    greeter_client::GreeterClient,
    greeter_server::{Greeter, GreeterServer},
    HelloReply, HelloRequest,
};

#[derive(Default)]
struct MyGreeter {
    msg: String,
}

#[tonic::async_trait]
impl Greeter for MyGreeter {
    async fn say_hello(&self, req: Request<HelloRequest>) -> Result<Response<HelloReply>, Status> {
        Ok(Response::new(HelloReply {
            message: format!("{}: {}", self.msg, req.into_inner().name),
        }))
    }
}

/// A greeter that fails with a configurable error rate or always fails.
struct FaultyGreeter {
    msg: String,
    /// If Some, fail this percentage of requests (0-100). If None, always fail.
    error_rate: Option<u32>,
    request_count: AtomicU64,
}

#[tonic::async_trait]
impl Greeter for FaultyGreeter {
    async fn say_hello(&self, req: Request<HelloRequest>) -> Result<Response<HelloReply>, Status> {
        let count = self.request_count.fetch_add(1, Ordering::Relaxed);

        let should_fail = match self.error_rate {
            None => true, // Always fail
            Some(rate) => (count % 100) < rate as u64,
        };

        if should_fail {
            Err(Status::new(Code::Unavailable, "server is unavailable"))
        } else {
            Ok(Response::new(HelloReply {
                message: format!("{}: {}", self.msg, req.into_inner().name),
            }))
        }
    }
}

/// A test server that runs a gRPC service and provides a channel for clients to connect.
pub(crate) struct TestServer {
    /// The gRPC channel for talking to the test server.
    pub channel: Channel,
    /// Signal the server to shutdown.
    pub shutdown: oneshot::Sender<()>,
    /// Handle to wait for server to exit.
    pub handle: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    /// Server address.
    pub addr: SocketAddr,
}

impl NamedService for TestServer {
    const NAME: &'static str = "TestServer";
}

/// Spawns a gRPC greeter server for testing purposes.
pub(crate) async fn spawn_greeter_server(
    msg: &str,
    server_tls: Option<ServerTlsConfig>,
    client_tls: Option<ClientTlsConfig>,
) -> Result<TestServer, Box<dyn Error>> {
    // Bind to an ephemeral port (random free port assigned by OS)
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let (tx, rx) = oneshot::channel();

    let svc = GreeterServer::new(MyGreeter {
        msg: msg.to_string(),
    });

    let handle = tokio::spawn(async move {
        let mut builder = if let Some(tls) = server_tls {
            Server::builder().tls_config(tls)?
        } else {
            Server::builder()
        };
        let res = builder
            .add_service(svc)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = rx.await;
            })
            .await;
        match res {
            Ok(_) => println!("Server exited cleanly"),
            Err(e) => eprintln!("Server error: {e}"),
        }

        Ok(())
    });

    let channel = if let Some(client_tls) = client_tls {
        let endpoint_str = format!("https://{addr}");
        Endpoint::from_shared(endpoint_str)?
            .tls_config(client_tls)?
            .connect()
            .await?
    } else {
        let endpoint_str = format!("http://{addr}");
        Endpoint::from_shared(endpoint_str)?.connect().await?
    };

    Ok(TestServer {
        channel,
        shutdown: tx,
        handle,
        addr,
    })
}

/// Spawns a faulty gRPC greeter server that returns errors.
///
/// # Arguments
/// * `msg` - Server identifier in responses
/// * `error_rate` - If Some(n), fail n% of requests. If None, always fail.
pub(crate) async fn spawn_faulty_greeter_server(
    msg: &str,
    error_rate: Option<u32>,
) -> Result<TestServer, Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);

    let (tx, rx) = oneshot::channel();

    let svc = GreeterServer::new(FaultyGreeter {
        msg: msg.to_string(),
        error_rate,
        request_count: AtomicU64::new(0),
    });

    let handle = tokio::spawn(async move {
        let res = Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(incoming, async {
                let _ = rx.await;
            })
            .await;
        if let Err(e) = &res {
            eprintln!("Faulty server error: {e}");
        }
        Ok(())
    });

    let endpoint_str = format!("http://{addr}");
    let channel = Endpoint::from_shared(endpoint_str)?.connect().await?;

    Ok(TestServer {
        channel,
        shutdown: tx,
        handle,
        addr,
    })
}
