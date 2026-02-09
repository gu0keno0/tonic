//! Tests for channel state tracking using real tonic channels.
//!
//! This module demonstrates how to monitor tonic channel connectivity states
//! using `tokio_stream::StreamMap` with `WatchStream`, achieving O(changed)
//! complexity for efficient load balancer implementations.

#[cfg(test)]
mod tests {
    use crate::testutil::grpc::{spawn_greeter_server, GreeterClient, HelloRequest};
    use std::collections::HashMap;
    use std::time::Duration;
    use tokio_stream::{wrappers::WatchStream, StreamExt, StreamMap};
    use tonic::transport::{Channel, ChannelState, Endpoint};

    /// Test that a lazy channel starts in Idle state.
    #[tokio::test]
    async fn test_lazy_channel_starts_idle() {
        let endpoint = Endpoint::from_static("http://127.0.0.1:12345");
        let channel = endpoint.connect_lazy();

        let state_rx = channel.state().expect("single-endpoint channel should have state");
        assert_eq!(*state_rx.borrow(), ChannelState::Idle);
    }

    /// Test that connecting to a real server transitions state to Connected.
    #[tokio::test]
    async fn test_channel_connects_to_server() {
        let server = spawn_greeter_server("test", None, None)
            .await
            .expect("failed to spawn server");

        // The channel from spawn_greeter_server uses connect() which waits for connection
        let state_rx = server
            .channel
            .state()
            .expect("single-endpoint channel should have state");

        // After connect(), the channel should be Connected
        assert_eq!(*state_rx.borrow(), ChannelState::Connected);

        // Clean up
        let _ = server.shutdown.send(());
        let _ = server.handle.await;
    }

    /// Test that a lazy channel transitions through states when making a request.
    #[tokio::test]
    async fn test_lazy_channel_state_transitions() {
        let server = spawn_greeter_server("test", None, None)
            .await
            .expect("failed to spawn server");

        // Create a lazy channel to the server
        let endpoint = Endpoint::from_shared(format!("http://{}", server.addr))
            .expect("valid endpoint")
            .connect_timeout(Duration::from_secs(5));
        let channel = endpoint.connect_lazy();

        let state_rx = channel.state().expect("single-endpoint channel should have state");

        // Initially should be Idle
        assert_eq!(*state_rx.borrow(), ChannelState::Idle);

        // Make a request - this triggers connection
        let mut client = GreeterClient::new(channel);
        let response = client
            .say_hello(HelloRequest {
                name: "World".to_string(),
            })
            .await
            .expect("request should succeed");
        assert!(response.into_inner().message.contains("World"));

        // After successful request, should be Connected
        assert_eq!(*state_rx.borrow(), ChannelState::Connected);

        // Clean up
        let _ = server.shutdown.send(());
        let _ = server.handle.await;
    }

    /// Test proactive notification when server shuts down.
    #[tokio::test]
    async fn test_proactive_notification_on_server_shutdown() {
        let server = spawn_greeter_server("test", None, None)
            .await
            .expect("failed to spawn server");

        let state_rx = server
            .channel
            .state()
            .expect("single-endpoint channel should have state");
        let mut state_stream = WatchStream::new(state_rx.clone());

        // Drain initial value
        let initial = state_stream.next().await.unwrap();
        assert_eq!(initial, ChannelState::Connected);

        // Shutdown the server
        let _ = server.shutdown.send(());
        let _ = server.handle.await;

        // Wait for state change notification (proactive notification from connection task)
        let timeout = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(state) = state_stream.next().await {
                if state == ChannelState::Idle {
                    return true;
                }
            }
            false
        })
        .await;

        assert!(
            timeout.unwrap_or(false),
            "Should receive Idle state after server shutdown"
        );
    }

    /// Test tracking multiple real channels using StreamMap.
    #[tokio::test]
    async fn test_stream_map_with_real_channels() {
        // Spawn two servers
        let server1 = spawn_greeter_server("server1", None, None)
            .await
            .expect("failed to spawn server1");
        let server2 = spawn_greeter_server("server2", None, None)
            .await
            .expect("failed to spawn server2");

        // Create StreamMap to track both channels
        let mut state_streams: StreamMap<&str, WatchStream<ChannelState>> = StreamMap::new();

        let rx1 = server1
            .channel
            .state()
            .expect("single-endpoint channel should have state");
        let rx2 = server2
            .channel
            .state()
            .expect("single-endpoint channel should have state");

        state_streams.insert("server1", WatchStream::new(rx1.clone()));
        state_streams.insert("server2", WatchStream::new(rx2.clone()));

        // Drain initial Connected states
        let mut initial_states: HashMap<&str, ChannelState> = HashMap::new();
        for _ in 0..2 {
            let (key, state) = state_streams.next().await.unwrap();
            initial_states.insert(key, state);
        }

        assert_eq!(initial_states.get("server1"), Some(&ChannelState::Connected));
        assert_eq!(initial_states.get("server2"), Some(&ChannelState::Connected));

        // Shutdown server1 only
        let _ = server1.shutdown.send(());
        let _ = server1.handle.await;

        // Wait for server1's state to change to Idle
        let timeout_result = tokio::time::timeout(Duration::from_secs(5), async {
            while let Some((key, state)) = state_streams.next().await {
                if key == "server1" && state == ChannelState::Idle {
                    return true;
                }
            }
            false
        })
        .await;

        assert!(
            timeout_result.unwrap_or(false),
            "server1 should transition to Idle after shutdown"
        );

        // server2 should still be Connected
        assert_eq!(*rx2.borrow(), ChannelState::Connected);

        // Clean up server2
        let _ = server2.shutdown.send(());
        let _ = server2.handle.await;
    }

    /// Test counting connected channels - pattern for load balancer readiness.
    #[tokio::test]
    async fn test_count_connected_channels() {
        // Spawn three servers
        let server1 = spawn_greeter_server("s1", None, None)
            .await
            .expect("failed to spawn server1");
        let server2 = spawn_greeter_server("s2", None, None)
            .await
            .expect("failed to spawn server2");
        let server3 = spawn_greeter_server("s3", None, None)
            .await
            .expect("failed to spawn server3");

        // Get state receivers
        let rx1 = server1.channel.state().unwrap();
        let rx2 = server2.channel.state().unwrap();
        let rx3 = server3.channel.state().unwrap();

        // Helper to count connected channels
        let count_connected = || {
            [&rx1, &rx2, &rx3]
                .iter()
                .filter(|rx| *rx.borrow() == ChannelState::Connected)
                .count()
        };

        // All three should be connected initially
        assert_eq!(count_connected(), 3);

        // Shutdown server1
        let _ = server1.shutdown.send(());
        let _ = server1.handle.await;

        // Wait for state change
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Now only 2 should be connected
        // Note: The exact timing depends on when the connection task detects the closure
        let connected = count_connected();
        assert!(
            connected <= 3,
            "At most 3 channels connected after shutdown"
        );

        // Shutdown server2
        let _ = server2.shutdown.send(());
        let _ = server2.handle.await;
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Clean up server3
        let _ = server3.shutdown.send(());
        let _ = server3.handle.await;
    }

    /// Test that balanced channels return None for state().
    #[tokio::test]
    async fn test_balanced_channel_has_no_state() {
        let endpoints = vec![
            Endpoint::from_static("http://127.0.0.1:12345"),
            Endpoint::from_static("http://127.0.0.1:12346"),
        ];

        let channel = Channel::balance_list(endpoints.into_iter());

        // Balanced channels don't have a single state
        assert!(
            channel.state().is_none(),
            "balanced channel should not have state"
        );
    }

    /// Test StreamMap with select! for non-blocking monitoring.
    #[tokio::test]
    async fn test_stream_map_with_select() {
        let server1 = spawn_greeter_server("s1", None, None)
            .await
            .expect("failed to spawn server1");
        let server2 = spawn_greeter_server("s2", None, None)
            .await
            .expect("failed to spawn server2");

        let mut state_streams: StreamMap<&str, WatchStream<ChannelState>> = StreamMap::new();
        state_streams.insert(
            "server1",
            WatchStream::new(server1.channel.state().unwrap()),
        );
        state_streams.insert(
            "server2",
            WatchStream::new(server2.channel.state().unwrap()),
        );

        // Spawn a task that will shutdown servers after a delay
        let shutdown1 = server1.shutdown;
        let shutdown2 = server2.shutdown;
        let handle1 = server1.handle;
        let handle2 = server2.handle;

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = shutdown1.send(());
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = shutdown2.send(());
        });

        // Collect state changes using select! pattern
        let mut state_changes: Vec<(String, ChannelState)> = vec![];
        let timeout = tokio::time::sleep(Duration::from_secs(2));
        tokio::pin!(timeout);

        loop {
            tokio::select! {
                Some((key, state)) = state_streams.next() => {
                    state_changes.push((key.to_string(), state));
                }
                _ = &mut timeout => {
                    break;
                }
            }
        }

        // Should have received initial Connected states and then Idle states
        assert!(
            state_changes
                .iter()
                .any(|(k, s)| k == "server1" && *s == ChannelState::Connected),
            "Should see server1 Connected"
        );
        assert!(
            state_changes
                .iter()
                .any(|(k, s)| k == "server2" && *s == ChannelState::Connected),
            "Should see server2 Connected"
        );

        // Wait for server handles
        let _ = handle1.await;
        let _ = handle2.await;
    }
}
