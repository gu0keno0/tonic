//! Channel connectivity state tracking.
//!
//! This module provides types for tracking and observing the connectivity state
//! of a gRPC channel.

use std::sync::Arc;
use tokio::sync::watch;

/// Channel connectivity state, mirrors `Reconnect::State` internally.
///
/// This enum represents the connection state of a tonic channel.
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

impl std::fmt::Display for ChannelState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChannelState::Idle => write!(f, "IDLE"),
            ChannelState::Connecting => write!(f, "CONNECTING"),
            ChannelState::Connected => write!(f, "CONNECTED"),
        }
    }
}

/// Internal tracker that updates channel connectivity state.
///
/// This is held by the channel's internal reconnect logic and the spawned
/// connection task, and used to broadcast state changes to all observers.
pub(crate) struct ChannelStateTracker {
    sender: watch::Sender<ChannelState>,
}

impl ChannelStateTracker {
    /// Creates a new state tracker with the given initial state.
    ///
    /// Returns the tracker and a receiver that can be cloned and distributed
    /// to observers.
    pub(crate) fn new(initial: ChannelState) -> (Self, watch::Receiver<ChannelState>) {
        let (sender, receiver) = watch::channel(initial);
        (Self { sender }, receiver)
    }

    /// Updates the connectivity state.
    ///
    /// Only sends a notification if the state actually changed.
    pub(crate) fn set(&self, state: ChannelState) {
        self.sender.send_if_modified(|current| {
            if *current != state {
                tracing::trace!(old = %current, new = %state, "channel connectivity state changed");
                *current = state;
                true
            } else {
                false
            }
        });
    }

    /// Gets the current connectivity state.
    #[allow(dead_code)]
    pub(crate) fn get(&self) -> ChannelState {
        *self.sender.borrow()
    }
}

/// A shared reference to a channel state tracker.
pub(crate) type SharedStateTracker = Arc<ChannelStateTracker>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_state_display() {
        assert_eq!(format!("{}", ChannelState::Idle), "IDLE");
        assert_eq!(format!("{}", ChannelState::Connecting), "CONNECTING");
        assert_eq!(format!("{}", ChannelState::Connected), "CONNECTED");
    }

    #[tokio::test]
    async fn test_state_tracker_basic() {
        let (tracker, mut rx) = ChannelStateTracker::new(ChannelState::Idle);

        assert_eq!(*rx.borrow(), ChannelState::Idle);

        tracker.set(ChannelState::Connecting);
        assert!(rx.changed().await.is_ok());
        assert_eq!(*rx.borrow(), ChannelState::Connecting);

        tracker.set(ChannelState::Connected);
        assert!(rx.changed().await.is_ok());
        assert_eq!(*rx.borrow(), ChannelState::Connected);
    }

    #[tokio::test]
    async fn test_state_tracker_no_duplicate_notification() {
        let (tracker, mut rx) = ChannelStateTracker::new(ChannelState::Idle);

        tracker.set(ChannelState::Connecting);
        assert!(rx.changed().await.is_ok());

        // Setting same state should not trigger notification
        tracker.set(ChannelState::Connecting);

        // Use timeout to verify no change notification
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(10), rx.changed()).await;
        assert!(result.is_err()); // Timeout means no change
    }

    #[tokio::test]
    async fn test_multiple_receivers() {
        let (tracker, rx1) = ChannelStateTracker::new(ChannelState::Idle);
        let mut rx2 = rx1.clone();
        let mut rx1 = rx1;

        tracker.set(ChannelState::Connected);

        // Both receivers should see the change
        assert!(rx1.changed().await.is_ok());
        assert!(rx2.changed().await.is_ok());
        assert_eq!(*rx1.borrow(), ChannelState::Connected);
        assert_eq!(*rx2.borrow(), ChannelState::Connected);
    }
}
