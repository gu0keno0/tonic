//! Endpoint discovery primitives for load balancing.
//!
//! This module defines the core discovery abstractions used by the load balancer:
//! - [`EndpointUpdate`]: Represents an insert or remove operation for an endpoint
//! - [`EndpointDiscover`]: A poll-based trait for discovering endpoint changes
//! - [`EndpointUpdateCache`]: Batches updates from a discover and provides atomic consume

use std::collections::HashMap;
use std::hash::Hash;
use std::task::{Context, Poll};
use tower::BoxError;

/// Represents an update to the set of available endpoints.
///
/// This is similar to `tower::discover::Change` but designed for our batched
/// update processing model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EndpointUpdate<K, S> {
    /// Insert or update an endpoint with the given key and service.
    Insert(K, S),
    /// Remove the endpoint with the given key.
    Remove(K),
}

impl<K, S> EndpointUpdate<K, S> {
    /// Returns the key associated with this update.
    pub(crate) fn key(&self) -> &K {
        match self {
            EndpointUpdate::Insert(k, _) => k,
            EndpointUpdate::Remove(k) => k,
        }
    }

    /// Returns `true` if this is an insert update.
    pub(crate) fn is_insert(&self) -> bool {
        matches!(self, EndpointUpdate::Insert(_, _))
    }

    /// Returns `true` if this is a remove update.
    pub(crate) fn is_remove(&self) -> bool {
        matches!(self, EndpointUpdate::Remove(_))
    }
}

/// A trait for discovering endpoint changes using poll-based semantics.
///
/// Unlike `tower::discover::Discover` which extends `Stream`, this trait uses
/// explicit `poll_discover` method that takes `Context` and returns `Poll`.
/// This design enables:
/// - Eager polling by the load balancer
/// - Batched processing of multiple updates
/// - More control over when and how updates are processed
///
/// # Type Parameters
///
/// - `K`: The key type used to identify endpoints (e.g., socket address)
/// - `S`: The service type for each endpoint
pub(crate) trait EndpointDiscover<K, S> {
    /// Poll for the next endpoint update.
    ///
    /// Returns:
    /// - `Poll::Ready(Ok(Some(update)))` when an update is available
    /// - `Poll::Ready(Ok(None))` when the discovery source is exhausted
    /// - `Poll::Ready(Err(e))` when an error occurs
    /// - `Poll::Pending` when no update is currently available
    fn poll_discover(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<EndpointUpdate<K, S>, BoxError>>>;
}

/// A cache that batches endpoint updates from an [`EndpointDiscover`].
///
/// The cache accumulates updates (inserts and removes) in a [`HashMap`].
/// The [`consume`](Self::consume) operation returns all pending updates and clears the cache.
///
/// # Design
///
/// - Updates are stored as `Option<S>`: `Some(service)` for inserts, `None` for removes
/// - Same key can be inserted then removed; the HashMap naturally handles this by overwriting
/// - `poll_updates()` polls the discover and accumulates updates
/// - `consume()` returns and clears all pending updates
///
/// # Thread Safety
///
/// This type is NOT thread-safe. It is designed to be used behind a `tower::Buffer`
/// which serializes access through its worker task.
pub(crate) struct EndpointUpdateCache<K, S, D>
where
    K: Hash + Eq,
{
    /// The endpoint discover source
    discover: D,
    /// Cached updates: Some(S) = insert, None = remove
    updates: HashMap<K, Option<S>>,
    /// Whether the discover has been exhausted
    exhausted: bool,
}

impl<K, S, D> EndpointUpdateCache<K, S, D>
where
    K: Hash + Eq + Clone,
    D: EndpointDiscover<K, S>,
{
    /// Creates a new `EndpointUpdateCache` with the given discover.
    pub(crate) fn new(discover: D) -> Self {
        Self {
            discover,
            updates: HashMap::new(),
            exhausted: false,
        }
    }

    /// Polls the discover for updates, accumulating them in the cache.
    ///
    /// Returns `Poll::Ready(())` when at least one update was processed or the discover is exhausted.
    /// Returns `Poll::Pending` when no updates are available.
    pub(crate) fn poll_updates(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.exhausted {
            return Poll::Ready(());
        }

        let mut got_update = false;

        // Poll discover until pending or exhausted
        loop {
            match self.discover.poll_discover(cx) {
                Poll::Ready(Some(Ok(EndpointUpdate::Insert(key, service)))) => {
                    self.updates.insert(key, Some(service));
                    got_update = true;
                }
                Poll::Ready(Some(Ok(EndpointUpdate::Remove(key)))) => {
                    self.updates.insert(key, None);
                    got_update = true;
                }
                Poll::Ready(Some(Err(e))) => {
                    tracing::warn!("Error polling endpoint discover: {e}");
                    got_update = true;
                }
                Poll::Ready(None) => {
                    // Discovery exhausted
                    self.exhausted = true;
                    return Poll::Ready(());
                }
                Poll::Pending => {
                    return if got_update {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    };
                }
            }
        }
    }

    /// Consumes all cached updates, returning them to the caller.
    ///
    /// The returned map contains:
    /// - `Some(service)` for endpoints that should be inserted/updated
    /// - `None` for endpoints that should be removed
    pub(crate) fn consume(&mut self) -> HashMap<K, Option<S>> {
        std::mem::take(&mut self.updates)
    }

    /// Returns the number of pending updates in the cache.
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.updates.len()
    }

    /// Returns `true` if there are no pending updates.
    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.updates.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    #[test]
    fn test_endpoint_update_key() {
        let insert: EndpointUpdate<&str, i32> = EndpointUpdate::Insert("addr1", 42);
        assert_eq!(insert.key(), &"addr1");
        assert!(insert.is_insert());
        assert!(!insert.is_remove());

        let remove: EndpointUpdate<&str, i32> = EndpointUpdate::Remove("addr2");
        assert_eq!(remove.key(), &"addr2");
        assert!(!remove.is_insert());
        assert!(remove.is_remove());
    }

    /// A mock discover that returns a predefined sequence of updates.
    struct MockDiscover<K, S> {
        updates: VecDeque<Result<EndpointUpdate<K, S>, BoxError>>,
    }

    impl<K, S> MockDiscover<K, S> {
        fn new(updates: Vec<Result<EndpointUpdate<K, S>, BoxError>>) -> Self {
            Self {
                updates: updates.into(),
            }
        }
    }

    impl<K, S> EndpointDiscover<K, S> for MockDiscover<K, S> {
        fn poll_discover(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<EndpointUpdate<K, S>, BoxError>>> {
            Poll::Ready(self.updates.pop_front())
        }
    }

    #[test]
    fn test_cache_accumulates_inserts() {
        let discover = MockDiscover::new(vec![
            Ok(EndpointUpdate::Insert("addr1", "svc1")),
            Ok(EndpointUpdate::Insert("addr2", "svc2")),
        ]);
        let mut cache = EndpointUpdateCache::new(discover);

        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(&waker);

        // Poll to process all updates
        let _ = cache.poll_updates(&mut cx);

        let consumed = cache.consume();
        assert_eq!(consumed.len(), 2);
        assert_eq!(*consumed.get("addr1").unwrap(), Some("svc1"));
        assert_eq!(*consumed.get("addr2").unwrap(), Some("svc2"));

        // Cache should be empty after consume
        assert!(cache.is_empty());
    }

    #[test]
    fn test_cache_handles_insert_then_remove() {
        let discover = MockDiscover::new(vec![
            Ok(EndpointUpdate::Insert("addr1", "svc1")),
            Ok(EndpointUpdate::Remove("addr1")),
        ]);
        let mut cache = EndpointUpdateCache::new(discover);

        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(&waker);

        // Poll to process all updates
        let _ = cache.poll_updates(&mut cx);

        // Should have one entry with None (remove)
        let consumed = cache.consume();
        assert_eq!(consumed.len(), 1);
        assert_eq!(*consumed.get("addr1").unwrap(), None);
    }

    #[test]
    fn test_cache_handles_remove_then_insert() {
        let discover = MockDiscover::new(vec![
            Ok(EndpointUpdate::Remove("addr1")),
            Ok(EndpointUpdate::Insert("addr1", "svc1")),
        ]);
        let mut cache = EndpointUpdateCache::new(discover);

        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(&waker);

        // Poll to process all updates
        let _ = cache.poll_updates(&mut cx);

        // Should have one entry with Some (insert wins)
        let consumed = cache.consume();
        assert_eq!(*consumed.get("addr1").unwrap(), Some("svc1"));
    }

    #[test]
    fn test_consume_clears_cache() {
        let discover = MockDiscover::new(vec![Ok(EndpointUpdate::Insert("addr1", "svc1"))]);
        let mut cache = EndpointUpdateCache::new(discover);

        let waker = std::task::Waker::noop();
        let mut cx = Context::from_waker(&waker);

        let _ = cache.poll_updates(&mut cx);

        // First consume gets the update
        let first = cache.consume();
        assert_eq!(first.len(), 1);

        // Second consume gets empty map
        let second = cache.consume();
        assert_eq!(second.len(), 0);
    }
}
