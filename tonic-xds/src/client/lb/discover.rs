//! Endpoint discovery primitives for load balancing.
//!
//! This module defines the core discovery abstractions used by the load balancer:
//! - [`EndpointUpdate`]: Represents an insert or remove operation for an endpoint
//! - [`EndpointDiscover`]: A poll-based trait for discovering endpoint changes
//! - [`EndpointUpdateCache`]: Batches updates from a discover and provides atomic consume

use arc_swap::ArcSwap;
use dashmap::DashMap;
use std::hash::Hash;
use std::sync::Arc;
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
/// The cache spawns a background actor task that eagerly polls the discover
/// and accumulates updates (inserts and removes) in a [`DashMap`]. The
/// [`consume`](Self::consume) operation atomically retrieves all pending updates.
///
/// # Design
///
/// - A background tokio task eagerly polls the discover
/// - Updates are stored as `Option<S>`: `Some(service)` for inserts, `None` for removes
/// - Same key can be inserted then removed; the DashMap naturally handles this by overwriting
/// - `consume()` uses [`ArcSwap`] to atomically swap out the current batch
pub(crate) struct EndpointUpdateCache<K, S>
where
    K: Hash + Eq,
{
    /// Cached updates: Some(S) = insert, None = remove
    updates: Arc<ArcSwap<DashMap<K, Option<S>>>>,
    /// Handle to the background polling task
    #[allow(dead_code)]
    task_handle: tokio::task::JoinHandle<()>,
}

impl<K, S> EndpointUpdateCache<K, S>
where
    K: Hash + Eq + Clone + Send + Sync + 'static,
    S: Send + Sync + 'static,
{
    /// Creates a new `EndpointUpdateCache` that spawns a background task to poll the discover.
    pub(crate) fn spawn<D>(mut discover: D) -> Self
    where
        D: EndpointDiscover<K, S> + Send + 'static,
    {
        let updates = Arc::new(ArcSwap::new(Arc::new(DashMap::new())));
        let updates_clone = updates.clone();

        let task_handle = tokio::spawn(async move {
            loop {
                let update = std::future::poll_fn(|cx| discover.poll_discover(cx)).await;
                match update {
                    Some(Ok(EndpointUpdate::Insert(key, service))) => {
                        updates_clone.load().insert(key, Some(service));
                    }
                    Some(Ok(EndpointUpdate::Remove(key))) => {
                        updates_clone.load().insert(key, None);
                    }
                    Some(Err(e)) => {
                        tracing::warn!("Error polling endpoint discover: {e}");
                    }
                    None => {
                        // Discovery exhausted
                        break;
                    }
                }
            }
        });

        Self {
            updates,
            task_handle,
        }
    }

    /// Atomically consumes all cached updates, returning them to the caller.
    ///
    /// This swaps the internal cache with an empty [`DashMap`] and returns the
    /// previous contents. The returned map contains:
    /// - `Some(service)` for endpoints that should be inserted/updated
    /// - `None` for endpoints that should be removed
    pub(crate) fn consume(&self) -> Arc<DashMap<K, Option<S>>> {
        self.updates.swap(Arc::new(DashMap::new()))
    }

    /// Returns the number of pending updates in the cache.
    #[allow(dead_code)]
    pub(crate) fn len(&self) -> usize {
        self.updates.load().len()
    }

    /// Returns `true` if there are no pending updates.
    #[allow(dead_code)]
    pub(crate) fn is_empty(&self) -> bool {
        self.updates.load().is_empty()
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

    impl<K: Send, S: Send> EndpointDiscover<K, S> for MockDiscover<K, S> {
        fn poll_discover(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<EndpointUpdate<K, S>, BoxError>>> {
            Poll::Ready(self.updates.pop_front())
        }
    }

    #[tokio::test]
    async fn test_cache_accumulates_inserts() {
        let discover = MockDiscover::new(vec![
            Ok(EndpointUpdate::Insert("addr1".to_string(), "svc1".to_string())),
            Ok(EndpointUpdate::Insert("addr2".to_string(), "svc2".to_string())),
        ]);
        let cache = EndpointUpdateCache::spawn(discover);

        // Wait for the background task to process all updates
        tokio::task::yield_now().await;

        let consumed = cache.consume();
        assert_eq!(consumed.len(), 2);
        assert_eq!(*consumed.get("addr1").unwrap(), Some("svc1".to_string()));
        assert_eq!(*consumed.get("addr2").unwrap(), Some("svc2".to_string()));

        // Cache should be empty after consume
        assert!(cache.is_empty());
    }

    #[tokio::test]
    async fn test_cache_handles_insert_then_remove() {
        let discover = MockDiscover::new(vec![
            Ok(EndpointUpdate::Insert("addr1".to_string(), "svc1".to_string())),
            Ok(EndpointUpdate::Remove("addr1".to_string())),
        ]);
        let cache = EndpointUpdateCache::spawn(discover);

        // Wait for the background task to process all updates
        tokio::task::yield_now().await;

        // Should have one entry with None (remove)
        let consumed = cache.consume();
        assert_eq!(consumed.len(), 1);
        assert_eq!(*consumed.get("addr1").unwrap(), None);
    }

    #[tokio::test]
    async fn test_cache_handles_remove_then_insert() {
        let discover = MockDiscover::new(vec![
            Ok(EndpointUpdate::Remove("addr1".to_string())),
            Ok(EndpointUpdate::Insert("addr1".to_string(), "svc1".to_string())),
        ]);
        let cache = EndpointUpdateCache::spawn(discover);

        // Wait for the background task to process all updates
        tokio::task::yield_now().await;

        // Should have one entry with Some (insert wins)
        let consumed = cache.consume();
        assert_eq!(*consumed.get("addr1").unwrap(), Some("svc1".to_string()));
    }

    #[tokio::test]
    async fn test_consume_is_atomic() {
        let discover = MockDiscover::new(vec![Ok(EndpointUpdate::Insert(
            "addr1".to_string(),
            "svc1".to_string(),
        ))]);
        let cache = EndpointUpdateCache::spawn(discover);

        // Wait for the background task to process all updates
        tokio::task::yield_now().await;

        // First consume gets the update
        let first = cache.consume();
        assert_eq!(first.len(), 1);

        // Second consume gets empty map
        let second = cache.consume();
        assert_eq!(second.len(), 0);
    }
}
