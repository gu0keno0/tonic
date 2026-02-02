use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use tower::load::Load;

/// Represents a change in the set of available endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointChange<K> {
    /// A new endpoint was added.
    Insert(K),
    /// An endpoint was removed.
    Remove(K),
    /// An endpoint was ejected due to outlier detection.
    Eject(K),
    /// An endpoint was restored after ejection period expired.
    Uneject(K),
}

/// A trait for selecting endpoints from a set of available services.
///
/// The picker maintains its own view of available endpoint keys and can
/// access the services map to read load metrics when making selections.
pub trait Picker<S> {
    /// The key type used to identify endpoints.
    type Key;

    /// Update the picker's view of available endpoints.
    fn update(&mut self, change: EndpointChange<Self::Key>);

    /// Pick an endpoint from the available set.
    ///
    /// The `ejected` set contains endpoints that should be skipped due to
    /// outlier detection.
    ///
    /// Returns `None` if no non-ejected endpoints are available.
    fn pick(
        &mut self,
        services: &HashMap<Self::Key, S>,
        ejected: &HashSet<Self::Key>,
    ) -> Option<Self::Key>;
}

/// Power of Two Choices (P2C) picker.
///
/// This picker implements the P2C algorithm: randomly select two endpoints
/// and pick the one with the lower load. This provides near-optimal load
/// balancing with O(1) selection time.
pub struct P2cPicker<K> {
    keys: Vec<K>,
    rng: SmallRng,
}

impl<K> P2cPicker<K> {
    /// Creates a new P2C picker.
    pub fn new() -> Self {
        Self {
            keys: Vec::new(),
            rng: SmallRng::from_entropy(),
        }
    }
}

impl<K> Default for P2cPicker<K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, S> Picker<S> for P2cPicker<K>
where
    K: Hash + Eq + Clone,
    S: Load,
    S::Metric: PartialOrd,
{
    type Key = K;

    fn update(&mut self, change: EndpointChange<K>) {
        match change {
            EndpointChange::Insert(k) => {
                if !self.keys.contains(&k) {
                    self.keys.push(k);
                }
            }
            EndpointChange::Remove(k) => {
                self.keys.retain(|x| x != &k);
            }
            // Eject/Uneject are handled via the ejected set passed to pick()
            // The picker doesn't need to maintain separate state for these
            EndpointChange::Eject(_) | EndpointChange::Uneject(_) => {}
        }
    }

    fn pick(
        &mut self,
        services: &HashMap<K, S>,
        ejected: &HashSet<K>,
    ) -> Option<K> {
        // Filter to non-ejected keys
        let available: Vec<&K> = self.keys.iter().filter(|k| !ejected.contains(*k)).collect();

        match available.len() {
            0 => None,
            1 => Some(available[0].clone()),
            len => {
                // Pick two random indices
                let idx1 = self.rng.gen_range(0..len);
                let idx2 = loop {
                    let idx = self.rng.gen_range(0..len);
                    if idx != idx1 {
                        break idx;
                    }
                };

                let k1 = available[idx1];
                let k2 = available[idx2];

                let load1 = services.get(k1).map(|s| s.load());
                let load2 = services.get(k2).map(|s| s.load());

                match (load1, load2) {
                    (Some(l1), Some(l2)) => {
                        if l1 <= l2 {
                            Some(k1.clone())
                        } else {
                            Some(k2.clone())
                        }
                    }
                    (Some(_), None) => Some(k1.clone()),
                    (None, Some(_)) => Some(k2.clone()),
                    (None, None) => None,
                }
            }
        }
    }
}
