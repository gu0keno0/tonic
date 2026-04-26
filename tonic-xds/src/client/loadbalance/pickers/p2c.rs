//! Power-of-two-choices (P2C) channel picker.

use indexmap::IndexMap;
use tower::load::Load;

use crate::client::endpoint::EndpointAddress;
use crate::client::loadbalance::pickers::ChannelPicker;

/// Pick two distinct random indices from `[0, length)` using Floyd's algorithm.
fn sample_floyd2(length: usize) -> [usize; 2] {
    debug_assert!(length >= 2);
    let a = fastrand::usize(..length - 1);
    let b = fastrand::usize(..length);
    let a = if a == b { length - 1 } else { a };
    [a, b]
}

/// Picks the least-loaded of two randomly chosen endpoints.
pub(crate) struct P2cPicker;

impl<S, Req> ChannelPicker<S, Req> for P2cPicker
where
    S: Load,
    S::Metric: PartialOrd,
{
    fn pick(
        &self,
        _req: &Req,
        ready: &IndexMap<EndpointAddress, S>,
    ) -> Option<usize> {
        let len = ready.len();
        match len {
            0 => None,
            1 => Some(0),
            _ => {
                let [a, b] = sample_floyd2(len);
                let (_, ch_a) = ready.get_index(a)?;
                let (_, ch_b) = ready.get_index(b)?;
                if ch_a.load() <= ch_b.load() {
                    Some(a)
                } else {
                    Some(b)
                }
            }
        }
    }
}
