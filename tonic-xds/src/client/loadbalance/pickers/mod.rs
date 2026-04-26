pub(crate) mod p2c;

use indexmap::IndexMap;

use crate::client::endpoint::EndpointAddress;
use crate::client::loadbalance::channel_state::ReadyChannel;

/// Trait for selecting a ready channel to handle a request.
pub(crate) trait ChannelPicker<S, Req> {
    /// Pick an endpoint address from the ready set to handle the given request.
    /// Returns `None` if no channel is suitable.
    fn pick(
        &self,
        req: &Req,
        ready: &IndexMap<EndpointAddress, ReadyChannel<S>>,
    ) -> Option<usize>;
}
