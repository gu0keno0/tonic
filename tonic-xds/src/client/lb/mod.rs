//! Load balancing module for tonic-xds.
//!
//! This module provides custom load balancing primitives that replace Tower's built-in
//! `tower::discover` and `tower::balance::p2c` with implementations optimized for xDS:
//!
//! - **Eager polling**: Updates are polled proactively rather than lazily
//! - **Batched processing**: Multiple endpoint updates are batched together
//! - **No Stream trait**: Uses `Poll`-based discovery instead of `Stream`
//! - **Customizable picking**: `LbPicker` trait allows P2C, RR, sticky routing, etc.

mod balancer;
mod discover;
mod service;

pub(crate) use balancer::{
    BalancerRequest, BalancerResponse, EndpointChangeType, LbPicker, LoadBalancer, P2cPicker,
    PollDiscoverResponse,
};
pub(crate) use discover::{EndpointDiscover, EndpointUpdate, EndpointUpdateCache};
pub(crate) use service::{LoadBalancingError, XdsLbService};
