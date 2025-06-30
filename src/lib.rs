//! Deniably distribute content-addressed data
//!
//! This crate implements a networking protocol similar to [Freenet](https://www.hyphanet.org),
//! which allows files and other data to be inserted into and retrieved from a decentralized
//! network of peers.
//!
//! The `proto` module provides a _sans-I/O_ protocol implementation as a state machine. The
//! `net` module connects this protocol to `iroh`'s networking stack.
//!
//! Also provided is an implementation of a command-line node binary to take part in the peer-to-peer
//! network mentioned above.

pub mod net;
pub mod proto;

pub use net::KREATILAS_ALPN as ALPN;
