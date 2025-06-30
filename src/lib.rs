//! Deniably distribute content-addressed data
//!
//! This crate implements a networking protocol similar to [Freenet](https://www.hyphanet.org),
//! which allows files and other data to be inserted into and retrieved from a decentralized
//! network of peers.
//!
//! The [`proto`] module provides a _sans-I/O_ protocol implementation as a state machine. The
//! [`net`] module connects this protocol to [`iroh`]'s networking stack.
//!
//! Also provided is an implementation of a command-line node binary to take part in the peer-to-peer
//! network mentioned above.
//!
//! # Getting started
//!
//! Start by constructing an [`Endpoint`](iroh::endpoint::Endpoint)
//! and an instance of [`Blobs`](iroh_blobs::net_protocol::Blobs).
//! ```rust
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()> {
//! # use iroh::endpoint::Endpoint;
//! # use iroh_blobs::net_protocol::Blobs;
//! let endpoint = Endpoint::builder().bind().await?;
//! let blobs = Blobs::memory().build(&endpoint);
//! # Ok(())
//! # }
//! ```
//!
//! Next, construct a [`Kreatilas`](net::Kreatilas) instance to
//! manage this node.
//! ```rust
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()> {
//! # use kreatilas::net::Kreatilas;
//! # use iroh::endpoint::Endpoint;
//! # use iroh_blobs::net_protocol::Blobs;
//! # let endpoint = Endpoint::builder().bind().await?;
//! # let blobs = Blobs::memory().build(&endpoint);
//! let node = Kreatilas::builder().spawn(endpoint.clone(), &blobs).await?;
//! # Ok(())
//! # }
//! ```
//!
//! Finally, begin accepting connections by spawning a [`Router`](iroh::protocol::Router)
//! and supplying it with handles to [`Kreatilas`](net::Kreatilas) and [`Blobs`](iroh_blobs::net_protocol::Blobs).
//! ```rust
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()> {
//! # use iroh::protocol::Router;
//! # use kreatilas::net::Kreatilas;
//! # use iroh::endpoint::Endpoint;
//! # use iroh_blobs::net_protocol::Blobs;
//! # let endpoint = Endpoint::builder().bind().await?;
//! # let blobs = Blobs::memory().build(&endpoint);
//! # let node = Kreatilas::builder().spawn(endpoint.clone(), &blobs).await?;
//! let router = Router::builder(endpoint)
//!   .accept(kreatilas::ALPN, node.clone())
//!   .accept(iroh_blobs::ALPN, blobs.clone())
//!   .spawn();
//! // do something cool with `node` and `blobs`
//! # Ok(())
//! # }
//! ```

pub mod net;
pub mod proto;

pub use net::KREATILAS_ALPN as ALPN;
