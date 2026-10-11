//! Generic bounded JSON-RPC transport. The application owns endpoint and peer authorization.

use tokio::time::Duration;

pub use jsonrpsee_types::ErrorObjectOwned;

pub mod client;
pub mod envelope;
pub mod framing;
pub mod server;

/// Resource bounds shared by a connection's codec, client and dispatcher.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_frame_bytes: usize,
    pub request_timeout: Duration,
    pub max_batch_members: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame_bytes: framing::MAX_FRAME_BYTES,
            request_timeout: Duration::from_secs(2),
            max_batch_members: 32,
        }
    }
}
