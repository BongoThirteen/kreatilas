use anyhow::bail;
use bytes::BytesMut;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::proto::Message;

/// Write a [`Message`] of at most `max_message_size` bytes into `writer`.
pub async fn write_message(
    writer: &mut (impl AsyncWrite + Unpin),
    buffer: &mut BytesMut,
    frame: &Message,
    max_message_size: usize,
) -> anyhow::Result<()> {
    let len = postcard::experimental::serialized_size(frame)?;
    if len >= max_message_size {
        bail!("message would be {len}B (larger than {max_message_size}B)");
    }

    buffer.clear();
    buffer.resize(len, 0);
    let slice = postcard::to_slice(frame, buffer)?;
    writer.write_u32(len as u32).await?;
    writer.write_all(slice).await?;
    Ok(())
}
