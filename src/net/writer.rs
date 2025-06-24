use bytes::BytesMut;
use iroh_gossip::net::util::WriteError;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::proto::Message;

pub async fn write_message(
    writer: &mut (impl AsyncWrite + Unpin),
    buffer: &mut BytesMut,
    frame: &Message,
    max_message_size: usize,
) -> Result<(), WriteError> {
    let len = postcard::experimental::serialized_size(frame)?;
    if len >= max_message_size {
        return Err(WriteError::TooLarge);
    }

    buffer.clear();
    buffer.resize(len, 0);
    let slice = postcard::to_slice(frame, buffer)?;
    writer.write_u32(len as u32).await?;
    writer.write_all(slice).await?;
    Ok(())
}
