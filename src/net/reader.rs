use std::io;

use anyhow::{anyhow, bail};
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::proto::Message;

/// Read a length-prefixed message of at most `max_message_size` bytes from `reader` into
/// `buffer`.
pub async fn read_message(
    reader: impl AsyncRead + Unpin,
    buffer: &mut BytesMut,
    max_message_size: usize,
) -> anyhow::Result<Option<Message>> {
    match read_lp(reader, buffer, max_message_size).await? {
        None => Ok(None),
        Some(data) => {
            let message = postcard::from_bytes(&data)?;
            Ok(Some(message))
        }
    }
}

async fn read_lp(
    mut reader: impl AsyncRead + Unpin,
    buffer: &mut BytesMut,
    max_message_size: usize,
) -> anyhow::Result<Option<Bytes>> {
    let size = match reader.read_u32().await {
        Ok(size) => size,
        Err(err) if err.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let mut reader = reader.take(size as u64);
    let size = usize::try_from(size)
        .map_err(|_| anyhow!("message of {size}B is larger than usize::MAX"))?;
    if size > max_message_size {
        bail!("message of {size}B is larger than the max of {max_message_size}B");
    }
    buffer.reserve(size);
    loop {
        let r = reader.read_buf(buffer).await?;
        if r == 0 {
            break;
        }
    }
    Ok(Some(buffer.split_to(size).freeze()))
}
