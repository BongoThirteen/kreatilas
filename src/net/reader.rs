use bytes::BytesMut;
use iroh_gossip::net::util::{ReadError, read_lp};
use tokio::io::AsyncRead;

use crate::proto::Message;

pub async fn read_message(
    reader: impl AsyncRead + Unpin,
    buffer: &mut BytesMut,
    max_message_size: usize,
) -> Result<Option<Message>, ReadError> {
    match read_lp(reader, buffer, max_message_size).await? {
        None => Ok(None),
        Some(data) => {
            let message = postcard::from_bytes(&data)?;
            Ok(Some(message))
        }
    }
}
