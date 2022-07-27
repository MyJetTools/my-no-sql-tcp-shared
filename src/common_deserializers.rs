use my_tcp_sockets::socket_reader::{ReadingTcpContractFail, SocketReader};

pub async fn read_pascal_string<TSocketReader: SocketReader>(
    reader: &mut TSocketReader,
) -> Result<String, ReadingTcpContractFail> {
    let size = reader.read_byte().await? as usize;

    let mut result: Vec<u8> = Vec::with_capacity(size);
    unsafe { result.set_len(size) }

    reader.read_buf(&mut result).await?;

    Ok(String::from_utf8(result)?)
}
