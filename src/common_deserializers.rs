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

pub async fn read_i32<TSocketReader: SocketReader>(
    reader: &mut TSocketReader,
) -> Result<i32, ReadingTcpContractFail> {
    let mut buffer = [0u8; 4];
    reader.read_buf(&mut buffer).await?;
    Ok(i32::from_le_bytes(buffer))
}

pub async fn read_vec_of_string<TSocketReader: SocketReader>(
    socket_reader: &mut TSocketReader,
) -> Result<Vec<String>, ReadingTcpContractFail> {
    let amount = read_i32(socket_reader).await? as usize;
    let mut result = Vec::with_capacity(amount);

    for _ in 0..amount {
        let itm = crate::common_deserializers::read_pascal_string(socket_reader).await?;
        result.push(itm);
    }

    Ok(result)
}
