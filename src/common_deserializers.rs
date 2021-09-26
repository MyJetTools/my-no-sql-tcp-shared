use super::{ReadingTcpContractFail, TSocketReader};

pub async fn read_pascal_string<T: TSocketReader>(
    reader: &mut T,
) -> Result<String, ReadingTcpContractFail> {
    let size = reader.read_byte().await? as usize;

    let mut result: Vec<u8> = Vec::with_capacity(size);
    unsafe { result.set_len(size) }

    reader.read_buf(&mut result).await?;

    Ok(String::from_utf8(result)?)
}
