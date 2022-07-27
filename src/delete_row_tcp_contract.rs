use my_tcp_sockets::socket_reader::{ReadingTcpContractFail, SocketReader};

#[derive(Debug)]
pub struct DeleteRowTcpContract {
    pub partition_key: String,
    pub row_key: String,
}

impl DeleteRowTcpContract {
    pub async fn deserialize<TSocketReader: SocketReader>(
        socket_reader: &mut TSocketReader,
    ) -> Result<Self, ReadingTcpContractFail> {
        let partition_key = crate::common_deserializers::read_pascal_string(socket_reader).await?;
        let row_key = crate::common_deserializers::read_pascal_string(socket_reader).await?;

        let result = Self {
            partition_key,
            row_key,
        };

        Ok(result)
    }

    pub fn serialize(&self, buffer: &mut Vec<u8>) {
        crate::common_serializers::serialize_pascal_string(buffer, self.partition_key.as_str());
        crate::common_serializers::serialize_pascal_string(buffer, self.row_key.as_str());
    }
}
