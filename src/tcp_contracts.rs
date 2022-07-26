use my_tcp_sockets::socket_reader::{ReadingTcpContractFail, SocketReader};

use crate::{tcp_packets::*, DeleteRowTcpContract};

pub enum TcpContract {
    Ping,
    Pong,
    Greeting {
        name: String,
    },
    Subscribe {
        table_name: String,
    },
    InitTable {
        table_name: String,
        data: Vec<u8>,
    },
    InitPartition {
        table_name: String,
        partition_key: String,
        data: Vec<u8>,
    },
    UpdateRows {
        table_name: String,
        data: Vec<u8>,
    },
    DeleteRows {
        table_name: String,
        rows: Vec<DeleteRowTcpContract>,
    },
    Error {
        message: String,
    },
    //Version 0 - we have 2 fields. Name and Version;
    GreetingFromNode {
        version: u8,
        node_location: String,
        node_version: String,
    },
}

impl TcpContract {
    pub async fn deserialize<TSocketReader: SocketReader>(
        socket_reader: &mut TSocketReader,
    ) -> Result<TcpContract, ReadingTcpContractFail> {
        let packet_no = socket_reader.read_byte().await?;

        let result = match packet_no {
            PING => Ok(TcpContract::Ping {}),
            PONG => Ok(TcpContract::Pong {}),
            GREETING => {
                let name = crate::common_deserializers::read_pascal_string(socket_reader).await?;
                Ok(TcpContract::Greeting { name })
            }
            SUBSCRIBE => {
                let table_name =
                    crate::common_deserializers::read_pascal_string(socket_reader).await?;
                Ok(TcpContract::Subscribe { table_name })
            }
            INIT_TABLE => {
                let table_name =
                    crate::common_deserializers::read_pascal_string(socket_reader).await?;
                let data = socket_reader.read_byte_array().await?;
                Ok(TcpContract::InitTable { table_name, data })
            }
            INIT_PARTITION => {
                let table_name =
                    crate::common_deserializers::read_pascal_string(socket_reader).await?;
                let partition_key =
                    crate::common_deserializers::read_pascal_string(socket_reader).await?;
                let data = socket_reader.read_byte_array().await?;
                Ok(TcpContract::InitPartition {
                    table_name,
                    partition_key,
                    data,
                })
            }
            UPDATE_ROWS => {
                let table_name =
                    crate::common_deserializers::read_pascal_string(socket_reader).await?;

                let data = socket_reader.read_byte_array().await?;
                Ok(TcpContract::UpdateRows { table_name, data })
            }
            DELETE_ROWS => {
                let table_name =
                    crate::common_deserializers::read_pascal_string(socket_reader).await?;

                let rows_amount = socket_reader.read_i32().await?;

                let mut rows = Vec::new();

                for _ in 0..rows_amount {
                    let row = DeleteRowTcpContract::deserialize(socket_reader).await?;
                    rows.push(row);
                }

                Ok(TcpContract::DeleteRows { table_name, rows })
            }
            ERROR => {
                let packet_version = socket_reader.read_byte().await?;

                if packet_version != 0 {
                    panic!(
                        "Unexpected packet version. Version is: {}, but expected version is 0",
                        packet_version
                    );
                }

                let message =
                    crate::common_deserializers::read_pascal_string(socket_reader).await?;

                panic!("TCP protocol error: {}", message);
            }
            GREETING_FROM_NODE => {
                let packet_version = socket_reader.read_byte().await?;

                if packet_version != 0 {
                    panic!(
                        "Unexpected packet version. Version is: {}, but expected version is 0",
                        packet_version
                    );
                }

                let node_location =
                    crate::common_deserializers::read_pascal_string(socket_reader).await?;

                let node_version =
                    crate::common_deserializers::read_pascal_string(socket_reader).await?;

                Ok(TcpContract::GreetingFromNode {
                    version: packet_version,
                    node_location,
                    node_version,
                })
            }
            _ => Err(ReadingTcpContractFail::InvalidPacketId(packet_no)),
        };

        return result;
    }
    pub fn serialize(&self) -> Vec<u8> {
        let mut buffer = Vec::new();
        self.serialize_into(&mut buffer);
        buffer
    }

    pub fn serialize_into(&self, buffer: &mut Vec<u8>) {
        match self {
            TcpContract::Ping => {
                buffer.push(PING);
            }
            TcpContract::Pong => {
                buffer.push(PONG);
            }
            TcpContract::Greeting { name } => {
                buffer.push(GREETING);
                crate::common_serializers::serialize_pascal_string(buffer, name);
            }
            TcpContract::Subscribe { table_name } => {
                buffer.push(SUBSCRIBE);
                crate::common_serializers::serialize_pascal_string(buffer, table_name);
            }
            TcpContract::InitTable { table_name, data } => {
                buffer.push(INIT_TABLE);
                crate::common_serializers::serialize_pascal_string(buffer, table_name);
                crate::common_serializers::serialize_byte_array(buffer, data.as_slice());
            }
            TcpContract::InitPartition {
                table_name,
                partition_key,
                data,
            } => {
                buffer.push(INIT_PARTITION);
                crate::common_serializers::serialize_pascal_string(buffer, table_name);
                crate::common_serializers::serialize_pascal_string(buffer, partition_key);
                crate::common_serializers::serialize_byte_array(buffer, data.as_slice());
            }
            TcpContract::UpdateRows { table_name, data } => {
                buffer.push(UPDATE_ROWS);
                crate::common_serializers::serialize_pascal_string(buffer, table_name);
                crate::common_serializers::serialize_byte_array(buffer, data.as_slice());
            }
            TcpContract::DeleteRows { table_name, rows } => {
                buffer.push(DELETE_ROWS);
                crate::common_serializers::serialize_pascal_string(buffer, table_name);
                crate::common_serializers::serialize_i32(buffer, rows.len() as i32);

                for row in rows {
                    row.serialize(buffer);
                }
            }
            TcpContract::Error { message } => {
                buffer.push(ERROR);
                // Version=0; Means we have one field - message;
                buffer.push(0);
                crate::common_serializers::serialize_pascal_string(buffer, message);
            }

            TcpContract::GreetingFromNode {
                version,
                node_location,
                node_version,
            } => {
                buffer.push(GREETING_FROM_NODE);
                buffer.push(*version);
                crate::common_serializers::serialize_pascal_string(buffer, node_location);
                crate::common_serializers::serialize_pascal_string(buffer, node_version);
            }
        }
    }
}
