use my_tcp_sockets::socket_reader::{ReadingTcpContractFail, SocketReader, SocketReaderInMem};
use rust_extensions::date_time::DateTimeAsMicroseconds;

use crate::{tcp_packets::*, DeleteRowTcpContract};

#[derive(Debug)]
pub enum MyNoSqlTcpContract {
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

    GreetingFromNode {
        node_location: String,
        node_version: String,
        compress: bool,
    },
    SubscribeAsNode(String),
    Unsubscribe(String),
    TableNotFound(String),
    CompressedPayload(Vec<u8>),
    UpdatePartitionsLastReadTime {
        confirmation_id: i64,
        table_name: String,
        partitions: Vec<String>,
    },
    UpdateRowsLastReadTime {
        confirmation_id: i64,
        table_name: String,
        partition_key: String,
        row_keys: Vec<String>,
    },
    UpdatePartitionsExpirationTime {
        confirmation_id: i64,
        table_name: String,
        partitions: Vec<(String, Option<DateTimeAsMicroseconds>)>,
    },
    UpdateRowsExpirationTime {
        confirmation_id: i64,
        table_name: String,
        partition_key: String,
        row_keys: Vec<String>,
        expiration_time: Option<DateTimeAsMicroseconds>,
    },
    Confirmation {
        confirmation_id: i64,
    },
}

impl MyNoSqlTcpContract {
    pub fn compress_if_make_sence_and_serialize(&self) -> Vec<u8> {
        if let Self::CompressedPayload(_) = self {
            panic!("You can not get compresed payload from compressed payload");
        }

        let non_compressed = self.serialize();

        let compressed = super::payload_comressor::compress(non_compressed.as_slice()).unwrap();

        if compressed.len() + 10 < non_compressed.len() {
            Self::CompressedPayload(compressed).serialize()
        } else {
            non_compressed
        }
    }

    pub async fn decompress_if_compressed(self) -> Result<Self, ReadingTcpContractFail> {
        if let Self::CompressedPayload(payload) = self {
            let uncompressed_payload =
                super::payload_comressor::decompress(payload.as_slice()).unwrap();

            let mut reader = SocketReaderInMem::new(uncompressed_payload);

            Self::deserialize(&mut reader).await
        } else {
            Ok(self)
        }
    }

    pub async fn deserialize<TSocketReader: SocketReader>(
        socket_reader: &mut TSocketReader,
    ) -> Result<Self, ReadingTcpContractFail> {
        let packet_no = socket_reader.read_byte().await?;

        let result = match packet_no {
            PING => Ok(Self::Ping {}),
            PONG => Ok(Self::Pong {}),
            GREETING => {
                let name = crate::common_deserializers::read_pascal_string(socket_reader).await?;
                Ok(Self::Greeting { name })
            }
            SUBSCRIBE => {
                let table_name =
                    crate::common_deserializers::read_pascal_string(socket_reader).await?;
                Ok(Self::Subscribe { table_name })
            }
            INIT_TABLE => {
                let table_name =
                    crate::common_deserializers::read_pascal_string(socket_reader).await?;
                let data = socket_reader.read_byte_array().await?;
                Ok(Self::InitTable { table_name, data })
            }
            INIT_PARTITION => {
                let table_name =
                    crate::common_deserializers::read_pascal_string(socket_reader).await?;
                let partition_key =
                    crate::common_deserializers::read_pascal_string(socket_reader).await?;
                let data = socket_reader.read_byte_array().await?;
                Ok(Self::InitPartition {
                    table_name,
                    partition_key,
                    data,
                })
            }
            UPDATE_ROWS => {
                let table_name =
                    crate::common_deserializers::read_pascal_string(socket_reader).await?;

                let data = socket_reader.read_byte_array().await?;
                Ok(Self::UpdateRows { table_name, data })
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

                Ok(Self::DeleteRows { table_name, rows })
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

                let mut compress = false;
                let node_location =
                    crate::common_deserializers::read_pascal_string(socket_reader).await?;

                let node_version =
                    crate::common_deserializers::read_pascal_string(socket_reader).await?;

                if packet_version > 0 {
                    compress = socket_reader.read_bool().await?;
                }

                Ok(Self::GreetingFromNode {
                    node_location,
                    node_version,
                    compress,
                })
            }
            SUBSCRIBE_AS_NODE => {
                // Version 0 = we read table_name only
                socket_reader.read_byte().await?;
                let table_name =
                    super::common_deserializers::read_pascal_string(socket_reader).await?;
                Ok(Self::SubscribeAsNode(table_name))
            }
            TABLES_NOT_FOUND => {
                // Version 0 = we read table_name only
                socket_reader.read_byte().await?;
                let table_name =
                    super::common_deserializers::read_pascal_string(socket_reader).await?;
                Ok(Self::TableNotFound(table_name))
            }
            UNSUBSCRIBE => {
                // Version 0 = we read table_name only
                socket_reader.read_byte().await?;
                let table_name =
                    super::common_deserializers::read_pascal_string(socket_reader).await?;
                Ok(Self::Unsubscribe(table_name))
            }
            COMPRESSED_PAYLOAD => {
                let data = socket_reader.read_byte_array().await?;
                Ok(Self::CompressedPayload(data))
            }
            UPDATE_PARTITIONS_LAST_READ_TIME => {
                let _protocol_version = socket_reader.read_byte().await?;
                let confirmation_id = socket_reader.read_i64().await?;
                let table_name =
                    super::common_deserializers::read_pascal_string(socket_reader).await?;

                let partitions =
                    super::common_deserializers::read_list_of_pascal_strings(socket_reader).await?;

                Ok(Self::UpdatePartitionsLastReadTime {
                    confirmation_id,
                    table_name,
                    partitions,
                })
            }
            UPDATE_ROWS_LAST_READ_TIME => {
                let _protocol_version = socket_reader.read_byte().await?;
                let confirmation_id = socket_reader.read_i64().await?;
                let table_name =
                    super::common_deserializers::read_pascal_string(socket_reader).await?;

                let partition_key =
                    super::common_deserializers::read_pascal_string(socket_reader).await?;

                let row_keys =
                    super::common_deserializers::read_list_of_pascal_strings(socket_reader).await?;

                Ok(Self::UpdateRowsLastReadTime {
                    confirmation_id,
                    table_name,
                    partition_key,
                    row_keys,
                })
            }

            UPDATE_PARTITIONS_EXPIRATION_TIME => {
                let _protocol_version = socket_reader.read_byte().await?;
                let confirmation_id = socket_reader.read_i64().await?;
                let table_name =
                    super::common_deserializers::read_pascal_string(socket_reader).await?;

                let amount = socket_reader.read_i32().await? as usize;

                let mut partitions = Vec::with_capacity(amount);

                for _ in 0..amount {
                    let partition_key =
                        super::common_deserializers::read_pascal_string(socket_reader).await?;
                    let expiration_time =
                        super::common_deserializers::read_date_time_opt(socket_reader).await?;

                    partitions.push((partition_key, expiration_time));
                }

                Ok(Self::UpdatePartitionsExpirationTime {
                    confirmation_id,
                    table_name,
                    partitions,
                })
            }

            UPDATE_ROWS_EXPIRATION_TIME => {
                let _protocol_version = socket_reader.read_byte().await?;
                let confirmation_id = socket_reader.read_i64().await?;
                let table_name =
                    super::common_deserializers::read_pascal_string(socket_reader).await?;

                let partition_key =
                    super::common_deserializers::read_pascal_string(socket_reader).await?;

                let row_keys =
                    super::common_deserializers::read_list_of_pascal_strings(socket_reader).await?;

                let expiration_time =
                    super::common_deserializers::read_date_time_opt(socket_reader).await?;

                Ok(Self::UpdateRowsExpirationTime {
                    confirmation_id,
                    table_name,
                    partition_key,
                    row_keys,
                    expiration_time: expiration_time,
                })
            }

            CONFIRMATION => {
                let _protocol_version = socket_reader.read_byte().await?;
                let confirmation_id = socket_reader.read_i64().await?;
                Ok(Self::Confirmation { confirmation_id })
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
            Self::Ping => {
                buffer.push(PING);
            }
            Self::Pong => {
                buffer.push(PONG);
            }
            Self::Greeting { name } => {
                buffer.push(GREETING);
                crate::common_serializers::serialize_pascal_string(buffer, name);
            }
            Self::Subscribe { table_name } => {
                buffer.push(SUBSCRIBE);
                crate::common_serializers::serialize_pascal_string(buffer, table_name);
            }
            Self::InitTable { table_name, data } => {
                buffer.push(INIT_TABLE);
                crate::common_serializers::serialize_pascal_string(buffer, table_name);
                crate::common_serializers::serialize_byte_array(buffer, data.as_slice());
            }
            Self::InitPartition {
                table_name,
                partition_key,
                data,
            } => {
                buffer.push(INIT_PARTITION);
                crate::common_serializers::serialize_pascal_string(buffer, table_name);
                crate::common_serializers::serialize_pascal_string(buffer, partition_key);
                crate::common_serializers::serialize_byte_array(buffer, data.as_slice());
            }
            Self::UpdateRows { table_name, data } => {
                buffer.push(UPDATE_ROWS);
                crate::common_serializers::serialize_pascal_string(buffer, table_name);
                crate::common_serializers::serialize_byte_array(buffer, data.as_slice());
            }
            Self::DeleteRows { table_name, rows } => {
                buffer.push(DELETE_ROWS);
                crate::common_serializers::serialize_pascal_string(buffer, table_name);
                crate::common_serializers::serialize_i32(buffer, rows.len() as i32);

                for row in rows {
                    row.serialize(buffer);
                }
            }
            Self::Error { message } => {
                buffer.push(ERROR);
                // Version=0; Means we have one field - message;
                buffer.push(0);
                crate::common_serializers::serialize_pascal_string(buffer, message);
            }

            Self::GreetingFromNode {
                node_location,
                node_version,
                compress,
            } => {
                if *compress {
                    buffer.push(GREETING_FROM_NODE);
                    buffer.push(1);
                    crate::common_serializers::serialize_pascal_string(buffer, node_location);
                    crate::common_serializers::serialize_pascal_string(buffer, node_version);
                    buffer.push(1);
                } else {
                    buffer.push(GREETING_FROM_NODE);
                    buffer.push(0);
                    crate::common_serializers::serialize_pascal_string(buffer, node_location);
                    crate::common_serializers::serialize_pascal_string(buffer, node_version);
                }
            }

            Self::SubscribeAsNode(table_name) => {
                buffer.push(SUBSCRIBE_AS_NODE);
                // Protocol version
                buffer.push(0);
                crate::common_serializers::serialize_pascal_string(buffer, table_name.as_str());
            }

            Self::TableNotFound(table_name) => {
                buffer.push(TABLES_NOT_FOUND);
                // Protocol version
                buffer.push(0);
                crate::common_serializers::serialize_pascal_string(buffer, table_name.as_str());
            }

            Self::Unsubscribe(table_name) => {
                buffer.push(UNSUBSCRIBE);
                // Protocol version
                buffer.push(0);
                crate::common_serializers::serialize_pascal_string(buffer, table_name.as_str());
            }
            Self::CompressedPayload(payload) => {
                buffer.push(COMPRESSED_PAYLOAD);
                crate::common_serializers::serialize_byte_array(buffer, payload.as_slice());
            }
            Self::UpdatePartitionsLastReadTime {
                table_name,
                confirmation_id,
                partitions,
            } => {
                buffer.push(UPDATE_PARTITIONS_LAST_READ_TIME);
                crate::common_serializers::serialize_byte(buffer, 0); // Protocol version
                crate::common_serializers::serialize_i64(buffer, *confirmation_id);
                crate::common_serializers::serialize_pascal_string(buffer, table_name.as_str());
                crate::common_serializers::serialize_list_of_pascal_strings(buffer, partitions);
            }
            Self::UpdateRowsLastReadTime {
                table_name,
                confirmation_id,
                partition_key,
                row_keys,
            } => {
                buffer.push(UPDATE_ROWS_LAST_READ_TIME);
                crate::common_serializers::serialize_byte(buffer, 0); // Protocol version
                crate::common_serializers::serialize_i64(buffer, *confirmation_id);
                crate::common_serializers::serialize_pascal_string(buffer, table_name.as_str());
                crate::common_serializers::serialize_pascal_string(buffer, &partition_key.as_str());
                crate::common_serializers::serialize_list_of_pascal_strings(buffer, row_keys);
            }

            Self::UpdatePartitionsExpirationTime {
                table_name,
                confirmation_id,
                partitions,
            } => {
                buffer.push(UPDATE_PARTITIONS_EXPIRATION_TIME);
                crate::common_serializers::serialize_byte(buffer, 0); // Protocol version
                crate::common_serializers::serialize_i64(buffer, *confirmation_id);
                crate::common_serializers::serialize_pascal_string(buffer, table_name.as_str());

                let amount = partitions.len() as i32;

                crate::common_serializers::serialize_i32(buffer, amount);

                for (partition_key, expiration_time) in partitions {
                    crate::common_serializers::serialize_pascal_string(
                        buffer,
                        partition_key.as_str(),
                    );

                    crate::common_serializers::serialize_date_time_opt(buffer, *expiration_time);
                }
            }

            Self::UpdateRowsExpirationTime {
                table_name,
                confirmation_id,
                partition_key,
                row_keys,
                expiration_time,
            } => {
                buffer.push(UPDATE_ROWS_EXPIRATION_TIME);
                crate::common_serializers::serialize_byte(buffer, 0); // Protocol version
                crate::common_serializers::serialize_i64(buffer, *confirmation_id);
                crate::common_serializers::serialize_pascal_string(buffer, table_name.as_str());
                crate::common_serializers::serialize_pascal_string(buffer, &partition_key.as_str());
                crate::common_serializers::serialize_list_of_pascal_strings(buffer, row_keys);
                crate::common_serializers::serialize_date_time_opt(buffer, *expiration_time);
            }

            Self::Confirmation { confirmation_id } => {
                buffer.push(CONFIRMATION);
                crate::common_serializers::serialize_byte(buffer, 0); // Protocol version
                crate::common_serializers::serialize_i64(buffer, *confirmation_id);
            }
        }
    }
}

impl my_tcp_sockets::tcp_connection::TcpContract for MyNoSqlTcpContract {
    fn is_pong(&self) -> bool {
        match self {
            Self::Pong => true,
            _ => false,
        }
    }
}
