mod sync_queues;
mod sync_to_main_node_event;
mod sync_to_main_node_handler;
pub use sync_queues::*;
mod update_entity_statistics_data;
mod update_partition_expiration_time_queue;
mod update_partitions_last_read_time_queue;
mod update_rows_expiration_time_queue;
mod update_rows_last_read_time_queue;
pub use sync_to_main_node_event::*;
pub use sync_to_main_node_handler::*;
pub use update_entity_statistics_data::*;
pub use update_partition_expiration_time_queue::*;
pub use update_partitions_last_read_time_queue::*;
pub use update_rows_expiration_time_queue::*;
pub use update_rows_last_read_time_queue::*;

type DataReaderTcpConnection = my_tcp_sockets::tcp_connection::SocketConnection<
    crate::MyNoSqlTcpContract,
    crate::MyNoSqlReaderTcpSerializer,
>;
