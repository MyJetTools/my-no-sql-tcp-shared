use my_tcp_sockets::{
    socket_reader::{ReadingTcpContractFail, SocketReader},
    TcpSocketSerializer,
};

use crate::TcpContract;

pub struct MyNoSqlReaderTcpSerializer {}

impl MyNoSqlReaderTcpSerializer {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl TcpSocketSerializer<TcpContract> for MyNoSqlReaderTcpSerializer {
    fn serialize(&self, contract: TcpContract) -> Vec<u8> {
        contract.serialize()
    }

    fn get_ping(&self) -> TcpContract {
        TcpContract::Ping
    }

    async fn deserialize<TSocketReader: Send + Sync + 'static + SocketReader>(
        &mut self,
        socket_reader: &mut TSocketReader,
    ) -> Result<TcpContract, ReadingTcpContractFail> {
        TcpContract::deserialize(socket_reader).await
    }
}
