use my_tcp_sockets::{
    socket_reader::{ReadingTcpContractFail, SocketReader},
    TcpSocketSerializer,
};

use crate::MyNoSqlTcpContract;

pub struct MyNoSqlReaderTcpSerializer {}

impl MyNoSqlReaderTcpSerializer {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl TcpSocketSerializer<MyNoSqlTcpContract> for MyNoSqlReaderTcpSerializer {
    fn serialize(&self, contract: MyNoSqlTcpContract) -> Vec<u8> {
        contract.serialize()
    }

    fn serialize_ref(&self, contract: &MyNoSqlTcpContract) -> Vec<u8> {
        contract.serialize()
    }

    fn get_ping(&self) -> MyNoSqlTcpContract {
        MyNoSqlTcpContract::Ping
    }

    fn apply_packet(&mut self, _: &MyNoSqlTcpContract) -> bool {
        false
    }

    async fn deserialize<TSocketReader: Send + Sync + 'static + SocketReader>(
        &mut self,
        socket_reader: &mut TSocketReader,
    ) -> Result<MyNoSqlTcpContract, ReadingTcpContractFail> {
        MyNoSqlTcpContract::deserialize(socket_reader).await
    }
}
