use std::sync::Arc;

use rust_extensions::{events_loop::EventsLoopTick, ApplicationStates, Logger};

use crate::sync_to_main::DeliverToMainNodeEvent;

use super::{DataReaderTcpConnection, SyncToMainNodeEvent, SyncToMainNodeQueues};

pub struct SyncToMainNodeHandler {
    pub event_notifier: Arc<SyncToMainNodeQueues>,
}

impl SyncToMainNodeHandler {
    pub fn new() -> Self {
        Self {
            event_notifier: Arc::new(SyncToMainNodeQueues::new()),
        }
    }

    pub async fn start(
        &self,
        app_states: Arc<impl ApplicationStates + Send + Sync + 'static>,
        logger: Arc<impl Logger + Send + Sync + 'static>,
    ) {
        let event_loop = SyncToMainNodeEventLoop::new(self.event_notifier.clone());
        self.event_notifier
            .event_loop
            .register_event_loop(Arc::new(event_loop))
            .await;
        self.event_notifier
            .event_loop
            .start(app_states, logger)
            .await
    }

    pub fn tcp_events_pusher_new_connection_established(
        &self,
        connection: Arc<DataReaderTcpConnection>,
    ) {
        self.event_notifier
            .event_loop
            .send(SyncToMainNodeEvent::Connected(connection));
    }

    pub fn tcp_events_pusher_connection_disconnected(
        &self,
        connection: Arc<DataReaderTcpConnection>,
    ) {
        self.event_notifier
            .event_loop
            .send(SyncToMainNodeEvent::Disconnected(connection));
    }

    pub fn tcp_events_pusher_got_confirmation(&self, confirmation_id: i64) {
        self.event_notifier
            .event_loop
            .send(SyncToMainNodeEvent::Delivered(confirmation_id));
    }
}

pub struct SyncToMainNodeEventLoop {
    queues: Arc<SyncToMainNodeQueues>,
}

impl SyncToMainNodeEventLoop {
    pub fn new(queues: Arc<SyncToMainNodeQueues>) -> Self {
        Self { queues }
    }
}

#[async_trait::async_trait]
impl EventsLoopTick<SyncToMainNodeEvent> for SyncToMainNodeEventLoop {
    async fn tick(&self, event: SyncToMainNodeEvent) {
        match event {
            SyncToMainNodeEvent::Connected(connection) => {
                self.queues.new_connection(connection).await;
                to_main_node_pusher(&self.queues, None).await;
            }
            SyncToMainNodeEvent::Disconnected(_) => {
                self.queues.disconnected().await;
            }
            SyncToMainNodeEvent::PingToDeliver => {
                to_main_node_pusher(&self.queues, None).await;
            }
            SyncToMainNodeEvent::Delivered(confirmation_id) => {
                to_main_node_pusher(&self.queues, Some(confirmation_id)).await;
            }
        }
    }
}

pub async fn to_main_node_pusher(
    queues: &Arc<SyncToMainNodeQueues>,
    delivered_confimration_id: Option<i64>,
) {
    use crate::MyNoSqlTcpContract;
    let next_event = queues
        .get_next_event_to_deliver(delivered_confimration_id)
        .await;

    if next_event.is_none() {
        return;
    }

    let (connection, next_event) = next_event.unwrap();

    match next_event {
        DeliverToMainNodeEvent::UpdatePartitionsExpiration {
            event,
            confirmation_id,
        } => {
            let mut partitions = Vec::with_capacity(event.partitions.len());

            for (partition, expiration_time) in event.partitions {
                partitions.push((partition, expiration_time));
            }

            connection
                .send(MyNoSqlTcpContract::UpdatePartitionsExpirationTime {
                    confirmation_id,
                    table_name: event.table_name,
                    partitions,
                })
                .await;
        }
        DeliverToMainNodeEvent::UpdatePartitionsLastReadTime {
            event,
            confirmation_id,
        } => {
            let mut partitions = Vec::with_capacity(event.partitions.len());

            for (partition, _) in event.partitions {
                partitions.push(partition);
            }

            connection
                .send(MyNoSqlTcpContract::UpdatePartitionsLastReadTime {
                    confirmation_id,
                    table_name: event.table_name,
                    partitions,
                })
                .await;
        }
        DeliverToMainNodeEvent::UpdateRowsExpirationTime {
            event,
            confirmation_id,
        } => {
            let mut row_keys = Vec::with_capacity(event.row_keys.len());

            for (row_key, _) in event.row_keys {
                row_keys.push(row_key);
            }

            connection
                .send(MyNoSqlTcpContract::UpdateRowsExpirationTime {
                    confirmation_id,
                    table_name: event.table_name,
                    partition_key: event.partition_key,
                    row_keys,
                    expiration_time: event.expiration_time,
                })
                .await;
        }
        DeliverToMainNodeEvent::UpdateRowsLastReadTime {
            event,
            confirmation_id,
        } => {
            let mut row_keys = Vec::with_capacity(event.row_keys.len());

            for (row_key, _) in event.row_keys {
                row_keys.push(row_key);
            }

            connection
                .send(MyNoSqlTcpContract::UpdateRowsLastReadTime {
                    confirmation_id,
                    table_name: event.table_name,
                    partition_key: event.partition_key,
                    row_keys,
                })
                .await;
        }
    }
}
