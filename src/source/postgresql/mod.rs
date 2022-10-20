use crate::events::Event;
use crate::source::Error;
use futures::{future, ready, Sink, StreamExt};
use std::pin::Pin;
use std::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio_postgres::NoTls;
use tokio_postgres::{Client, CopyBothDuplex, SimpleQueryRow};
use yaml_rust::Yaml;

mod event;

pub(crate) struct Connection(Client);

pub(crate) async fn initialize(config: &Yaml, sender: mpsc::Sender<Error>) -> Connection {
    let url = config["url"].as_str().unwrap();
    let (client, connection) = tokio_postgres::connect(url, NoTls).await.unwrap();
    println!("Spawning connection monitoring");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            sender.send(Error::from(e)).unwrap();
        }
    });

    Connection(client)
}

impl From<bytes::Bytes> for crate::events::Event {
    fn from(_: bytes::Bytes) -> Self {
        Self::default()
    }
}

impl Connection {
    async fn start_replication(&mut self, row: &SimpleQueryRow, sender: Sender<Event>) {
        let slot = row.get("slot_name").unwrap().to_string();
        let lsn = row.get("consistent_point").unwrap().to_string();

        let query = format!("START_REPLICATION SLOT {} LOGICAL {}", slot, lsn);
        let duplex_stream = self
            .0
            .copy_both_simple::<bytes::Bytes>(&query)
            .await
            .unwrap();
        let mut duplex_stream_pin = Box::pin(duplex_stream);

        tokio::spawn(async move {
            loop {
                let event_res_opt = duplex_stream_pin.as_mut().next().await;
                if event_res_opt.is_none() {
                    break;
                }
                //if event_res_opt.is_none() { continue; }
                let event_res = event_res_opt.unwrap();
                if event_res.is_err() {
                    continue;
                }
                let event = event_res.unwrap();

                if event[0] == b'w' {
                    // Data Change event are sent out to the channel so it can be processed.
                    let events = event::from_json(&event[25..]).unwrap();
                    let mut iterator = events.into_iter();
                    while let Some(event) = iterator.next() {
                        sender.send(event).await.unwrap();
                    }
                }
                // type: keepalive message
                else if event[0] == b'k' {
                    let last_byte = event.last().unwrap();
                    let timeout_imminent = last_byte == &1;
                    println!(
                        "Got keepalive message:{:x?} @timeoutImminent:{}",
                        event, timeout_imminent
                    );
                    if timeout_imminent {
                        keepalive(&mut duplex_stream_pin).await;
                    }
                }
            }
        });
    }
}

async fn keepalive(stream: &mut Pin<Box<CopyBothDuplex<bytes::Bytes>>>) {
    use bytes::Bytes;
    use std::task::Poll;
    use std::time::{SystemTime, UNIX_EPOCH};
    // not sure if sending the client system's "time since 2000-01-01" is actually necessary, but lets do as postgres asks just in case
    const SECONDS_FROM_UNIX_EPOCH_TO_2000: u128 = 946684800;
    let time_since_2000: u64 = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
        - (SECONDS_FROM_UNIX_EPOCH_TO_2000 * 1000 * 1000))
        .try_into()
        .unwrap();

    // see here for format details: https://www.postgresql.org/docs/10/protocol-replication.html
    let mut data_to_send: Vec<u8> = vec![];
    // Byte1('r'); Identifies the message as a receiver status update.
    data_to_send.extend_from_slice(&[114]); // "r" in ascii
                                            // The location of the last WAL byte + 1 received and written to disk in the standby.
    data_to_send.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
    // The location of the last WAL byte + 1 flushed to disk in the standby.
    data_to_send.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
    // The location of the last WAL byte + 1 applied in the standby.
    data_to_send.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
    // The client's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
    //0, 0, 0, 0, 0, 0, 0, 0,
    data_to_send.extend_from_slice(&time_since_2000.to_be_bytes());
    // Byte1; If 1, the client requests the server to reply to this message immediately. This can be used to ping the server, to test if the connection is still healthy.
    data_to_send.extend_from_slice(&[1]);

    let buf = Bytes::from(data_to_send);

    let mut next_step = 1;
    future::poll_fn(|cx| loop {
        println!("Doing step:{}", next_step);
        match next_step {
            1 => {
                ready!(stream.as_mut().poll_ready(cx)).unwrap();
            }
            2 => {
                stream.as_mut().start_send(buf.clone()).unwrap();
            }
            3 => {
                ready!(stream.as_mut().poll_flush(cx)).unwrap();
            }
            4 => return Poll::Ready(()),
            _ => panic!(),
        }
        next_step += 1;
    })
    .await;

    println!("Sent response to keepalive message/warning!:{:x?}", buf);
}

#[async_trait::async_trait]
impl super::Client for Connection {
    async fn connect(&mut self, sender: Sender<Event>) {
        use tokio_postgres::SimpleQueryMessage;

        let mut rows = self
            .0
            .simple_query("CREATE_REPLICATION_SLOT test1 TEMPORARY LOGICAL wal2json")
            .await
            .unwrap();

        // There should only be 1 row that is returned for the replication
        // information. However, postgres will usually return more than 1;
        // usually a CommandComplete(bytes) response which can be ignored.
        rows = rows
            .into_iter()
            .filter(|row| match row {
                SimpleQueryMessage::Row(_) => true,
                _ => false,
            })
            .collect::<Vec<SimpleQueryMessage>>();

        if rows.len() != 1 {
            panic!("Expected only 1 row from the query, got {}", rows.len())
        }

        for row in rows {
            match row {
                SimpleQueryMessage::Row(r) => self.start_replication(&r, sender.clone()).await,
                SimpleQueryMessage::CommandComplete(u) => println!("Bytes written: {}", u),
                _ => println!("Unknown message"),
            }
        }
    }
}
