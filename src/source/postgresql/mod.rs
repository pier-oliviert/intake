use crate::events::Event;
use crate::source::Error;
use futures::{future, ready, Sink, StreamExt};
use std::pin::Pin;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::Sender;
use tokio_postgres::NoTls;
use tokio_postgres::{Client, CopyBothDuplex, SimpleQueryRow};
use yaml_rust::Yaml;

mod errors;
mod event;
mod state;

pub(crate) struct Connection(Client, Arc<Mutex<state::State>>);

pub(crate) async fn initialize(config: &Yaml, sender: mpsc::Sender<Error>) -> Connection {
    let state = state::retrieve(config["state"].as_str().expect("state to be a string"));
    println!("State: {:?}", &state);

    let url = config["url"].as_str().unwrap();

    let (client, connection) = tokio_postgres::connect(url, NoTls).await.unwrap();
    println!("Spawning connection monitoring");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            sender.send(Error::from(e)).unwrap();
        }
    });

    Connection(client, Arc::new(Mutex::new(state)))
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

        tokio::spawn(Self::ingest(
            Box::pin(duplex_stream),
            self.1.clone(),
            sender,
        ));
    }

    async fn ingest(
        mut stream: Pin<Box<CopyBothDuplex<bytes::Bytes>>>,
        state: Arc<Mutex<state::State>>,
        sender: Sender<Event>,
    ) {
        loop {
            let event_res_opt = stream.as_mut().next().await;
            if event_res_opt.is_none() {
                break;
            }
            let event_res = event_res_opt.unwrap();
            if event_res.is_err() {
                continue;
            }
            let event = event_res.unwrap();

            if event[0] == b'w' {
                let wal = &event[1..25];
                let data = &event[25..];

                state
                    .lock()
                    .expect("could not aquire lock for state")
                    .start(wal)
                    .unwrap();

                let events = event::from_json(data).unwrap();
                let mut iterator = events.into_iter();
                while let Some(event) = iterator.next() {
                    sender.send(event).await.unwrap();
                }

                state
                    .lock()
                    .expect("could not aquire lock for state")
                    .done(wal)
                    .unwrap();
            }
            // type: keepalive message
            else if event[0] == b'k' {
                let last_byte = event.last().unwrap();
                let timeout_imminent = last_byte == &1;
                if timeout_imminent {
                    keepalive(&mut stream, state.clone()).await;
                }
            }
        }
    }
}

async fn keepalive(
    stream: &mut Pin<Box<CopyBothDuplex<bytes::Bytes>>>,
    state: Arc<Mutex<state::State>>,
) {
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

    let mut data_to_send: Vec<u8> = vec![114]; // "r" in ascii

    {
        let state = state.lock().expect("unable to obtain lock on state");

        // see here for format details: https://www.postgresql.org/docs/10/protocol-replication.html
        data_to_send.extend_from_slice(&state.last_flushed());
        data_to_send.extend_from_slice(&state.last_flushed());
        data_to_send.extend_from_slice(&state.last_applied());
        data_to_send.extend_from_slice(&time_since_2000.to_be_bytes());
        data_to_send.extend_from_slice(&[1]);
    }

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

        let query = format!(
            "CREATE_REPLICATION_SLOT {} TEMPORARY LOGICAL wal2json",
            self.1
                .lock()
                .expect("could not obtain lock for state")
                .slot()
        );
        println!("Query: {}", &query);

        let mut rows = self.0.simple_query(&query).await.unwrap();

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
