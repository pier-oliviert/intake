// Events is the module that is responsible for ingesting replication items from the
// connected source. This module has a public struct that should be used by different source
// to normalize the datastructure that comes from those sources into a known type that the
// event manager here can process.
//
// The conversion and rules of getting from the replications stream into the event's generic struct
// is up to each source.

use std::collections::HashMap;
use tokio::sync::mpsc;
use uuid::Uuid;
use yaml_rust::Yaml;

mod cache;
mod collection;
mod errors;
mod schema;
mod terminator;

pub(crate) mod segment;

pub(crate) type Values = HashMap<String, Value>;

#[derive(Debug, Clone)]
pub(crate) enum Event {
    Insert(String, Values),
    Update(String, Values),
    Delete(String, Values),
    SegmentExpired(String, Uuid),
}

impl Default for Event {
    fn default() -> Self {
        Event::Insert("undefined index".into(), Values::default())
    }
}

#[derive(Debug, Clone)]
pub enum Value {
    Int64(i64),
    Float(f64),
    String(String),
}

impl Default for Value {
    fn default() -> Self {
        Value::Int64(-1)
    }
}

impl From<&Value> for parquet::basic::Type {
    fn from(v: &Value) -> Self {
        match v {
            Value::Int64(_) => Self::INT64,
            Value::Float(_) => Self::FLOAT,
            Value::String(_) => Self::BYTE_ARRAY,
        }
    }
}

pub(crate) fn listen(config: &Yaml) -> mpsc::Sender<Event> {
    let (sender, mut receiver) = mpsc::channel(10);
    let mut segments = collection::new(config, sender.clone());

    tokio::spawn(async move {
        loop {
            match receiver.recv().await {
                Some(e) => match e {
                    Event::Insert(index, data) => {
                        segments.insert(&index, data).unwrap();
                    }
                    Event::SegmentExpired(index, id) => {
                        segments.expired(&index, &id);
                    }
                    _ => unimplemented!("Not yet"),
                },
                None => {}
            }
        }
    });

    sender
}
