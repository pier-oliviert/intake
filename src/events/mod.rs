// Events is the module that is responsible for ingesting replication items from the
// connected source. This module has a public struct that should be used by different source
// to normalize the datastructure that comes from those sources into a known type that the
// event manager here can process.
//
// The conversion and rules of getting from the replications stream into the event's generic struct
// is up to each source.

use std::collections::BTreeMap;
use std::ops::Index;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub enum Event {
    Insert(BTreeMap<String, Value>),
    Update(BTreeMap<String, Value>),
    Delete(BTreeMap<String, Value>),
}

impl Default for Event {
    fn default() -> Self {
        Event::Insert(BTreeMap::default())
    }
}

impl<'a> Index<&'a str> for Event {
    type Output = Value;
    fn index(&self, index: &'a str) -> &Self::Output {
        let tree = match &*self {
            Event::Insert(map) => map,
            Event::Update(map) => map,
            Event::Delete(map) => map,
        };

        &tree[index]
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

pub fn listen() -> mpsc::Sender<Event> {
    let (sender, mut receiver) = mpsc::channel(10);

    tokio::spawn(async move {
        loop {
            match receiver.recv().await {
                Some(e) => println!("Received {:?}", e),
                None => {}
            }
        }
    });

    sender
}
