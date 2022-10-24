use std::collections::HashMap;
use tokio::sync::mpsc::Sender;
use yaml_rust::Yaml;

use crate::events::{self, errors::Error, schema::Schema, segment, terminator};

pub(crate) struct Collection {
    schemas: HashMap<String, Schema>,
    terminator: terminator::Terminator,
    expiration: Sender<events::Event>,
}

// Return a new Collection configured with the given config.
//
// The Collection is responsible to monitor different indices
// that intake will ingest. Each index will have an entry in the collection
// connecting the Schema and the ongoing Segment together.
pub(crate) fn new(_config: &Yaml, expiration_sender: Sender<events::Event>) -> Collection {
    Collection {
        schemas: HashMap::new(),
        expiration: expiration_sender,
        terminator: terminator::new(),
    }
}

// Public
impl Collection {
    pub(crate) fn insert(&mut self, index: &str, data: crate::events::Values) -> Result<(), Error> {
        match self.schemas.get_mut(index) {
            Some(schema) => {
                if let Some(seg) = schema.segment() {
                    println!("Adding data to existing segment: {}", &seg.uuid);
                    seg.add(data)?
                } else {
                    let mut seg = segment::new(&schema, self.expiration.clone());
                    println!("Creating new segment for existing schema: {}", &seg.uuid);
                    seg.add(data)?;
                    *schema.segment() = Some(seg);
                }
            }
            None => {
                println!("Creating new entry for index: {}", &index);
                let mut schema = Schema::try_from((index, &data))?;
                *schema.segment() = Some(segment::new(&schema, self.expiration.clone()));
                self.schemas.insert(schema.name(), schema.into());
            }
        }
        Ok(())
    }

    pub(crate) fn expired(&mut self, index: &str, id: &uuid::Uuid) {
        if let Some(schema) = self.schemas.get_mut(index) {
            if let Some(seg) = schema.segment().take() {
                self.terminator.terminate(seg);
            }
        }
    }
}
