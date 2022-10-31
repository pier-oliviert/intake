use std::collections::HashMap;

use crate::events::{Value, Values};

pub(crate) type Columns = HashMap<String, Vec<Value>>;

#[derive(Debug)]
pub(crate) struct Cache(Vec<Values>);

impl Cache {
    pub(crate) fn new() -> Cache {
        Cache(Vec::new())
    }

    pub(crate) fn add(&mut self, values: Values) {
        self.0.push(values);
    }

    pub(crate) fn full(&self) -> bool {
        self.0.len() >= 1_000
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn to_columns(mut self) -> Columns {
        let mut columns: HashMap<String, Vec<Value>> = HashMap::new();

        for mut data in self.0.into_iter() {
            for (key, value) in data.drain().into_iter() {
                if let Some(v) = columns.get_mut(&key) {
                    v.push(value)
                } else {
                    columns.insert(key.to_owned(), Vec::new());
                }
            }
        }

        columns
    }
}
