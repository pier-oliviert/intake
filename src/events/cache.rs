use std::collections::HashMap;

use crate::events::{Value, Values};

pub(crate) type Columns = HashMap<String, Column>;

#[derive(Debug)]
pub(crate) struct Cache(Vec<Values>);

pub(crate) enum Column {
    Int64(Vec<i64>),
    String(Vec<String>),
}

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
        let mut columns: HashMap<String, Column> = HashMap::new();

        for mut data in self.0.into_iter() {
            for (key, value) in data.drain().into_iter() {
                if let Some(column) = columns.get_mut(&key) {
                    match column {
                        Column::Int64(collection) => {
                            if let Value::Int64(v) = value {
                                collection.push(v);
                            } else {
                                panic!("Wrong type!");
                            }
                        }
                        Column::String(collection) => {
                            if let Value::String(v) = value {
                                collection.push(v);
                            } else {
                                panic!("Wrong type!");
                            }
                        }
                    }
                } else {
                    let typed_values = match value {
                        Value::Int64(_) => Column::Int64(Vec::new()),
                        Value::String(_) => Column::String(Vec::new()),
                        _ => panic!("Not yet"),
                    };

                    columns.insert(key.to_owned(), typed_values);
                }
            }
        }

        columns
    }
}
