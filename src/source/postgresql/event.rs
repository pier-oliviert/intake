use crate::events::{Event, Value};
use crate::source::Error;
use serde::{Deserialize, Serialize};
use serde_json::Value as JSONValue;

pub(crate) fn from_json(payload: &[u8]) -> Result<Vec<Event>, Error> {
    let mutations: Mutations = serde_json::from_slice(payload)?;
    Ok(mutations.mutations.into_iter().collect())
}

#[derive(Deserialize, Serialize, Debug)]
struct Mutations {
    #[serde(rename = "change")]
    mutations: Vec<Mutation>,
}

#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "kind")]
#[serde(rename_all = "camelCase")]
enum Mutation {
    Insert {
        #[serde(rename = "columnnames")]
        columns: Vec<String>,
        #[serde(rename = "columnvalues")]
        values: Vec<JSONValue>,
        #[serde(rename = "columntypes")]
        types: Vec<String>,
    },
    Update,
    Delete,
}

impl From<Mutation> for Event {
    fn from(mutation: Mutation) -> Self {
        use std::collections::HashMap;

        match mutation {
            Mutation::Insert {
                columns,
                values,
                types,
            } => {
                let mut map = HashMap::new();
                for (i, column) in columns.into_iter().enumerate() {
                    map.insert(column, Value::from((&types[i], &values[i])));
                }
                Event::Insert("test".into(), map)
            }
            _ => Self::default(),
        }
    }
}

impl FromIterator<Mutation> for Vec<Event> {
    fn from_iter<T: IntoIterator<Item = Mutation>>(mutations: T) -> Self {
        let mut events = Vec::new();
        for m in mutations {
            events.push(m.into());
        }

        events
    }
}

impl From<(&String, &JSONValue)> for Value {
    fn from(tuple: (&String, &JSONValue)) -> Self {
        match &**tuple.0 {
            "integer" => Value::Int64(tuple.1.as_i64().unwrap()),
            "text" => Value::String(tuple.1.as_str().unwrap().into()),
            _ => Value::String("invalid".to_string()),
        }
    }
}
