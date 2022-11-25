use super::errors::StateUpdateError;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Default)]
pub(crate) struct State {
    slot: String,
    last_consistent_point: String,
    wal: LastKnownWalState,

    #[serde(skip_serializing, default)]
    path: String,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub(crate) struct LastKnownWalState {
    start: i64,
    flushed: i64,
    applied: i64,
    clock: i64,
}

impl State {
    pub(crate) fn slot(&self) -> &str {
        self.slot.as_str()
    }

    pub(crate) fn last_flushed(&self) -> [u8; 8] {
        let flushed = self.wal.flushed + 1;
        flushed.to_be_bytes()
    }

    pub(crate) fn last_applied(&self) -> [u8; 8] {
        let flushed = self.wal.applied + 1;
        flushed.to_be_bytes()
    }

    pub(crate) fn start(&mut self, data: &[u8]) -> Result<(), StateUpdateError> {
        self.wal.start = i64::from_be_bytes(data[0..8].try_into()?);
        self.wal.clock = i64::from_be_bytes(data[16..24].try_into()?);

        Ok(())
    }

    pub(crate) fn done(&mut self, _data: &[u8]) -> Result<(), StateUpdateError> {
        self.wal.flushed = self.wal.start;
        self.wal.applied = self.wal.start;

        persist(self.path.as_str(), &self)
    }
}

pub(crate) fn retrieve(path: &str) -> State {
    use std::fs::File;
    use std::io::prelude::*;

    match File::open(path) {
        Ok(mut f) => {
            let mut buffer = Vec::new();
            f.read_to_end(&mut buffer).unwrap();
            State {
                path: path.to_string(),
                ..serde_json::from_slice(&buffer).unwrap()
            }
        }
        Err(_) => {
            let s = State {
                path: path.to_string(),
                slot: "test1".to_string(),
                ..State::default()
            };
            persist(path, &s).unwrap();
            s
        }
    }
}

fn persist(path: &str, state: &State) -> Result<(), StateUpdateError> {
    use std::fs::File;
    use std::io::Write;

    let mut file = File::create(path).unwrap();
    let data = serde_json::to_vec(state).unwrap();
    file.write(data.as_slice()).unwrap();
    Ok(())
}
