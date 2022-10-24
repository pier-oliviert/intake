use crate::events::Values;

#[derive(Debug)]
pub(crate) struct Cache(Vec<Values>);

impl Cache {
    pub(crate) fn new() -> Cache {
        Cache(Vec::new())
    }

    pub(crate) fn add(&mut self, values: Values) {
        self.0.push(values);
        println!("Cache size {:?}", self.0.len());
    }

    pub(crate) fn full(&self) -> bool {
        self.0.len() >= 1_000
    }
}
