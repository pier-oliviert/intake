use crate::events::Event;
use std::sync::mpsc;
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use yaml_rust::Yaml;

mod postgresql;

#[derive(Error, Debug)]
pub(crate) enum Error {
    #[error("configuration error: `{0}`")]
    ConfigError(String),

    #[error("connection error: `{0}`")]
    ConnectionError(String),

    #[error("parse error: `{0}`")]
    ParseError(String),
}

impl From<tokio_postgres::Error> for Error {
    fn from(e: tokio_postgres::Error) -> Self {
        Error::ConnectionError(e.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::ParseError(e.to_string())
    }
}

#[async_trait::async_trait]
pub(crate) trait Client {
    async fn connect(&mut self, sender: Sender<Event>);
}

pub(crate) struct Driver<T: Client> {
    client: T,
}

impl<T: Client> Driver<T> {
    pub(crate) async fn connect(&mut self, sender: Sender<Event>) {
        self.client.connect(sender).await;
    }
}

pub(crate) async fn initialize(
    config: &Yaml,
) -> Result<(Driver<impl Client>, mpsc::Receiver<Error>), Error> {
    let (sender, receiver) = mpsc::channel();
    let driver = match config["driver"].as_str().expect(
        "source.driver should be a string. Possible values: http://github.com/intake/wiki/sources",
    ) {
        "postgresql" => {
            let client = postgresql::initialize(config, sender).await;
            Driver { client }
        }
        invalid => return Err(Error::ConfigError(format!("invalid driver: {}", invalid))),
    };

    Ok((driver, receiver))
}
