use crate::events::{self, cache::Cache, errors::Error, schema::Schema};
use parquet::errors::ParquetError;
use parquet::file::writer::SerializedFileWriter;
use parquet::format::FileMetaData;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct Segment {
    pub uuid: Uuid,
    cache: Cache,
}

impl From<ParquetError> for Error {
    fn from(e: ParquetError) -> Self {
        Self::ParquetError(e.to_string())
    }
}

pub(crate) fn new(schema: &Schema, expiration: Sender<events::Event>) -> Segment {
    use std::time::Duration;

    let segment = Segment {
        uuid: Uuid::new_v4(),
        cache: Cache::new(),
    };

    let name = schema.name().to_owned();
    let uuid = segment.uuid;

    tokio::task::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;

        if let Err(e) = expiration
            .send(events::Event::SegmentExpired(name, uuid))
            .await
        {
            panic!("Could not send expiration event: {:?}", e);
        }
    });

    segment
}

impl Segment {
    // closes the Segment
    pub(crate) fn close(&mut self, schema: &Schema) -> Result<FileMetaData, Error> {
        use std::fs::File;
        use std::path::Path;

        let filename = format!("./{}.parquet", self.uuid.as_hyphenated().to_string());
        let path = Path::new(&filename);
        let file = File::create(&path)?;
        let writer = SerializedFileWriter::new(file, schema.types(), schema.properties())?;

        Ok(writer.close()?)
    }

    pub(crate) fn upload(&mut self) {}

    pub(crate) fn add(&mut self, values: events::Values) -> Result<(), Error> {
        self.cache.add(values);
        Ok(())
    }
}
