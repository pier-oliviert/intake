use crate::events::{
    self,
    cache::{Cache, Columns},
    errors::Error,
    schema::Schema,
};
use parquet::errors::ParquetError;
use parquet::file::{properties::WriterPropertiesPtr, writer::SerializedFileWriter};
use parquet::format::FileMetaData;
use parquet::schema::types::TypePtr;
use std::fs::File;
use tokio::sync::mpsc::Sender;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct Segment {
    pub uuid: Uuid,
    cache: Option<Cache>,
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
        cache: Some(Cache::new()),
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
    pub(crate) fn close(
        self,
        types: TypePtr,
        properties: WriterPropertiesPtr,
    ) -> Result<FileMetaData, Error> {
        use std::path::Path;

        let columns = self
            .cache
            .expect("A cache should exists. This is a bug")
            .to_columns();

        let filename = format!("./{}.parquet", self.uuid.as_hyphenated().to_string());
        let path = Path::new(&filename);
        let file = File::create(&path)?;
        let mut writer = SerializedFileWriter::new(file, types, properties)?;

        Self::write(columns, &mut writer);
        Ok(writer.close()?)
    }

    // Return whether the underlying cache is empty or not.
    // Notice that a segment with no cache will return empty
    // and not panic or anything.
    pub(crate) fn is_empty(&self) -> bool {
        if let Some(c) = self.cache.as_ref() {
            return c.is_empty();
        }

        true
    }

    // Add event to the underlying cache if there's a cache present. If no cache
    // is set, an error will be returned.
    // It's possible that the behavior change to panic overtime as it is expected
    // that the segment should always have an underlying cache.
    pub(crate) fn add(&mut self, values: events::Values) -> Result<(), Error> {
        match self.cache.as_mut() {
            None => Err(Error::SegmentWithoutCache),
            Some(c) => {
                c.add(values);
                Ok(())
            }
        }
    }
}

impl Segment {
    fn write(columns: Columns, writer: &mut SerializedFileWriter<File>) {
        println!("Columns: {:?}", &columns);
    }
}
