use crate::events::{
    self,
    cache::{Cache, Columns},
    errors::Error,
    schema::Schema,
};
use parquet::errors::ParquetError;
use parquet::file::{
    properties::WriterPropertiesPtr,
    writer::{SerializedFileWriter, SerializedRowGroupWriter},
};
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
        let mut writer = SerializedFileWriter::new(file, types.clone(), properties)?;
        let mut group = writer.next_row_group()?;

        Self::write(columns, &mut group);
        group.close()?;

        Ok(writer.close().unwrap())
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
    fn write(columns: Columns, writer: &mut SerializedRowGroupWriter<File>) {
        use crate::events::cache::Column;
        use parquet::column::writer::ColumnWriter;
        use parquet::data_type::ByteArray;

        while let Ok(Some(mut col)) = writer.next_column() {
            match col.untyped() {
                ColumnWriter::Int64ColumnWriter(writer) => {
                    let descriptor = writer.get_descriptor();
                    let name = descriptor.name();
                    let column = columns.get(name).unwrap();
                    if let Column::Int64(collection) = column {
                        writer
                            .write_batch(
                                collection.as_slice(),
                                Some(&[descriptor.max_def_level()]),
                                Some(&[descriptor.max_rep_level()]),
                            )
                            .unwrap();
                    }
                }
                ColumnWriter::ByteArrayColumnWriter(writer) => {
                    let descriptor = writer.get_descriptor();
                    let name = descriptor.name();
                    let column = columns.get(name).unwrap();
                    if let Column::String(collection) = column {
                        let values: Vec<ByteArray> = collection
                            .iter()
                            .map(|v| ByteArray::from(v.as_str()))
                            .collect();
                        writer
                            .write_batch(
                                values.as_slice(),
                                Some(&[descriptor.max_def_level()]),
                                Some(&[descriptor.max_rep_level()]),
                            )
                            .unwrap();
                    }
                }
                _ => unimplemented!("Coming up soon."),
            }

            col.close().unwrap();
        }

        println!("Done writing columns");
    }
}
