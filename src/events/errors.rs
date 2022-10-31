#[derive(Debug)]
pub(crate) enum Error {
    ParquetError(String),
    FileError(String),
    SegmentWithoutCache,
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::FileError(e.to_string())
    }
}
