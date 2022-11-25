use std::array::TryFromSliceError;

use thiserror::Error;

#[derive(Error, Debug)]
pub(crate) enum StateUpdateError {
    #[error("could not convert slice into primitive value")]
    ParseError(#[from] TryFromSliceError),
}
