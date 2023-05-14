use polars::prelude::PolarsError;

#[derive(Debug)]
pub struct StrError(pub String);

impl From<PolarsError> for StrError {
    fn from(value: PolarsError) -> Self {
        Self(value.to_string())
    }
}

impl From<std::io::Error> for StrError {
    fn from(value: std::io::Error) -> Self {
        Self(value.to_string())
    }
}

impl From<std::str::Utf8Error> for StrError {
    fn from(value: std::str::Utf8Error) -> Self {
        Self(value.to_string())
    }
}

impl From<String> for StrError {
    fn from(value: String) -> Self {
        Self(value)
    }
}

pub type Result<T> = std::result::Result<T, StrError>;
