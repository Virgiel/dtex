use std::fmt::Display;

#[derive(Debug, Clone)]
pub struct StrError(pub String);

impl<D: Display> From<D> for StrError {
    fn from(value: D) -> Self {
        Self(value.to_string())
    }
}

pub type Result<T> = std::result::Result<T, StrError>;
