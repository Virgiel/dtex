use std::sync::Arc;

use crate::{
    duckdb::Connection,
    error::Result,
    fmt::Col,
    grid::Frame,
    source::{DataFrame, Source},
    task::{DuckTask, Runner},
    StrError,
};

pub enum Describer {
    Pending(DuckTask<Description>),
    Ready(Description),
    Error(StrError),
}

impl Describer {
    pub fn describe(source: Arc<Source>, runner: &Runner) -> Self {
        Self::Pending(runner.duckdb(move |ctx| describe(ctx, &source)))
    }

    pub fn tick(&mut self) {
        match self {
            Describer::Pending(task) => match task.tick() {
                Some(Ok(df)) => *self = Self::Ready(df),
                Some(Err(it)) => *self = Self::Error(it),
                None => {}
            },
            Describer::Ready(_) | Describer::Error(_) => {}
        }
    }

    pub fn df(&self) -> Option<Result<&Description>> {
        match self {
            Describer::Pending(_) => None,
            Describer::Ready(df) => Some(Ok(df)),
            Describer::Error(e) => Some(Err(e.clone())),
        }
    }

    pub fn is_loading(&self) -> Option<f64> {
        match self {
            Describer::Pending(task) => Some(task.progress()),
            Describer::Ready(_) | Describer::Error(_) => None,
        }
    }
}

pub struct Description(DataFrame);

impl Frame for Description {
    fn nb_col(&self) -> usize {
        self.0.num_columns() - 1
    }

    fn nb_row(&self) -> usize {
        self.0.num_rows()
    }

    fn idx_iter(&self, skip: usize, take: usize) -> Col {
        self.0.iter(0, skip, take)
    }

    fn col_name(&self, idx: usize) -> String {
        self.0.schema().all_fields()[idx + 1].name().clone()
    }

    fn col_iter(&self, idx: usize, skip: usize, take: usize) -> Col {
        self.0.iter(idx + 1, skip, take)
    }
}

pub fn describe(con: Connection, source: &Source) -> crate::error::Result<Description> {
    let df: Result<DataFrame> = source
        .describe(con)?
        .map(|d| d.map_err(|e| e.into()))
        .collect();
    Ok(Description(df?))
}
