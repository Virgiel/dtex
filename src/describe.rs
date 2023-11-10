use std::sync::Arc;

use crate::{
    error::Result,
    fmt::{Col, GridBuffer},
    grid::{Frame, Grid},
    source::{DataFrame, Source},
    task::{DuckTask, Runner},
    view::{View, ViewState},
};

pub struct DescriberView {
    task: Option<DuckTask<Description>>,
    description: Description,
    error: Option<String>,
    pub grid: Grid,
}

impl DescriberView {
    pub fn new(source: Arc<Source>, runner: &Runner) -> Self {
        Self {
            grid: Grid::new(),
            description: Description(DataFrame::empty()),
            error: None,
            task: Some(runner.duckdb(source, move |source, con| {
                let df: Result<DataFrame> = source
                    .describe(con)?
                    .map(|d| d.map_err(|e| e.into()))
                    .collect();
                Ok(Description(df?))
            })),
        }
    }
}

impl View for DescriberView {
    fn tick(&mut self) -> ViewState {
        match self.task.as_mut().and_then(|t| t.tick()) {
            Some(Ok(df)) => {
                self.description = df;
                self.task = None;
            }
            Some(Err(it)) => {
                self.error = Some(it.0);
                self.task = None;
            }
            None => {}
        }

        ViewState {
            loading: self.task.as_ref().map(|t| ("describe", t.progress())),
            streaming: false,
            frame: &self.description,
            grid: &mut self.grid,
            err: self.error.as_deref(),
        }
    }
}

struct Description(DataFrame);

impl Frame for Description {
    fn nb_col(&self) -> usize {
        self.0.num_columns().saturating_sub(1)
    }

    fn nb_row(&self) -> usize {
        self.0.num_rows()
    }

    fn idx_iter(&self, buf: &mut GridBuffer, skip: usize, take: usize) -> Col {
        self.0.iter(buf, 0, skip, take)
    }

    fn col_name(&self, idx: usize) -> String {
        self.0.schema().all_fields()[idx + 1].name().clone()
    }

    fn col_iter(&self, buf: &mut GridBuffer, idx: usize, skip: usize, take: usize) -> Col {
        self.0.iter(buf, idx + 1, skip, take)
    }
}
