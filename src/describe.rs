use std::sync::Arc;

use crate::{
    error::Result,
    grid::Frame,
    source::{DataFrame, Source},
    task::{OnceCtx, OnceTask, Runner},
    StrError,
};

pub enum Describer {
    Pending(OnceTask<Description>),
    Ready(Description),
    Error(StrError),
}

impl Describer {
    pub fn describe(source: Arc<Source>, runner: &Runner) -> Self {
        Self::Pending(runner.once(move |ctx| describe(ctx, &source)))
    }

    pub fn tick(&mut self) {
        match self {
            Describer::Pending(task) => match task.tick() {
                Ok(Some(df)) => *self = Self::Ready(df),
                Ok(None) => {}
                Err(it) => *self = Self::Error(it),
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

    pub fn is_loading(&self) -> bool {
        matches!(self, Describer::Pending(_))
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

    fn idx_iter(&self, skip: usize) -> Box<dyn Iterator<Item = crate::Ty> + '_> {
        Box::new(self.0.iter(0, skip))
    }

    fn col_name(&self, idx: usize) -> String {
        self.0.schema().all_fields()[idx + 1].name().clone()
    }

    fn col_iter(&self, idx: usize, skip: usize) -> Box<dyn Iterator<Item = crate::Ty> + '_> {
        Box::new(self.0.iter(idx + 1, skip))
    }
}

pub fn describe(ctx: OnceCtx, source: &Source) -> crate::error::Result<Description> {
    let pending = source.describe()?;
    while !pending.tick()? {
        if ctx.canceled() {
            return Err("canceled".into());
        }
    }
    let df: Result<DataFrame> = pending
        .execute()?
        .map(|d| d.map_err(|e| e.into()))
        .collect();
    Ok(Description(df?))
}
