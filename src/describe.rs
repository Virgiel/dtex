use std::sync::Arc;

use crate::{
    error::Result,
    event::{Orchestrator, Task},
    grid::Frame,
    source::Source,
};

pub struct Describer {
    pub df: Option<Description>,
    task: Option<Task<Description>>,
}

impl Describer {
    pub fn new() -> Self {
        Self {
            df: None,
            task: None,
        }
    }

    pub fn describe(&mut self, source: Arc<Source>, orchestrator: &Orchestrator) {
        self.task = Some(orchestrator.task(move || describe(&source)))
    }

    pub fn tick(&mut self) -> Result<()> {
        if let Some(task) = &mut self.task {
            match task.tick() {
                Ok(Some(df)) => {
                    self.task = None;
                    self.df = Some(df)
                }
                Ok(None) => {}
                Err(it) => {
                    self.task = None;
                    return Err(it);
                }
            }
        }
        Ok(())
    }

    pub fn cancel(&mut self) {
        self.task.take();
    }

    pub fn is_running(&self) -> bool {
        self.task.is_some() || self.df.is_some()
    }

    pub fn is_loading(&self) -> bool {
        self.task.is_some()
    }
}

pub struct Description {
    df: polars::prelude::DataFrame,
    types: Vec<String>,
}

impl Frame for Description {
    fn nb_col(&self) -> usize {
        self.df.nb_col() - 1
    }

    fn nb_row(&self) -> usize {
        self.df.nb_row() + 1
    }

    fn idx_iter(&self) -> Box<dyn Iterator<Item = crate::Ty> + '_> {
        Box::new(
            ["type".into()]
                .into_iter()
                .chain(self.df.get_columns()[0].phys_iter().map(Into::into)),
        )
    }

    fn col_name(&self, idx: usize) -> &str {
        self.df.get_columns()[idx + 1].name()
    }

    fn col_iter(&self, idx: usize) -> Box<dyn Iterator<Item = crate::Ty> + '_> {
        Box::new(
            [self.types[idx].as_str().into()]
                .into_iter()
                .chain(self.df.get_columns()[idx + 1].phys_iter().map(Into::into)),
        )
    }
}

pub fn describe(source: &Source) -> crate::error::Result<Description> {
    source.apply(|l| {
        let mut df = l.collect()?.describe(None)?;
        df.rechunk();
        Ok(Description {
            types: df
                .get_columns()
                .iter()
                .map(|s| s.dtype().to_string())
                .collect(),
            df,
        })
    })
}
