use std::sync::Arc;

use crate::{
    error::Result,
    event::{Orchestrator, Task},
    grid::Frame,
    source::{DataFrame, Source},
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

pub fn describe(source: &Source) -> crate::error::Result<Description> {
    let df: Result<DataFrame> = source
        .describe()?
        .map(|d| d.map_err(|e| e.into()))
        .collect();
    Ok(Description(df?))
}
