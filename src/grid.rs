use std::sync::Arc;

use reedline::KeyCode;
use tui::{crossterm::event::KeyEvent, Canvas};

use crate::{
    describe::Describer,
    event::Orchestrator,
    source::{Loader, Source},
    style,
    tab::{GridUI, Status},
    OnKey, Ty,
};

use self::frame_grid::FrameGrid;

mod frame_grid;
pub mod nav;
mod projection;
mod sizer;

#[derive(PartialEq, Eq)]
enum State {
    Normal,
    Description,
}
pub struct SourceGrid {
    source: Arc<Source>,
    orchestrator: Orchestrator,
    loader: Loader,
    description: Describer,
    error: String,
    state: State,
    grid: FrameGrid,
    d_grid: FrameGrid,
}

impl SourceGrid {
    pub fn new(source: Arc<Source>, orchestrator: Orchestrator) -> Self {
        Self {
            loader: Loader::new(source.clone(), &orchestrator),
            description: Describer::new(),
            orchestrator,
            source,
            error: String::new(),
            state: State::Normal,
            grid: FrameGrid::new(),
            d_grid: FrameGrid::new(),
        }
    }

    pub fn is_loading(&self) -> Option<&'static str> {
        if self.description.is_loading() {
            Some("describing")
        } else if self.loader.is_loading() {
            Some("loading")
        } else {
            None
        }
    }

    pub fn set_source(&mut self, source: Arc<Source>) {
        self.source = source.clone();
        self.loader.reload(source, &self.orchestrator);

        // Clear current description
        self.description.cancel();
    }

    pub fn set_err(&mut self, err: String) {
        self.error = err;
    }

    pub fn draw(&mut self, c: &mut Canvas) -> GridUI {
        match self.loader.tick() {
            Ok(new) => {
                if new && self.description.is_running() {
                    self.description
                        .describe(self.source.clone(), &self.orchestrator)
                }
            }
            Err(e) => self.error = format!("loader: {}", e.0),
        }
        if let Err(e) = self.description.tick() {
            self.error = format!("describe: {}", e.0)
        }

        // Draw error bar
        if !self.error.is_empty() {
            let mut l = c.btm();
            l.draw(&self.error, style::error());
        }

        if let Some(df) = self
            .description
            .df
            .as_ref()
            .filter(|_| self.state == State::Description)
        {
            self.d_grid.draw(c, df).normal(Status::Description)
        } else {
            self.grid.draw(c, &self.loader.df)
        }
    }

    pub fn on_key(&mut self, event: &KeyEvent) -> OnKey {
        self.set_err(String::new());
        match self.state {
            State::Normal => match (self.grid.on_key(event), event.code) {
                (OnKey::Pass, KeyCode::Char('a')) => {
                    self.loader
                        .load_more(self.source.clone(), None, &self.orchestrator);
                }
                (OnKey::Pass, KeyCode::Char('d')) => {
                    self.state = State::Description;
                    self.description
                        .describe(self.source.clone(), &self.orchestrator)
                }
                (e, _) => return e,
            },
            State::Description => match (self.d_grid.on_key(event), event.code) {
                (OnKey::Quit, _) | (OnKey::Pass, KeyCode::Esc) => self.state = State::Normal,
                (e, _) => return e,
            },
        }
        OnKey::Continue
    }
}

pub trait Frame {
    fn nb_col(&self) -> usize;
    fn nb_row(&self) -> usize;
    fn idx_iter(&self) -> Box<dyn Iterator<Item = Ty> + '_>;
    fn col_name(&self, idx: usize) -> &str;
    fn col_iter(&self, idx: usize) -> Box<dyn Iterator<Item = Ty> + '_>;
}

impl Frame for polars::prelude::DataFrame {
    fn nb_col(&self) -> usize {
        self.get_columns().len()
    }

    fn nb_row(&self) -> usize {
        self.height()
    }

    fn idx_iter(&self) -> Box<dyn Iterator<Item = Ty>> {
        Box::new((0..self.height()).map(|n| Ty::U64(n as u64 + 1)))
    }

    fn col_name(&self, idx: usize) -> &str {
        self.get_columns()[idx].name()
    }

    fn col_iter(&self, idx: usize) -> Box<dyn Iterator<Item = Ty> + '_> {
        Box::new(self.get_columns()[idx].phys_iter().map(Into::into))
    }
}
