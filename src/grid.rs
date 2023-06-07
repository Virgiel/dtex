use std::sync::Arc;

use reedline::KeyCode;
use tui::{crossterm::event::KeyEvent, Canvas};

use crate::{
    describe::Describer,
    source::{DataFrame, FrameSource, Loader, Source},
    style,
    tab::{GridUI, Status},
    task::Runner,
    OnKey, Ty,
};

use self::frame_grid::FrameGrid;

mod frame_grid;
pub mod nav;
mod projection;
mod sizer;

enum State {
    Normal,
    Description(Describer),
}
pub struct SourceGrid {
    source: Arc<Source>,
    frame: FrameSource,
    runner: Runner,
    loader: Loader,
    error: String,
    state: State,
    grid: FrameGrid,
    d_grid: FrameGrid,
}

impl SourceGrid {
    pub fn new(source: Arc<Source>, runner: Runner) -> Self {
        Self {
            loader: Loader::load(source.clone(), &runner),
            frame: FrameSource::empty(),
            runner,
            source,
            error: String::new(),
            state: State::Normal,
            grid: FrameGrid::new(),
            d_grid: FrameGrid::new(),
        }
    }

    pub fn is_loading(&self) -> Option<&'static str> {
        if let State::Description(desrc) = &self.state {
            if desrc.is_loading() {
                return Some("describe");
            }
        }
        if self.loader.is_loading() {
            Some("load")
        } else if self.frame.is_loading() {
            Some("stream")
        } else {
            None
        }
    }

    pub fn set_source(&mut self, source: Arc<Source>) {
        self.source = source.clone();
        self.loader = Loader::load(source.clone(), &self.runner);

        // Clear current description
        if let State::Description(desrc) = &mut self.state {
            *desrc = Describer::describe(source, &self.runner);
        }
    }

    pub fn set_err(&mut self, err: String) {
        self.error = err;
    }

    pub fn draw(&mut self, c: &mut Canvas) -> GridUI {
        match self.loader.tick() {
            Ok(Some(new)) => self.frame = new,
            Ok(None) => {}
            Err(e) => self.error = format!("loader: {}", e.0),
        }
        if let State::Description(desrc) = &mut self.state {
            desrc.tick();
        }
        self.frame.goal(self.grid.nav.c_row() + 1);
        self.frame.tick();

        // Draw error bar
        if !self.error.is_empty() {
            let mut l = c.btm();
            l.draw(&self.error, style::error());
        }

        match &self.state {
            State::Normal => self.grid.draw(c, self.frame.df()),
            State::Description(d) => match d.df() {
                None => self.grid.draw(c, self.frame.df()),
                Some(Ok(df)) => self.d_grid.draw(c, df).normal(Status::Description),
                Some(Err(err)) => {
                    let mut l = c.btm();
                    l.draw(err.0, style::error());
                    self.d_grid
                        .draw(c, &DataFrame::empty())
                        .normal(Status::Description)
                }
            },
        }
    }

    pub fn on_key(&mut self, event: &KeyEvent) -> OnKey {
        self.set_err(String::new());
        match self.state {
            State::Normal => match (self.grid.on_key(event), event.code) {
                (OnKey::Pass, KeyCode::Char('a')) => self.frame.load_all(),
                (OnKey::Pass, KeyCode::Char('d')) => {
                    self.state =
                        State::Description(Describer::describe(self.source.clone(), &self.runner))
                }
                (e, _) => return e,
            },
            State::Description(_) => match (self.d_grid.on_key(event), event.code) {
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
    fn idx_iter(&self, skip: usize) -> Box<dyn Iterator<Item = Ty> + '_>;
    fn col_name(&self, idx: usize) -> String;
    fn col_iter(&self, idx: usize, skip: usize) -> Box<dyn Iterator<Item = Ty> + '_>;
}

impl Frame for DataFrame {
    fn nb_col(&self) -> usize {
        self.num_columns()
    }

    fn nb_row(&self) -> usize {
        self.num_rows()
    }

    fn idx_iter(&self, skip: usize) -> Box<dyn Iterator<Item = Ty>> {
        Box::new((skip..self.num_rows()).map(|n| Ty::U(n as u64 + 1)))
    }

    fn col_name(&self, idx: usize) -> String {
        self.schema().all_fields()[idx].name().clone()
    }

    fn col_iter(&self, idx: usize, skip: usize) -> Box<dyn Iterator<Item = Ty> + '_> {
        Box::new(self.iter(idx, skip))
    }
}
