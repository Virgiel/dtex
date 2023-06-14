use std::sync::Arc;

use reedline::KeyCode;
use tui::{crossterm::event::KeyEvent, Canvas};

use crate::{
    describe::Describer,
    fmt::Col,
    source::{DataFrame, FrameSource, Loader, Source},
    style,
    tab::{GridUI, Status},
    task::Runner,
    OnKey,
};

use self::{frame_grid::FrameGrid, nav::Nav};

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

    pub fn is_loading(&self) -> Option<(&'static str, f64)> {
        if let State::Description(desrc) = &self.state {
            if let Some(progress) = desrc.is_loading() {
                return Some(("describe", progress));
            }
        }
        if let Some(progress) = self.loader.is_loading() {
            Some(("load", progress))
        } else if self.frame.is_loading() {
            Some(("stream", -1.))
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
        self.frame.goal(self.grid.nav.goal().saturating_add(1));
        self.frame.tick();

        // Draw error bar
        if !self.error.is_empty() {
            let mut l = c.btm();
            l.draw(&self.error, style::error());
        }

        match &self.state {
            State::Normal => self
                .grid
                .draw(c, self.frame.df())
                .streaming(self.frame.is_streaming()),
            State::Description(d) => match d.df() {
                None => self
                    .grid
                    .draw(c, self.frame.df())
                    .streaming(self.frame.is_streaming()),
                Some(Ok(df)) => self.d_grid.draw(c, df),
                Some(Err(err)) => {
                    let mut l = c.btm();
                    l.draw(err.0, style::error());
                    self.d_grid.draw(c, &DataFrame::empty())
                }
            }
            .normal(Status::Description),
        }
    }

    pub fn nav(&self) -> Nav {
        match &self.state {
            State::Normal => self.grid.nav.clone(),
            State::Description(_) => self.d_grid.nav.clone(),
        }
    }

    pub fn set_nav(&mut self, nav: Nav) {
        match &self.state {
            State::Normal => self.grid.nav = nav,
            State::Description(_) => self.d_grid.nav = nav,
        }
    }

    pub fn on_key(&mut self, event: &KeyEvent) -> OnKey {
        self.set_err(String::new());
        match self.state {
            State::Normal => match (self.grid.on_key(event), event.code) {
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
    fn idx_iter(&self, skip: usize, take: usize) -> Col;
    fn col_name(&self, idx: usize) -> String;
    fn col_iter(&self, idx: usize, skip: usize, take: usize) -> Col;
}

impl Frame for DataFrame {
    fn nb_col(&self) -> usize {
        self.num_columns()
    }

    fn nb_row(&self) -> usize {
        self.num_rows()
    }

    fn idx_iter(&self, skip: usize, take: usize) -> Col {
        let mut col = Col::new();
        for i in skip..skip + take {
            col.add_nb(i);
        }
        col
    }

    fn col_name(&self, idx: usize) -> String {
        self.schema().all_fields()[idx].name().clone()
    }

    fn col_iter(&self, idx: usize, skip: usize, take: usize) -> Col {
        self.iter(idx, skip, take)
    }
}
