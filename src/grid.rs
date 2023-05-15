use std::sync::Arc;

use reedline::{KeyCode, KeyModifiers};
use tui::{crossterm::event::KeyEvent, unicode_width::UnicodeWidthStr, Canvas};

use crate::{
    describe::Describer,
    event::Orchestrator,
    fmt::{self, rtrim, ColStat},
    nav::Nav,
    projection::{self, Projection},
    sizer::{self, Sizer},
    source::{Loader, Source},
    style,
    tab::{State, Status},
    Ty,
};

/// Source Grid
pub struct Grid {
    pub source: Arc<Source>,
    // Task
    orchestrator: Orchestrator,
    loader: Loader,
    description: Describer,
    // Common state
    error: String,
    focus_description: bool,
    pub grid: InnerGrid,
    d_grid: InnerGrid,
}

impl Grid {
    pub fn new(state: State, source: Source, orchestrator: Orchestrator) -> Self {
        let source = Arc::new(source);
        Self {
            loader: Loader::new(source.clone(), &orchestrator),
            description: Describer::new(),
            orchestrator,
            source,
            error: String::new(),
            focus_description: false,
            grid: InnerGrid::new(state),
            d_grid: InnerGrid::new(State::Explore),
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

    pub fn set_source(&mut self, source: Source) {
        self.source = Arc::new(source);
        self.loader.reload(self.source.clone(), &self.orchestrator);

        // Clear current description
        self.description.cancel();
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
            .filter(|_| self.focus_description)
        {
            self.d_grid.draw(c, df)
        } else {
            self.grid.draw(c, &self.loader.df)
        }
    }

    pub fn on_key(&mut self, event: &KeyEvent) -> Status {
        if self.description.df.is_some() && self.focus_description {
            match self.d_grid.state {
                State::Explore => match event.code {
                    KeyCode::Esc => self.focus_description = false,
                    _ => match self.d_grid.on_key(event) {
                        Status::Exit => self.focus_description = false,
                        s => return s,
                    },
                },
                _ => match self.d_grid.on_key(event) {
                    Status::Exit => self.focus_description = false,
                    s => return s,
                },
            };
        } else {
            match self.grid.state {
                State::Explore => match event.code {
                    KeyCode::Char('a') => {
                        self.loader
                            .load_more(self.source.clone(), None, &self.orchestrator);
                    }
                    KeyCode::Char('d') => {
                        self.focus_description = true;
                        self.description
                            .describe(self.source.clone(), &self.orchestrator)
                    }
                    _ => return self.grid.on_key(event),
                },
                _ => return self.grid.on_key(event),
            };
        }
        Status::Continue
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
        Box::new((0..self.height()).map(|n| Ty::U64(n as u64)))
    }

    fn col_name(&self, idx: usize) -> &str {
        self.get_columns()[idx].name()
    }

    fn col_iter(&self, idx: usize) -> Box<dyn Iterator<Item = Ty> + '_> {
        Box::new(self.get_columns()[idx].phys_iter().map(Into::into))
    }
}

/// DataFrame UI state
pub struct InnerGrid {
    projection: Projection,
    nav: Nav,
    sizer: Sizer,
    pub state: State,
}

impl InnerGrid {
    pub fn new(state: State) -> Self {
        Self {
            projection: Projection::new(),
            nav: Nav::new(),
            sizer: Sizer::new(),
            state,
        }
    }

    pub fn on_key(&mut self, event: &KeyEvent) -> Status {
        let shift = event.modifiers.contains(KeyModifiers::SHIFT);
        let idx = self.nav.c_col;
        match self.state {
            State::Explore => match event.code {
                KeyCode::Char('s') => {
                    self.state = State::Size;
                }
                KeyCode::Char('m') => {
                    self.state = State::Projection;
                }
                KeyCode::Char('$') => return Status::OpenShell,
                KeyCode::Char('g') => self.nav.top(),
                KeyCode::Char('G') => self.nav.btm(),
                KeyCode::Left | KeyCode::Char('H') if shift => self.nav.win_left(),
                KeyCode::Down | KeyCode::Char('J') if shift => self.nav.win_down(),
                KeyCode::Up | KeyCode::Char('K') if shift => self.nav.win_up(),
                KeyCode::Right | KeyCode::Char('L') if shift => self.nav.win_right(),
                KeyCode::Left | KeyCode::Char('h') => self.nav.left(),
                KeyCode::Down | KeyCode::Char('j') => self.nav.down(),
                KeyCode::Up | KeyCode::Char('k') => self.nav.up(),
                KeyCode::Right | KeyCode::Char('l') => self.nav.right(),
                KeyCode::Char('q') => return Status::Exit,
                _ => {}
            },
            State::Projection => match event.code {
                KeyCode::Left | KeyCode::Char('H') if shift => {
                    self.projection.cmd(idx, projection::Cmd::Left);
                    self.nav.left()
                }
                KeyCode::Down | KeyCode::Char('J') if shift => {
                    self.projection.cmd(idx, projection::Cmd::Hide);
                }
                KeyCode::Up | KeyCode::Char('K') if shift => {
                    self.projection.reset() // TODO keep column focus
                }
                KeyCode::Right | KeyCode::Char('L') if shift => {
                    self.projection.cmd(idx, projection::Cmd::Right);
                    self.nav.right();
                }
                KeyCode::Esc | KeyCode::Char('q') => self.state = State::Explore,
                KeyCode::Left | KeyCode::Char('h') => self.nav.left(),
                KeyCode::Down | KeyCode::Char('j') => self.nav.down(),
                KeyCode::Up | KeyCode::Char('k') => self.nav.up(),
                KeyCode::Right | KeyCode::Char('l') => self.nav.right(),
                _ => {}
            },
            State::Size => {
                let mut reset = true;
                match event.code {
                    KeyCode::Esc | KeyCode::Char('q') => self.state = State::Explore,
                    KeyCode::Char('r') => self.sizer.reset(),
                    KeyCode::Char('f') => self.sizer.fit(),
                    KeyCode::Char(' ') => self.sizer.toggle(),
                    KeyCode::Left | KeyCode::Char('h') => {
                        self.sizer.cmd(idx, sizer::Cmd::Less);
                        reset = false;
                    }
                    KeyCode::Down | KeyCode::Char('j') => {
                        self.sizer.cmd(idx, sizer::Cmd::Constrain)
                    }
                    KeyCode::Up | KeyCode::Char('k') => self.sizer.cmd(idx, sizer::Cmd::Free),
                    KeyCode::Right | KeyCode::Char('l') => {
                        self.sizer.cmd(idx, sizer::Cmd::More);
                        reset = false;
                    }
                    _ => reset = false,
                }
                if reset {
                    self.state = State::Explore;
                }
            }
            State::Shell => unreachable!(),
        };

        Status::Continue
    }

    pub fn draw(&mut self, c: &mut Canvas, df: &dyn Frame) -> GridUI {
        pub fn size_col<'a>(
            values: impl Iterator<Item = Ty<'a>>,
            off: usize,
            n: usize,
        ) -> (Vec<Ty<'a>>, ColStat) {
            values.skip(off).take(n).fold(
                (Vec::new(), ColStat::new()),
                |(mut vec, mut stat), ty| {
                    stat.add(&ty);
                    vec.push(ty);
                    (vec, stat)
                },
            )
        }

        let nb_col = df.nb_col();
        let nb_row = df.nb_row();
        self.projection.set_nb_cols(nb_col);
        let visible_cols = self.projection.nb_cols();

        let v_row = c.height() - 1; // header bar
        let row_off = self.nav.row_offset(nb_row, v_row);
        // Nb call necessary to print the biggest index
        let (ids, mut id_stat) = size_col(df.idx_iter(), row_off, v_row);
        id_stat.align_right();
        // Whole canvas minus index col
        let mut remaining_width = c.width() - id_stat.budget();
        let mut cols = Vec::new();
        let mut coll_off_iter = self.nav.col_iter(visible_cols);
        // Fill canvas with columns
        while remaining_width > cols.len() {
            if let Some(off) = coll_off_iter.next() {
                let idx = self.projection.project(off);
                let name = df.col_name(idx);
                let (fields, stat) = size_col(df.col_iter(idx), row_off, v_row);
                let size = self.sizer.size(idx, stat.budget(), name.width());
                let allowed = size.min(remaining_width - cols.len());
                remaining_width = remaining_width.saturating_sub(allowed);
                cols.push((off, name, fields, stat, allowed));
            } else {
                break;
            }
        }
        cols.sort_unstable_by_key(|(i, _, _, _, _)| *i);
        drop(coll_off_iter);

        let fmt_buf = &mut String::with_capacity(256);
        // Draw headers
        {
            let line = &mut c.top();
            line.draw(
                format_args!("{:>1$} ", '#', id_stat.budget()),
                style::index().bold(),
            );

            for (off, name, _, _, budget) in &cols {
                let style = if *off == self.nav.c_col {
                    style::selected().bold()
                } else {
                    style::primary().bold()
                };
                line.draw(
                    format_args!("{:<1$}", rtrim(name, fmt_buf, *budget), budget),
                    style,
                );
                line.draw("│", style::separator());
            }
        }

        // Draw rows
        for r in 0..v_row.min(nb_row - row_off) {
            let style = if r == self.nav.c_row - self.nav.o_row {
                style::selected()
            } else {
                style::primary()
            };
            let line = &mut c.top();
            line.draw(
                format_args!(
                    "{} ",
                    fmt::fmt_field(fmt_buf, &ids[r], &id_stat, id_stat.budget())
                ),
                style::index(),
            );
            for (_, _, fields, stat, budget) in &cols {
                let ty = &fields[r];
                line.draw(
                    format_args!("{}", fmt::fmt_field(fmt_buf, ty, stat, *budget)),
                    style,
                );
                line.draw("│", style::separator());
            }
        }

        GridUI {
            col_name: (self.projection.nb_cols() > 0)
                .then(|| df.col_name(self.nav.c_col).to_string()),
            progress: ((self.nav.c_row + 1) * 100) / nb_row.max(1),
            state: self.state,
            describe: false,
        }
    }
}

pub struct GridUI {
    pub col_name: Option<String>, // TODO borrow
    pub progress: usize,
    pub state: State,
    pub describe: bool,
}
