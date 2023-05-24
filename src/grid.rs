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
        Box::new((0..self.height()).map(|n| Ty::U64(n as u64)))
    }

    fn col_name(&self, idx: usize) -> &str {
        self.get_columns()[idx].name()
    }

    fn col_iter(&self, idx: usize) -> Box<dyn Iterator<Item = Ty> + '_> {
        Box::new(self.get_columns()[idx].phys_iter().map(Into::into))
    }
}

mod frame_grid {
    use reedline::{KeyCode, KeyModifiers};
    use tui::{crossterm::event::KeyEvent, unicode_width::UnicodeWidthStr, Canvas};

    use crate::{
        fmt::{self, rtrim, ColStat},
        nav::Nav,
        projection::{self, Projection},
        sizer::{self, Sizer},
        style,
        tab::{GridUI, Status},
        OnKey, Ty,
    };

    use super::Frame;

    enum State {
        Normal,
        Size,
        Projection,
    }

    pub struct FrameGrid {
        projection: Projection,
        nav: Nav,
        sizer: Sizer,
        state: State,
    }

    impl FrameGrid {
        pub fn new() -> Self {
            Self {
                projection: Projection::new(),
                nav: Nav::new(),
                sizer: Sizer::new(),
                state: State::Normal,
            }
        }

        pub fn on_key(&mut self, event: &KeyEvent) -> OnKey {
            let shift = event.modifiers.contains(KeyModifiers::SHIFT);
            let idx = self.nav.c_col;
            match self.state {
                State::Normal => match event.code {
                    KeyCode::Char('s') => {
                        self.state = State::Size;
                    }
                    KeyCode::Char('m') => {
                        self.state = State::Projection;
                    }
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
                    KeyCode::Char('q') => return OnKey::Quit,
                    _ => return OnKey::Pass,
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
                    KeyCode::Esc | KeyCode::Char('q') => self.state = State::Normal,
                    KeyCode::Left | KeyCode::Char('h') => self.nav.left(),
                    KeyCode::Down | KeyCode::Char('j') => self.nav.down(),
                    KeyCode::Up | KeyCode::Char('k') => self.nav.up(),
                    KeyCode::Right | KeyCode::Char('l') => self.nav.right(),
                    _ => {}
                },
                State::Size => {
                    let mut reset = true;
                    match event.code {
                        KeyCode::Esc | KeyCode::Char('q') => self.state = State::Normal,
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
                        self.state = State::Normal;
                    }
                }
            };

            OnKey::Continue
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
                let current = r == self.nav.c_row - self.nav.o_row;
                let style = if current {
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
                    if current {
                        style::index().bold()
                    } else {
                        style::index()
                    },
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
                status: match self.state {
                    State::Normal => Status::Normal,
                    State::Size => Status::Size,
                    State::Projection => Status::Projection,
                },
            }
        }
    }
}
