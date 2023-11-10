use tui::{
    crossterm::event::{KeyCode as Key, KeyEvent, KeyModifiers},
    unicode_width::UnicodeWidthStr,
    Canvas,
};

use crate::{
    fmt::{rtrim, Col, ColBuilder, GridBuffer},
    source::DataFrame,
    style,
    tab::{GridUI, Status},
    OnKey,
};

use self::{nav::Nav, projection::Projection, sizer::Sizer};

pub mod nav;
mod projection;
mod sizer;

enum State {
    Normal,
    Size,
    Projection,
}

pub struct Grid {
    projection: Projection,
    pub nav: Nav,
    sizer: Sizer,
    state: State,
}

impl Grid {
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
        let idx = self.nav.c_col();
        let proj_idx = self.projection.project(idx);
        match self.state {
            State::Normal => match event.code {
                Key::Char('s') => self.state = State::Size,
                Key::Char('p') => self.state = State::Projection,
                Key::Left | Key::Char('H') if shift => self.nav.win_left(),
                Key::Down | Key::Char('J') if shift => self.nav.win_down(),
                Key::Up | Key::Char('K') if shift => self.nav.win_up(),
                Key::Right | Key::Char('L') if shift => self.nav.win_right(),
                Key::Left | Key::Char('h') => self.nav.left(),
                Key::Down | Key::Char('j') => self.nav.down(),
                Key::Up | Key::Char('k') => self.nav.up(),
                Key::Right | Key::Char('l') => self.nav.right(),
                Key::Char('q') => return OnKey::Quit,
                _ => return OnKey::Pass,
            },
            State::Projection => match event.code {
                Key::Esc | Key::Char('p') => self.state = State::Normal,
                Key::Left | Key::Char('h') => {
                    self.projection.cmd(idx, projection::Cmd::Left);
                    self.nav.left()
                }
                Key::Right | Key::Char('l') => {
                    self.projection.cmd(idx, projection::Cmd::Right);
                    self.nav.right();
                }
                Key::Down | Key::Char('j') => {
                    self.projection.cmd(idx, projection::Cmd::Hide);
                    self.state = State::Normal
                }
                Key::Up | Key::Char('k') => {
                    self.projection.reset(); // TODO keep column focus
                    self.state = State::Normal
                }
                _ => {}
            },
            State::Size => match event.code {
                Key::Esc | Key::Char('s') => self.state = State::Normal,
                Key::Char('r') => {
                    self.sizer.reset();
                    self.state = State::Normal;
                }
                Key::Char('f') => {
                    self.sizer.fit_current_size();
                    self.state = State::Normal;
                }
                Key::Char(' ') => {
                    self.sizer.toggle();
                    self.state = State::Normal;
                }
                Key::Left | Key::Char('h') => {
                    self.sizer.cmd(proj_idx, sizer::Cmd::Less);
                }
                Key::Down | Key::Char('j') => {
                    self.sizer.cmd(proj_idx, sizer::Cmd::Constrain);
                    self.state = State::Normal;
                }
                Key::Up | Key::Char('k') => {
                    self.sizer.cmd(proj_idx, sizer::Cmd::Free);
                    self.state = State::Normal;
                }
                Key::Right | Key::Char('l') => {
                    self.sizer.cmd(proj_idx, sizer::Cmd::More);
                }
                _ => {}
            },
        };

        OnKey::Continue
    }

    pub fn draw(&mut self, c: &mut Canvas, buf: &mut GridBuffer, df: &dyn Frame) -> GridUI {
        let nb_col = df.nb_col();
        let nb_row = df.nb_row();
        self.projection.set_nb_cols(nb_col);
        let visible_cols = self.projection.nb_cols();

        let v_row = c.height() - 1; // header bar
        let row_off = self.nav.row_offset(nb_row, v_row);
        // Nb call necessary to print the biggest index
        let mut ids_col = df.idx_iter(buf, row_off, v_row);
        ids_col.align_right();
        // Whole canvas minus index col
        let mut remaining_width = c.width() - ids_col.budget() - 1;
        let mut cols = Vec::new();
        let mut coll_off_iter = self.nav.col_iter(visible_cols);
        // Fill canvas with columns
        while remaining_width > 0 {
            if let Some(off) = coll_off_iter.next() {
                let idx = self.projection.project(off);
                let name = df.col_name(idx);
                let col = df.col_iter(buf, idx, row_off, v_row);
                let size = self.sizer.fit(idx, col.budget(), name.width());
                let allowed = size.min(remaining_width);
                cols.push((off, name, col, allowed));
                let separator = if cols.len() == nb_col { 0 } else { 1 }; // Skip last separator
                remaining_width = remaining_width.saturating_sub(allowed + separator);
            } else {
                break;
            }
        }
        // Redistribute remaining width
        for (off, _, _, allowed) in &mut cols {
            if remaining_width == 0 {
                break;
            }
            let idx = self.projection.project(*off);
            *allowed = self.sizer.fill(idx, &mut remaining_width);
        }

        cols.sort_unstable_by_key(|(i, _, _, _)| *i);
        drop(coll_off_iter);

        // Draw headers
        {
            let line = &mut c.top();
            line.draw(
                format_args!("{:>1$} ", '#', ids_col.budget()),
                style::index().bold(),
            );

            for (off, name, _, budget) in &cols {
                let style = if *off == self.nav.c_col() {
                    style::selected().bold()
                } else {
                    style::primary().bold()
                };
                line.draw(
                    format_args!("{:<1$}", rtrim(name, buf.fmt_buf(), *budget), budget),
                    style,
                );
                line.draw("│", style::separator());
            }
        }

        // Draw rows
        for r in 0..v_row.min(nb_row - row_off) {
            let line = &mut c.top();
            line.draw(
                format_args!("{} ", ids_col.fmt(buf, r, ids_col.budget())),
                style::index(),
            );
            for (_, _, col, budget) in &cols {
                line.draw(
                    format_args!("{}", col.fmt(buf, r, *budget)),
                    style::primary(),
                );
                line.draw("│", style::separator());
            }
        }

        GridUI {
            col_name: (self.projection.nb_cols() > 0)
                .then(|| df.col_name(self.projection.project(self.nav.c_col()))),
            status: match self.state {
                State::Normal => Status::Normal,
                State::Size => Status::Size,
                State::Projection => Status::Projection,
            },
        }
    }
}

pub trait Frame {
    fn nb_col(&self) -> usize;
    fn nb_row(&self) -> usize;
    fn idx_iter(&self, buf: &mut GridBuffer, skip: usize, take: usize) -> Col;
    fn col_name(&self, idx: usize) -> String;
    fn col_iter(&self, buf: &mut GridBuffer, idx: usize, skip: usize, take: usize) -> Col;
}

impl Frame for DataFrame {
    fn nb_col(&self) -> usize {
        self.num_columns()
    }

    fn nb_row(&self) -> usize {
        self.num_rows()
    }

    fn idx_iter(&self, buf: &mut GridBuffer, skip: usize, take: usize) -> Col {
        let mut col = ColBuilder::new(buf);
        for i in skip..skip + take {
            col.add_nb(i);
        }
        col.build()
    }

    fn col_name(&self, idx: usize) -> String {
        self.schema().all_fields()[idx].name().clone()
    }

    fn col_iter(&self, buf: &mut GridBuffer, idx: usize, skip: usize, take: usize) -> Col {
        self.iter(buf, idx, skip, take)
    }
}
