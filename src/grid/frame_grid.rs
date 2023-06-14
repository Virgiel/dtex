use reedline::{KeyCode, KeyModifiers};
use tui::{crossterm::event::KeyEvent, unicode_width::UnicodeWidthStr, Canvas};

use crate::{
    fmt::rtrim,
    style,
    tab::{GridUI, Status},
    OnKey,
};

use super::{
    nav::Nav,
    projection::{self, Projection},
    sizer::{self, Sizer},
    Frame,
};

enum State {
    Normal,
    Size,
    Projection,
}

pub struct FrameGrid {
    projection: Projection,
    pub nav: Nav,
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
        let idx = self.nav.c_col();
        match self.state {
            State::Normal => match event.code {
                KeyCode::Char('s') => {
                    self.state = State::Size;
                }
                KeyCode::Char('p') => {
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
        let nb_col = df.nb_col();
        let nb_row = df.nb_row();
        self.projection.set_nb_cols(nb_col);
        let visible_cols = self.projection.nb_cols();

        let v_row = c.height() - 1; // header bar
        let row_off = self.nav.row_offset(nb_row, v_row);
        // Nb call necessary to print the biggest index
        let mut ids_col = df.idx_iter(row_off, v_row);
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
                let col = df.col_iter(idx, row_off, v_row);
                let size = self.sizer.size(idx, col.budget(), name.width());
                let allowed = size.min(remaining_width);
                remaining_width = remaining_width.saturating_sub(allowed + 1); // +1 for the separator
                cols.push((off, name, col, allowed));
            } else {
                break;
            }
        }
        cols.sort_unstable_by_key(|(i, _, _, _)| *i);
        drop(coll_off_iter);

        let fmt_buf = &mut String::with_capacity(256);
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
                    format_args!("{:<1$}", rtrim(name, fmt_buf, *budget), budget),
                    style,
                );
                line.draw("│", style::separator());
            }
        }

        // Draw rows
        for r in 0..v_row.min(nb_row - row_off) {
            let current = r == self.nav.c_row() - row_off;
            let style = if current {
                style::selected()
            } else {
                style::primary()
            };
            let line = &mut c.top();
            line.draw(
                format_args!("{} ", ids_col.fmt(fmt_buf, r, ids_col.budget())),
                if current {
                    style::index().bold()
                } else {
                    style::index()
                },
            );
            for (_, _, col, budget) in &cols {
                line.draw(format_args!("{}", col.fmt(fmt_buf, r, *budget)), style);
                line.draw("│", style::separator());
            }
        }

        GridUI {
            col_name: (self.projection.nb_cols() > 0).then(|| df.col_name(self.nav.c_col())),
            progress: ((self.nav.c_row() + 1) * 100) / nb_row.max(1),
            streaming: true,
            status: match self.state {
                State::Normal => Status::Normal,
                State::Size => Status::Size,
                State::Projection => Status::Projection,
            },
        }
    }
}
