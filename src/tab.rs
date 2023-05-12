use polars::prelude::DataFrame;
use reedline::{KeyCode, KeyModifiers};
use tui::{crossterm::event::Event, Canvas, unicode_width::UnicodeWidthStr};

use crate::{
    fmt::{self, rtrim, ColStat},
    nav::Nav,
    projection::{self, Projection},
    sizer::{self, Sizer},
    source::Source,
    style, to_ty,
};

enum State {
    Explore,
    Size,
    Projection,
}

pub struct Tab {
    pub name: String,
    source: Source,
    display_path: Option<String>,
    df: DataFrame,
    nav: Nav,
    sizer: Sizer,
    projection: Projection,
    state: State,
}

impl Tab {
    pub fn open(source: Source) -> Self {
        // TODO in background
        let df = source.preload().unwrap();
        Self {
            display_path: source.display_path(),
            name: source.name(),
            source,
            df,
            sizer: Sizer::new(),
            projection: Projection::new(),
            nav: Nav::new(),
            state: State::Explore,
        }
    }

    pub fn draw(&mut self, c: &mut Canvas) {
        c.bg(style::BG_1);
        let nb_col = self.df.get_columns().len();
        let nb_row = self.df.height();
        self.projection.set_nb_cols(nb_col);
        let visible_cols = self.projection.nb_cols();

        let v_row = c.height() - 2; // header bar + status bar
        let row_off = self.nav.row_offset(nb_row, v_row);
        let mut coll_off_iter = self.nav.col_iter(visible_cols);
        // Nb call necessary to print the biggest index
        let id_len = ((row_off + v_row) as f32).log10() as usize + 1;
        // Whole canvas minus index col
        let mut remaining_width = c.width() - id_len + 1;
        let mut cols = Vec::new();
        // Fill canvas with columns
        while remaining_width > cols.len() {
            if let Some(off) = coll_off_iter.next() {
                let idx = self.projection.project(off);
                let col = &self.df.get_columns()[idx];
                let (fields, stat) = col.phys_iter().skip(row_off).take(v_row).fold(
                    (Vec::new(), ColStat::new()),
                    |(mut vec, mut stat), value| {
                        let ty = to_ty(value);
                        stat.add(&ty);
                        vec.push(ty);
                        (vec, stat)
                    },
                );
                let size = self.sizer.size(idx, stat.budget(), col.name().width());
                let allowed = size.min(remaining_width - cols.len());
                remaining_width = remaining_width.saturating_sub(allowed);
                cols.push((off, col.name(), fields, stat, allowed));
            } else {
                break;
            }
        }
        cols.sort_unstable_by_key(|(i, _, _, _, _)| *i);
        drop(coll_off_iter);

        // Draw status bar
        let mut l = c.btm();
        let (status, style) = match self.state {
            State::Explore => ("  EX  ", style::state_default()),
            State::Size => (" SIZE ", style::state_action()),
            State::Projection => (" MOVE ", style::state_alternate()),
        };
        l.draw(status, style);
        l.draw(" ", style::primary());

        let progress = ((self.nav.c_row + 1) * 100) / nb_row.max(1);
        l.rdraw(format_args!(" {progress:>3}%"), style::primary());

        if visible_cols > 0 {
            let name = self.df.get_columns()[self.nav.c_col].name();
            l.rdraw(name, style::primary());
            l.rdraw(" ", style::primary());
        }
        if let Some(path) = &self.display_path {
            l.draw(path, style::progress());
        }

        let fmt_buf = &mut String::with_capacity(256);
        // Draw headers
        {
            let line = &mut c.top();
            line.draw(format_args!("{:>1$} ", '#', id_len), style::index().bold());

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
        c.bg(style::BG_DIM);
        for r in 0..v_row.min(nb_row - row_off) {
            let style = if r == self.nav.c_row - self.nav.o_row {
                style::selected()
            } else {
                style::primary()
            };
            let line = &mut c.top();
            line.draw(
                format_args!("{:>1$} ", r + self.nav.o_row + 1, id_len),
                style::index(),
            );
            for (_, _, fields, stat, budget) in &cols {
                let ty = &fields[r];
                line.draw(
                    format_args!("{}", fmt::fmt_field(fmt_buf, &ty, stat, *budget)),
                    style,
                );
                line.draw("│", style::separator());
            }
        }
    }

    pub fn on_event(&mut self, event: Event) -> bool {
        if let Event::Key(event) = event {
            let shift = event.modifiers.contains(KeyModifiers::SHIFT);
            let off = self.nav.c_col;
            match self.state {
                State::Explore => match event.code {
                    KeyCode::Char('q') => return true,
                    KeyCode::Char('s') => self.state = State::Size,
                    KeyCode::Char('m') => self.state = State::Projection,
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
                    _ => {}
                },
                State::Projection => match event.code {
                    KeyCode::Char('q') | KeyCode::Esc => self.state = State::Explore,
                    KeyCode::Left | KeyCode::Char('H') if shift => {
                        self.projection.cmd(off, projection::Cmd::Left);
                        self.nav.left()
                    }
                    KeyCode::Down | KeyCode::Char('J') if shift => {
                        self.projection.cmd(off, projection::Cmd::Hide);
                    }
                    KeyCode::Up | KeyCode::Char('K') if shift => self.projection.reset(), // TODO stay on focus column
                    KeyCode::Right | KeyCode::Char('L') if shift => {
                        self.projection.cmd(off, projection::Cmd::Right);
                        self.nav.right();
                    }
                    KeyCode::Left | KeyCode::Char('h') => self.nav.left(),
                    KeyCode::Down | KeyCode::Char('j') => self.nav.down(),
                    KeyCode::Up | KeyCode::Char('k') => self.nav.up(),
                    KeyCode::Right | KeyCode::Char('l') => self.nav.right(),
                    _ => {}
                },
                State::Size => {
                    let col_idx = self.nav.c_col;
                    let mut exit_size = true;
                    match event.code {
                        KeyCode::Esc => {}
                        KeyCode::Char('r') => self.sizer.reset(),
                        KeyCode::Char('f') => self.sizer.fit(),
                        KeyCode::Char(' ') => self.sizer.toggle(),
                        KeyCode::Left | KeyCode::Char('h') => {
                            self.sizer.cmd(col_idx, sizer::Cmd::Less);
                            exit_size = false
                        }
                        KeyCode::Down | KeyCode::Char('j') => {
                            self.sizer.cmd(col_idx, sizer::Cmd::Constrain)
                        }
                        KeyCode::Up | KeyCode::Char('k') => {
                            self.sizer.cmd(col_idx, sizer::Cmd::Free)
                        }
                        KeyCode::Right | KeyCode::Char('l') => {
                            self.sizer.cmd(col_idx, sizer::Cmd::More);
                            exit_size = false;
                        }
                        _ => exit_size = false,
                    };
                    if exit_size {
                        self.state = State::Explore
                    }
                }
            }
        }
        false
    }
}
