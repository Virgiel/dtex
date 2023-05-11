use std::{borrow::Cow, error::Error, io, path::PathBuf, time::Duration};

use bstr::{BStr, BString};
use fmt::{rtrim, ColStat};
use nav::Nav;
use polars::prelude::{AnyValue, DataFrame};
use projection::Projection;
use sizer::{Cmd, Sizer};
use source::Source;
use tui::{
    crossterm::event::{self, Event, KeyCode, KeyModifiers},
    unicode_width::UnicodeWidthStr,
    Canvas, Terminal,
};

mod fmt;
mod nav;
mod projection;
mod sizer;
mod source;
mod style;
mod utils;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

pub enum Open {
    Polars(DataFrame),
    File(PathBuf),
}

pub fn run(source: Vec<Open>) {
    let mut app = App::new();
    for source in source {
        app.add_tab(Tab::open(Source::new(source).unwrap()));
    }
    let mut redraw = true;
    let mut terminal = Terminal::new(io::stdout()).unwrap();
    loop {
        // Check loading state before drawing to no skip completed task during drawing
        let is_loading = false; //app.is_loading();
        if redraw {
            terminal.draw(|c| app.draw(c)).unwrap();
            redraw = false;
        }
        if event::poll(Duration::from_millis(250)).unwrap() {
            loop {
                if app.on_event(event::read().unwrap()) {
                    return;
                }
                // Ingest more event before drawing if we can
                if !event::poll(Duration::from_millis(0)).unwrap() {
                    break;
                }
            }
            redraw = true;
        }
        if is_loading {
            redraw = true;
        }
    }
}

struct App {
    tabs: Vec<Tab>,
    nav: Nav,
}
impl App {
    pub fn new() -> Self {
        Self {
            tabs: vec![],
            nav: Nav::new(),
        }
    }

    pub fn add_tab(&mut self, tab: Tab) {
        self.tabs.push(tab);
    }

    pub fn draw(&mut self, c: &mut Canvas) {
        if self.tabs.len() == 1 {
            self.tabs[0].draw(c)
        } else if !self.tabs.is_empty() {
            let mut coll_off_iter = self.nav.col_iter(self.tabs.len());
            let mut cols = Vec::new();
            // Fill canvas with tabs name
            let mut remaining_width = c.width();
            while remaining_width > cols.len() {
                if let Some(off) = coll_off_iter.next() {
                    let tab = &self.tabs[off];
                    remaining_width = remaining_width.saturating_sub(tab.name.width());
                    cols.push((off, &tab.name));
                } else {
                    break;
                }
            }
            cols.sort_unstable_by_key(|(i, _)| *i);
            drop(coll_off_iter);
            // Draw headers
            let mut fmt_buf = String::new();
            let mut line = c.top();
            for (off, name) in &cols {
                let style = if *off == self.nav.c_col {
                    style::tab_selected()
                } else {
                    style::tab()
                };

                line.draw(
                    format_args!(
                        "{:}",
                        rtrim(name, &mut fmt_buf, name.width().min(line.width())),
                    ),
                    style,
                );
                line.draw(" ", style::separator());
            }
            self.tabs[self.nav.c_col].draw(c)
        } else {
            c.top().draw("Empty", style::primary());
        }
    }

    pub fn on_event(&mut self, event: Event) -> bool {
        let mut pass = true;
        if let Event::Key(event) = event {
            if KeyCode::Tab == event.code {
                self.nav.right();
                pass = false;
            } else if KeyCode::BackTab == event.code {
                self.nav.left();
                pass = false;
            }
        }
        if pass {
            if self.tabs.len() == 1 {
                if self.tabs[0].on_event(event) {
                    self.tabs.clear();
                }
            } else if let Some(tab) = self.tabs.get_mut(self.nav.c_col) {
                if tab.on_event(event) {
                    self.tabs.remove(self.nav.c_col);
                }
            }
        }
        self.tabs.is_empty()
    }
}

enum State {
    Explore,
    Size,
    Projection,
}

struct Tab {
    name: String,
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
                let (fields, mut stat) = col.phys_iter().skip(row_off).take(v_row).fold(
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

        let fmt_buf = &mut String::with_capacity(256);

        // Draw headers
        {
            let line = &mut c.top();
            line.draw(
                format_args!("{:>1$} ", '#', id_len),
                style::secondary().bold(),
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
                format_args!("{:>1$} ", r + self.nav.o_row + 1, id_len),
                style::secondary(),
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
                            self.sizer.cmd(col_idx, Cmd::Less);
                            exit_size = false
                        }
                        KeyCode::Down | KeyCode::Char('j') => {
                            self.sizer.cmd(col_idx, Cmd::Constrain)
                        }
                        KeyCode::Up | KeyCode::Char('k') => self.sizer.cmd(col_idx, Cmd::Free),
                        KeyCode::Right | KeyCode::Char('l') => {
                            self.sizer.cmd(col_idx, Cmd::More);
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

pub enum Ty<'a> {
    Null,
    Bool(bool),
    Str(Cow<'a, BStr>),
    U64(u64),
    I64(i64),
    F64(f64),
}

impl Ty<'_> {
    pub fn is_str(&self) -> bool {
        matches!(self, Ty::Str(_))
    }
}

pub fn to_ty(data: AnyValue) -> Ty {
    match data {
        AnyValue::Null => Ty::Null,
        AnyValue::Boolean(bool) => Ty::Bool(bool),
        AnyValue::UInt8(nb) => Ty::U64(nb as u64),
        AnyValue::UInt16(nb) => Ty::U64(nb as u64),
        AnyValue::UInt32(nb) => Ty::U64(nb as u64),
        AnyValue::UInt64(nb) => Ty::U64(nb),
        AnyValue::Int8(nb) => Ty::I64(nb as i64),
        AnyValue::Int16(nb) => Ty::I64(nb as i64),
        AnyValue::Int32(nb) => Ty::I64(nb as i64),
        AnyValue::Int64(nb) => Ty::I64(nb),
        AnyValue::Float32(nb) => Ty::F64(nb as f64),
        AnyValue::Float64(nb) => Ty::F64(nb),
        AnyValue::Utf8(str) => Ty::Str(Cow::Borrowed(BStr::new(str))),
        AnyValue::Utf8Owned(str) => Ty::Str(Cow::Owned(BString::new(str.as_bytes().to_vec()))),
        AnyValue::Binary(bs) => Ty::Str(Cow::Borrowed(BStr::new(bs))),
        AnyValue::BinaryOwned(bs) => Ty::Str(Cow::Owned(BString::new(bs))),
        AnyValue::Date(_) => todo!(),
        AnyValue::Datetime(_, _, _) => todo!(),
        AnyValue::Duration(_, _) => todo!(),
        AnyValue::Time(_) => todo!(),
        AnyValue::List(_) => todo!(),
        AnyValue::Struct(_, _, _) => todo!(),
        AnyValue::StructOwned(_) => todo!(),
    }
}
