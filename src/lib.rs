use std::{borrow::Cow, error::Error, io, path::PathBuf, time::Duration};

use bstr::{BStr, BString};
use fmt::rtrim;
use nav::Nav;
use polars::prelude::{AnyValue, DataFrame};
use source::Source;
use tab::Tab;
use tui::{
    crossterm::event::{self, Event, KeyCode},
    Canvas, Terminal, unicode_width::UnicodeWidthStr,
};

mod fmt;
mod nav;
mod projection;
mod sizer;
mod source;
mod style;
mod tab;
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
                self.nav.right_roll();
                pass = false;
            } else if KeyCode::BackTab == event.code {
                self.nav.left_roll();
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
