use std::{borrow::Cow, io, path::PathBuf, time::Duration};

use bstr::{BStr, BString};
use event::event_listener;
use fmt::rtrim;
use nav::Nav;
use notify::{RecommendedWatcher, Watcher};
use polars::prelude::{AnyValue, DataFrame};
use reedline::KeyModifiers;
use source::Source;
use tab::Tab;
use tui::{
    crossterm::event::{Event, KeyCode},
    unicode_width::UnicodeWidthStr,
    Canvas, Terminal,
};

mod error;
mod event;
mod fmt;
mod nav;
mod projection;
mod sizer;
mod source;
mod style;
mod tab;
mod utils;

pub enum Open {
    Polars(DataFrame),
    File(PathBuf),
}

pub fn run(source: Vec<Open>) {
    let (receiver, watcher, orchestrator) = event_listener();
    let mut app = App::new(watcher);
    for source in source {
        app.add_tab(Tab::open(orchestrator.clone(), Source::new(source)));
    }
    let mut terminal = Terminal::new(io::stdout()).unwrap();
    loop {
        terminal.draw(|c| app.draw(c)).unwrap();
        let mut event = if app.is_loading() {
            receiver.recv_timeout(Duration::from_millis(2500)).unwrap()
        } else {
            receiver.recv().unwrap()
        };
        loop {
            if app.on_event(event) {
                return;
            }
            // Ingest more event before drawing if we can
            if let Ok(more) = receiver.try_recv() {
                event = more;
            } else {
                break;
            }
        }
    }
}

struct App {
    tabs: Vec<Tab>,
    nav: Nav,
    watcher: RecommendedWatcher,
}
impl App {
    pub fn new(watcher: RecommendedWatcher) -> Self {
        Self {
            tabs: vec![],
            nav: Nav::new(),
            watcher,
        }
    }

    pub fn add_tab(&mut self, tab: Tab) {
        if let Some(path) = tab.loader.source.path() {
            self.watcher
                .watch(path, notify::RecursiveMode::NonRecursive)
                .unwrap();
        }
        self.tabs.push(tab);
    }

    pub fn draw(&mut self, c: &mut Canvas) {
        let mut coll_off_iter = self.nav.col_iter(self.tabs.len());
        if self.tabs.len() == 1 {
            self.tabs[0].draw(c)
        } else if !self.tabs.is_empty() {
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

    pub fn on_event(&mut self, event: event::Event) -> bool {
        match event {
            event::Event::Term(event) => {
                let mut pass = true;
                if let Event::Key(event) = event {
                    match event.code {
                        KeyCode::Tab => {
                            self.nav.right_roll();
                            pass = false;
                        }
                        KeyCode::BackTab => {
                            self.nav.left_roll();
                            pass = false;
                        }
                        KeyCode::Char('c' | 'd')
                            if event.modifiers.contains(KeyModifiers::CONTROL) =>
                        {
                            return true;
                        }
                        _ => {}
                    }
                }
                if pass {
                    if let Some(tab) = self.tabs.get_mut(self.nav.c_col) {
                        if tab.on_event(&event) {
                            if let Some(path) = tab.loader.source.path() {
                                self.watcher.unwatch(path).unwrap();
                            }
                            self.tabs.remove(self.nav.c_col);
                        }
                    }
                }
            }
            event::Event::File(e) => {
                // TODO handle more event
                // TODO perf with many tabs
                if e.kind.is_modify() {
                    for path in e.paths {
                        if let Some(tab) = self
                            .tabs
                            .iter_mut()
                            .find(|t| t.loader.source.path() == Some(path.as_path()))
                        {
                            tab.set_source(Source::new(Open::File(path)))
                        }
                    }
                }
            }
            event::Event::Task => {}
        }

        self.tabs.is_empty()
    }

    fn is_loading(&self) -> bool {
        self.tabs[self.nav.c_col].is_loading()
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
