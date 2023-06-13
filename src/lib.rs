use std::{
    borrow::Cow, fmt::Write, io, str::FromStr, sync::mpsc::RecvTimeoutError, time::Duration,
};

use arrow::{
    array::{ArrayRef, AsArray, Decimal128Array},
    datatypes::{
        DataType, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
        UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
};
use bstr::{BStr, BString};
use event::event_listener;
use fmt::rtrim;
use grid::nav::Nav;
use notify::{RecommendedWatcher, Watcher};
use notify_debouncer_full::FileIdMap;
use reedline::KeyModifiers;
use tab::Tab;
use tui::{
    crossterm::event::{Event, KeyCode, KeyEventKind},
    unicode_width::UnicodeWidthStr,
    Canvas, Terminal,
};

pub use arrow;
pub use error::{Result, StrError};
pub use source::{DataFrame, Source};

mod describe;
mod duckdb;
mod error;
mod event;
mod fmt;
mod grid;
mod navigator;
mod shell;
mod source;
mod spinner;
mod style;
mod tab;
mod task;
mod utils;

pub fn run(sources: impl Iterator<Item = Source>) {
    let (receiver, watcher, runner) = event_listener();
    let mut app = App::new(watcher);
    for source in sources {
        app.add_tab(Tab::open(runner.clone(), source));
    }
    if app.tabs.is_empty() {
        app.add_tab(Tab::open(runner, Source::empty()));
    }
    let mut terminal = Terminal::new(io::stdout()).unwrap();
    loop {
        terminal.draw(|c| app.draw(c)).unwrap();
        let mut event = if app.is_loading() {
            match receiver.recv_timeout(Duration::from_millis(100)) {
                Ok(e) => Some(e),
                Err(err) => match err {
                    RecvTimeoutError::Timeout => None,
                    RecvTimeoutError::Disconnected => {
                        panic!("{err}")
                    }
                },
            }
        } else {
            Some(receiver.recv().unwrap())
        };
        while let Some(e) = event {
            if app.on_event(e) {
                return;
            }
            // Ingest more event before drawing if we can
            event = receiver.try_recv().ok()
        }
    }
}

struct App {
    tabs: Vec<Tab>,
    nav: Nav,
    debouncer: notify_debouncer_full::Debouncer<RecommendedWatcher, FileIdMap>,
}
impl App {
    pub fn new(debouncer: notify_debouncer_full::Debouncer<RecommendedWatcher, FileIdMap>) -> Self {
        Self {
            tabs: vec![],
            nav: Nav::new(),
            debouncer,
        }
    }

    pub fn add_tab(&mut self, tab: Tab) {
        if let Some(path) = tab.source.path() {
            self.debouncer
                .watcher()
                .watch(path, notify::RecursiveMode::NonRecursive)
                .ok();
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
                    remaining_width = remaining_width.saturating_sub(tab.source.name().width());
                    cols.push((off, tab.source.name()));
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
                let style = if *off == self.nav.c_col() {
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
            self.tabs[self.nav.c_col()].draw(c)
        }
    }

    pub fn on_event(&mut self, event: event::Event) -> bool {
        match event {
            event::Event::Term(event) => {
                if let Event::Key(event) = event {
                    if event.kind != KeyEventKind::Press {
                        return false;
                    }
                    let mut pass = true;
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

                    if pass {
                        if let Some(tab) = self.tabs.get_mut(self.nav.c_col()) {
                            if tab.on_key(&event) {
                                if let Some(path) = tab.source.path() {
                                    self.debouncer.watcher().unwatch(path).unwrap();
                                }
                                self.tabs.remove(self.nav.c_col());
                            }
                        }
                    }
                }
            }
            event::Event::FS(e) => {
                match e {
                    Ok(events) => {
                        for e in events {
                            // TODO handle more event
                            // TODO perf with many tabs
                            if e.kind.is_modify() {
                                for path in e.paths {
                                    if let Some(tab) = self
                                        .tabs
                                        .iter_mut()
                                        .find(|t| t.source.path() == Some(path.as_path()))
                                    {
                                        tab.set_source(Source::from_path(path))
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => todo!(),
                }
            }
            event::Event::Task => {}
        }
        self.tabs.is_empty()
    }

    fn is_loading(&self) -> bool {
        self.tabs[self.nav.c_col()].is_loading()
    }
}

#[derive(PartialEq, Eq)]
pub enum OnKey {
    // Exit current state
    Quit,
    // Pass the event down
    Pass,
    Continue,
}

pub enum Ty<'a> {
    Null,
    Bool(bool),
    Str(Cow<'a, BStr>),
    U(u64),
    I(i128),
    F(f64),
}

impl Ty<'_> {
    pub fn is_str(&self) -> bool {
        matches!(self, Ty::Str(_))
    }

    pub fn fmt(&self, out: &mut String) {
        match self {
            Ty::Null => {}
            Ty::Bool(it) => writeln!(out, "{it}").unwrap(),
            Ty::Str(it) => writeln!(out, "{it}").unwrap(),
            Ty::U(it) => writeln!(out, "{it}").unwrap(),
            Ty::I(it) => writeln!(out, "{it}").unwrap(),
            Ty::F(it) => writeln!(out, "{it}").unwrap(),
        }
    }
}

impl<'a> From<&'a str> for Ty<'a> {
    fn from(value: &'a str) -> Self {
        Self::Str(Cow::Borrowed(value.into()))
    }
}

macro_rules! iter {
    ($m:expr, $map:expr) => {
        Box::new($m.iter().map(|b| b.map($map).unwrap_or(Ty::Null)))
    };
    ($array:expr, $m:ty, $map:expr) => {
        iter!($array.as_primitive::<$m>(), $map)
    };
    ($array:expr, $m:ty, $ty:ident, $native:ty) => {
        iter!($array, $m, |v| Ty::$ty(v as $native))
    };
}

fn fmt_list(array: ArrayRef) -> Ty<'static> {
    let mut out = "[".into();
    for ty in array_to_iter(&array) {
        ty.fmt(&mut out);
        out.push(',');
    }
    if out.len() > 1 {
        out.pop();
    }
    out.push_str("]");
    Ty::Str(Cow::Owned(BString::from(out)))
}

pub fn array_to_iter(array: &ArrayRef) -> Box<dyn Iterator<Item = Ty<'_>> + '_> {
    #[allow(clippy::unnecessary_cast)]
    match array.data_type() {
        DataType::Null => Box::new((0..array.len()).map(|_| Ty::Null)),
        DataType::Boolean => iter!(array.as_boolean(), Ty::Bool),
        DataType::Int8 => iter!(array, Int8Type, I, i128),
        DataType::Int16 => iter!(array, Int16Type, I, i128),
        DataType::Int32 => iter!(array, Int32Type, I, i128),
        DataType::Int64 => iter!(array, Int64Type, I, i128),
        DataType::UInt8 => iter!(array, UInt8Type, U, u64),
        DataType::UInt16 => iter!(array, UInt16Type, U, u64),
        DataType::UInt32 => iter!(array, UInt32Type, U, u64),
        DataType::UInt64 => iter!(array, UInt64Type, U, u64),
        DataType::Float16 => {
            iter!(array, Float16Type, |v| Ty::F(v.to_f64()))
        }
        DataType::Float32 => iter!(array, Float32Type, F, f64),
        DataType::Float64 => iter!(array, Float64Type, F, f64),
        DataType::Utf8 => {
            iter!(array.as_string::<i32>(), |v| Ty::Str(Cow::Borrowed(
                v.into()
            )))
        }
        DataType::LargeUtf8 => iter!(array.as_string::<i64>(), |v| Ty::Str(Cow::Borrowed(
            v.into()
        ))),
        DataType::Decimal128(_, _) => {
            let array: &Decimal128Array = array.as_any().downcast_ref().unwrap();
            iter!(array, |v| Ty::I(v as i128))
        }
        DataType::List(_) => {
            iter!(array.as_list::<i32>(), |v| fmt_list(v))
        }
        ty => unimplemented!("{ty}"),
    }
}
