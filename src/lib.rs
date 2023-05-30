use std::{borrow::Cow, io, sync::mpsc::RecvTimeoutError, time::Duration};

use bstr::{BStr, BString};
use event::event_listener;
use fmt::rtrim;
use grid::nav::Nav;
use notify::{RecommendedWatcher, Watcher};
use notify_debouncer_full::FileIdMap;
use polars::prelude::AnyValue;
use reedline::KeyModifiers;
use source::Source;
use tab::Tab;
use tui::{
    crossterm::event::{Event, KeyCode, KeyEventKind},
    unicode_width::UnicodeWidthStr,
    Canvas, Terminal,
};

mod describe;
pub mod error;
mod event;
mod fmt;
mod grid;
mod shell;
pub mod source;
mod spinner;
mod style;
mod tab;
mod utils;

pub fn run(sources: impl Iterator<Item = Source>) {
    let (receiver, watcher, orchestrator) = event_listener();
    let mut app = App::new(watcher);
    for source in sources {
        app.add_tab(Tab::open(orchestrator.clone(), source));
    }
    let mut terminal = Terminal::new(io::stdout()).unwrap();
    loop {
        terminal.draw(|c| app.draw(c)).unwrap();
        let mut event = if app.is_loading() {
            match receiver.recv_timeout(Duration::from_millis(250)) {
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
                        if let Some(tab) = self.tabs.get_mut(self.nav.c_col) {
                            if tab.on_key(&event) {
                                if let Some(path) = tab.source.path() {
                                    self.debouncer.watcher().unwatch(path).unwrap();
                                }
                                self.tabs.remove(self.nav.c_col);
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
                                        tab.set_source(Source::from_path(path).unwrap())
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
        self.tabs[self.nav.c_col].is_loading()
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
    U64(u64),
    I64(i64),
    F64(f64),
}

impl Ty<'_> {
    pub fn is_str(&self) -> bool {
        matches!(self, Ty::Str(_))
    }
}

impl<'a> From<&'a str> for Ty<'a> {
    fn from(value: &'a str) -> Self {
        Self::Str(Cow::Borrowed(value.into()))
    }
}

impl<'a> From<AnyValue<'a>> for Ty<'a> {
    fn from(value: AnyValue<'a>) -> Self {
        match value {
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
            AnyValue::Utf8(str) => Ty::Str(Cow::Borrowed(str.into())),
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
}
