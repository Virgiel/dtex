use std::{io, ops::Range, sync::mpsc::RecvTimeoutError, time::Duration};

use arrow::{
    array::{ArrayRef, AsArray, Decimal128Array},
    datatypes::{
        DataType, Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
        UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
    util::display::{ArrayFormatter, FormatOptions},
};
use event::event_listener;
use fmt::{rtrim, ColBuilder, GridBuffer};
use grid::nav::Nav;
use notify::{RecommendedWatcher, Watcher};
use notify_debouncer_full::FileIdMap;
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
mod view;

pub fn run(sources: impl Iterator<Item = Source>) {
    let (receiver, watcher, runner) = event_listener();
    let mut app = App::new(watcher);
    for source in sources {
        app.add_tab(Tab::open(runner.clone(), source));
    }
    if app.tabs.is_empty() {
        app.add_tab(Tab::open(runner, Source::empty("#".into())));
    }
    let mut terminal = Terminal::new(io::stdout()).unwrap();
    loop {
        let mut is_loading = false;
        terminal
            .draw(|c| {
                is_loading = app.draw(c);
            })
            .unwrap();
        let mut event = if is_loading {
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
    buf: GridBuffer,
}
impl App {
    pub fn new(debouncer: notify_debouncer_full::Debouncer<RecommendedWatcher, FileIdMap>) -> Self {
        Self {
            tabs: vec![],
            nav: Nav::new(),
            buf: GridBuffer::new(),
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

    pub fn draw(&mut self, c: &mut Canvas) -> bool {
        self.buf.new_frame(c.width());
        let mut coll_off_iter = self.nav.col_iter(self.tabs.len());
        if self.tabs.len() == 1 {
            self.tabs[0].draw(c, &mut self.buf)
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
            self.tabs[self.nav.c_col()].draw(c, &mut self.buf)
        } else {
            false
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
                            if event
                                .modifiers
                                .contains(tui::crossterm::event::KeyModifiers::CONTROL) =>
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
                                for path in &e.paths {
                                    if let Some(_tab) = self
                                        .tabs
                                        .iter_mut()
                                        .find(|t| t.source.path() == Some(path.as_path()))
                                    {

                                        //tab.set_source(Source::from_path(path)) TODO
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
}

#[derive(PartialEq, Eq)]
pub enum OnKey {
    // Exit current state
    Quit,
    // Pass the event down
    Pass,
    Continue,
}

pub enum Cell<'a> {
    Null,
    Bool(bool),
    Str(&'a str),
    Dsp(Range<usize>),
    Nb {
        range: Range<usize>,
        lhs: usize,
        rhs: usize,
    },
}

macro_rules! iter {
    ($array:expr, $col:expr, $skip:expr, $take:expr, $map:ident) => {
        for v in $array.into_iter().skip($skip).take($take) {
            match v {
                Some(v) => $col.$map(v),
                None => $col.add_null(),
            }
        }
    };
}
macro_rules! prim {
    ($array:expr, $col:expr, $skip:expr, $take:expr, $m:ty) => {
        iter!($array.as_primitive::<$m>(), $col, $skip, $take, add_nb)
    };
}

pub fn array_to_iter<'a>(
    array: &'a ArrayRef,
    bd: &mut ColBuilder<'a, '_>,
    skip: usize,
    take: usize,
) {
    #[allow(clippy::unnecessary_cast)]
    match array.data_type() {
        DataType::Null => iter!(
            (0..array.len()).map(|_| None::<bool>),
            bd,
            skip,
            take,
            add_bool
        ),
        DataType::Boolean => {
            iter!(array.as_boolean(), bd, skip, take, add_bool)
        }
        DataType::Int8 => prim!(array, bd, skip, take, Int8Type),
        DataType::Int16 => prim!(array, bd, skip, take, Int16Type),
        DataType::Int32 => prim!(array, bd, skip, take, Int32Type),
        DataType::Int64 => prim!(array, bd, skip, take, Int64Type),
        DataType::UInt8 => prim!(array, bd, skip, take, UInt8Type),
        DataType::UInt16 => prim!(array, bd, skip, take, UInt16Type),
        DataType::UInt32 => prim!(array, bd, skip, take, UInt32Type),
        DataType::UInt64 => prim!(array, bd, skip, take, UInt64Type),
        DataType::Float16 => {
            let array = array
                .as_primitive::<Float16Type>()
                .into_iter()
                .map(|f| f.map(|f| f.to_f32()));
            iter!(array, bd, skip, take, add_nb)
        }
        DataType::Float32 => prim!(array, bd, skip, take, Float32Type),
        DataType::Float64 => prim!(array, bd, skip, take, Float64Type),
        DataType::Utf8 => {
            iter!(array.as_string::<i32>(), bd, skip, take, add_str)
        }
        DataType::LargeUtf8 => iter!(array.as_string::<i64>(), bd, skip, take, add_str),
        DataType::Decimal128(_, _) => {
            let array: &Decimal128Array = array.as_any().downcast_ref().unwrap();
            iter!(array, bd, skip, take, add_nb)
        }
        _ => {
            let fmt =
                ArrayFormatter::try_new(array, &FormatOptions::default().with_display_error(false))
                    .unwrap();
            for i in (0..array.len()).skip(skip).take(take) {
                bd.add_dsp(fmt.value(i));
            }
        }
    }
}
