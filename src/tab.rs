use std::sync::Arc;

use reedline::KeyCode;
use tui::{crossterm::event::KeyEvent, Canvas};

use crate::{
    describe::Describer,
    grid::Grid,
    navigator::Navigator,
    shell::Shell,
    source::{FrameSource, Loader, Source},
    spinner::Spinner,
    style,
    task::Runner,
    DataFrame, OnKey,
};

enum State {
    Normal,
    Shell,
    Nav(Navigator),
}

enum View {
    Normal,
    Description { descr: Describer, grid: Grid },
}

pub struct Tab {
    pub source: Arc<Source>,
    view: View,
    frame: FrameSource,
    runner: Runner,
    loader: Loader,
    error: String,
    grid: Grid,
    shell: Shell,
    state: State,
    spinner: Spinner,
}

impl Tab {
    pub fn open(runner: Runner, source: Source) -> Self {
        let source = Arc::new(source);
        Self {
            grid: Grid::new(),
            state: State::Normal,
            view: View::Normal,
            loader: Loader::load(source.clone(), &runner),
            shell: Shell::new(source.sql()),
            spinner: Spinner::new(),
            frame: FrameSource::empty(),
            error: String::new(),
            runner,
            source,
        }
    }

    pub fn set_source(&mut self, source: Source) {
        self.source = Arc::new(source);
        self.loader = Loader::load(self.source.clone(), &self.runner);

        // Refresh current description
        if let View::Description {
            descr: describer, ..
        } = &mut self.view
        {
            *describer = Describer::describe(self.source.clone(), &self.runner);
        }
    }

    pub fn draw(&mut self, c: &mut Canvas) {
        // Tick
        match self.loader.tick() {
            Ok(Some(new)) => self.frame = new,
            Ok(None) => {}
            Err(e) => self.error = format!("loader: {}", e.0),
        }
        if let View::Description {
            descr: describer, ..
        } = &mut self.view
        {
            describer.tick();
        }
        self.frame.goal(self.grid.nav.goal().saturating_add(1));
        self.frame.tick();

        // Draw

        let status_line = c.reserve_btm(1);

        match &mut self.state {
            State::Normal => {}
            State::Shell => self.shell.draw(c),
            State::Nav(nav) => nav.draw(c),
        }

        // Draw error bar
        if !self.error.is_empty() {
            let mut l = c.btm();
            l.draw(&self.error, style::error());
        }

        // Draw grid
        let GridUI {
            col_name,
            progress,
            status,
        } = match &mut self.view {
            View::Normal => self.grid.draw(c, self.frame.df()),
            View::Description {
                descr: describer,
                grid,
            } => match describer.df() {
                None => self.grid.draw(c, self.frame.df()),
                Some(Ok(df)) => grid.draw(c, df),
                Some(Err(err)) => {
                    let mut l = c.btm();
                    l.draw(err.0, style::error());
                    grid.draw(c, &DataFrame::empty())
                }
            },
        };

        let mut l = c.consume(status_line).btm();
        let (status, style) = match status {
            Status::Normal => match self.state {
                State::Normal => match self.view {
                    View::Normal => (" DTEX ", style::state_default()),
                    View::Description { .. } => (" DESC ", style::state_other()),
                },
                State::Shell => (" $SQL ", style::state_action()),
                State::Nav(_) => (" GOTO ", style::state_action()),
            },
            Status::Size => (" SIZE ", style::state_action()),
            Status::Projection => (" PROJ ", style::state_alternate()),
        };
        l.draw(status, style);
        l.draw(" ", style::primary());
        let mut task_progress = false;
        if let Some((task, progress)) = self.is_loading() {
            if let Some(c) = self.spinner.state(true) {
                l.rdraw(format_args!("{c}"), style::progress());
                if progress > 0. {
                    l.rdraw(format_args!(" {progress:>2.0}%"), style::progress());
                }
                l.rdraw(format_args!(" {task}"), style::progress());
                task_progress = true;
            }
        } else {
            self.spinner.state(false);
        }
        if !task_progress {
            if self.frame.is_streaming() {
                l.rdraw(format_args!(" ~"), style::primary());
            } else {
                l.rdraw(format_args!(" {progress:>3}%"), style::primary());
            }
        }

        if let Some(name) = col_name {
            l.rdraw(name, style::primary());
            l.rdraw(" ", style::primary());
        }
        if let Some(path) = &self.source.display_path() {
            l.draw(path, style::progress());
        }
    }

    pub fn on_key(&mut self, event: &KeyEvent) -> bool {
        let describing = matches!(self.view, View::Description { .. });
        match &mut self.state {
            State::Normal => match (self.grid().on_key(event), event.code) {
                (OnKey::Pass, KeyCode::Char('$')) => self.state = State::Shell,
                (OnKey::Pass, code) if Navigator::activate(&code) => {
                    self.state = State::Nav(Navigator::new(self.grid().nav.clone()));
                    return self.on_key(event);
                }
                (OnKey::Pass, KeyCode::Esc) if describing => self.view = View::Normal,
                (OnKey::Pass, KeyCode::Char('d')) if !describing => {
                    self.view = View::Description {
                        descr: Describer::describe(self.source.clone(), &self.runner),
                        grid: Grid::new(),
                    }
                }
                (OnKey::Quit, _) if !describing => return true,
                _ => {}
            },
            State::Shell => {
                let (result, sql) = self.shell.on_key(event);
                if let Some(sql) = sql {
                    let source = Source::from_sql(&sql, Some(self.source.clone()));
                    self.set_source(source);
                }
                if OnKey::Quit == result {
                    self.state = State::Normal
                }
            }
            State::Nav(nav) => match nav.on_key(event.code) {
                Ok(nav) => self.grid().nav = nav,
                Err(nav) => {
                    self.grid().nav = nav;
                    self.state = State::Normal
                }
            },
        }
        false
    }

    pub fn grid(&mut self) -> &mut Grid {
        match &mut self.view {
            View::Normal => &mut self.grid,
            View::Description { grid, .. } => grid,
        }
    }

    pub fn is_loading(&self) -> Option<(&'static str, f64)> {
        if let View::Description {
            descr: describer, ..
        } = &self.view
        {
            if let Some(progress) = describer.is_loading() {
                return Some(("describe", progress));
            }
        }
        if let Some(progress) = self.loader.is_loading() {
            Some(("load", progress))
        } else if self.frame.is_loading() {
            Some(("stream", -1.))
        } else {
            None
        }
    }
}

#[derive(PartialEq, Eq)]
pub enum Status {
    Normal,
    Size,
    Projection,
}

pub struct GridUI {
    pub col_name: Option<String>, // TODO borrow
    pub progress: usize,
    pub status: Status,
}
