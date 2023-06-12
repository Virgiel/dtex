use std::sync::Arc;

use reedline::KeyCode;
use tui::{crossterm::event::KeyEvent, Canvas};

use crate::{
    grid::SourceGrid, shell::Shell, source::Source, spinner::Spinner, style, task::Runner, OnKey,
};

enum State {
    Normal,
    Shell,
}
pub struct Tab {
    grid: SourceGrid,
    pub source: Arc<Source>,
    shell: Shell,
    state: State,
    spinner: Spinner,
}

impl Tab {
    pub fn open(runner: Runner, source: Source) -> Self {
        let source = Arc::new(source);
        Self {
            grid: SourceGrid::new(source.clone(), runner),
            state: State::Normal,
            shell: Shell::new(source.sql()),
            source,
            spinner: Spinner::new(),
        }
    }

    pub fn set_source(&mut self, source: Source) {
        let source = Arc::new(source);
        self.source = source.clone();
        self.grid.set_source(source);
    }

    pub fn draw(&mut self, c: &mut Canvas) {
        let status_line = c.reserve_btm(1);

        if let State::Shell = self.state {
            self.shell.draw(c);
        }

        // Draw grid
        let GridUI {
            col_name,
            progress,
            status,
        } = self.grid.draw(c);

        let mut l = c.consume(status_line).btm();
        let (status, style) = match status {
            Status::Normal => match self.state {
                State::Normal => ("  EX  ", style::state_default()),
                State::Shell => (" SHELL ", style::state_action()),
            },
            Status::Description => (" DESC ", style::state_other()),
            Status::Size => (" SIZE ", style::state_action()),
            Status::Projection => (" PROJ ", style::state_alternate()),
        };
        l.draw(status, style);
        l.draw(" ", style::primary());
        let mut task_progress = false;
        if let Some((task, progress)) = self.grid.is_loading() {
            if let Some(c) = self.spinner.state(true) {
                l.rdraw(format_args!("{c}"), style::progress());
                if progress > 0. {
                    l.rdraw(format_args!(" {progress:<2.0}%"), style::progress());
                }
                l.rdraw(format_args!(" {task}"), style::progress());
                task_progress = true;
            }
        } else {
            self.spinner.state(false);
        }
        if !task_progress {
            l.rdraw(format_args!(" {progress:>3}%"), style::primary());
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
        match self.state {
            State::Normal => match (self.grid.on_key(event), event.code) {
                (OnKey::Pass, KeyCode::Char('$')) => self.state = State::Shell,
                (e, _) => return e == OnKey::Quit,
            },
            State::Shell => {
                if let OnKey::Quit = self.shell.on_key(event, |str| {
                    match Source::from_sql(str, Some(self.source.clone())) {
                        Ok(source) => {
                            self.grid.set_source(Arc::new(source));
                            true
                        }
                        Err(e) => {
                            self.grid.set_err(e.0);
                            false
                        }
                    }
                }) {
                    self.state = State::Normal
                }
            }
        }
        false
    }

    pub fn is_loading(&self) -> bool {
        self.grid.is_loading().is_some()
    }
}

#[derive(PartialEq, Eq)]
pub enum Status {
    Normal,
    Description,
    Size,
    Projection,
}

pub struct GridUI {
    pub col_name: Option<String>, // TODO borrow
    pub progress: usize,
    pub status: Status,
}

impl GridUI {
    pub fn normal(mut self, status: Status) -> Self {
        if self.status == Status::Normal {
            self.status = status
        }
        self
    }
}
