use std::sync::Arc;

use tui::{
    crossterm::event::{KeyCode as Key, KeyEvent},
    none, Canvas,
};

use crate::{
    describe::DescriberView,
    fmt::GridBuffer,
    grid::Grid,
    navigator::Navigator,
    shell::Shell,
    source::{FrameLoader, Source, StreamingFrame},
    spinner::Spinner,
    style,
    task::Runner,
    view::{View, ViewState},
    OnKey,
};

enum State {
    Normal,
    Description(DescriberView),
    Shell(SourceView),
    Nav(Navigator),
}

pub struct SourceView {
    pub source: Arc<Source>,
    frame: StreamingFrame,
    loader: FrameLoader,
    pub grid: Grid,
    load_error: Option<String>,
}

impl SourceView {
    pub fn new(source: Arc<Source>, runner: &Runner) -> Self {
        Self {
            source: source.clone(),
            frame: StreamingFrame::empty(),
            loader: FrameLoader::load(source, runner),
            grid: Grid::new(),
            load_error: None,
        }
    }

    pub fn take(&self) -> Self {
        Self {
            source: self.source.clone(),
            frame: self.frame.take(),
            loader: FrameLoader::Finished(None),
            grid: self.grid.clone(),
            load_error: None,
        }
    }

    pub fn set_source(&mut self, source: Arc<Source>, runner: &Runner) {
        self.source = source.clone();
        self.loader = FrameLoader::load(source, runner);
    }
}

impl View for SourceView {
    fn tick(&mut self) -> ViewState {
        // Tick
        match self.loader.tick() {
            Some(Ok(new)) => {
                self.frame = new;
                self.grid = Grid::new();
                self.load_error = None;
            }
            Some(Err(e)) => self.load_error = Some(e.0),
            None => {}
        }
        self.frame.goal(self.grid.nav.goal().saturating_add(1));
        self.frame.tick();

        ViewState {
            loading: if let Some(progress) = self.loader.is_loading() {
                Some(("load", progress))
            } else if self.frame.is_loading() {
                Some(("stream", -1.))
            } else {
                None
            },
            streaming: self.frame.is_streaming(),
            frame: self.frame.df(),
            grid: &mut self.grid,
            err: self.frame.err().or(self.load_error.as_deref()),
        }
    }
}

pub struct Tab {
    pub view: SourceView,
    runner: Runner,
    shell: Shell,
    state: State,
    spinner: Spinner,
}

impl Tab {
    pub fn open(runner: Runner, source: Source) -> Self {
        let source = Arc::new(source);
        Self {
            state: State::Normal,
            shell: Shell::new(source.init_sql()),
            view: SourceView::new(source, &runner),
            spinner: Spinner::new(),
            runner,
        }
    }

    pub fn draw(&mut self, c: &mut Canvas, buf: &mut GridBuffer) -> bool {
        let status_line = c.reserve_btm(1);
        let state_line = match &self.state {
            State::Normal | State::Description(_) => c.reserve_btm(0),
            State::Shell(_) | State::Nav(_) => c.reserve_btm(1),
        };

        // Tick
        let view: &mut dyn View = match &mut self.state {
            State::Shell(view) => view,
            State::Description(desrc) => desrc,
            _ => &mut self.view,
        };
        let ViewState {
            loading,
            streaming,
            err,
            frame,
            grid,
        } = view.tick();

        let spinner = self.spinner.state(loading.is_some());

        // Print error message
        if let Some(err) = &err {
            for line in err.lines().rev() {
                c.btm().draw(line, style::error());
            }
        }
        // Draw grid
        let GridUI { col_name, status } = grid.draw(c, buf, frame);

        // Draw full screen info if frame is empty
        if frame.nb_row() == 0 {
            if let Some((task, progress)) = loading {
                // Loading bar
                if spinner.is_some() {
                    let pad_top = c.height().saturating_sub(1) / 2;
                    let pad_left = c
                        .width()
                        .saturating_sub(task.len() + if progress > 0. { 8 } else { 0 })
                        / 2;
                    for _ in 0..pad_top {
                        c.line("", none());
                    }
                    let mut line = c.top();
                    for _ in 0..pad_left {
                        line.draw(" ", none());
                    }
                    if progress > 0. {
                        line.draw(format_args!("{task} - {progress:>2.0}%"), style::progress());
                    } else {
                        line.draw(format_args!("{task}"), style::progress());
                    }
                }
            } else {
                // Empty
                let pad_top = c.height().saturating_sub(1) / 2;
                let pad_left = c.width().saturating_sub(15) / 2;
                for _ in 0..pad_top {
                    c.line("", none());
                }
                let mut line = c.top();
                for _ in 0..pad_left {
                    line.draw(" ", none());
                }
                line.draw("Empty dataframe", style::separator());
            }
        }

        // Draw status
        let mut l = c.consume(status_line).btm();
        let (status, style) = match status {
            Status::Normal => match self.state {
                State::Normal => ("DTEX", style::state_default()),
                State::Description(_) => ("DESC", style::state_other()),
                State::Shell(_) => ("SQL", style::state_action()),
                State::Nav(_) => ("GOTO", style::state_action()),
            },
            Status::Size => ("SIZE", style::state_action()),
            Status::Projection => ("PROJ", style::state_alternate()),
        };
        l.draw(format_args!(" {status} "), style);
        l.draw(" ", style::primary());

        if let Some((task, progress)) = loading {
            if let Some(c) = spinner {
                l.rdraw(format_args!("{c}"), style::progress());
                if progress > 0. {
                    l.rdraw(format_args!(" {progress:>2.0}%"), style::progress());
                }
                l.rdraw(format_args!(" {task}"), style::progress());
            }
        }
        if spinner.is_none() {
            if streaming {
                l.rdraw(format_args!(" ~"), style::primary());
            } else {
                l.rdraw(
                    format_args!(" {:>3}%", self.grid().nav.progress()),
                    style::primary(),
                );
            }
        }

        if let Some(name) = col_name {
            l.rdraw(name, style::primary());
            l.rdraw(" ", style::primary());
        }
        if let Some(path) = &self.view.source.display_path() {
            l.draw(path, style::progress());
        }

        // Draw state specific
        c.consume(state_line);
        match &mut self.state {
            State::Normal | State::Description(_) => {}
            State::Shell(v) => {
                self.shell
                    .draw(c, v.loader.is_loading().is_some(), v.load_error.is_some())
            }
            State::Nav(nav) => nav.draw(c),
        }

        loading.is_some()
    }

    pub fn on_key(&mut self, event: &KeyEvent) -> bool {
        match &mut self.state {
            State::Normal => match (self.grid().on_key(event), event.code) {
                (OnKey::Pass, code) => match code {
                    Key::Char('$') => self.state = State::Shell(self.view.take()),
                    Key::Char('g') => {
                        self.state = State::Nav(Navigator::new(self.grid().nav.clone()))
                    }
                    Key::Char('d') => {
                        self.state = State::Description(DescriberView::new(
                            self.view.source.clone(),
                            &self.runner,
                        ))
                    }
                    _ => {}
                },
                (OnKey::Quit, _) => return true,
                _ => {}
            },
            State::Description(_) => match (self.grid().on_key(event), event.code) {
                (OnKey::Pass, code) => match code {
                    Key::Char('$') => self.state = State::Shell(self.view.take()),
                    Key::Char('g') => {
                        self.state = State::Nav(Navigator::new(self.grid().nav.clone()))
                    }
                    Key::Esc => self.state = State::Normal,
                    _ => {}
                },
                (OnKey::Quit, _) => self.state = State::Normal,
                _ => {}
            },
            State::Shell(view) => {
                let (result, new_sql, apply) = self.shell.on_key(event);
                if let Some(sql) = new_sql {
                    if view.source.init_sql() != sql {
                        view.set_source(Arc::new(view.source.query(sql.into())), &self.runner);
                    }
                }
                if apply {
                    if view.load_error.is_none()
                        && view.loader.is_loading().is_none()
                        && !view.frame.is_loading()
                        && view.frame.err().is_none()
                    {
                        std::mem::swap(&mut self.view, view);
                        self.state = State::Normal
                    }
                } else if OnKey::Quit == result {
                    self.state = State::Normal
                }
            }
            State::Nav(navigator) => match navigator.on_key(event.code) {
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
        match &mut self.state {
            State::Shell(view) => &mut view.grid,
            State::Description(desrc) => &mut desrc.grid,
            _ => &mut self.view.grid,
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
    pub status: Status,
}
