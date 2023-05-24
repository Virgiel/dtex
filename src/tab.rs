use std::sync::Arc;

use reedline::KeyCode;
use tui::{crossterm::event::KeyEvent, unicode_width::UnicodeWidthChar, Canvas};

use crate::{event::Orchestrator, grid::SourceGrid, source::Source, style, OnKey};

use self::prompt::{Prompt, PromptCmd};

pub struct Shell {
    prompt: Prompt,
    offset: usize,
}

impl Shell {
    pub fn new() -> Self {
        Self {
            prompt: Prompt::new(""),
            offset: 0,
        }
    }

    pub fn draw_prompt(&mut self, c: &mut Canvas) {
        let mut l = c.btm();
        l.draw("$ ", style::separator());
        let (str, cursor) = self.prompt.state();
        //let mut highlighter = Highlighter::new(str);
        let mut pending_cursor = true;

        let mut w = l.width();
        self.offset = self.offset.min(cursor);

        let mut before = str[..cursor].chars().rev();
        let mut start = cursor;
        let after = str[cursor..].chars();
        let mut end = cursor;
        // Read left until goal
        loop {
            if start == self.offset {
                break;
            }
            if let Some(c) = before.next() {
                let c_width = c.width().unwrap_or(0);
                if c_width > 0 && w <= c_width {
                    break;
                }
                w -= c_width;
                start -= c.len_utf8();
            } else {
                break;
            }
        }
        self.offset = start;
        // Read right until eof
        for c in after {
            let c_width = c.width().unwrap_or(0);
            if c_width > 0 && w <= c_width {
                break;
            }
            w -= c_width;
            end += c.len_utf8();
        }
        // Read left until eof
        for c in before {
            let c_width = c.width().unwrap_or(0);
            if c_width > 0 && w <= c_width {
                break;
            }
            w -= c_width;
            start -= c.len_utf8();
        }

        for (i, c) in str[start..end].char_indices() {
            let i = start + i;
            if l.width() == 1 {
                break;
            }
            if pending_cursor && cursor <= i {
                l.cursor();
                pending_cursor = false
            }
            l.draw(
                c,
                tui::none(), /*match highlighter.style(i) {
                                 Style::None | Style::Logi => none(),
                                 Style::Id => none().fg(Color::Blue),
                                 Style::Nb => none().fg(Color::Yellow),
                                 Style::Str => none().fg(Color::Green),
                                 Style::Regex => none().fg(Color::Magenta),
                                 Style::Action => none().fg(Color::Red),
                             }*/
            );
        }
        if pending_cursor {
            l.cursor();
        }
        // Draw error message
        /*if let Some((range, msg)) = &self.err {
            let mut l = c.btm();
            l.draw("  ", none());
            if range.end >= start && range.start <= end {
                let mut range = range.clone();
                range.start = range.start.max(start);
                range.end = range.end.min(end);
                let space_left = str[start..range.start].width();
                let space_right = str[range.end..end]
                    .width()
                    .max(l.width().saturating_sub(space_left + range.len() + 1));
                if space_right > msg.width() && space_right >= space_left {
                    l.draw(
                        format_args!(
                            "{s:<0$}{s:▾<1$} {msg}",
                            range.start.saturating_sub(start),
                            range.len(),
                            s = ""
                        ),
                        none().fg(Color::Red),
                    );
                } else if space_left > msg.width() {
                    l.rdraw(
                        format_args!(
                            "{msg} {s:▾<1$}{s:<0$}",
                            l.width().saturating_sub(range.end.saturating_sub(start)),
                            range.len(),
                            s = ""
                        ),
                        none().fg(Color::Red),
                    );
                } else {
                    l.draw(format_args!("{msg}"), none().fg(Color::Red));
                }
            } else {
                if range.start > end {
                    l.rdraw(format_args!("{msg} ▸"), none().fg(Color::Red));
                } else {
                    l.draw(format_args!("◂ {msg}"), none().fg(Color::Red));
                }
            }
        }*/
    }
}

pub struct Tab {
    grid: SourceGrid,
    pub source: Arc<Source>,
    shell: Shell,
    state: State,
}

impl Tab {
    pub fn open(orchestrator: Orchestrator, source: Source) -> Self {
        let source = Arc::new(source);
        Self {
            grid: SourceGrid::new(source.clone(), orchestrator),
            state: State::Normal,
            source,
            shell: Shell::new(),
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
            self.shell.draw_prompt(c);
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
        if let Some(task) = self.grid.is_loading() {
            l.rdraw(format_args!("{task}..."), style::progress());
        }
        l.rdraw(format_args!(" {progress:>3}%"), style::primary());
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
            State::Shell => match event.code {
                KeyCode::Esc => self.state = State::Normal,
                KeyCode::Char(c) => {
                    self.shell.prompt.exec(PromptCmd::Write(c));
                }
                KeyCode::Left => self.shell.prompt.exec(PromptCmd::Left),
                KeyCode::Right => self.shell.prompt.exec(PromptCmd::Right),
                KeyCode::Up => self.shell.prompt.exec(PromptCmd::Prev),
                KeyCode::Down => self.shell.prompt.exec(PromptCmd::Next),
                KeyCode::Backspace => {
                    self.shell.prompt.exec(PromptCmd::Delete);
                }
                KeyCode::Enter => {
                    let (str, _) = self.shell.prompt.state();
                    match Source::from_sql(str, Some(&self.source)) {
                        Ok(source) => {
                            self.grid.set_source(Arc::new(source));
                            self.state = State::Normal
                        }
                        Err(e) => self.grid.set_err(e.0),
                    }
                }
                _ => {}
            },
        }
        false
    }

    pub fn is_loading(&self) -> bool {
        self.grid.is_loading().is_some()
    }
}

// tab -> Source
//     -> SQL

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

pub enum State {
    Normal,
    Shell,
}

pub mod prompt {
    use reedline::LineBuffer;

    struct HistoryBuffer<T, const N: usize> {
        ring: [T; N],
        head: usize,
        filled: bool,
    }

    impl<T: Default, const N: usize> HistoryBuffer<T, N> {
        pub fn new() -> Self {
            Self {
                ring: std::array::from_fn(|_| T::default()),
                head: 0,
                filled: false,
            }
        }

        pub fn push(&mut self, item: T) {
            self.ring[self.head] = item;
            if self.head + 1 == self.ring.len() {
                self.filled = true;
            }
            self.head = (self.head + 1) % self.ring.len();
        }

        pub fn get(&self, idx: usize) -> &T {
            assert!(idx <= self.ring.len() && self.len() > 0);
            let pos = (self.ring.len() + self.head - idx - 1) % self.ring.len();
            &self.ring[pos]
        }

        pub fn len(&self) -> usize {
            if self.filled {
                self.ring.len()
            } else {
                self.head
            }
        }
    }

    pub struct Prompt {
        history: HistoryBuffer<String, 5>,
        pos: Option<usize>,
        buffer: LineBuffer,
    }

    impl Prompt {
        pub fn new(init: &str) -> Self {
            Self {
                history: HistoryBuffer::new(),
                pos: None,
                buffer: LineBuffer::from(init),
            }
        }

        /// Ensure buffer contains the right data
        fn solidify(&mut self) {
            if let Some(pos) = self.pos.take() {
                self.buffer.clear();
                self.buffer.insert_str(self.history.get(pos));
            }
        }

        pub fn exec(&mut self, cmd: PromptCmd) {
            match cmd {
                PromptCmd::Write(c) => {
                    self.solidify();
                    self.buffer.insert_char(c);
                }
                PromptCmd::Left => {
                    self.solidify();
                    self.buffer.move_left();
                }
                PromptCmd::Right => {
                    self.solidify();
                    self.buffer.move_right()
                }
                PromptCmd::Delete => {
                    self.solidify();
                    self.buffer.delete_left_grapheme();
                }
                PromptCmd::Prev => match &mut self.pos {
                    Some(pos) if *pos + 1 < self.history.len() => *pos += 1,
                    None if self.history.len() > 0 => self.pos = Some(0),
                    _ => {}
                },
                PromptCmd::Next => match &mut self.pos {
                    Some(0) => self.pos = None,
                    Some(pos) => *pos = pos.saturating_sub(1),
                    None => {}
                },
                PromptCmd::New(keep) => {
                    let (str, _) = self.state();
                    self.history.push(str.into());
                    self.pos = keep.then_some(0);
                    self.buffer.clear();
                }
                PromptCmd::Jump(pos) => self.buffer.set_insertion_point(pos),
            }
        }

        pub fn state(&self) -> (&str, usize) {
            match self.pos {
                Some(pos) => {
                    let str = self.history.get(pos);
                    (str, str.len())
                }
                None => (self.buffer.get_buffer(), self.buffer.insertion_point()),
            }
        }
    }

    pub enum PromptCmd {
        Write(char),
        Left,
        Right,
        Prev,
        Next,
        New(bool),
        Delete,
        Jump(usize),
    }
}
