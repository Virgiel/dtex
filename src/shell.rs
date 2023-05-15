use polars::prelude::DataFrame;
use reedline::KeyCode;
use tui::{crossterm::event::KeyEvent, none, unicode_width::UnicodeWidthChar, Canvas};

use crate::{
    event::Orchestrator,
    grid::{Grid, GridUI},
    source::Source,
    style,
    tab::{
        prompt::{Prompt, PromptCmd},
        State, Status,
    },
};

pub struct Shell {
    pub grid: Grid,
    prompt: Prompt,
    offset: usize,
}

impl Shell {
    pub fn open(orchestrator: Orchestrator, sql: String) -> Self {
        Self {
            prompt: Prompt::new(&sql),
            grid: Grid::new(
                State::Shell,
                if sql.is_empty() {
                    Source::Memory(DataFrame::empty())
                } else {
                    Source::Sql(sql)
                },
                orchestrator,
            ),
            offset: 0,
        }
    }

    pub fn draw(&mut self, c: &mut Canvas) {
        let status = c.reserve_btm(1);

        self.draw_prompt(c);

        // Draw grid
        let GridUI {
            col_name,
            progress,
            state,
            describe,
        } = self.grid.draw(c);

        let mut l = c.consume(status).btm();
        let (status, style) = match state {
            State::Explore if describe => (" DESC ", style::state_other()),
            State::Explore => ("  EX  ", style::state_default()),
            State::Size => (" SIZE ", style::state_action()),
            State::Projection => (" MOVE ", style::state_alternate()),
            State::Shell => (" SQL  ", style::state_action()),
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
    }

    pub fn on_key(&mut self, event: &KeyEvent) -> Status {
        match self.grid.grid.state {
            State::Explore => match event.code {
                KeyCode::Char('$') => {
                    self.grid.grid.state = State::Shell;
                }
                _ => return self.grid.on_key(event),
            },
            State::Shell => match event.code {
                KeyCode::Esc => self.grid.grid.state = State::Explore,
                KeyCode::Char(c) => {
                    self.prompt.exec(PromptCmd::Write(c));
                }
                KeyCode::Left => self.prompt.exec(PromptCmd::Left),
                KeyCode::Right => self.prompt.exec(PromptCmd::Right),
                KeyCode::Up => self.prompt.exec(PromptCmd::Prev),
                KeyCode::Down => self.prompt.exec(PromptCmd::Next),
                KeyCode::Backspace => {
                    self.prompt.exec(PromptCmd::Delete);
                }
                KeyCode::Enter => {
                    let (str, _) = self.prompt.state();
                    self.grid.set_source(Source::Sql(str.to_string()));
                    self.grid.grid.state = State::Explore
                }
                _ => {}
            },
            State::Projection | State::Size => return self.grid.on_key(event),
        };
        Status::Continue
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
                none(), /*match highlighter.style(i) {
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

    pub fn is_loading(&self) -> bool {
        self.grid.is_loading().is_some()
    }
}
