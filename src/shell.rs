use reedline::KeyCode;
use tui::{crossterm::event::KeyEvent, unicode_width::UnicodeWidthChar, Canvas};

use crate::{style, OnKey};

use self::{
    highlighter::Highlighter,
    prompt::{Prompt, PromptCmd},
};

mod highlighter;
pub mod prompt;

pub struct Shell {
    prompt: Prompt<15>,
    offset: usize,
}

impl Shell {
    pub fn new(init: &str) -> Self {
        Self {
            prompt: Prompt::new(init),
            offset: 0,
        }
    }

    pub fn on_key(&mut self, event: &KeyEvent) -> (OnKey, Option<String>) {
        match event.code {
            KeyCode::Esc => return (OnKey::Quit, None),
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
                let str = str.into();
                self.prompt.exec(PromptCmd::New(true));
                return (OnKey::Quit, Some(str));
            }
            _ => return (OnKey::Pass, None),
        }
        (OnKey::Continue, None)
    }

    pub fn draw(&mut self, c: &mut Canvas) {
        let mut l = c.btm();
        l.draw("$ ", style::separator());
        let (str, cursor) = self.prompt.state();
        let mut highlighter = Highlighter::load(str);
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
            l.draw(c, highlighter.style(i as u64));
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
