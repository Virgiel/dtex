use reedline::KeyCode as Key;
use tui::{none, Canvas};

use crate::{
    grid::nav::Nav,
    shell::prompt::{Prompt, PromptCmd},
    style,
};

pub struct Navigator {
    prompt: Option<Prompt<0>>,
    prev: Nav,
    curr: Nav,
}

impl Navigator {
    pub fn new(nav: Nav) -> Self {
        Self {
            prompt: None,
            prev: nav.clone(),
            curr: nav,
        }
    }

    pub fn on_key(&mut self, code: Key) -> Result<Nav, Nav> {
        if self.prompt.is_none() {
            let mut pass = false;
            match code {
                Key::Left | Key::Char('h') => self.curr.start(),
                Key::Down | Key::Char('j') => self.curr.btm(),
                Key::Up | Key::Char('k') => self.curr.top(),
                Key::Right | Key::Char('l') => self.curr.end(),
                _ => pass = true,
            }
            if !pass {
                return Err(self.curr.clone());
            }
        }
        let prompt = self.prompt.get_or_insert_with(|| Prompt::new(""));
        match code {
            Key::Char(c) if c.is_ascii_digit() => {
                prompt.exec(PromptCmd::Write(c));
                if let Ok(row) = prompt.state().0.parse::<usize>() {
                    self.curr.go_to((row, self.curr.c_col()));
                }
            }
            Key::Left => prompt.exec(PromptCmd::Left),
            Key::Right => prompt.exec(PromptCmd::Right),
            Key::Up => prompt.exec(PromptCmd::Prev),
            Key::Down => prompt.exec(PromptCmd::Next),
            Key::Backspace => {
                prompt.exec(PromptCmd::Delete);
                if let Ok(row) = prompt.state().0.parse::<usize>() {
                    self.curr.go_to((row, self.curr.c_col()));
                }
            }
            Key::Esc => return Err(self.prev.clone()),
            Key::Enter => return Err(self.curr.clone()),
            _ => {}
        }

        Ok(self.curr.clone())
    }

    pub fn draw(&mut self, c: &mut Canvas) {
        if let Some(prompt) = &self.prompt {
            let mut l = c.btm();
            l.draw("$ ", style::separator());
            let (str, cursor) = prompt.state();
            l.draw(&str[..cursor], none());
            l.cursor();
            l.draw(&str[cursor..], none());
        }
    }
}
