use reedline::KeyCode;
use tui::{none, Canvas};

use crate::{
    grid::nav::Nav,
    shell::prompt::{Prompt, PromptCmd},
    style,
};

pub struct Navigator {
    prompt: Prompt<0>,
    prev: Nav,
    curr: Nav,
}

impl Navigator {
    pub fn new(nav: Nav) -> Self {
        Self {
            prompt: Prompt::new(""),
            prev: nav.clone(),
            curr: nav,
        }
    }

    pub fn activate(code: &KeyCode) -> bool {
        match code {
            KeyCode::Char(c) if c.is_ascii_digit() => true,
            _ => false,
        }
    }

    pub fn on_key(&mut self, code: KeyCode) -> Result<Nav, Nav> {
        match code {
            KeyCode::Char(c) if c.is_ascii_digit() => {
                self.prompt.exec(PromptCmd::Write(c));
                if let Ok(row) = self.prompt.state().0.parse::<usize>() {
                    self.curr.go_to((row, self.curr.c_col()));
                }
            }
            KeyCode::Left => self.prompt.exec(PromptCmd::Left),
            KeyCode::Right => self.prompt.exec(PromptCmd::Right),
            KeyCode::Up => self.prompt.exec(PromptCmd::Prev),
            KeyCode::Down => self.prompt.exec(PromptCmd::Next),
            KeyCode::Backspace => {
                self.prompt.exec(PromptCmd::Delete);
            }
            KeyCode::Esc => {
                return Err(self.prev.clone());
            }
            _ => {}
        }

        Ok(self.curr.clone())
    }

    pub fn draw(&mut self, c: &mut Canvas) {
        let mut l = c.btm();
        l.draw("$ ", style::separator());
        let (str, cursor) = self.prompt.state();
        l.draw(&str[..cursor], none());
        l.cursor();
        l.draw(&str[cursor..], none());
    }
}
