use tui::{none, Color, Style};

pub fn primary() -> Style {
    none()
}

pub fn progress() -> Style {
    none().fg(Color::Green)
}

pub fn index() -> Style {
    none().fg(Color::DarkGrey)
}

pub fn selected() -> Style {
    none().fg(Color::DarkYellow)
}

pub fn separator() -> Style {
    none().fg(Color::DarkGrey).dim()
}

fn state() -> Style {
    none().fg(Color::Black).bold()
}

pub fn state_action() -> Style {
    state().bg(Color::Green)
}

pub fn state_default() -> Style {
    state().bg(Color::DarkGrey)
}

pub fn state_alternate() -> Style {
    state().bg(Color::Magenta)
}

pub fn state_other() -> Style {
    state().bg(Color::Cyan)
}

pub fn tab() -> Style {
    none().fg(Color::DarkGrey).bold()
}

pub fn tab_selected() -> Style {
    none().bold()
}

pub(crate) fn error() -> Style {
    none().fg(Color::Red).bold()
}
