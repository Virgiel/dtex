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
    none().fg(Color::DarkGrey)
}

pub fn state_action() -> Style {
    none().bg(Color::Green).bold()
}

pub fn state_default() -> Style {
    none().bg(Color::DarkGrey).bold()
}

pub fn state_alternate() -> Style {
    none().bg(Color::Magenta).bold()
}

pub fn tab() -> Style {
    none().fg(Color::DarkGrey).bold()
}

pub fn tab_selected() -> Style {
    none().bold()
}
