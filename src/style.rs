use tui::{none, Color, Style};

// Gruvbox Material https://github.com/sainnhe/gruvbox-material TODO customize ?
pub const BG_DIM: Color = Color::Rgb {
    r: 20,
    g: 22,
    b: 23,
};
pub const BG_0: Color = Color::Rgb {
    r: 29,
    g: 32,
    b: 33,
};
pub const BG_1: Color = Color::Rgb {
    r: 38,
    g: 40,
    b: 40,
};
pub const BG_3: Color = Color::Rgb {
    r: 60,
    g: 56,
    b: 54,
};
pub const BG_5: Color = Color::Rgb {
    r: 80,
    g: 73,
    b: 69,
};
pub const BG: Color = Color::Rgb {
    r: 50,
    g: 48,
    b: 47,
};
pub const GREY_0: Color = Color::Rgb {
    r: 124,
    g: 111,
    b: 100,
};
pub const YELLOW: Color = Color::Rgb {
    r: 216,
    g: 168,
    b: 87,
};
pub const GREEN: Color = Color::Rgb {
    r: 169,
    g: 182,
    b: 101,
};
pub const AQUA: Color = Color::Rgb {
    r: 137,
    g: 180,
    b: 130,
};
pub const PURPLE: Color = Color::Rgb {
    r: 211,
    g: 134,
    b: 155,
};

pub fn primary() -> Style {
    none()
}

pub fn progress() -> Style {
    none().fg(GREEN)
}

pub fn index() -> Style {
    none().fg(GREY_0)
}

pub fn selected() -> Style {
    none().fg(YELLOW)
}

pub fn separator() -> Style {
    none().fg(BG_3)
}

pub fn state_action() -> Style {
    none().bg(GREEN).fg(BG_DIM).bold()
}

pub fn state_default() -> Style {
    none().bg(GREY_0).fg(BG_0).bold()
}

pub fn state_alternate() -> Style {
    none().bg(PURPLE).fg(BG_0).bold()
}

pub fn tab() -> Style {
    none().fg(GREY_0).bold()
}

pub fn tab_selected() -> Style {
    none().bold()
}
