use std::ops::Add;

use bstr::{BStr, ByteSlice};
use tui::unicode_width::UnicodeWidthChar;

pub trait BStrWidth {
    fn width(&self) -> usize;
}

impl BStrWidth for BStr {
    fn width(&self) -> usize {
        self.chars()
            .map(|c| c.width().unwrap_or(0))
            .fold(0, Add::add)
    }
}
