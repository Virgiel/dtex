use std::{
    fmt::{Display, Write},
    ops::Range,
};

use tui::unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::Cell;

pub struct Col<'a> {
    max_lhs: usize,
    max_rhs: usize,
    align_right: bool,
    buf: String,
    cells: Vec<Cell<'a>>,
}

impl<'a> Col<'a> {
    pub fn new() -> Self {
        Self {
            max_lhs: 0,
            max_rhs: 0,
            align_right: false,
            buf: String::new(),
            cells: Vec::new(),
        }
    }

    fn buff_dsp(&mut self, dsp: impl Display) -> Range<usize> {
        let start = self.buf.len();
        write!(&mut self.buf, "{dsp}").unwrap();
        let end = self.buf.len();
        start..end
    }

    pub fn add_null(&mut self) {
        self.cells.push(Cell::Null)
    }

    pub fn add_bool(&mut self, bool: bool) {
        self.cells.push(Cell::Bool(bool));
        self.max_lhs = self.max_lhs.max(5);
    }

    pub fn add_str(&mut self, str: &'a str) {
        self.cells.push(Cell::Str(str));
        self.max_lhs = self.max_lhs.max(str.width())
    }

    pub fn add_nb(&mut self, nb: impl lexical_core::ToLexical) {
        let stack = &mut [b'0'; lexical_core::BUFFER_SIZE];
        let slc = lexical_core::write(nb, stack);
        let str = unsafe { std::str::from_utf8_unchecked(slc) };
        let (lhs, rhs) = if let Some((lhs, rhs)) = str.split_once('.') {
            (lhs.len(), rhs.len() + 1)
        } else {
            (str.len(), 0)
        };
        self.max_lhs = self.max_lhs.max(lhs);
        self.max_rhs = self.max_rhs.max(rhs);
        let ty = Cell::Nb {
            range: self.buff_dsp(str),
            lhs,
            rhs,
        };
        self.cells.push(ty);
    }

    pub(crate) fn add_dsp(&mut self, dsp: impl Display) {
        let range = self.buff_dsp(dsp);
        self.max_lhs = self.max_lhs.max(self.buf[range.clone()].width());
        self.cells.push(Cell::Dsp(range));
    }

    pub fn align_right(&mut self) {
        self.align_right = true;
    }

    pub fn budget(&self) -> usize {
        self.max_lhs + self.max_rhs
    }

    pub fn fmt<'b>(&self, buf: &'b mut String, idx: usize, budget: usize) -> &'b str {
        buf.clear();
        fn pad(buff: &mut String, amount: usize) {
            for _ in 0..amount {
                buff.push(' ');
            }
        }
        let ty = &self.cells[idx];
        // Align left numerical values
        if matches!(ty, Cell::Nb { .. }) {
            pad(buf, budget.saturating_sub(self.max_lhs + self.max_rhs))
        }
        match ty {
            Cell::Bool(bool) => {
                write!(buf, "{bool}").unwrap();
            }
            Cell::Str(str) if self.align_right => write!(buf, "{str:>0$}", self.budget()).unwrap(),
            Cell::Str(str) => write!(buf, "{str}").unwrap(),
            Cell::Dsp(range) => write!(buf, "{}", &self.buf[range.clone()]).unwrap(),
            Cell::Null => { /* TODO grey null ? */ }
            Cell::Nb { range, rhs, .. } => {
                let str = &self.buf[range.clone()];
                pad(buf, (self.max_lhs + rhs) - str.len());
                buf.push_str(str);
            }
        };
        // Fill remaining budget
        pad(buf, budget.saturating_sub(buf.width()));
        // Trim buffer
        trim_buffer(buf, budget)
    }
}

fn trim_buffer(buf: &mut String, budget: usize) -> &str {
    let overflow = buf
        .char_indices()
        .scan((0, 0), |(sum, prev), (mut pos, c)| {
            std::mem::swap(prev, &mut pos);
            *sum += c.width().unwrap_or(0);
            Some((pos, *sum > budget))
        })
        .find_map(|(pos, overflow)| (overflow).then_some(pos));
    if let Some(pos) = overflow {
        buf.replace_range(pos.., "…");
    }
    buf
}

pub fn rtrim(it: impl Display, buf: &mut String, budget: usize) -> &str {
    buf.clear();
    write!(buf, "{it}").unwrap();
    trim_buffer(buf, budget)
}
