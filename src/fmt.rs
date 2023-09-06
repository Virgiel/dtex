use std::{
    fmt::{Display, Write},
    ops::Range,
};

use tui::unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::Cell;

pub struct GridBuffer {
    cell_buf: String,
    fmt_buf: String,
    max: usize,
}

impl GridBuffer {
    pub fn new() -> Self {
        Self {
            cell_buf: String::new(),
            fmt_buf: String::new(),
            max: 0,
        }
    }

    pub fn fmt_buf(&mut self) -> &mut String {
        &mut self.fmt_buf
    }

    pub fn new_frame(&mut self, width: usize) {
        self.cell_buf.clear();
        self.fmt_buf.clear();
        self.max = width;
    }
}

pub struct CellFmtLimit<'a> {
    buf: &'a mut String,
    max: usize,
    curr: usize,
}

impl<'a> CellFmtLimit<'a> {
    pub fn new(buf: &'a mut GridBuffer) -> Self {
        Self {
            buf: &mut buf.cell_buf,
            max: buf.max,
            curr: 0,
        }
    }

    pub fn reset(&mut self) {
        self.curr = 0;
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }
}

impl<'a> std::fmt::Write for CellFmtLimit<'a> {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        if self.curr < self.max {
            self.buf.push_str(s);
            self.curr += s.width();
            Ok(())
        } else {
            Err(std::fmt::Error::default())
        }
    }
}

pub struct ColBuilder<'a, 'b> {
    buf: CellFmtLimit<'b>,
    col: Col<'a>,
}

impl<'a, 'b> ColBuilder<'a, 'b> {
    pub fn new(buf: &'b mut GridBuffer) -> Self {
        Self {
            buf: CellFmtLimit::new(buf),
            col: Col {
                max_lhs: 0,
                max_rhs: 0,
                align_right: false,
                cells: Vec::new(),
            },
        }
    }

    fn buff_dsp(&mut self, dsp: impl Display) -> Range<usize> {
        self.buf.reset();
        let start = self.buf.len();
        write!(&mut self.buf, "{dsp}").ok();
        let end = self.buf.len();
        start..end
    }

    pub fn add_null(&mut self) {
        self.col.cells.push(Cell::Null)
    }

    pub fn add_bool(&mut self, bool: bool) {
        self.col.cells.push(Cell::Bool(bool));
        self.col.max_lhs = self.col.max_lhs.max(5);
    }

    pub fn add_str(&mut self, str: &'a str) {
        self.col.cells.push(Cell::Str(str));
        self.col.max_lhs = self.col.max_lhs.max(str.width())
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
        self.col.max_lhs = self.col.max_lhs.max(lhs);
        self.col.max_rhs = self.col.max_rhs.max(rhs);
        let ty = Cell::Nb {
            range: self.buff_dsp(str),
            lhs,
            rhs,
        };
        self.col.cells.push(ty);
    }

    pub(crate) fn add_dsp(&mut self, dsp: impl Display) {
        let range = self.buff_dsp(dsp);
        self.col.max_lhs = self.col.max_lhs.max(self.buf.buf[range.clone()].width());
        self.col.cells.push(Cell::Dsp(range));
    }

    pub fn build(self) -> Col<'a> {
        self.col
    }
}

pub struct Col<'a> {
    max_lhs: usize,
    max_rhs: usize,
    align_right: bool,
    cells: Vec<Cell<'a>>,
}

impl<'a> Col<'a> {
    pub fn align_right(&mut self) {
        self.align_right = true;
    }

    pub fn budget(&self) -> usize {
        self.max_lhs + self.max_rhs
    }

    pub fn fmt<'b>(&self, grid: &'b mut GridBuffer, idx: usize, budget: usize) -> &'b str {
        let buf = &mut grid.fmt_buf;
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
            Cell::Dsp(range) => write!(buf, "{}", &grid.cell_buf[range.clone()]).unwrap(),
            Cell::Null => { /* TODO grey null ? */ }
            Cell::Nb { range, rhs, .. } => {
                let str = &grid.cell_buf[range.clone()];
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
