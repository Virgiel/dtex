use std::fmt::{Display, Write};

use bstr::ByteSlice;
use tui::unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::{utils::BStrWidth, Ty};

pub struct ColStat {
    max_lhs: usize,
    max_rhs: usize,
    is_str: bool,
    align_right: bool,
}

fn split_nb<NB: lexical_core::ToLexical>(stack: &mut [u8], nb: NB) -> (&str, usize, usize) {
    let slc = lexical_core::write(nb, stack);
    let str = std::str::from_utf8(slc).expect("lexical_core always generate ascii");
    if let Some((lhs, rhs)) = str.split_once('.') {
        (str, lhs.len(), rhs.len() + 1)
    } else {
        (str, str.len(), 0)
    }
}

impl ColStat {
    pub fn new() -> Self {
        Self {
            max_lhs: 0,
            max_rhs: 0,
            is_str: false,
            align_right: false,
        }
    }

    fn size_nb<NB: lexical_core::ToLexical>(&mut self, nb: NB) {
        let stack = &mut [b'0'; lexical_core::BUFFER_SIZE];
        let (_, lhs, rhs) = split_nb(stack, nb);
        self.max_lhs = self.max_lhs.max(lhs);
        self.max_rhs = self.max_rhs.max(rhs);
    }

    pub fn add(&mut self, ty: &Ty) {
        match ty {
            Ty::Null => {}
            Ty::Bool(_) => self.max_lhs = self.max_lhs.max(5),
            Ty::Str(s) => {
                if !s.is_empty() {
                    self.max_lhs = self.max_lhs.max(s.width());
                    self.is_str = true;
                }
            }
            Ty::U64(nb) => self.size_nb(*nb),
            Ty::I64(nb) => self.size_nb(*nb),
            Ty::F64(nb) => self.size_nb(*nb),
        }
    }

    pub fn align_right(&mut self) {
        self.align_right = true;
    }

    pub fn budget(&self) -> usize {
        self.max_lhs + self.max_rhs
    }
}

/// Buffer used by fmt functions
pub fn fmt_field<'a>(buf: &'a mut String, ty: &Ty, stat: &ColStat, budget: usize) -> &'a str {
    buf.clear();
    fn pad(buff: &mut String, amount: usize) {
        for _ in 0..amount {
            buff.push(' ');
        }
    }
    // Align left numerical values
    if matches!(ty, Ty::U64(_) | Ty::I64(_) | Ty::F64(_)) {
        pad(buf, budget.saturating_sub(stat.max_lhs + stat.max_rhs))
    }
    // Write value
    fn write_nb<NB: lexical_core::ToLexical>(buf: &mut String, stat: &ColStat, nb: NB) {
        let stack = &mut [b'0'; lexical_core::BUFFER_SIZE];
        let (str, _, rhs) = split_nb(stack, nb);
        pad(buf, (stat.max_lhs + rhs) - str.len());
        buf.push_str(str);
    }
    match ty {
        Ty::Bool(bool) => {
            write!(buf, "{bool}").unwrap();
        }
        Ty::Str(str) if stat.align_right => write!(buf, "{str:>0$}", stat.budget()).unwrap(),
        Ty::Str(str) => write!(buf, "{str}").unwrap(),
        Ty::Null => { /* TODO grey null ? */ }
        Ty::U64(nb) => write_nb(buf, stat, *nb),
        Ty::I64(nb) => write_nb(buf, stat, *nb),
        Ty::F64(nb) => write_nb(buf, stat, *nb),
    };
    // Fill remaining budget
    pad(buf, budget.saturating_sub(buf.width()));
    // Trim buffer
    trim_buffer(buf, budget)
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
