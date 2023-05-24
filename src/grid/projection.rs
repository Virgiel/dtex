#[derive(Clone, Copy)]
pub enum Cmd {
    Hide,
    Left,
    Right,
}

/// Column projection
pub struct Projection {
    cols: Vec<usize>,
    nb_col: usize,
}

impl Projection {
    pub fn new() -> Self {
        Self {
            cols: vec![],
            nb_col: 0,
        }
    }

    /// Sync the number of columns
    pub fn set_nb_cols(&mut self, nb_col: usize) {
        self.cols.retain(|n| *n < nb_col);
        self.cols.extend(self.nb_col..nb_col);
        self.nb_col = nb_col;
    }

    /// Number of visible columns
    pub fn nb_cols(&self) -> usize {
        self.cols.len()
    }

    /// Get the column idx at this offset
    pub fn project(&self, off: usize) -> usize {
        self.cols[off]
    }

    /// Apply command
    pub fn cmd(&mut self, off: usize, cmd: Cmd) {
        if self.cols.is_empty() {
            return;
        }
        let len = self.cols.len();
        match cmd {
            Cmd::Hide => {
                self.cols.remove(off);
            }
            Cmd::Left => self.cols.swap(off, off.saturating_sub(1)),
            Cmd::Right => self
                .cols
                .swap(off, off.saturating_add(1).min(len.saturating_sub(1))),
        }
    }

    /// Show all columns in their original position
    pub fn reset(&mut self) {
        self.cols.clear();
        self.cols.extend(0..self.nb_col);
    }
}
