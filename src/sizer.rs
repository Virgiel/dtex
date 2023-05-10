#[derive(Clone, Copy)]
pub enum Cmd {
    Constrain,
    Free,
    Less,
    More,
}

#[derive(Clone, Copy)]
enum Constraint {
    Limited,
    Free,
    Fixed(usize),
}

/// Size column based on previous observed length and constraints  
pub struct Sizer {
    cols: Vec<(usize, Constraint)>,
}

impl Sizer {
    pub fn new() -> Self {
        Self { cols: vec![] }
    }

    /// Size a column
    pub fn size(&mut self, idx: usize, len: usize) -> usize {
        // Ensure we store info for this column
        if idx >= self.cols.len() {
            self.cols.resize(idx + 1, (0, Constraint::Limited));
        }
        // Sync max len
        self.cols[idx].0 = self.cols[idx].0.max(len);

        self.get_size(idx)
    }

    // Size the column based on its constraint
    fn get_size(&self, idx: usize) -> usize {
        let (size, constraint) = self.cols[idx];
        match constraint {
            Constraint::Limited => size.min(25),
            Constraint::Free => size,
            Constraint::Fixed(size) => size,
        }
    }

    /// Apply command
    pub fn cmd(&mut self, idx: usize, cmd: Cmd) {
        if idx < self.cols.len() {
            return;
        }
        self.cols[idx].1 = match cmd {
            Cmd::Constrain => Constraint::Limited,
            Cmd::Free => Constraint::Free,
            Cmd::Less => Constraint::Fixed(self.get_size(idx).saturating_sub(1)),
            Cmd::More => Constraint::Fixed(self.get_size(idx).saturating_add(1)),
        }
    }

    /// Reset all columns dimensions to default
    pub fn reset(&mut self) {
        self.cols.clear()
    }

    /// Reset all columns observed max size
    pub fn fit(&mut self) {
        for (max, _) in &mut self.cols {
            *max = 0;
        }
    }
}
