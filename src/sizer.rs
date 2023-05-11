#[derive(Clone, Copy)]
pub enum Cmd {
    Constrain,
    Free,
    Less,
    More,
}

#[derive(Clone, Copy)]
enum Constraint {
    Limit,
    Fit,
    Fixe(usize),
}

/// Size column based on previous observed length and constraints  
pub struct Sizer {
    cols: Vec<(usize, usize, Constraint)>,
    fit_content: bool,
}

impl Sizer {
    pub fn new() -> Self {
        Self {
            cols: vec![],
            fit_content: false,
        }
    }

    /// Size a column
    pub fn size(&mut self, idx: usize, len: usize, header_len: usize) -> usize {
        // Ensure we store info for this column
        if idx >= self.cols.len() {
            self.cols.resize(idx + 1, (0, 0, Constraint::Limit));
        }
        // Sync max len
        self.cols[idx].0 = self.cols[idx].0.max(len);
        self.cols[idx].1 = self.cols[idx].1.max(header_len);

        self.get_size(idx)
    }

    // Size the column based on its constraint
    fn get_size(&self, idx: usize) -> usize {
        let (content, header, constraint) = self.cols[idx];
        match constraint {
            Constraint::Limit if self.fit_content => content.min(25),
            Constraint::Limit => header.max(content).min(25),
            Constraint::Fit => content,
            Constraint::Fixe(size) => size,
        }
    }

    /// Apply command
    pub fn cmd(&mut self, idx: usize, cmd: Cmd) {
        if idx < self.cols.len() {
            return;
        }
        self.cols[idx].2 = match cmd {
            Cmd::Constrain => Constraint::Limit,
            Cmd::Free => Constraint::Fit,
            Cmd::Less => Constraint::Fixe(self.get_size(idx).saturating_sub(1)),
            Cmd::More => Constraint::Fixe(self.get_size(idx).saturating_add(1)),
        }
    }

    /// Toggle constrain priority
    pub fn toggle(&mut self) {
        self.fit_content = !self.fit_content;
    }

    /// Reset all columns dimensions to default
    pub fn reset(&mut self) {
        self.cols.clear()
    }

    /// Reset all columns observed max size
    pub fn fit(&mut self) {
        for (max, _, _) in &mut self.cols {
            *max = 0;
        }
    }
}
