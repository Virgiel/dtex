#[derive(Clone, Copy)]
pub enum Cmd {
    Constrain,
    Free,
    Less,
    More,
}

#[derive(Clone, Copy)]
enum Constraint {
    Fit,
    Fill,
    Fixe(usize),
}

#[derive(Clone, Copy)]
struct SizeStat {
    content: usize,
    header: usize,
    size: usize,
}

/// Size column based on previous observed length and constraints
/// Prevent column size from flickering on scroll and use all available space
#[derive(Clone)]
pub struct Sizer {
    cols: Vec<(SizeStat, Constraint)>,
    fit_content: bool,
}

impl Sizer {
    pub fn new() -> Self {
        Self {
            cols: vec![],
            fit_content: false,
        }
    }

    /// Size a column taking minimal amount of space
    pub fn fit(&mut self, idx: usize, len: usize, header_len: usize) -> usize {
        // Ensure we store info for this column
        if idx >= self.cols.len() {
            self.cols.resize(
                idx + 1,
                (
                    SizeStat {
                        content: 0,
                        header: 0,
                        size: 0,
                    },
                    Constraint::Fit,
                ),
            );
        }
        // Sync max len
        self.cols[idx].0.content = self.cols[idx].0.content.max(len);
        self.cols[idx].0.header = self.cols[idx].0.header.max(header_len);

        let size = self.get_size(idx, false);
        self.cols[idx].0.size = size;
        size
    }

    /// Size a column taking maximal amount of space
    pub fn fill(&mut self, idx: usize, available: &mut usize) -> usize {
        let fill_size = self.get_size(idx, true);
        let mut size = self.cols[idx].0.size;
        let missing = fill_size.saturating_sub(size);
        if missing > 0 {
            size += missing.min(*available);
            self.cols[idx].0.size = size;
            *available = available.saturating_sub(missing);
        }
        size
    }

    /// Size the column based on its constraint
    fn get_size(&self, idx: usize, fill: bool) -> usize {
        let (stat, constraint) = self.cols[idx];
        let max = if fill { usize::MAX } else { 25 };
        match constraint {
            Constraint::Fit if self.fit_content => stat.content.min(max),
            Constraint::Fit => stat.header.max(stat.content).min(max),
            Constraint::Fill => stat.header.max(stat.content),
            Constraint::Fixe(size) => size,
        }
        .max(self.min_size(idx))
    }

    /// Required size to display a column
    fn min_size(&self, idx: usize) -> usize {
        let (stat, _) = self.cols[idx];
        stat.content.max(stat.header).min(5)
    }

    /// Apply command
    pub fn cmd(&mut self, idx: usize, cmd: Cmd) {
        if idx >= self.cols.len() {
            return;
        }
        let stat = self.cols[idx].0;
        self.cols[idx].1 = match cmd {
            Cmd::Constrain => Constraint::Fit,
            Cmd::Free => Constraint::Fill,
            Cmd::Less => Constraint::Fixe(stat.size.saturating_sub(1).max(self.min_size(idx))),
            Cmd::More => Constraint::Fixe(stat.size.saturating_add(1).min(stat.content)),
        };
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
    pub fn fit_current_size(&mut self) {
        for (stat, _) in &mut self.cols {
            stat.content = 0;
        }
    }
}
