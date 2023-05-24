pub struct Nav {
    // Offset positions
    pub o_row: usize,
    pub o_col: usize,
    // Cursor positions
    pub c_row: usize,
    pub c_col: usize,
    // Max positions
    pub m_row: usize,
    pub m_col: usize,
    // View dimensions
    pub v_row: usize,
    pub v_col: usize,
}

impl Nav {
    pub fn new() -> Self {
        Self {
            o_row: 0,
            o_col: 0,
            c_row: 0,
            c_col: 0,
            m_row: 0,
            m_col: 0,
            v_row: 0,
            v_col: 0,
        }
    }

    pub fn up(&mut self) {
        self.c_row = self.c_row.saturating_sub(1);
    }

    pub fn down(&mut self) {
        self.c_row = self.c_row.saturating_add(1);
    }

    pub fn left(&mut self) {
        self.c_col = self.c_col.saturating_sub(1);
    }

    pub fn right(&mut self) {
        self.c_col = self.c_col.saturating_add(1);
    }

    pub fn left_roll(&mut self) {
        if self.c_col == 0 {
            self.c_col = self.m_col;
        } else {
            self.c_col -= 1;
        }
    }

    pub fn right_roll(&mut self) {
        if self.c_col == self.m_col {
            self.c_col = 0
        } else {
            self.c_col += 1;
        }
    }

    pub fn top(&mut self) {
        self.c_row = 0;
    }

    pub fn btm(&mut self) {
        self.c_row = self.m_row;
    }

    pub fn win_up(&mut self) {
        self.o_row = self.o_row.saturating_sub(self.v_row);
        self.c_row = self.o_row;
    }

    pub fn win_down(&mut self) {
        self.o_row = self.o_row.saturating_add(self.v_row);
        self.c_row = self.o_row + self.v_row.saturating_sub(1);
    }

    pub fn win_left(&mut self) {
        self.o_col = self.o_col.saturating_sub(self.v_col);
        self.c_col = self.o_col;
    }

    pub fn win_right(&mut self) {
        self.o_col = self.o_col.saturating_add(self.v_col);
        self.c_col = self.o_col + self.v_col.saturating_sub(1);
    }

    pub fn row_offset(&mut self, nb_row: usize, v_row: usize) -> usize {
        // Sync view dimension
        self.v_row = v_row;
        // Sync grid dimension
        self.m_row = nb_row.saturating_sub(1);
        // Ensure cursor pos fit in grid dimension
        self.c_row = self.c_row.min(self.m_row);
        // Ensure cursor is in view
        if self.c_row < self.o_row {
            self.o_row = self.c_row;
        } else if self.c_row >= self.o_row + v_row {
            self.o_row = self.c_row - v_row + 1;
        }
        self.o_row
    }

    pub fn col_iter(&mut self, nb_col: usize) -> impl Iterator<Item = usize> + '_ {
        // Sync grid dimension
        self.m_col = nb_col.saturating_sub(1);
        // Ensure cursor pos fit in grid dimension
        self.c_col = self.c_col.min(self.m_col);
        // Ensure cursor is in view
        if self.c_col < self.o_col {
            self.o_col = self.c_col;
        }
        // Reset view dimension
        self.v_col = 0;

        let amount_right = nb_col - self.c_col;
        let goal_left = self.c_col.saturating_sub(self.o_col);

        // Coll offset iterator
        std::iter::from_fn(move || -> Option<usize> {
            if self.v_col < nb_col {
                let step = self.v_col;
                self.v_col += 1;
                let result = if step <= goal_left {
                    // Reach goal
                    self.c_col - step
                } else if step < goal_left + amount_right {
                    // Then fill right
                    self.c_col + (step - goal_left)
                } else {
                    // Then fill left
                    self.c_col - (step - goal_left - amount_right)
                };
                if result < self.o_col {
                    self.o_col = result;
                } else if result > self.o_col + step {
                    self.o_col = result - step;
                }
                Some(result)
            } else {
                None
            }
        })
    }

    pub fn go_to(&mut self, (row, col): (usize, usize)) {
        self.c_row = row;
        self.c_col = col;
    }
}