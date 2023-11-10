use reedline::LineBuffer;

struct History<T, const N: usize> {
    buf: Vec<T>,
}

impl<T: Default + Eq, const N: usize> History<T, N> {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(N),
        }
    }

    pub fn push(&mut self, item: T) {
        if let Some(pos) = self.buf.iter().position(|it| it == &item) {
            self.buf.remove(pos);
        }
        if self.buf.len() == N {
            self.buf.pop();
        }
        self.buf.insert(0, item);
    }

    pub fn get(&self, idx: usize) -> &T {
        &self.buf[idx]
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }
}

pub struct Prompt<const H: usize> {
    history: History<String, H>,
    pos: Option<usize>,
    buffer: LineBuffer,
}

impl<const H: usize> Prompt<H> {
    pub fn new(init: &str) -> Self {
        let mut history = History::new();
        if !init.trim().is_empty() {
            history.push(init.into())
        }
        Self {
            history,
            pos: None,
            buffer: LineBuffer::from(init),
        }
    }

    /// Ensure buffer contains the right data
    fn solidify(&mut self) {
        if let Some(pos) = self.pos.take() {
            self.buffer.clear();
            self.buffer.insert_str(self.history.get(pos));
        }
    }

    pub fn exec(&mut self, cmd: PromptCmd) {
        match cmd {
            PromptCmd::Write(c) => {
                self.solidify();
                self.buffer.insert_char(c);
            }
            PromptCmd::Left => {
                self.solidify();
                self.buffer.move_left();
            }
            PromptCmd::Right => {
                self.solidify();
                self.buffer.move_right()
            }
            PromptCmd::Delete => {
                self.solidify();
                self.buffer.delete_left_grapheme();
            }
            PromptCmd::Prev => match &mut self.pos {
                Some(pos) if *pos + 1 < self.history.len() => *pos += 1,
                None if self.history.len() > 0 => self.pos = Some(0),
                _ => {}
            },
            PromptCmd::Next => match &mut self.pos {
                Some(0) => self.pos = None,
                Some(pos) => *pos = pos.saturating_sub(1),
                None => {}
            },
            PromptCmd::New(keep) => {
                let (str, _) = self.state();
                self.history.push(str.into());
                self.pos = keep.then_some(0);
                self.buffer.clear();
            }
        }
    }

    pub fn state(&self) -> (&str, usize) {
        match self.pos {
            Some(pos) => {
                let str = self.history.get(pos);
                (str, str.len())
            }
            None => (self.buffer.get_buffer(), self.buffer.insertion_point()),
        }
    }
}

pub enum PromptCmd {
    Write(char),
    Left,
    Right,
    Prev,
    Next,
    New(bool),
    Delete,
}
