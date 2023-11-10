use crate::grid::{Frame, Grid};

pub struct ViewState<'a> {
    pub loading: Option<(&'static str, f64)>,
    pub streaming: bool,
    pub frame: &'a dyn Frame,
    pub err: Option<&'a str>,
    pub grid: &'a mut Grid,
}

pub trait View {
    fn tick(&mut self) -> ViewState;
}