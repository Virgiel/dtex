use ::dtex::source::Source;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use pyo3_polars::PyDataFrame;

#[pyfunction]
fn ex(pydf: Vec<PyDataFrame>) -> PyResult<()> {
    ::dtex::run(pydf.into_iter().map(|pdt| Source::from_polars(pdt.0)));
    Ok(())
}

#[pymodule]
fn dtex(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(ex))?;

    Ok(())
}
