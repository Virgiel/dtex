use ::dtex::Open;
use polars::prelude::DataFrame;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use pyo3_polars::PyDataFrame;

#[pyfunction]
fn ex(pydf: PyDataFrame) -> PyResult<()> {
    let df: DataFrame = pydf.into();
    ::dtex::run(Open::Polars(df));
    Ok(())
}

#[pymodule]
fn dtex(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(ex))?;

    Ok(())
}
