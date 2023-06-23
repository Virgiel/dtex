use std::{ptr::addr_of_mut, sync::OnceLock};

use ::dtex::{
    arrow::{
        array::{make_array, ArrayRef, AsArray},
        ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema},
        record_batch::RecordBatch,
    },
    DataFrame,
};
use pyo3::{
    exceptions::PyValueError, ffi::Py_uintptr_t, prelude::*, types::PyList, wrap_pyfunction,
};

static CACHE: OnceLock<Extractor> = OnceLock::new();
struct Extractor(Vec<(Py<PyAny>, fn(&PyAny) -> PyResult<DataFrame>)>);

impl Extractor {
    pub fn load(py: Python) -> Self {
        let mut extractors: Vec<(Py<PyAny>, fn(&PyAny) -> PyResult<DataFrame>)> = vec![];
        if let Ok(polars) = PyModule::import(py, "polars") {
            if let Ok(eager) = polars.getattr("DataFrame") {
                extractors.push((eager.into(), Self::extract_polars_eager))
            }
            if let Ok(eager) = polars.getattr("LazyFrame") {
                extractors.push((eager.into(), Self::extract_polars_lazy))
            }
        }
        if let Ok(pyarrow) = PyModule::import(py, "pyarrow") {
            if let Ok(batch) = pyarrow.getattr("RecordBatch") {
                extractors.push((batch.into(), Self::extract_py_arrow_batch))
            }
            if let Ok(table) = pyarrow.getattr("Table") {
                extractors.push((table.into(), Self::extract_py_arrow_table))
            }
        }
        if let Ok(pyarrow) = PyModule::import(py, "duckdb") {
            if let Ok(batch) = pyarrow.getattr("DuckDBPyRelation") {
                extractors.push((batch.into(), Self::extract_duckdb))
            }
        }

        Self(extractors)
    }

    fn extract_py_arrow(obj: &PyAny) -> PyResult<ArrayRef> {
        let mut array = FFI_ArrowArray::empty();
        let mut schema = FFI_ArrowSchema::empty();

        obj.call_method1(
            "_export_to_c",
            (
                addr_of_mut!(array) as Py_uintptr_t,
                addr_of_mut!(schema) as Py_uintptr_t,
            ),
        )?;

        let data = from_ffi(array, &schema).unwrap();
        let array = make_array(data);
        Ok(array)
    }

    fn extract_polars_eager(it: &PyAny) -> PyResult<DataFrame> {
        let series = it.call_method0("get_columns")?;
        let n = it.getattr("width")?.extract::<usize>()?;
        let mut columns = Vec::with_capacity(n);
        for c in series.iter()? {
            let c = c?.call_method0("rechunk")?;

            let name = c.getattr("name")?;
            let name = name.str()?.to_str()?;

            let arr = c.call_method0("to_arrow")?;
            let arr = Self::extract_py_arrow(arr)?;
            columns.push((name, arr));
        }
        Ok(RecordBatch::try_from_iter(columns.into_iter())
            .unwrap()
            .into())
    }

    fn extract_polars_lazy(it: &PyAny) -> PyResult<DataFrame> {
        let eager = it.call_method0("collect")?;
        Self::extract_polars_eager(eager)
    }

    fn extract_py_arrow_batch(it: &PyAny) -> PyResult<DataFrame> {
        let array = Self::extract_py_arrow(it)?;
        let struct_array = array.as_struct();
        Ok(RecordBatch::from(struct_array).into())
    }

    fn extract_py_arrow_table(it: &PyAny) -> PyResult<DataFrame> {
        let batches = it.call_method0("to_batches")?;
        let batches: &PyList = batches.downcast()?;
        batches
            .iter()
            .map(|b| {
                let array = Self::extract_py_arrow(b)?;
                let struct_array = array.as_struct();
                Ok(RecordBatch::from(struct_array))
            })
            .collect::<PyResult<_>>()
    }

    fn extract_duckdb(it: &PyAny) -> PyResult<DataFrame> {
        let table = it.call_method0("arrow")?;
        Self::extract_py_arrow_table(table)
    }

    pub fn extract(&self, py: Python, it: &PyAny) -> PyResult<DataFrame> {
        for (ty, lambda) in &self.0 {
            if it.is_instance(&ty.as_ref(py))? {
                return lambda(it);
            }
        }
        Err(PyValueError::new_err("not a supported source"))
    }
}

struct Source(DataFrame);

impl<'a> FromPyObject<'a> for Source {
    fn extract(ob: &'a PyAny) -> PyResult<Self> {
        Python::with_gil(|py| {
            CACHE
                .get_or_init(|| Extractor::load(py))
                .extract(py, ob)
                .map(|df| Source(df))
        })
    }
}

#[derive(FromPyObject)]
enum Args {
    Named(String, Source),
    #[pyo3(transparent)]
    Simple(Source), // This extraction never fails
}

impl Args {
    pub fn parts(self) -> (String, DataFrame) {
        match self {
            Args::Named(n, s) => (n, s.0),
            Args::Simple(s) => ("py".into(), s.0),
        }
    }
}

#[pyfunction]
fn ex(sources: Vec<Args>) -> PyResult<()> {
    ::dtex::run(sources.into_iter().map(|s| {
        let (name, df) = s.parts();
        ::dtex::Source::from_mem(name, df)
    }));
    Ok(())
}

#[pymodule]
fn dtex(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(ex))?;

    Ok(())
}
