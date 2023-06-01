use std::{
    ffi::{CStr, CString},
    fmt::Display,
    mem::MaybeUninit,
    sync::Arc, time::Duration,
};

use arrow::{
    array::{make_array, Array, StructArray},
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    ffi_stream::FFI_ArrowArrayStream,
    record_batch::RecordBatch,
};
use arrow2::{
    array::{to_data, PrimitiveArray, Utf8Array},
    types::NativeType,
};
use libduckdb_sys::{
    duckdb_arrow_array_scan, duckdb_close, duckdb_column_name, duckdb_connect, duckdb_connection,
    duckdb_data_chunk, duckdb_data_chunk_get_column_count, duckdb_data_chunk_get_size,
    duckdb_data_chunk_get_vector, duckdb_database, duckdb_destroy_data_chunk,
    duckdb_destroy_logical_type, duckdb_destroy_pending, duckdb_destroy_prepare,
    duckdb_destroy_result, duckdb_disconnect, duckdb_execute_pending, duckdb_free,
    duckdb_get_type_id, duckdb_open_ext, duckdb_pending_prepared_streaming, duckdb_pending_result,
    duckdb_prepare, duckdb_prepare_error, duckdb_prepared_statement, duckdb_query, duckdb_result,
    duckdb_result_chunk_count, duckdb_result_error, duckdb_result_get_chunk,
    duckdb_stream_fetch_chunk, duckdb_string_is_inlined, duckdb_string_t, duckdb_vector,
    duckdb_vector_get_column_type, duckdb_vector_get_data, duckdb_vector_get_validity,
    DuckDBSuccess, DUCKDB_TYPE_DUCKDB_TYPE_BIGINT, DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE,
    DUCKDB_TYPE_DUCKDB_TYPE_FLOAT, DUCKDB_TYPE_DUCKDB_TYPE_INTEGER,
    DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT, DUCKDB_TYPE_DUCKDB_TYPE_TIME,
    DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS,
    DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS, DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S,
    DUCKDB_TYPE_DUCKDB_TYPE_TINYINT, DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT,
    DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER, DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT,
    DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT, DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR,
};
use polars::{prelude::DataFrame, series::Series};

#[derive(Debug)]
pub enum Error {
    Open(String),
    Connect,
    Prepare(String),
    Execute(String),
    Chunk(String),
    Unknown,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Open(msg) => writeln!(f, "open: {msg}"),
            Error::Connect => writeln!(f, "connect"),
            Error::Prepare(msg) => writeln!(f, "open: {msg}"),
            Error::Execute(msg) => writeln!(f, "open: {msg}"),
            Error::Chunk(msg) => writeln!(f, "open: {msg}"),
            Error::Unknown => writeln!(f, "unknown"),
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;

struct DB(duckdb_database);

impl DB {
    pub fn mem() -> Result<Self> {
        let mut db: duckdb_database = std::ptr::null_mut();
        unsafe {
            let mut err = std::ptr::null_mut();
            if duckdb_open_ext(
                std::ptr::null_mut(),
                &mut db,
                std::ptr::null_mut(),
                &mut err,
            ) != DuckDBSuccess
            {
                let msg = CStr::from_ptr(err).to_string_lossy().to_string();
                duckdb_free(err as *mut _);
                duckdb_close(&mut db);
                return Err(Error::Open(msg));
            }
        }
        Ok(Self(db))
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        unsafe { duckdb_close(&mut self.0) }
    }
}

pub struct Chunks {
    result: duckdb_result,
}

impl Iterator for Chunks {
    type Item = Result<DataFrame>;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            let mut chunk = duckdb_stream_fetch_chunk(self.result);
            if chunk.is_null() {
                let err = duckdb_result_error(&mut self.result);
                if !err.is_null() {
                    let msg = CStr::from_ptr(err).to_string_lossy().to_string();
                    return Some(Err(Error::Chunk(msg)));
                }
                return None;
            }
            let new = data_chunk_to_arrow(&mut self.result, chunk);
            duckdb_destroy_data_chunk(&mut chunk);
            Some(Ok(new))
        }
    }
}

impl Drop for Chunks {
    fn drop(&mut self) {
        unsafe { duckdb_destroy_result(&mut self.result) }
    }
}

pub struct Connection {
    db: Arc<DB>,
    con: duckdb_connection,
}

impl Connection {
    /// Open a in memory database
    pub fn mem() -> Result<Self> {
        Self::connect(Arc::new(DB::mem()?))
    }

    fn connect(db: Arc<DB>) -> Result<Self> {
        let mut con: duckdb_connection = std::ptr::null_mut();
        unsafe {
            if duckdb_connect(db.0, &mut con) != DuckDBSuccess {
                duckdb_disconnect(&mut con);
                return Err(Error::Connect);
            }
        }
        Ok(Self { db, con })
    }

    pub fn bind_arrow(&self, name: &str, frame: &DataFrame) -> Result<()> {
        assert_eq!(frame.n_chunks(), 1);
        let batch = RecordBatch::try_from_iter(
            frame
                .get_columns()
                .iter()
                .map(|s| (s.name(), make_array(to_data(s.to_arrow(0).as_ref())))),
        )
        .unwrap();
        let schema = FFI_ArrowSchema::try_from(batch.schema().as_ref()).unwrap();
        let struct_array = StructArray::from(batch);
        let array = FFI_ArrowArray::new(&struct_array.to_data());
        let array = Box::into_raw(Box::new(array));
        let schema = Box::into_raw(Box::new(schema));
        let mut stream = std::ptr::null_mut();
        unsafe {
            if duckdb_arrow_array_scan(
                self.con,
                name.as_ptr() as *mut _,
                schema as *mut _,
                array as *mut _,
                &mut stream,
            ) != DuckDBSuccess
            {
                return Err(Error::Unknown);
            }
            let stream = Box::from_raw(stream as *mut FFI_ArrowArrayStream);
            Box::leak(stream); // TODO finalize handle stream
            std::thread::sleep(Duration::from_millis(10000));
        }
        Ok(())
    }

    pub fn execute(&self, query: &str) -> Result<()> {
        let sql = CString::new(query).unwrap();
        let mut result: MaybeUninit<duckdb_result> = std::mem::MaybeUninit::uninit();
        unsafe {
            if duckdb_query(self.con, sql.as_ptr(), result.as_mut_ptr()) != DuckDBSuccess {
                let err = duckdb_result_error(result.as_mut_ptr());
                let message = CStr::from_ptr(err).to_string_lossy().to_string();
                duckdb_destroy_result(result.as_mut_ptr());
                return Err(Error::Execute(message));
            } else {
                duckdb_destroy_result(result.as_mut_ptr());
            }
        }
        Ok(())
    }

    pub fn frame(&self, query: &str) -> Result<DataFrame> {
        let sql = CString::new(query).unwrap();
        let mut result: MaybeUninit<duckdb_result> = std::mem::MaybeUninit::uninit();

        unsafe {
            if duckdb_query(self.con, sql.as_ptr(), result.as_mut_ptr()) != DuckDBSuccess {
                let err = duckdb_result_error(result.as_mut_ptr());
                let message = CStr::from_ptr(err).to_string_lossy().to_string();
                return Err(Error::Execute(message));
            }

            let mut result = result.assume_init();
            let nb_chunk = duckdb_result_chunk_count(result);

            let df = (0..nb_chunk)
                .map(|i| {
                    let mut chunk = duckdb_result_get_chunk(result, i);
                    if chunk.is_null() {
                        let err = duckdb_result_error(&mut result);

                        let msg = CStr::from_ptr(err).to_string_lossy().to_string();
                        return Err(Error::Chunk(msg));
                    }
                    let new = data_chunk_to_arrow(&mut result, chunk);
                    duckdb_destroy_data_chunk(&mut chunk);
                    Ok(new)
                })
                .map(|d| d.unwrap())
                .reduce(|mut a, b| {
                    a.extend(&b).unwrap();
                    a
                })
                .unwrap_or_default();
            Ok(df)
        }
    }

    pub fn chunks(&self, query: &str) -> Result<Chunks> {
        let sql = CString::new(query).unwrap();
        let mut stmt: duckdb_prepared_statement = std::ptr::null_mut();
        let mut pending: duckdb_pending_result = std::ptr::null_mut();
        let mut result: MaybeUninit<duckdb_result> = std::mem::MaybeUninit::uninit();

        let tmp = (|| unsafe {
            if duckdb_prepare(self.con, sql.as_ptr(), &mut stmt) != DuckDBSuccess {
                let err = duckdb_prepare_error(stmt);
                let message = CStr::from_ptr(err).to_string_lossy().to_string();
                return Err(Error::Prepare(message));
            }
            if duckdb_pending_prepared_streaming(stmt, &mut pending) != DuckDBSuccess {
                return Err(Error::Unknown);
            }
            if duckdb_execute_pending(pending, result.as_mut_ptr()) != DuckDBSuccess {
                duckdb_destroy_result(result.as_mut_ptr());
                return Err(Error::Unknown);
            }
            Ok(())
        })();
        unsafe {
            duckdb_destroy_prepare(&mut stmt);
            duckdb_destroy_pending(&mut pending);

            tmp.map(|_| Chunks {
                result: result.assume_init(),
            })
        }
    }

    pub fn try_clone(&self) -> Result<Self> {
        Self::connect(self.db.clone())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        unsafe { duckdb_disconnect(&mut self.con) }
    }
}

unsafe fn get_str(str: &duckdb_string_t) -> &str {
    let raw: &[u8] = if duckdb_string_is_inlined(*str) {
        let size = str.value.inlined.length;
        let ptr = &str.value.inlined.inlined as *const _ as *const u8;
        std::slice::from_raw_parts(ptr, size as usize)
    } else {
        let size = str.value.pointer.length;
        std::slice::from_raw_parts(str.value.pointer.ptr.cast(), size as usize)
    };
    let str = std::str::from_utf8_unchecked(raw);
    str
}

unsafe fn utf_8_array(nb: u64, vector: duckdb_vector) -> Utf8Array<i32> {
    let data = duckdb_vector_get_data(vector);
    let validity = duckdb_vector_get_validity(vector);
    let data: &[duckdb_string_t] = unsafe { std::slice::from_raw_parts(data.cast(), nb as usize) };
    let validity: Option<&[u64]> = (!validity.is_null())
        .then(|| unsafe { std::slice::from_raw_parts(validity, nb as usize / 64 + 1) });
    let iter = data.iter().enumerate().map(|(i, s)| {
        validity
            .map(|v| {
                let entry_idx = i / 64;
                let idx_in_entry = i % 64;
                v[entry_idx] & (1 << idx_in_entry) != 0
            })
            .unwrap_or(true)
            .then(|| get_str(s))
    });

    Utf8Array::from_trusted_len_iter(iter)
}

unsafe fn primitive_array<T: NativeType>(nb: u64, vector: duckdb_vector) -> PrimitiveArray<T> {
    let data = duckdb_vector_get_data(vector);
    let validity = duckdb_vector_get_validity(vector);
    let data: &[T] = unsafe { std::slice::from_raw_parts(data.cast(), nb as usize) };
    let validity: Option<&[u64]> = validity
        .is_null()
        .then(|| unsafe { std::slice::from_raw_parts(validity, nb as usize / 64 + 1) });
    let iter = data.iter().enumerate().map(|(i, s)| {
        validity
            .map(|v| {
                let entry_idx = i / 64;
                let idx_in_entry = i % 64;
                v[entry_idx] & (1 << idx_in_entry) != 0
            })
            .unwrap_or(true)
            .then_some(*s)
    });

    PrimitiveArray::from_trusted_len_iter(iter)
}

unsafe fn data_chunk_to_arrow(result: &mut duckdb_result, chunk: duckdb_data_chunk) -> DataFrame {
    let column_count = duckdb_data_chunk_get_column_count(chunk);
    let row_count = duckdb_data_chunk_get_size(chunk);
    let mut series = Vec::with_capacity(column_count as usize);
    for c in 0..column_count {
        let vector = duckdb_data_chunk_get_vector(chunk, c);
        let mut ty = duckdb_vector_get_column_type(vector);
        let id = duckdb_get_type_id(ty);
        let array = match id {
            DUCKDB_TYPE_DUCKDB_TYPE_TINYINT => primitive_array::<i8>(row_count, vector).boxed(),
            DUCKDB_TYPE_DUCKDB_TYPE_SMALLINT => primitive_array::<i16>(row_count, vector).boxed(),
            DUCKDB_TYPE_DUCKDB_TYPE_INTEGER => primitive_array::<i32>(row_count, vector).boxed(),
            DUCKDB_TYPE_DUCKDB_TYPE_BIGINT => primitive_array::<i64>(row_count, vector).boxed(),
            DUCKDB_TYPE_DUCKDB_TYPE_UTINYINT => primitive_array::<u8>(row_count, vector).boxed(),
            DUCKDB_TYPE_DUCKDB_TYPE_USMALLINT => primitive_array::<u16>(row_count, vector).boxed(),
            DUCKDB_TYPE_DUCKDB_TYPE_UINTEGER => primitive_array::<u32>(row_count, vector).boxed(),
            DUCKDB_TYPE_DUCKDB_TYPE_UBIGINT => primitive_array::<u64>(row_count, vector).boxed(),
            DUCKDB_TYPE_DUCKDB_TYPE_FLOAT => primitive_array::<f32>(row_count, vector).boxed(),
            DUCKDB_TYPE_DUCKDB_TYPE_DOUBLE => primitive_array::<f64>(row_count, vector).boxed(),
            DUCKDB_TYPE_DUCKDB_TYPE_TIME
            | DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP
            | DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_MS
            | DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_NS
            | DUCKDB_TYPE_DUCKDB_TYPE_TIMESTAMP_S => {
                // TODO
                primitive_array::<i64>(row_count, vector).boxed()
            }
            DUCKDB_TYPE_DUCKDB_TYPE_VARCHAR => utf_8_array(row_count, vector).boxed(),
            ty => unimplemented!("DuckDB type {ty}"),
        };
        let name = duckdb_column_name(result, c);
        let name = std::ffi::CStr::from_ptr(name);
        series.push(Series::try_from((name.to_string_lossy().as_ref(), array)).unwrap());
        duckdb_destroy_logical_type(&mut ty);
    }
    DataFrame::new_no_checks(series)
}
