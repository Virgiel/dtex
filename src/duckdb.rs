use std::{
    ffi::{CStr, CString},
    fmt::Display,
    mem::MaybeUninit,
    sync::Arc,
};

use arrow::{
    array::{ArrayData, StructArray},
    ffi::{ArrowArray, FFI_ArrowArray, FFI_ArrowSchema},
    record_batch::RecordBatch,
};
use libduckdb_sys::{
    duckdb_arrow_array, duckdb_arrow_schema, duckdb_close, duckdb_connect, duckdb_connection,
    duckdb_data_chunk, duckdb_data_chunk_arrow_array, duckdb_database, duckdb_destroy_data_chunk,
    duckdb_destroy_pending, duckdb_destroy_prepare, duckdb_destroy_result, duckdb_disconnect,
    duckdb_execute_pending, duckdb_free, duckdb_open_ext, duckdb_pending_prepared_streaming,
    duckdb_pending_result, duckdb_prepare, duckdb_prepare_error, duckdb_prepared_statement,
    duckdb_query, duckdb_result, duckdb_result_arrow_schema, duckdb_result_error,
    duckdb_result_get_chunk, duckdb_result_is_streaming, duckdb_stream_fetch_chunk, DuckDBSuccess,
};

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

struct DB {
    db: duckdb_database,
}

impl DB {
    pub fn tmp() -> Result<Self> {
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
        Ok(Self { db })
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        unsafe { duckdb_close(&mut self.db) }
    }
}

pub struct Chunks {
    result: duckdb_result,
    idx: u64,
}

unsafe impl Send for Chunks {}

impl Iterator for Chunks {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        unsafe {
            let mut chunk = if duckdb_result_is_streaming(self.result) {
                duckdb_stream_fetch_chunk(self.result)
            } else {
                let chunk = duckdb_result_get_chunk(self.result, self.idx);
                self.idx += 1;
                chunk
            };
            if chunk.is_null() {
                let err = duckdb_result_error(&mut self.result);
                if !err.is_null() {
                    let msg = CStr::from_ptr(err).to_string_lossy().to_string();
                    return Some(Err(Error::Chunk(msg)));
                }
                return None;
            }
            let new = data_chunk_to_arrow(self.result, chunk);
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
        let db = DB::tmp()?;
        let mut con: duckdb_connection = std::ptr::null_mut();
        unsafe {
            if duckdb_connect(db.db, &mut con) != DuckDBSuccess {
                duckdb_disconnect(&mut con);
                return Err(Error::Connect);
            }
        }
        Ok(Self {
            db: Arc::new(db),
            con,
        })
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

    pub fn materialize(&self, query: &str) -> Result<Chunks> {
        let sql = CString::new(query).unwrap();
        let mut result: MaybeUninit<duckdb_result> = std::mem::MaybeUninit::uninit();

        unsafe {
            if duckdb_query(self.con, sql.as_ptr(), result.as_mut_ptr()) != DuckDBSuccess {
                let err = duckdb_result_error(result.as_mut_ptr());
                let message = CStr::from_ptr(err).to_string_lossy().to_string();
                return Err(Error::Execute(message));
            }

            Ok(Chunks {
                result: result.assume_init(),
                idx: 0,
            })
        }
    }

    pub fn stream(&self, query: &str) -> Result<Chunks> {
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
                idx: 0,
            })
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        unsafe { duckdb_disconnect(&mut self.con) }
    }
}

unsafe fn data_chunk_to_arrow(result: duckdb_result, chunk: duckdb_data_chunk) -> RecordBatch {
    let mut schema = FFI_ArrowSchema::empty();
    duckdb_result_arrow_schema(
        result,
        &mut std::ptr::addr_of_mut!(schema) as *mut _ as *mut duckdb_arrow_schema,
    );
    let mut arrays = FFI_ArrowArray::empty();
    duckdb_data_chunk_arrow_array(
        chunk,
        &mut std::ptr::addr_of_mut!(arrays) as *mut _ as *mut duckdb_arrow_array,
    );

    let arrow_array = ArrowArray::new(arrays, schema);
    let array_data = ArrayData::try_from(arrow_array).unwrap();
    let struct_array = StructArray::from(array_data);
    RecordBatch::from(struct_array)
}
