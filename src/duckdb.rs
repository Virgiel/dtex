use std::{
    ffi::{CStr, CString},
    fmt::Display,
    mem::MaybeUninit,
    sync::Arc,
};

use arrow::{
    array::{Array, ArrayData, StructArray},
    ffi::{ArrowArray, FFI_ArrowArray, FFI_ArrowSchema},
    record_batch::RecordBatch,
};
use libduckdb_sys::{
    duckdb_arrow_array, duckdb_arrow_array_scan, duckdb_arrow_schema, duckdb_arrow_stream,
    duckdb_close, duckdb_connect, duckdb_connection, duckdb_database, duckdb_destroy_data_chunk,
    duckdb_destroy_pending, duckdb_destroy_prepare, duckdb_destroy_result, duckdb_disconnect,
    duckdb_execute_pending, duckdb_free, duckdb_interrupt, duckdb_open_ext, duckdb_pending_error,
    duckdb_pending_execute_task, duckdb_pending_prepared_streaming, duckdb_pending_result,
    duckdb_pending_state_DUCKDB_PENDING_ERROR,
    duckdb_pending_state_DUCKDB_PENDING_RESULT_NOT_READY,
    duckdb_pending_state_DUCKDB_PENDING_RESULT_READY, duckdb_prepare, duckdb_prepare_error,
    duckdb_prepared_statement, duckdb_query, duckdb_query_progress, duckdb_result,
    duckdb_result_arrow_array, duckdb_result_arrow_schema, duckdb_result_error,
    duckdb_result_get_chunk, duckdb_result_is_streaming, duckdb_stream_fetch_chunk, DuckDBSuccess,
};

use crate::DataFrame;

#[derive(Debug)]
pub struct Error(String);

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
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
                return Err(Error(msg));
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
    _handle: Arc<Con>,
    result: duckdb_result,
    schema: Arc<FFI_ArrowSchema>,
    idx: u64,
}

impl Chunks {
    fn new(_handle: Arc<Con>, result: duckdb_result) -> Self {
        let mut schema = FFI_ArrowSchema::empty();
        unsafe {
            duckdb_result_arrow_schema(
                result,
                &mut std::ptr::addr_of_mut!(schema) as *mut _ as *mut duckdb_arrow_schema,
            );
        }
        Self {
            _handle,
            result,
            schema: Arc::new(schema),
            idx: 0,
        }
    }
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
                    return Some(Err(Error(msg)));
                }
                return None;
            }
            let mut array = FFI_ArrowArray::empty();
            duckdb_result_arrow_array(
                self.result,
                chunk,
                &mut std::ptr::addr_of_mut!(array) as *mut _ as *mut duckdb_arrow_array,
            );

            let array_data =
                ArrayData::try_from(ArrowArray::new(array, self.schema.clone())).unwrap();
            let struct_array = StructArray::from(array_data);
            let new = RecordBatch::from(struct_array);
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

struct Con {
    _db: Arc<DB>,
    con: duckdb_connection,
}

unsafe impl Send for Con {}
unsafe impl Sync for Con {}

impl Drop for Con {
    fn drop(&mut self) {
        unsafe { duckdb_disconnect(&mut self.con) }
    }
}

pub struct ConnCtx(Arc<Con>);

impl ConnCtx {
    pub fn progress(&self) -> f64 {
        unsafe { duckdb_query_progress(self.0.con as *mut _) }
    }

    pub fn interrupt(&self) {
        unsafe { duckdb_interrupt(self.0.con as *mut _) }
    }
}

pub struct Connection(Arc<Con>);

impl Connection {
    /// Open a in memory database
    pub fn mem() -> Result<Self> {
        let db = DB::tmp()?;
        let mut con: duckdb_connection = std::ptr::null_mut();
        unsafe {
            if duckdb_connect(db.db, &mut con) != DuckDBSuccess {
                duckdb_disconnect(&mut con);
                return Err(Error("Unknown connect error".into()));
            }
        }
        Ok(Self(Arc::new(Con {
            _db: Arc::new(db),
            con,
        })))
    }

    pub fn ctx(&self) -> ConnCtx {
        ConnCtx(self.0.clone())
    }

    pub fn bind(&self, frame: DataFrame) -> Result<()> {
        let name = CString::new("current").unwrap();
        assert_eq!(frame.0.batchs.len(), 1, "TODO concat array");
        let array = &frame.0.batchs[0];
        let schema = array.schema();
        let array = StructArray::try_from(array.clone()).unwrap();
        let schema = FFI_ArrowSchema::try_from(schema.as_ref()).unwrap();
        let array = FFI_ArrowArray::new(&array.to_data());
        let schema = Box::leak(Box::new(schema));
        let array = Box::leak(Box::new(array));
        let mut it: duckdb_arrow_stream = std::ptr::null_mut();
        unsafe {
            if duckdb_arrow_array_scan(
                self.0.con as *mut _,
                name.as_ptr(),
                schema as *mut _ as *mut _,
                array as *mut _ as *mut _,
                &mut it as *mut _,
            ) != DuckDBSuccess
            {
                Err(Error("Unknown arrow scan error".into()))
            } else {
                Ok(())
            }
        }
    }

    pub fn execute(&self, query: &str) -> Result<()> {
        let sql = CString::new(query).unwrap();
        let mut result: MaybeUninit<duckdb_result> = std::mem::MaybeUninit::uninit();
        unsafe {
            if duckdb_query(self.0.con, sql.as_ptr(), result.as_mut_ptr()) != DuckDBSuccess {
                let err = duckdb_result_error(result.as_mut_ptr());
                let message = CStr::from_ptr(err).to_string_lossy().to_string();
                duckdb_destroy_result(result.as_mut_ptr());
                return Err(Error(message));
            } else {
                duckdb_destroy_result(result.as_mut_ptr());
            }
        }
        Ok(())
    }

    pub fn query(&self, query: &str) -> Result<Chunks> {
        let sql = CString::new(query).unwrap();
        let mut stmt: duckdb_prepared_statement = std::ptr::null_mut();
        let mut pending: duckdb_pending_result = std::ptr::null_mut();
        let mut result: MaybeUninit<duckdb_result> = std::mem::MaybeUninit::uninit();

        unsafe {
            if duckdb_prepare(self.0.con, sql.as_ptr(), &mut stmt) != DuckDBSuccess {
                let err = duckdb_prepare_error(stmt);
                let message = CStr::from_ptr(err).to_string_lossy().to_string();
                duckdb_destroy_prepare(&mut stmt);
                return Err(Error(message));
            }
            if duckdb_pending_prepared_streaming(stmt, &mut pending) != DuckDBSuccess {
                duckdb_destroy_prepare(&mut stmt);
                duckdb_destroy_pending(&mut pending);
                return Err(Error("unknown pending error".into()));
            }
            duckdb_destroy_prepare(&mut stmt);

            // We need to manually consume all subtasks to catch errors
            loop {
                let state = duckdb_pending_execute_task(pending);

                #[allow(non_upper_case_globals)]
                match state {
                    duckdb_pending_state_DUCKDB_PENDING_RESULT_NOT_READY => continue,
                    duckdb_pending_state_DUCKDB_PENDING_RESULT_READY => break,
                    duckdb_pending_state_DUCKDB_PENDING_ERROR => {
                        let err = duckdb_pending_error(pending);
                        let msg = CStr::from_ptr(err).to_string_lossy().to_string();
                        duckdb_destroy_pending(&mut pending);
                        return Err(Error(msg));
                    }
                    state => unreachable!("Unexpected pending result state: {state}"),
                }
            }

            if duckdb_execute_pending(pending, result.as_mut_ptr()) != DuckDBSuccess {
                let err = duckdb_pending_error(pending);
                let msg = CStr::from_ptr(err).to_string_lossy().to_string();
                duckdb_destroy_pending(&mut pending);
                duckdb_destroy_result(result.as_mut_ptr());
                return Err(Error(msg));
            }
            duckdb_destroy_pending(&mut pending);

            Ok(Chunks::new(self.0.clone(), result.assume_init()))
        }
    }
}
