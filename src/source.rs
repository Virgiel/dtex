use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    thread::Thread,
};

use arrow::{
    datatypes::{Schema, SchemaRef},
    record_batch::RecordBatch,
};
use once_cell::sync::OnceCell;
use tempfile::NamedTempFile;

use crate::{
    array_to_iter,
    duckdb::{Chunks, Connection},
    error::Result,
    event::{Orchestrator, Task},
    Ty,
};

struct Pending {
    batches: Vec<RecordBatch>,
    full: bool,
    error: Option<String>,
}

pub struct StreamingState {
    goal: AtomicUsize,
    pending: Mutex<Pending>,
}

pub enum FrameSource {
    Full(DataFrame),
    Error {
        df: DataFrame,
        error: String,
    },
    Streaming {
        worker: Thread,
        state: Arc<StreamingState>,
        df: DataFrame,
        is_loading: bool,
    },
}

impl FrameSource {
    pub fn empty() -> Self {
        Self::full(DataFrame::empty())
    }

    pub fn full(full: DataFrame) -> Self {
        Self::Full(full)
    }

    pub fn streaming(preload: DataFrame, chunks: Chunks, orchestrator: Orchestrator) -> Self {
        let state = Arc::new(StreamingState {
            goal: AtomicUsize::new(0),
            pending: Mutex::new(Pending {
                batches: vec![],
                full: false,
                error: None,
            }),
        });
        let worker = {
            let state = state.clone();
            let loaded = preload.num_rows();
            std::thread::Builder::new()
                .name("streamer".into())
                .spawn(move || worker(loaded, state, chunks, orchestrator))
                .unwrap()
        }
        .thread()
        .clone();
        Self::Streaming {
            worker,
            state,
            df: preload,
            is_loading: true,
        }
    }

    pub fn tick(&mut self) {
        if let FrameSource::Streaming {
            state,
            df,
            is_loading,
            ..
        } = self
        {
            let mut lock = state.pending.lock().unwrap();
            df.extend(lock.batches.drain(..));
            if lock.full {
                drop(lock);
                *self = FrameSource::Full(std::mem::take(df))
            } else if let Some(error) = lock.error.take() {
                drop(lock);
                *self = FrameSource::Error {
                    df: std::mem::take(df),
                    error,
                }
            } else {
                drop(lock);
                *is_loading = state.goal.load(Ordering::Relaxed) > df.num_rows();
            }
        }
    }

    pub fn goal(&self, goal: usize) {
        // Goal is only used when streaming
        if let FrameSource::Streaming {
            state, df, worker, ..
        } = self
        {
            // No need to update loading goal if already loaded
            if goal > df.num_rows() {
                state.goal.store(goal, Ordering::Relaxed);
                worker.unpark(); // Wake loader
            }
        }
    }

    pub fn df(&self) -> &DataFrame {
        match self {
            FrameSource::Full(df)
            | FrameSource::Error { df, .. }
            | FrameSource::Streaming { df, .. } => df,
        }
    }

    pub fn is_loading(&self) -> bool {
        match self {
            FrameSource::Full(_) | FrameSource::Error { .. } => false,
            FrameSource::Streaming { is_loading, .. } => *is_loading,
        }
    }
}

impl Drop for FrameSource {
    fn drop(&mut self) {
        if let Self::Streaming { worker, state, .. } = self {
            drop(state); // Reduce arc count
            worker.unpark() // Wake worker for cancelation
        }
    }
}

fn worker(
    mut loaded: usize,
    state: Arc<StreamingState>,
    mut chunks: Chunks,
    orchestrator: Orchestrator,
) {
    let mut buff = Vec::new();
    loop {
        while loaded < state.goal.load(Ordering::Relaxed) {
            if Arc::strong_count(&state) == 1 {
                return;
            }
            match chunks.next() {
                Some(Ok(batch)) => {
                    loaded += batch.num_rows();
                    buff.push(batch)
                }
                Some(Err(err)) => {
                    state.pending.lock().unwrap().error = Some(err.to_string());
                    orchestrator.wake();
                    return;
                }
                None => {
                    state.pending.lock().unwrap().full = true;
                    orchestrator.wake();
                    return;
                }
            }
        }
        if Arc::strong_count(&state) == 1 {
            return;
        }

        if !buff.is_empty() {
            state.pending.lock().unwrap().batches.append(&mut buff);
            orchestrator.wake();
        }
        std::thread::park();
    }
}

pub enum Loader {
    Finished(Option<FrameSource>),
    Pending(Task<FrameSource>),
}

impl Loader {
    pub fn streaming(source: Arc<Source>, orchestrator: &Orchestrator) -> Self {
        Self::load(source, orchestrator, false)
    }

    pub fn full(source: Arc<Source>, orchestrator: &Orchestrator) -> Self {
        Self::load(source, orchestrator, true)
    }

    fn load(source: Arc<Source>, orchestrator: &Orchestrator, full: bool) -> Self {
        if let Some(df) = source.sync_full() {
            Self::Finished(Some(FrameSource::full(df)))
        } else {
            let orch = orchestrator.clone();
            Self::Pending(orchestrator.task(move || {
                let mut chunks = source.load(full)?; // TODO solve full
                if full {
                    let df: Result<DataFrame> = chunks.map(|r| r.map_err(|r| r.into())).collect();
                    Ok(FrameSource::Full(df?))
                } else {
                    let preload = chunks
                        .next()
                        .map(|r| r.map(|r| r.into()))
                        .unwrap_or_else(|| Ok(DataFrame::default()))?;
                    let orch = orch.clone();
                    Ok(FrameSource::streaming(preload, chunks, orch))
                }
            }))
        }
    }

    pub fn tick(&mut self) -> Result<Option<FrameSource>> {
        match self {
            Loader::Finished(src) => Ok(src.take()),
            Loader::Pending(task) => match task.tick() {
                Ok(Some(src)) => {
                    *self = Loader::Finished(None);
                    Ok(Some(src))
                }
                Ok(None) => Ok(None),
                Err(it) => {
                    *self = Loader::Finished(None);
                    Err(it)
                }
            },
        }
    }

    pub fn is_loading(&self) -> bool {
        matches!(self, Loader::Pending(_))
    }
}

enum Kind {
    Eager {
        df: DataFrame,
        parquet: OnceCell<NamedTempFile>, // TODO remove when using 'arrow_scan'
    },
    Sql {
        current: Option<Arc<Source>>,
        sql: String,
    },
    File {
        path: PathBuf,
        display_path: String,
    },
}

pub struct Source {
    name: String,
    kind: Kind,
}

impl Source {
    pub fn empty() -> Self {
        Self {
            name: "#".into(),
            kind: Kind::Eager {
                parquet: OnceCell::new(),
                df: DataFrame::empty(),
            },
        }
    }

    pub fn from_mem(name: String, df: DataFrame) -> Self {
        Self {
            name,
            kind: Kind::Eager {
                parquet: OnceCell::new(),
                df,
            },
        }
    }

    pub fn from_path(path: PathBuf) -> Result<Self> {
        let con = Connection::mem()?;
        con.execute(&format!("CREATE VIEW current AS SELECT * FROM {path:?}"))?;

        Ok(Self {
            name: path
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
            kind: Kind::File {
                display_path: path.to_string_lossy().to_string(),
                path: path.canonicalize().unwrap_or(path),
            },
        })
    }

    pub fn from_sql(sql: &str, current: Option<Arc<Self>>) -> Result<Self> {
        Ok(Self {
            name: "shell".into(),
            kind: Kind::Sql {
                sql: sql.to_string(),
                current,
            },
        })
    }

    fn con(&self) -> Result<Connection> {
        Ok(match &self.kind {
            Kind::Eager { df, parquet } => {
                let file = parquet.get_or_try_init(|| df.to_parquet())?;
                let con = Connection::mem()?;
                con.execute(&format!(
                    "CREATE VIEW current AS SELECT * FROM read_parquet({:?})",
                    file.path()
                ))?;
                con
            }
            Kind::Sql { current, .. } => match current {
                Some(it) => it.con()?,
                None => Connection::mem()?,
            },
            Kind::File { display_path, .. } => {
                let con = Connection::mem()?;
                con.execute(&format!(
                    "CREATE VIEW current AS SELECT * FROM '{display_path}'"
                ))?;
                con
            }
        })
    }

    pub fn sql(&self) -> &str {
        match &self.kind {
            Kind::Sql { sql, .. } => sql,
            Kind::Eager { .. } | Kind::File { .. } => "SELECT * FROM current",
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn path(&self) -> Option<&Path> {
        match &self.kind {
            Kind::Eager { .. } | Kind::Sql { .. } => None,
            Kind::File { path, .. } => Some(path),
        }
    }

    pub fn display_path(&self) -> Option<&str> {
        match &self.kind {
            Kind::Eager { .. } | Kind::Sql { .. } => None,
            Kind::File { display_path, .. } => Some(display_path),
        }
    }

    /// Fast load of a in memory data frame
    fn sync_full(&self) -> Option<DataFrame> {
        match &self.kind {
            Kind::Eager { df, .. } => Some(df.clone()),
            Kind::File { .. } | Kind::Sql { .. } => None,
        }
    }

    pub fn describe(&self) -> Result<Chunks> {
        let sql = match &self.kind {
            Kind::Sql { sql, .. } => format!("SUMMARIZE {sql}"),
            Kind::Eager { .. } | Kind::File { .. } => format!("SUMMARIZE SELECT * FROM current"),
        };
        let df = self.con()?.materialize(&sql)?;
        Ok(df)
    }

    pub fn load(&self, full: bool) -> Result<Chunks> {
        let sql = match &self.kind {
            Kind::Sql { sql, .. } => sql,
            Kind::Eager { .. } | Kind::File { .. } => "SELECT * FROM current",
        };
        if full {
            Ok(self.con()?.materialize(sql)?)
        } else {
            Ok(self.con()?.stream(sql)?)
        }
    }
}

#[derive(Clone)]
struct DataFrameImpl {
    schema: SchemaRef,
    pub batchs: Vec<RecordBatch>,
    row_count: usize,
}

impl DataFrameImpl {
    fn push(&mut self, batch: RecordBatch) {
        if self.schema.fields.is_empty() {
            self.schema = batch.schema();
            self.row_count = batch.num_rows();
            self.batchs = vec![batch];
        } else {
            assert_eq!(self.schema, batch.schema());
            self.row_count += batch.num_rows();
            self.batchs.push(batch);
        }
    }

    pub fn extend(&mut self, iter: impl Iterator<Item = RecordBatch>) {
        for batch in iter {
            self.push(batch);
        }
    }
}

impl Default for DataFrameImpl {
    fn default() -> Self {
        Self {
            batchs: vec![],
            schema: Arc::new(Schema::empty()),
            row_count: 0,
        }
    }
}

#[derive(Clone, Default)]
pub struct DataFrame(Arc<DataFrameImpl>);

impl DataFrame {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn iter(&self, idx: usize, mut skip: usize) -> impl Iterator<Item = Ty<'_>> + '_ {
        let pos = self.0.batchs.iter().position(|a| {
            if a.num_rows() > skip {
                true
            } else {
                skip -= a.num_rows();
                false
            }
        });
        let chunks = if let Some(pos) = pos {
            &self.0.batchs[pos..]
        } else {
            &[]
        };

        chunks
            .iter()
            .flat_map(move |c| array_to_iter(&c.columns()[idx]))
            .skip(skip)
    }

    pub fn num_rows(&self) -> usize {
        self.0.row_count
    }

    pub fn num_columns(&self) -> usize {
        self.0.schema.fields().len()
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.0.schema
    }

    pub fn to_parquet(&self) -> Result<NamedTempFile> {
        let mut tmp = NamedTempFile::new()?;
        let mut w = parquet::arrow::ArrowWriter::try_new(&mut tmp, self.0.schema.clone(), None)?;
        for batch in &self.0.batchs {
            w.write(batch)?;
        }
        w.close()?;
        Ok(tmp)
    }

    pub fn concat(&self, iter: impl Iterator<Item = RecordBatch>) -> Self {
        let mut tmp = self.0.as_ref().clone();
        tmp.extend(iter);
        Self(Arc::new(tmp))
    }

    pub fn extend(&mut self, iter: impl Iterator<Item = RecordBatch>) {
        match Arc::get_mut(&mut self.0) {
            Some(inner) => inner.extend(iter),
            None => *self = self.concat(iter),
        }
    }
}

impl From<RecordBatch> for DataFrame {
    fn from(value: RecordBatch) -> Self {
        std::iter::once(value).collect()
    }
}

impl FromIterator<RecordBatch> for DataFrame {
    fn from_iter<T: IntoIterator<Item = RecordBatch>>(iter: T) -> Self {
        let iter = iter.into_iter();
        let mut inner = DataFrameImpl::default();
        inner.extend(iter);
        Self(Arc::new(inner))
    }
}
