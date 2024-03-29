use std::{
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use arrow::{
    datatypes::{Schema, SchemaRef},
    record_batch::RecordBatch,
};
use libduckdb_sys::duckdb_query_progress_type;

use crate::{
    array_to_iter,
    duckdb::{Chunks, Connection, DuckDb},
    error::Result,
    fmt::{Col, ColBuilder, GridBuffer},
    task::{Ctx, DuckTask, Runner, Task},
};

pub struct Pending {
    batches: Vec<RecordBatch>,
    full: bool,
    error: Option<String>,
}

pub enum StreamingFrame {
    Loaded(DataFrame),
    Error {
        df: DataFrame,
        error: String,
    },
    Streaming {
        task: Task<AtomicUsize, Pending>,
        df: DataFrame,
        is_loading: bool,
    },
}

impl StreamingFrame {
    pub fn empty() -> Self {
        Self::full(DataFrame::empty())
    }

    pub fn full(full: DataFrame) -> Self {
        Self::Loaded(full)
    }

    pub fn streaming(preloaded: DataFrame, chunks: Chunks, runner: Runner) -> Self {
        let loaded = preloaded.num_rows();
        let task = runner.task(
            AtomicUsize::new(0),
            Pending {
                batches: vec![],
                full: false,
                error: None,
            },
            move |ctx| Self::streaming_task(ctx, loaded, chunks),
        );
        Self::Streaming {
            task,
            df: preloaded,
            is_loading: true,
        }
    }

    pub fn take(&self) -> Self {
        Self::Loaded(self.df().clone())
    }

    pub fn tick(&mut self) {
        if let StreamingFrame::Streaming {
            task,
            df,
            is_loading,
            ..
        } = self
        {
            let (full, error) = task.lock(|p| {
                df.extend(p.batches.drain(..));
                (p.full, p.error.take())
            });
            if full {
                *self = StreamingFrame::Loaded(std::mem::take(df))
            } else if let Some(error) = error {
                *self = StreamingFrame::Error {
                    df: std::mem::take(df),
                    error,
                }
            } else {
                *is_loading = task.state().load(Ordering::Relaxed) > df.num_rows();
            }
        }
    }

    /// Update the loading goal
    pub fn goal(&self, goal: usize) {
        // Goal is only used when streaming
        if let StreamingFrame::Streaming { task, df, .. } = self {
            let prev = task.state().load(Ordering::Relaxed);
            if prev != goal {
                // Update goal
                task.state().store(goal, Ordering::Relaxed);
                // Wake worker if it need to start/stop working
                if prev > df.num_rows() || goal > df.num_rows() {
                    task.wake();
                }
            }
        }
    }

    pub fn df(&self) -> &DataFrame {
        match self {
            StreamingFrame::Loaded(df)
            | StreamingFrame::Error { df, .. }
            | StreamingFrame::Streaming { df, .. } => df,
        }
    }

    pub fn err(&self) -> Option<&str> {
        if let StreamingFrame::Error { error, .. } = &self {
            Some(error)
        } else {
            None
        }
    }

    pub fn is_streaming(&self) -> bool {
        !matches!(self, StreamingFrame::Loaded(_))
    }

    pub fn is_loading(&self) -> bool {
        match self {
            StreamingFrame::Loaded(_) | StreamingFrame::Error { .. } => false,
            StreamingFrame::Streaming { is_loading, .. } => *is_loading,
        }
    }

    /// Background streaming task
    fn streaming_task(ctx: Ctx<AtomicUsize, Pending>, mut loaded: usize, mut chunks: Chunks) {
        loop {
            while loaded < ctx.state().load(Ordering::Relaxed) {
                if ctx.canceled() {
                    return;
                }
                match chunks.next() {
                    Some(Ok(batch)) => {
                        loaded += batch.num_rows();
                        ctx.lock(|p| p.batches.push(batch))
                    }
                    Some(Err(err)) => {
                        ctx.lock(|p| p.error = Some(err.to_string()));
                        return;
                    }
                    None => {
                        ctx.lock(|p| p.full = true);
                        return;
                    }
                }
            }
            if ctx.canceled() {
                return;
            }
            ctx.wait();
        }
    }
}

pub enum FrameLoader {
    Finished(Option<StreamingFrame>),
    Pending(DuckTask<StreamingFrame>),
}

impl FrameLoader {
    pub fn load(source: Arc<Source>, runner: &Runner) -> Self {
        if let Some(df) = source.sync_full() {
            Self::Finished(Some(StreamingFrame::full(df)))
        } else {
            let _runner = runner.clone();
            Self::Pending(runner.duckdb(source, move |source, con| {
                let mut chunks = source.load(con)?;
                let preload = chunks
                    .next()
                    .map(|r| r.map(|r| r.into()))
                    .unwrap_or_else(|| Ok(DataFrame::default()))?;
                Ok(StreamingFrame::streaming(preload, chunks, _runner))
            }))
        }
    }

    pub fn tick(&mut self) -> Option<Result<StreamingFrame>> {
        match self {
            FrameLoader::Finished(src) => Ok(src.take()).transpose(),
            FrameLoader::Pending(task) => match task.tick() {
                Some(Ok(src)) => {
                    *self = FrameLoader::Finished(None);
                    Some(Ok(src))
                }
                Some(Err(it)) => {
                    *self = FrameLoader::Finished(None);
                    Some(Err(it))
                }
                None => None,
            },
        }
    }

    pub fn is_loading(&self) -> Option<duckdb_query_progress_type> {
        match self {
            FrameLoader::Finished(_) => None,
            FrameLoader::Pending(task) => Some(task.progress()),
        }
    }
}

#[derive(Clone)]
enum Kind {
    Empty,
    Eager(DataFrame),
    File { path: PathBuf, display_path: String },
}

pub struct Source {
    name: String,
    kind: Kind,
    sql: String,
    db: DuckDb,
}

impl Source {
    fn new(name: String, kind: Kind, sql: String) -> Result<Self> {
        let db = DuckDb::mem()?;
        let conn = db.conn()?;
        conn.execute("SET enable_progress_bar=true; SET enable_progress_bar_print=false;")?;

        match &kind {
            Kind::Empty => {}
            Kind::Eager(df) => {
                conn.bind(df.clone())?;
            }
            Kind::File { display_path, .. } => {
                if display_path.ends_with(".sql") {
                    let content = std::fs::read_to_string(display_path)?;
                    let queries = content
                        .split(';')
                        .filter(|s| !s.trim().is_empty())
                        .collect::<Vec<_>>();

                    match queries.as_slice() {
                        [] => {
                            conn.execute("CREATE TABLE current (i INTEGER)")?; // TODO what to do as default ?
                        }
                        [content @ .., tail] => {
                            for q in content {
                                conn.execute(q)?;
                            }
                            conn.execute(&format!("CREATE VIEW current AS {tail}"))?;
                        }
                    }
                } else {
                    let path = display_path
                        .trim_end_matches(".gz")
                        .trim_end_matches(".zst");
                    if [".parquet", ".csv", ".tsv", ".json", ".jsonl", ".ndjson"]
                        .iter()
                        .any(|s| path.ends_with(s))
                    {
                        conn.execute(&format!(
                            "CREATE VIEW current AS SELECT * FROM '{display_path}'"
                        ))?;
                    } else {
                        return Err("Unsupported file format".into());
                    }
                }
            }
        }

        Ok(Self {
            name,
            kind,
            sql,
            db,
        })
    }

    pub fn empty(name: String) -> Self {
        Self::new(name, Kind::Empty, String::new()).unwrap()
    }

    pub fn from_mem(name: String, df: DataFrame) -> Self {
        Self::new(name, Kind::Eager(df), "FROM current SELECT *".into()).unwrap()
    }

    pub fn from_path(path: &Path) -> Self {
        Self::new(
            path.file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
            Kind::File {
                display_path: path.to_string_lossy().to_string(),
                path: path.canonicalize().unwrap_or(path.to_path_buf()),
            },
            "FROM current SELECT *".into(),
        )
        .unwrap()
    }

    pub fn query(&self, sql: String) -> Self {
        Self {
            name: self.name.clone(),
            kind: self.kind.clone(),
            sql,
            db: self.db.clone(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn path(&self) -> Option<&Path> {
        match &self.kind {
            Kind::Empty | Kind::Eager { .. } => None,
            Kind::File { path, .. } => Some(path),
        }
    }

    pub fn display_path(&self) -> Option<&str> {
        match &self.kind {
            Kind::Empty | Kind::Eager { .. } => None,
            Kind::File { display_path, .. } => Some(display_path),
        }
    }

    /// Fast load of a in memory data frame
    fn sync_full(&self) -> Option<DataFrame> {
        match &self.kind {
            Kind::Empty => Some(DataFrame::empty()),
            Kind::Eager(df) => Some(df.clone()),
            Kind::File { .. } => None,
        }
    }

    pub fn conn(&self) -> Result<Connection> {
        Ok(self.db.conn()?)
    }

    pub fn describe(&self, con: Connection) -> Result<Chunks> {
        // TODO handle empty
        if let Kind::Empty = self.kind {
            if self.sql.is_empty() {
                return Err("Nothing to summarize".into());
            }
        }
        let sql = format!("SUMMARIZE {}", self.sql);
        Ok(con.query(&sql)?)
    }

    pub fn load(&self, con: Connection) -> Result<Chunks> {
        let sql = self.init_sql();
        Ok(con.query(sql)?)
    }

    pub fn init_sql(&self) -> &str {
        if self.sql.is_empty() {
            match self.kind {
                Kind::Empty => "",
                Kind::Eager(_) | Kind::File { .. } => "SELECT * FROM current",
            }
        } else {
            &self.sql
        }
    }
}

#[derive(Clone)]
pub struct DataFrameImpl {
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

impl Drop for DataFrameImpl {
    fn drop(&mut self) {
        // We might have to free a lot of memory so we defer to another thread
        let batchs = std::mem::take(&mut self.batchs);
        std::thread::spawn(move || drop(batchs));
    }
}

#[derive(Clone, Default)]
pub struct DataFrame(pub Arc<DataFrameImpl>);

impl DataFrame {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn iter(&self, buf: &mut GridBuffer, idx: usize, mut skip: usize, mut take: usize) -> Col {
        let mut col = ColBuilder::new(buf);
        let tmp = &mut col;
        for chunks in &self.0.batchs {
            if skip > chunks.num_rows() {
                skip -= chunks.num_rows()
            } else if take > 0 {
                array_to_iter(&chunks.columns()[idx], tmp, skip, take);
                take = take.saturating_sub(chunks.num_rows() - skip);
                skip = 0
            } else {
                break;
            }
        }
        col.build()
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
