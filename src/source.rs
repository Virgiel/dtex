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
use once_cell::sync::OnceCell;
use tempfile::NamedTempFile;

use crate::{
    array_to_iter,
    duckdb::{Chunks, Connection},
    error::Result,
    task::{Ctx, DuckTask, Runner, Task},
    Ty,
};

pub struct Pending {
    batches: Vec<RecordBatch>,
    full: bool,
    error: Option<String>,
}

pub enum FrameSource {
    Full(DataFrame),
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

impl FrameSource {
    pub fn empty() -> Self {
        Self::full(DataFrame::empty())
    }

    pub fn full(full: DataFrame) -> Self {
        Self::Full(full)
    }

    pub fn streaming(preload: DataFrame, chunks: Chunks, runner: Runner) -> Self {
        let loaded = preload.num_rows();
        let task = runner.task(
            AtomicUsize::new(0),
            Pending {
                batches: vec![],
                full: false,
                error: None,
            },
            move |ctx| worker(ctx, loaded, chunks),
        );
        Self::Streaming {
            task,
            df: preload,
            is_loading: true,
        }
    }

    pub fn tick(&mut self) {
        if let FrameSource::Streaming {
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
                *self = FrameSource::Full(std::mem::take(df))
            } else if let Some(error) = error {
                *self = FrameSource::Error {
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
        if let FrameSource::Streaming { task, df, .. } = self {
            let prev = task.state().load(Ordering::Relaxed);
            if prev != goal {
                // Update goal
                task.state().store(goal, Ordering::Relaxed);
                // Wake worker if it need to start/stop working
                if prev > df.num_rows() || goal > df.num_rows() || true {
                    task.wake();
                }
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

    pub fn is_streaming(&self) -> bool {
        !matches!(self, FrameSource::Full(_))
    }

    pub fn is_loading(&self) -> bool {
        match self {
            FrameSource::Full(_) | FrameSource::Error { .. } => false,
            FrameSource::Streaming { is_loading, .. } => *is_loading,
        }
    }
}

fn worker(ctx: Ctx<AtomicUsize, Pending>, mut loaded: usize, mut chunks: Chunks) {
    let mut buff = Vec::with_capacity(50);
    loop {
        while loaded < ctx.state().load(Ordering::Relaxed) {
            if ctx.canceled() {
                return;
            }
            match chunks.next() {
                Some(Ok(batch)) => {
                    loaded += batch.num_rows();
                    buff.push(batch);
                    if buff.len() == buff.capacity() {
                        ctx.lock(|p| p.batches.append(&mut buff))
                    }
                }
                Some(Err(err)) => {
                    ctx.lock(|p| p.error = Some(err.to_string()));
                    return;
                }
                None => {
                    ctx.lock(|p| {
                        p.batches.append(&mut buff);
                        p.full = true;
                    });
                    return;
                }
            }
        }
        if ctx.canceled() {
            return;
        }

        if !buff.is_empty() {
            ctx.lock(|p| p.batches.append(&mut buff))
        }
        std::thread::park();
    }
}

pub enum Loader {
    Finished(Option<FrameSource>),
    Pending(DuckTask<FrameSource>),
}

impl Loader {
    pub fn load(source: Arc<Source>, runner: &Runner) -> Self {
        if let Some(df) = source.sync_full() {
            Self::Finished(Some(FrameSource::full(df)))
        } else {
            let _runner = runner.clone();
            Self::Pending(runner.duckdb(move |con| {
                let mut chunks = source.load(con)?;
                let preload = chunks
                    .next()
                    .map(|r| r.map(|r| r.into()))
                    .unwrap_or_else(|| Ok(DataFrame::default()))?;
                Ok(FrameSource::streaming(preload, chunks, _runner))
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

    pub fn is_loading(&self) -> Option<f64> {
        match self {
            Loader::Finished(_) => None,
            Loader::Pending(task) => Some(task.progress()),
        }
    }
}

enum Kind {
    Empty,
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
            kind: Kind::Empty,
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

    pub fn from_path(path: PathBuf) -> Self {
        Self {
            name: path
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
            kind: Kind::File {
                display_path: path.to_string_lossy().to_string(),
                path: path.canonicalize().unwrap_or(path),
            },
        }
    }

    pub fn from_sql(sql: &str, current: Option<Arc<Self>>) -> Self {
        Self {
            name: "shell".into(),
            kind: Kind::Sql {
                sql: sql.to_string(),
                current,
            },
        }
    }

    fn init(&self, con: Connection) -> Result<Connection> {
        Ok(match &self.kind {
            Kind::Empty => con,
            Kind::Eager { df, parquet } => {
                let file = parquet.get_or_try_init(|| df.to_parquet())?;
                con.execute(&format!(
                    "CREATE VIEW current AS SELECT * FROM read_parquet({:?})",
                    file.path()
                ))?;
                con
            }
            Kind::Sql { current, .. } => match current {
                Some(it) => it.init(con)?,
                None => con,
            },
            Kind::File { display_path, .. } => {
                con.execute(&format!(
                    "CREATE VIEW current AS SELECT * FROM '{display_path}'"
                ))?;
                con
            }
        })
    }

    pub fn sql(&self) -> &str {
        match &self.kind {
            Kind::Empty => "",
            Kind::Sql { sql, .. } => sql,
            Kind::Eager { .. } | Kind::File { .. } => "SELECT * FROM current",
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn path(&self) -> Option<&Path> {
        match &self.kind {
            Kind::Empty | Kind::Eager { .. } | Kind::Sql { .. } => None,
            Kind::File { path, .. } => Some(path),
        }
    }

    pub fn display_path(&self) -> Option<&str> {
        match &self.kind {
            Kind::Empty | Kind::Eager { .. } | Kind::Sql { .. } => None,
            Kind::File { display_path, .. } => Some(display_path),
        }
    }

    /// Fast load of a in memory data frame
    fn sync_full(&self) -> Option<DataFrame> {
        match &self.kind {
            Kind::Empty => Some(DataFrame::empty()),
            Kind::Eager { df, .. } => Some(df.clone()),
            Kind::File { .. } | Kind::Sql { .. } => None,
        }
    }

    pub fn describe(&self, con: Connection) -> Result<Chunks> {
        let sql = match &self.kind {
            Kind::Empty => return Err("Nothing to describe".into()),
            Kind::Sql { sql, .. } => format!("SUMMARIZE {sql}"),
            Kind::Eager { .. } | Kind::File { .. } => "SUMMARIZE SELECT * FROM current".into(),
        };
        Ok(self.init(con)?.query(&sql)?)
    }

    pub fn load(&self, con: Connection) -> Result<Chunks> {
        let sql = match &self.kind {
            Kind::Empty => return Err("Nothing to load".into()),
            Kind::Sql { sql, .. } => sql,
            Kind::Eager { .. } | Kind::File { .. } => "SELECT * FROM current",
        };
        Ok(self.init(con)?.query(sql)?)
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

impl Drop for DataFrameImpl {
    fn drop(&mut self) {
        // We might have to free a lot of memory so we defer to another thread
        let batchs = std::mem::take(&mut self.batchs);
        std::thread::spawn(move || drop(batchs));
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
