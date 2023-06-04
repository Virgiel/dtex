use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow::{
    datatypes::{Schema, SchemaRef},
    record_batch::RecordBatch,
};
use once_cell::sync::OnceCell;
use tempfile::NamedTempFile;

use crate::{
    array_to_iter,
    duckdb::Connection,
    error::Result,
    event::{Orchestrator, Task},
    Ty,
};

const PRELOAD_LEN: usize = 1024;

pub struct LoadingTask {
    receiver: Task<DataFrameRef>, // Receiver for the backend task
    goal: Option<usize>,
}

pub struct Loader {
    task: Option<LoadingTask>,
    pub df: DataFrameRef,
    full: bool,
}

impl Loader {
    pub fn new(source: Arc<Source>, orchestrator: &Orchestrator) -> Self {
        let mut tmp = Self {
            task: None,
            df: Arc::new(DataFrame::empty()),
            full: false,
        };
        tmp.bg_load(source, Some(PRELOAD_LEN), orchestrator);
        tmp
    }

    pub fn reload(&mut self, source: Arc<Source>, orchestrator: &Orchestrator) {
        // Current task goal or current data frame length + 1 if full to handle size change
        let goal = self
            .task
            .as_ref()
            .map(|t| t.goal.unwrap_or(usize::MAX))
            .unwrap_or(self.df.num_rows() + self.full as usize)
            .max(PRELOAD_LEN);
        self.bg_load(source, Some(goal), orchestrator);
    }

    pub fn load_more(
        &mut self,
        source: Arc<Source>,
        goal: Option<usize>,
        orchestrator: &Orchestrator,
    ) {
        // Skip loading if we already loaded, or are loading, a bigger data frame
        if self.df.num_rows().max(
            self.task
                .as_ref()
                .map(|t| t.goal.unwrap_or(usize::MAX))
                .unwrap_or(0),
        ) >= goal.unwrap_or(usize::MAX)
        {
            return;
        }
        self.bg_load(source, goal, orchestrator);
    }

    // Start background loading task
    fn bg_load(&mut self, source: Arc<Source>, goal: Option<usize>, orchestrator: &Orchestrator) {
        if let Some(df) = source.sync_full() {
            self.df = df;
            self.full = true;
            self.task = None;
        } else {
            self.task = {
                let receiver = orchestrator.task(move || source.load(goal));
                Some(LoadingTask { receiver, goal })
            };
        };
    }

    pub fn tick(&mut self) -> Result<bool> {
        if let Some(task) = &mut self.task {
            match task.receiver.tick() {
                Ok(Some(df)) => {
                    self.df = df;
                    self.full = self.df.num_rows() < task.goal.unwrap_or(usize::MAX);
                    self.task = None;
                    Ok(true)
                }
                Ok(None) => Ok(false),
                Err(it) => {
                    self.task = None;
                    Err(it)
                }
            }
        } else {
            Ok(false)
        }
    }

    pub fn is_loading(&self) -> bool {
        self.task.is_some()
    }
}

enum Kind {
    Eager {
        df: DataFrameRef,
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
        let df = Arc::new(DataFrame::empty());
        Self {
            name: "#".into(),
            kind: Kind::Eager {
                parquet: OnceCell::new(),
                df,
            },
        }
    }

    pub fn from_mem(name: String, df: DataFrameRef) -> Self {
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
    fn sync_full(&self) -> Option<DataFrameRef> {
        match &self.kind {
            Kind::Eager { df, .. } => Some(df.clone()),
            Kind::File { .. } | Kind::Sql { .. } => None,
        }
    }

    pub fn describe(&self) -> Result<DataFrameRef> {
        let sql = match &self.kind {
            Kind::Sql { sql, .. } => format!("SUMMARIZE {sql}"),
            Kind::Eager { .. } | Kind::File { .. } => format!("SUMMARIZE SELECT * FROM current"),
        };
        let df = self.con()?.frame(&sql)?;
        Ok(df)
    }

    /// Load up to `limit` rows handling schema errors
    pub fn load(&self, limit: Option<usize>) -> Result<DataFrameRef> {
        let sql = match &self.kind {
            Kind::Eager { df, .. } => return Ok(df.clone()),
            Kind::Sql { sql, .. } => sql,
            Kind::File { .. } => "SELECT * FROM current",
        };
        let mut limit = limit.unwrap_or(usize::MAX);
        let chunks = self.con()?.chunks(sql)?;
        let df = chunks
            .map(|d| d.unwrap())
            .take_while(|d| {
                let taken = limit > 0;
                limit = limit.saturating_sub(d.num_rows());
                taken
            })
            .collect();
        Ok(Arc::new(df))
    }
}

pub type DataFrameRef = Arc<DataFrame>;

pub struct DataFrame {
    schema: SchemaRef,
    pub batchs: Vec<RecordBatch>,
    row_count: usize,
}

impl DataFrame {
    pub fn empty() -> Self {
        Self {
            batchs: vec![],
            schema: Arc::new(Schema::empty()),
            row_count: 0,
        }
    }

    pub fn iter(&self, idx: usize, mut skip: usize) -> impl Iterator<Item = Ty<'_>> + '_ {
        let pos = self.batchs.iter().position(|a| {
            if a.num_rows() > skip {
                true
            } else {
                skip -= a.num_rows();
                false
            }
        });
        let chunks = if let Some(pos) = pos {
            &self.batchs[pos..]
        } else {
            &[]
        };

        chunks
            .iter()
            .flat_map(move |c| array_to_iter(&c.columns()[idx]))
            .skip(skip)
    }

    pub fn num_rows(&self) -> usize {
        self.row_count
    }

    pub fn num_columns(&self) -> usize {
        self.schema.fields().len()
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn to_parquet(&self) -> Result<NamedTempFile> {
        let mut tmp = NamedTempFile::new()?;
        let mut w = parquet::arrow::ArrowWriter::try_new(&mut tmp, self.schema.clone(), None)?;
        for batch in &self.batchs {
            w.write(batch)?;
        }
        w.close()?;
        Ok(tmp)
    }
}

impl FromIterator<RecordBatch> for DataFrame {
    fn from_iter<T: IntoIterator<Item = RecordBatch>>(iter: T) -> Self {
        let mut iter = iter.into_iter();
        let mut empty = Self {
            batchs: vec![],
            schema: Arc::new(Schema::empty()),
            row_count: 0,
        };
        if let Some(first) = iter.next() {
            empty.schema = first.schema();
            empty.row_count = first.num_rows();
            empty.batchs = vec![first];

            for batch in iter {
                assert_eq!(empty.schema, batch.schema());
                empty.row_count += batch.num_rows();
                empty.batchs.push(batch);
            }
        }

        empty
    }
}
