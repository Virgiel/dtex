use std::{
    fs::File,
    io::{BufRead, BufReader},
    panic::AssertUnwindSafe,
    path::{Path, PathBuf},
    sync::Arc,
};

use polars::prelude::{
    DataFrame, IntoLazy, LazyCsvReader, LazyFileListReader, LazyFrame, LazyJsonLineReader,
    ScanArgsIpc, ScanArgsParquet, Schema,
};

use crate::{
    error::Result,
    event::{Orchestrator, Task},
    utils::cache_regex,
};

const PRELOAD_LEN: usize = 1024;

pub struct LoadingTask {
    receiver: Task<DataFrame>, // Receiver for the backend task
    goal: Option<usize>,
}

pub struct Loader {
    task: Option<LoadingTask>,
    pub df: DataFrame,
    full: bool,
}

impl Loader {
    pub fn new(source: Arc<Source>, orchestrator: &Orchestrator) -> Self {
        let mut tmp = Self {
            task: None,
            df: DataFrame::default(),
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
            .unwrap_or(self.df.height() + self.full as usize)
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
        if self.df.height().max(
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
                    self.full = self.df.height() < task.goal.unwrap_or(usize::MAX);
                    self.task = None;
                    Ok(true)
                }
                Ok(None) => Ok(false),
                Err(it) => Err(it),
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
    Eager(DataFrame),
    Sql {
        lf: LazyFrame,
        sql: String,
    },
    File {
        path: PathBuf,
        display_path: String,
        kind: FileKind,
    },
}

enum FileKind {
    Csv,
    Json,
    Parquet,
    Arrow,
    SQL,
}

pub struct Source {
    name: String,
    kind: Kind,
}

impl Source {
    pub fn empty() -> Self {
        Self {
            name: "#".into(),
            kind: Kind::Eager(DataFrame::default()),
        }
    }

    pub fn from_polars(df: DataFrame) -> Self {
        Self {
            name: "polars".into(),
            kind: Kind::Eager(df),
        }
    }

    pub fn from_path(path: PathBuf) -> Result<Self> {
        let extension = path
            .extension()
            .unwrap_or_default()
            .to_str()
            .unwrap_or_default();
        let kind = match extension {
            "csv" | "tsv" => FileKind::Csv,
            "json" | "ndjson" | "jsonl" | "ldjson" | "ldj" => FileKind::Json,
            "parquet" | "pqt" => FileKind::Parquet,
            "arrow" => FileKind::Arrow,
            "sql" => FileKind::SQL,
            unsupported => return Err(format!("Unsupported file extension .{unsupported}").into()),
        };

        Ok(Self {
            name: path
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
            kind: Kind::File {
                display_path: path.to_string_lossy().to_string(),
                path: path.canonicalize().unwrap_or(path),
                kind,
            },
        })
    }

    pub fn from_sql(sql: &str, current: Option<&Self>) -> Result<Self> {
        let mut ctx = polars::sql::SQLContext::new();
        if let Some(current) = current {
            ctx.register("current", current.lazy_frame(&Schema::new())?);
        }
        let lf = ctx.execute(sql)?;
        Ok(Self {
            name: "shell".into(),
            kind: Kind::Sql {
                lf,
                sql: sql.into(),
            },
        })
    }

    pub fn sql(&self) -> &str {
        match &self.kind {
            Kind::Sql { sql, .. } => sql,
            Kind::Eager(_) | Kind::File { .. } => "SELECT * FROM current",
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn path(&self) -> Option<&Path> {
        match &self.kind {
            Kind::Eager(_) | Kind::Sql { .. } => None,
            Kind::File { path, .. } => Some(path),
        }
    }

    pub fn display_path(&self) -> Option<&str> {
        match &self.kind {
            Kind::Eager(_) | Kind::Sql { .. } => None,
            Kind::File { display_path, .. } => Some(display_path),
        }
    }

    /// Fast load of a in memory data frame
    fn sync_full(&self) -> Option<DataFrame> {
        match &self.kind {
            Kind::Eager(p) => Some(p.clone()),
            Kind::File { .. } | Kind::Sql { .. } => None,
        }
    }

    /// Automatic schema inference
    pub fn apply<T>(&self, lambda: impl Fn(LazyFrame) -> Result<T>) -> Result<T> {
        let mut schema = Schema::new();
        loop {
            // polars can panic
            let result: Result<T> = std::panic::catch_unwind(AssertUnwindSafe(|| {
                let lazy = self.lazy_frame(&schema)?;
                lambda(lazy)
            }))
            .map_err(|_| "polars failure")?;

            match result {
                Ok(df) => return Ok(df),
                Err(err) => {
                    let reg = cache_regex!("Could not parse `.*` as dtype `.*` at column '(.*)'");
                    if let Some(ma) = reg.captures(&err.0) {
                        schema.with_column(
                            ma.get(1).unwrap().as_str().into(),
                            polars::prelude::DataType::Utf8,
                        );
                    } else {
                        return Err(err);
                    }
                }
            }
        }
    }

    /// Load up to `limit` rows handling schema errors
    fn load(&self, limit: Option<usize>) -> Result<DataFrame> {
        self.apply(|lazy| {
            Ok(lazy
                .limit(limit.map(|n| n as u32).unwrap_or(u32::MAX))
                .collect()?)
        })
    }

    /// Lazy frame from source
    fn lazy_frame(&self, schema: &Schema) -> Result<LazyFrame> {
        Ok(match &self.kind {
            Kind::Eager(df) => df.clone().lazy(),
            Kind::Sql { lf, .. } => lf.clone(),
            Kind::File { path, kind, .. } => match kind {
                FileKind::Csv => {
                    let mut file = std::fs::File::open(path)?;
                    let delimiter = infer_cdv_delimiter(&mut file)?;
                    LazyCsvReader::new(path)
                        .with_dtype_overwrite(Some(schema))
                        .with_delimiter(delimiter)
                        .finish()?
                }
                FileKind::Json => {
                    LazyJsonLineReader::new(path.to_string_lossy().to_string()).finish()?
                }
                FileKind::Parquet => LazyFrame::scan_parquet(path, ScanArgsParquet::default())?,
                FileKind::Arrow => LazyFrame::scan_ipc(path, ScanArgsIpc::default())?,
                FileKind::SQL => {
                    let sql = std::fs::read_to_string(path)?;
                    let mut ctx = polars::sql::SQLContext::new();
                    ctx.execute(&sql)?
                }
            },
        })
    }
}

fn infer_cdv_delimiter(file: &mut File) -> std::io::Result<u8> {
    const DELIMITER: [u8; 4] = [b',', b';', b':', b'|'];
    let mut counter = [0; DELIMITER.len()];
    let mut file = BufReader::new(file);

    'main: loop {
        let buff = file.fill_buf()?;
        if buff.is_empty() {
            break 'main;
        }
        for c in buff {
            if *c == b'\n' {
                break 'main;
            }
            // Count occurrence of delimiter char
            if let Some((count, _)) = counter.iter_mut().zip(DELIMITER).find(|(_, d)| d == c) {
                *count += 1;
            }
        }
        let amount = buff.len();
        file.consume(amount);
    }

    // Return most used delimiter or ',' by default
    Ok(counter
        .iter()
        .zip(DELIMITER)
        .max_by_key(|(c, _)| *c)
        .map(|(_, d)| d)
        .unwrap_or(DELIMITER[0]))
}
