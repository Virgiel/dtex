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
    Open,
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

pub enum Source {
    Memory(DataFrame),
    File {
        input: PathBuf,
        path: PathBuf,
        kind: FileKind,
    },
    Sql(String),
}

pub enum FileKind {
    Csv,
    Json,
    Parquet,
    Arrow,
    SQL,
}

impl Source {
    pub fn new(open: Open) -> Source {
        match open {
            Open::Polars(df) => Self::Memory(df),
            Open::File(path) => {
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
                    unsupported => {
                        panic!("Unsupported file extension .{unsupported}")
                    }
                };
                Self::File {
                    path: path.canonicalize().unwrap_or_else(|_| path.clone()),
                    input: path,
                    kind,
                }
            }
        }
    }

    pub fn name(&self) -> String {
        match self {
            Self::Memory(_) => "polars".to_string(),
            Self::Sql(_) => "shell".to_string(),
            Self::File { input, .. } => input
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
        }
    }

    pub fn path(&self) -> Option<&Path> {
        match self {
            Self::Memory(_) | Self::Sql(_) => None,
            Self::File { path, .. } => Some(path),
        }
    }

    pub fn display_path(&self) -> Option<String> {
        match self {
            Self::Memory(_) | Self::Sql(_) => None,
            Self::File { input, .. } => Some(input.to_string_lossy().to_string()),
        }
    }

    /// Fast load of a in memory data frame
    fn sync_full(&self) -> Option<DataFrame> {
        match self {
            Self::Memory(p) => Some(p.clone()),
            Self::File { .. } | Self::Sql(_) => None,
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
        Ok(match self {
            Self::Memory(df) => df.clone().lazy(),
            Self::Sql(sql) => polars::sql::SQLContext::new().execute(sql)?,
            Self::File { path, kind, .. } => match kind {
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
