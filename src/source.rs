use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
    sync::Arc,
};

use polars::prelude::{
    DataFrame, IntoLazy, LazyCsvReader, LazyFileListReader, LazyFrame, LazyJsonLineReader,
    ScanArgsIpc, ScanArgsParquet, Schema, SerReader,
};

use crate::{error::Result, event::Orchestrator, utils::cache_regex, Open};

const PRELOAD_LEN: usize = 1024;

pub struct LoadingTask {
    receiver: oneshot::Receiver<Result<DataFrame>>, // Receiver for the backend task
    goal: Option<usize>,
}

pub struct Loader {
    source: Arc<Source>,
    task: Option<LoadingTask>,
    pub df: DataFrame,
    full: bool,
    pub error: String,
}

impl Loader {
    pub fn new(source: Source, orchestrator: &Orchestrator) -> Self {
        let source = Arc::new(source);
        let (df, full, task) = if let Some(df) = source.sync_full() {
            (df, true, None)
        } else {
            let schema = source.sync_schema().ok().flatten().unwrap_or_default();
            let df = DataFrame::from_rows_and_schema(&[], &schema).unwrap();
            let task = {
                let source = source.clone();
                let receiver = orchestrator.task(move || source.load(Some(PRELOAD_LEN)));
                Some(LoadingTask {
                    receiver,
                    goal: Some(PRELOAD_LEN),
                })
            };
            (df, false, task)
        };
        Self {
            task,
            df,
            full,
            source,
            error: String::new(),
        }
    }

    pub fn load(&mut self, goal: Option<usize>, orchestrator: &Orchestrator) {
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
        // Start background loading task
        let source = self.source.clone();
        self.task = Some(LoadingTask {
            receiver: orchestrator.task(move || source.load(goal)),
            goal,
        })
    }

    pub fn tick(&mut self) {
        if let Some(task) = &self.task {
            match task.receiver.try_recv() {
                Ok(result) => {
                    match result {
                        Ok(df) => {
                            self.full = df.height() < task.goal.unwrap_or(usize::MAX);
                            self.df = df;
                        }
                        Err(err) => self.error = err.0,
                    }
                    self.task = None;
                }
                Err(it) => match it {
                    oneshot::TryRecvError::Empty => {}
                    oneshot::TryRecvError::Disconnected => {
                        self.error = format!("Loader failed without error")
                    }
                },
            }
        }
    }

    pub fn is_loading(&self) -> bool {
        self.task.is_some()
    }
}

pub enum Source {
    Polars(DataFrame),
    Csv { path: PathBuf, delimiter: u8 },
    Json(PathBuf),
    Parquet(PathBuf),
    Arrow(PathBuf),
    SQL(PathBuf),
}

impl Source {
    pub fn new(open: Open) -> Result<Source> {
        Ok(match open {
            Open::Polars(df) => Self::Polars(df),
            Open::File(path) => {
                let extension = path
                    .extension()
                    .unwrap_or_default()
                    .to_str()
                    .unwrap_or_default();
                match extension {
                    "csv" | "tsv" => {
                        let mut file = std::fs::File::open(&path).unwrap();
                        let delimiter = infer_cdv_delimiter(&mut file).unwrap();
                        Self::Csv { path, delimiter }
                    }
                    "json" | "ndjson" | "jsonl" | "ldjson" | "ldj" => Self::Json(path),
                    "parquet" | "pqt" => Self::Parquet(path),
                    "arrow" => Self::Arrow(path),
                    "sql" => Self::SQL(path),
                    unsupported => {
                        return Err(format!("Unsupported file extension .{unsupported}").into())
                    }
                }
            }
        })
    }

    pub fn name(&self) -> String {
        match self {
            Self::Polars(_) => "polars".to_string(),
            Self::Csv { path, .. }
            | Self::Json(path)
            | Self::Parquet(path)
            | Self::Arrow(path)
            | Self::SQL(path) => path
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
        }
    }

    pub fn display_path(&self) -> Option<String> {
        match self {
            Self::Polars(_) => None,
            Self::Csv { path, .. }
            | Self::Json(path)
            | Self::Parquet(path)
            | Self::Arrow(path)
            | Self::SQL(path) => Some(path.to_string_lossy().to_string()),
        }
    }

    /// Fast load of the schema
    fn sync_schema(&self) -> Result<Option<Schema>> {
        match self {
            Source::Polars(p) => Ok(Some(p.schema())),
            Source::Csv { .. } | Source::Json { .. } | Source::SQL(_) => Ok(None),
            Source::Parquet(p) => {
                let fs = std::fs::File::open(p)?;
                Ok(Some(polars::io::parquet::ParquetReader::new(fs).schema()?))
            }
            Source::Arrow(p) => {
                let fs = std::fs::File::open(p)?;
                Ok(Some(polars::io::ipc::IpcReader::new(fs).schema()?))
            }
        }
    }

    /// Fast load of a in memory data frame
    fn sync_full(&self) -> Option<DataFrame> {
        match self {
            Source::Polars(p) => Some(p.clone()),
            Source::Csv { .. }
            | Source::Json { .. }
            | Source::SQL(_)
            | Source::Parquet(_)
            | Source::Arrow(_) => None,
        }
    }

    /// Load up to `limit` rows handling schema errors
    fn load(&self, limit: Option<usize>) -> Result<DataFrame> {
        let mut schema = Schema::new();
        loop {
            let lazy = self.lazy_frame(&schema)?;
            let result = lazy
                .limit(limit.map(|n| n as u32).unwrap_or(u32::MAX))
                .collect();
            match result {
                Ok(df) => return Ok(df),
                Err(err) => {
                    let str = err.to_string();
                    let reg = cache_regex!("Could not parse `.*` as dtype `.*` at column '(.*)'");
                    if let Some(ma) = reg.captures(&str) {
                        schema.with_column(
                            ma.get(1).unwrap().as_str().into(),
                            polars::prelude::DataType::Utf8,
                        );
                    } else {
                        return Err(err.into());
                    }
                }
            }
        }
    }

    /// Lazy frame from source
    fn lazy_frame(&self, schema: &Schema) -> Result<LazyFrame> {
        Ok(match self {
            Self::Polars(df) => df.clone().lazy(),
            Self::Csv { path, delimiter } => LazyCsvReader::new(path)
                .with_dtype_overwrite(Some(schema))
                .with_delimiter(*delimiter)
                .finish()?,
            Self::Parquet(path) => LazyFrame::scan_parquet(path, ScanArgsParquet::default())?,
            Self::Arrow(path) => LazyFrame::scan_ipc(path, ScanArgsIpc::default())?,
            Self::Json(path) => {
                LazyJsonLineReader::new(path.to_string_lossy().to_string()).finish()?
            }
            Self::SQL(path) => {
                let sql = std::fs::read_to_string(path)?;
                let mut ctx = polars::sql::SQLContext::new();
                ctx.execute(&sql)?
            }
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
