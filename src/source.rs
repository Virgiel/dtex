use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
};

use polars::prelude::{DataFrame, JsonFormat, LazyFrame, SerReader};

use crate::Open;

const PRELOAD_LEN: usize = 1024;

pub enum Source {
    Polars(DataFrame),
    Csv { path: PathBuf, delimiter: u8 },
    Json { path: PathBuf, format: JsonFormat },
    Avro(PathBuf),
    Parquet(PathBuf),
    Arrow(PathBuf),
    SQL(PathBuf),
}

impl Source {
    pub fn new(open: Open) -> crate::Result<Source> {
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
                    "json" => Self::Json {
                        path,
                        format: JsonFormat::Json,
                    },
                    "ndjson" | "jsonl" | "ldjson" | "ldj" => Self::Json {
                        path,
                        format: JsonFormat::JsonLines,
                    },
                    "avro" => Self::Avro(path),
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
            | Self::Json { path, .. }
            | Self::Avro(path)
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
            | Self::Json { path, .. }
            | Self::Avro(path)
            | Self::Parquet(path)
            | Self::Arrow(path)
            | Self::SQL(path) => Some(path.to_string_lossy().to_string()),
        }
    }

    pub fn preload(&self) -> crate::Result<DataFrame> {
        Ok(match self {
            Self::Polars(df) => df.clone(), // TODO whyyyyy
            Self::Csv { path, delimiter } => polars::io::csv::CsvReader::from_path(&path)?
                .with_delimiter(*delimiter)
                .infer_schema(Some(PRELOAD_LEN))
                .with_n_rows(Some(PRELOAD_LEN))
                .with_n_threads(Some(1))
                .finish()?,
            Self::Avro(path) => {
                let file = std::fs::File::open(path)?;
                polars::io::avro::AvroReader::new(file)
                    .with_n_rows(Some(PRELOAD_LEN))
                    .finish()?
            }
            Self::Parquet(path) => {
                let file = std::fs::File::open(path)?;
                polars::io::parquet::ParquetReader::new(file)
                    .with_n_rows(Some(PRELOAD_LEN))
                    .finish()?
            }
            Self::Arrow(path) => {
                let file = std::fs::File::open(path)?;
                polars::io::ipc::IpcReader::new(file)
                    .with_n_rows(Some(PRELOAD_LEN))
                    .finish()?
            }
            Self::Json { path, format } => match format {
                JsonFormat::Json => {
                    let file = std::fs::File::open(path)?;
                    polars::io::json::JsonReader::new(file)
                        .with_json_format(JsonFormat::Json)
                        .infer_schema_len(Some(PRELOAD_LEN))
                        .finish()?
                }
                JsonFormat::JsonLines => {
                    polars::io::ndjson_core::ndjson::JsonLineReader::from_path(path)?
                        .infer_schema_len(Some(PRELOAD_LEN))
                        .with_n_rows(Some(PRELOAD_LEN))
                        .with_n_threads(Some(1))
                        .finish()?
                }
            },
            Self::SQL(path) => {
                let file = std::fs::File::open(path)?;
                let mut file = BufReader::new(file);
                let mut buf = Vec::new();
                let mut ctx = polars::sql::SQLContext::new();
                let mut lazy = LazyFrame::default();
                while file.read_until(b';', &mut buf)? > 0 {
                    let sql = std::str::from_utf8(&buf)?;
                    std::mem::replace(&mut lazy, ctx.execute(sql)?);
                    buf.clear();
                }
                lazy.limit(PRELOAD_LEN as u32).collect()?
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

/*let mut schema = Schema::new();
let dfg = loop {
    let dfg = polars::io::csv::CsvReader::from_path(&path)
        .unwrap()
        .with_dtypes(Some(Arc::new(schema.clone())))
        .with_delimiter(b';')
        .with_n_rows(Some(1024))
        .finish();
    match dfg {
        Ok(dfg) => break dfg,
        Err(e) => {
            dbg!(&e);
            let str = e.to_string();
            if let Some(capture) = rg.captures(&str) {
                dbg!(&capture);
                let name = capture.get(1).unwrap().as_str();
                dbg!(name);
                schema.with_column(name.into(), DataType::Utf8);
            } else {
                Err(e).unwrap()
            }
        }
    }
};*/
