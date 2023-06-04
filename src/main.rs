use std::path::PathBuf;

use clap::Parser;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(clap::Parser, Debug)]
pub struct Args {
    pub files: Vec<PathBuf>,
    #[arg(long)]
    pub sql: Option<String>,
}

fn main() {
    let args = Args::parse();
    dtex::run(
        args.files
            .into_iter()
            .map(dtex::source::Source::from_path)
            .chain(args.sql.map(|s| dtex::source::Source::from_sql(&s, None)))
            .map(|s| s.unwrap()),
    );
}
