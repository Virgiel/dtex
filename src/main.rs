use std::path::PathBuf;

use clap::Parser;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(clap::Parser, Debug)]
pub struct Args {
    pub filename: Option<PathBuf>,
}

fn main() {
    let args = Args::parse();
    let filename = args.filename.unwrap();
    dtex::run(dtex::Open::File(filename));
}
