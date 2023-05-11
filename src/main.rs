use std::path::PathBuf;

use clap::Parser;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(clap::Parser, Debug)]
pub struct Args {
    pub files: Vec<PathBuf>,
}

fn main() {
    let args = Args::parse();
    dtex::run(args.files.into_iter().map(|f| dtex::Open::File(f)).collect());
}
