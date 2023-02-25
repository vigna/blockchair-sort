use clap::Parser;
use ext_sort::{buffer::LimitedBufferBuilder, ExternalSorter, ExternalSorterBuilder};
use num_cpus;
use parse_size::parse_size;
use std::fs::File;
use std::io::{self, prelude::*};
use std::path;
use bytelines;

/// Extract and sort blockchair data
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input directory
    input_dir: path::PathBuf,

    /// Fields to be extracted
    fields: Vec<usize>,

    /// Size of the memory buffer in gigabytes.
    #[arg(short, long, default_value = "1GiB")]
    buffer_size: String,

    /// Number of initial lines to skip
    #[arg(short, long, default_value_t = 1)]
    skip: usize,
}

fn main() {
    let args = Args::parse();
    let fields = std::sync::Arc::new(args.fields);
    let buffer_size = parse_size(args.buffer_size).expect("Wrong format for buffer size") as usize;

    let files = args
        .input_dir
        .read_dir()
        .unwrap()
        .map(Result::unwrap)
        .map(|d| d.path())
        .collect::<Vec<std::path::PathBuf>>();

        let mut readers = vec![];

        let mut s = String::new();
        for dir_entry in files {
            let file = File::open(dir_entry).unwrap();
            let mut reader = io::BufReader::with_capacity(1024 * 1024, file);
            
            for _ in 0..args.skip { 
                reader.read_line(&mut s).unwrap();
            }

            readers.push(reader.lines());
        }

    let sorter: ExternalSorter<_, io::Error, LimitedBufferBuilder> =
        ExternalSorterBuilder::new()
            .with_tmp_dir(path::Path::new("/tmp"))
            .with_buffer(LimitedBufferBuilder::new(buffer_size, true))
            .with_threads_number(num_cpus::get())
            .build()
            .unwrap();

    sorter.sort(readers.into_iter().flatten()).unwrap().count();
}
