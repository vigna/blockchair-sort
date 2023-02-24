use clap::Parser;
use std::fs::File;
use ext_sort::{buffer::LimitedBufferBuilder, ExternalSorter, ExternalSorterBuilder};
use parse_size::parse_size;
use std::io::{self, prelude::*};
use num_cpus;
use std::path;
use std::sync::mpsc;
use std::thread;

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
    let buffer_size = parse_size(args.buffer_size).expect("Wrong format for buffer size") as usize;

    let (sender, receiver) = mpsc::channel();

    let handler = thread::spawn(move || {
        for dir_entry in args.input_dir.read_dir().unwrap() {
            let file = File::open(dir_entry.unwrap().path()).unwrap();
            for line in io::BufReader::with_capacity(1024*1024, file).lines().skip(args.skip) {
                let mut tab_pos = Vec::with_capacity(16);
                let mut chunks = Vec::with_capacity(args.fields.len());
                let s = line.unwrap();

                let mut last = 0;
                tab_pos.push(0);
                while let Some(pos) = s[last + 1..].find('\t') {
                    last += pos + 2;
                    tab_pos.push(last);
                }
                
                for f in &args.fields {
                    chunks.push(&s[tab_pos[*f]..tab_pos[*f+1] - 1]);
                }

                sender.send(chunks.join("\t")).unwrap();
            }
        }
    });

    let sorter: ExternalSorter<String, io::Error, LimitedBufferBuilder> =
        ExternalSorterBuilder::new()
            .with_tmp_dir(path::Path::new("./"))
            .with_buffer(LimitedBufferBuilder::new(buffer_size, true))
            .with_threads_number(num_cpus::get())
            .build()
            .unwrap();

    let sorted = sorter.sort(receiver.into_iter().map(|x| Ok(x))).unwrap();

    let ln = &[b'\n'];
    for item in sorted.map(Result::unwrap) {
        std::io::stdout().write_all(item.as_bytes()).unwrap();
        std::io::stdout().write(ln).unwrap();
    }
    std::io::stdout().flush().unwrap();
    handler.join().unwrap();
}
