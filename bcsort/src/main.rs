use clap::Parser;
use csv;
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
}

fn main() {
    let args = Args::parse();
    let buffer_size = parse_size(args.buffer_size).expect("Wrong format for buffer size") as usize;

    let (sender, receiver) = mpsc::channel();

    let handler = thread::spawn(move || {
        for dir_entry in args.input_dir.read_dir().unwrap() {
            csv::ReaderBuilder::new()
                .delimiter(b'\t')
                .from_path(dir_entry.unwrap().path())
                .unwrap()
                .records()
                .map(|x| x.unwrap())
                .for_each(|l| {
                    let mut a = Vec::new();
                    for f in &args.fields {
                        a.push(l.get(*f).unwrap());
                    }

                    sender.send(a.join("\t")).unwrap();
                });
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
