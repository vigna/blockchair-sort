use clap::Parser;
use ext_sort::{buffer::LimitedBufferBuilder, ExternalSorter, ExternalSorterBuilder};
use num_cpus;
use parse_size::parse_size;
use std::fs::File;
use std::io::{self, prelude::*};
use std::path;
use std::sync::mpsc::{self, Sender};
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

fn process_chunk(chunk : Vec<std::path::PathBuf>, sender: Sender<String>, buffer_size: usize, fields: Vec<usize>, skip: usize) {
    let (thread_sender, thread_receiver) = mpsc::channel();

    let handler = thread::spawn(move || {

        for dir_entry in chunk {
            let file = File::open(dir_entry).unwrap();
            for line in io::BufReader::with_capacity(1024 * 1024, file)
                .lines()
                .skip(skip)
            {
                let mut tab_pos = Vec::with_capacity(16);
                let mut chunks = Vec::with_capacity(fields.len());
                let s = line.unwrap();

                let mut last = 0;
                tab_pos.push(0);
                while let Some(pos) = s[last + 1..].find('\t') {
                    last += pos + 2;
                    tab_pos.push(last);
                }

                for f in &fields {
                    chunks.push(&s[tab_pos[*f]..tab_pos[*f + 1] - 1]);
                }

                thread_sender.send(chunks.join("\t")).unwrap();
            }
        }
    });

    let sorter: ExternalSorter<String, io::Error, LimitedBufferBuilder> =
        ExternalSorterBuilder::new()
            .with_tmp_dir(path::Path::new("/tmp"))
            .with_buffer(LimitedBufferBuilder::new(buffer_size, true))
            .with_threads_number(num_cpus::get())
            .build()
            .unwrap();

    let sorted = sorter.sort(thread_receiver.into_iter().map(|x| Ok(x))).unwrap();

    let ln = &[b'\n'];
    for item in sorted.map(Result::unwrap) {
        std::io::stdout().write_all(item.as_bytes()).unwrap();
        std::io::stdout().write(ln).unwrap();
    }
    std::io::stdout().flush().unwrap();
    handler.join().unwrap();
}
fn main() {
    let args = Args::parse();
    let buffer_size = parse_size(args.buffer_size).expect("Wrong format for buffer size") as usize;

    let files = args
        .input_dir
        .read_dir()
        .unwrap()
        .map(Result::unwrap)
        .map(|d| d.path())
        .collect::<Vec<std::path::PathBuf>>();

    let mut thread_handles = vec![];

    let mut receivers: Vec<std::sync::mpsc::Receiver<String>> = vec![];

    for chunk in files.chunks(num_cpus::get()) {
        let (sender, receiver) = mpsc::channel();
        receivers.push(receiver);
        let fields = args.fields.to_owned();
        let chunk = chunk.to_owned();
        thread_handles.push(thread::spawn(move || {
            process_chunk(chunk, sender, buffer_size, fields.to_owned(), args.skip);
        }));
    }
    for handle in thread_handles {
        handle.join().unwrap();
    }

}
