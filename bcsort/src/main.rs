use bytesize::MB;
use clap::Parser;
use csv;
use ext_sort::{buffer::LimitedBufferBuilder, ExternalSorter, ExternalSorterBuilder};
use std::io::{self, prelude::*};
use std::path;
use std::sync::mpsc;
use std::{fs, thread};

/// Extract and sort blockchair data
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input directory
    input_dir: path::PathBuf,

    /// Fields to be extracted
    fields: Vec<usize>,
}

fn main() {
    let args = Args::parse();

    let (sender, receiver) = mpsc::channel();

    let handler = thread::spawn(move || {
        for dir_entry in args.input_dir.read_dir().unwrap() {

            let mut rdr = csv::ReaderBuilder::new()
                .delimiter(b'\t')
                .from_path(dir_entry.unwrap().path()).unwrap();

			for record in rdr.records() {
				sender.send(record).unwrap();
            }
        }
    });

    for r in receiver {
    	let l = r.unwrap();
		write!(io::stdout(), "{}", &l.get(args.fields[0]).unwrap());
    	for f in &args.fields[1..] {
			write!(io::stdout(), "\t{}", &l.get(*f).unwrap());
    	}
    	
    	writeln!(io::stdout());
    }

    handler.join().unwrap();

/*    //    env_logger::Builder::new().filter_level(log::LevelFilter::Debug).init();

    let input_reader = io::BufReader::new(fs::File::open("input.txt").unwrap());
    let mut output_writer = io::BufWriter::new(fs::File::create("output.txt").unwrap());

    let sorter: ExternalSorter<String, io::Error, LimitedBufferBuilder> =
        ExternalSorterBuilder::new()
            .with_tmp_dir(path::Path::new("./"))
            .with_buffer(LimitedBufferBuilder::new(50 * MB as usize, true))
            .build()
            .unwrap();

    let sorted = sorter.sort(input_reader.lines()).unwrap();

    for item in sorted.map(Result::unwrap) {
        output_writer
            .write_all(format!("{}\n", item).as_bytes())
            .unwrap();
    }
    output_writer.flush().unwrap();*/
}
