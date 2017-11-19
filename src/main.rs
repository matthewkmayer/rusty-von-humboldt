extern crate rusty_von_humboldt;

extern crate clap;
extern crate serde;
extern crate serde_json;
extern crate rayon;
extern crate stopwatch;

use stopwatch::Stopwatch;
use clap::App;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use rayon::prelude::*;

use rusty_von_humboldt::*;

fn main() {
    let _ = App::new("Rusty von Humboldt")
                          .version("0.1.0")
                          .author("Matthew Mayer <matthewkmayer@gmail.com>")
                          .about("Explore GitHub Archive data")
                          .get_matches();

    println!("Welcome to Rusty von Humboldt.");

    // In the future we'd have the list of files
    let file_list = vec!["../2017-01-01-15.json", "../2017-05-01-15.json", "../2017-10-01-15.json"];
    // parse_ze_file does file IO which is an antipattern with rayon.
    // Should figure out a way to read things in with a threadpool perhaps.
    let events: Vec<Event> = file_list
        .par_iter()
        .flat_map(|file_name| parse_ze_file(file_name).expect("Issue with file ingest"))
        .collect();

    // display something interesting
    println!("\nFound {} events", events.len());
    println!("\nevents first item is {:?}", events.first());
}

fn parse_ze_file(file_location: &str) -> Result<Vec<Event>, String> {
    let f = File::open(file_location).expect("file not found");

    let mut sw = Stopwatch::start_new();
    // temp_stringy only present since I can't get a par_iter directly from .split()
    let mut temp_stringy: Vec<String> = Vec::with_capacity(25000);
    for line in BufReader::new(f).lines() {
        match line {
            Ok(l) => temp_stringy.push(l),
            Err(_) => (),
        }
    }
    println!("file reading fun took {}ms", sw.elapsed_ms());

    sw.restart();
    let events: Vec<Event> = temp_stringy
        .par_iter()
        .map(|l| serde_json::from_str(&l).expect("Couldn't deserialize event file."))
        .collect();

    println!("Deserialization took {}ms", sw.elapsed_ms());
    Ok(events)
}