extern crate clap;
#[macro_use]
extern crate serde_derive;
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

#[derive(Serialize, Deserialize, Debug)]
struct Actor {
    id: i64,
    display_login: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Repo {
    id: i64,
    name: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Event {
    id: String,
    #[serde(rename = "type")]
    event_type: String,
    actor: Actor,
    repo: Repo,
}

fn main() {
    let _ = App::new("Rusty von Humboldt")
                          .version("0.0.1")
                          .author("Matthew Mayer <matthewkmayer@gmail.com>")
                          .about("Explore GitHub Archive data")
                          .get_matches();

    println!("Welcome to Rusty von Humboldt.");

    // Hardcoded path to a github archive file for now
    let sample_file = "../2017-01-01-15.json";
    let f = File::open(sample_file).expect("file not found");

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

    // display something interesting
    println!("\nFound {} events", events.len());
    println!("\nevents first item is {:?}", events.first());
}
