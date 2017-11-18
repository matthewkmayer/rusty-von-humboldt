extern crate clap;
#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;

use clap::App;
use std::fs::File;
use std::io::prelude::*;

#[derive(Serialize, Deserialize, Debug)]
struct Event {
    id: String,
    #[serde(rename = "type")]
    event_type: String,
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
    let mut f = File::open(sample_file).expect("file not found");

    let mut contents = String::new();
    f.read_to_string(&mut contents)
        .expect("something went wrong reading the file");

    println!("With text length: {}", contents.len());

    let foo = contents.split("\n").into_iter().map(|line| println!("line is {}\n", line));

    println!("foo is {:?}", foo);

    // parse it
    // let deserialized: Event = serde_json::from_str(&contents).expect("Couldn't deserialize event file.");

    // display something interesting
    // println!("deserialized is {:?}", deserialized);
}
