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
    let mut sw = Stopwatch::start_new();
    let _ = App::new("Rusty von Humboldt")
                          .version("0.1.0")
                          .author("Matthew Mayer <matthewkmayer@gmail.com>")
                          .about("Explore GitHub Archive data")
                          .get_matches();

    println!("Welcome to Rusty von Humboldt.");

    // In the future we'd have the list of files
    let file_list = vec!["../2017-05-01-0.json",
                         "../2017-05-01-1.json",
                         "../2017-05-01-2.json",
                         "../2017-05-01-3.json",
                         "../2017-05-01-4.json",
                         "../2017-05-01-5.json",
                         "../2017-05-01-6.json",
                         "../2017-05-01-7.json",
                         "../2017-05-01-8.json",
                         "../2017-05-01-9.json",
                         "../2017-05-01-10.json",
                         "../2017-05-01-11.json",
                         "../2017-05-01-12.json",
                         "../2017-05-01-13.json",
                         "../2017-05-01-14.json",
                         "../2017-05-01-15.json",
                         "../2017-05-01-16.json",
                         "../2017-05-01-17.json",
                         "../2017-05-01-18.json",
                         "../2017-05-01-19.json",
                         "../2017-05-01-20.json",
                         "../2017-05-01-21.json",
                         "../2017-05-01-22.json",
                         "../2017-05-01-23.json",
                         ];
    // parse_ze_file does file IO which is an antipattern with rayon.
    // Should figure out a way to read things in with a threadpool perhaps.
    let events: Vec<Event> = file_list
        .par_iter()
        .flat_map(|file_name| parse_ze_file(file_name).expect("Issue with file ingest"))
        .collect();

    println!("\nGetting events took {}ms\n", sw.elapsed_ms());
    
    // display something interesting
    println!("\nFound {} events", events.len());

    sw.restart();
    print_committers_per_repo(events);
    println!("\nprint_committers_per_repo took {}ms\n", sw.elapsed_ms());
}

// TODO: find only accepted PRs.  It's under payload => action in the JSON.
// TODO: commits directly on the repo count, too.  That's the "PushEvent" type.
fn print_committers_per_repo(events: Vec<Event>) {
    let mut sw = Stopwatch::start_new();
    let pr_events: Vec<Event> = events
        .into_par_iter()
        .filter(|event| event.event_type == "PullRequestEvent")
        .collect();

    println!("finding PR events took {}ms", sw.elapsed_ms());
    sw.restart();

    let mut pr_by_actors: Vec<Pr_by_actor> = Vec::new();

    // naive dumping data into a vec then sort+dedup is faster than checking in each iteration
    for event in pr_events {
        let tmp_pr_by_actor = Pr_by_actor {
            repo: event.repo,
            actor: event.actor,
        };
        pr_by_actors.push(tmp_pr_by_actor);
    }
    pr_by_actors.sort();
    pr_by_actors.dedup();

    println!("Combining PRs and actors took {}ms", sw.elapsed_ms());
    sw.restart();

    // for each repo, count PRs made to it
    use std::collections::BTreeMap;
    let mut repo_actors_count: BTreeMap<Repo, i32> = BTreeMap::new();
    for pr in pr_by_actors {
        *repo_actors_count.entry(pr.repo).or_insert(0) += 1;
    }
    println!("Tying repos to actors took {}ms", sw.elapsed_ms());
    sw.restart();

    // println!("\n repo_actors_count: {:?}", repo_actors_count);
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