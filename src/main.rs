extern crate rusty_von_humboldt;

extern crate serde;
extern crate serde_json;
extern crate rayon;
extern crate stopwatch;

use stopwatch::Stopwatch;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use rayon::prelude::*;

use rusty_von_humboldt::*;

fn main() {
    let mut sw = Stopwatch::start_new();

    println!("Welcome to Rusty von Humboldt.");

    // In the future we'd have the list of files from the authoritative location
    // such as an S3 bucket.
    let file_list = vec!["../2017-05-01-0.json",
                         "../2017-05-01-1.json",
                         "../2017-05-01-2.json",
                        //  "../2017-05-01-3.json",
                        //  "../2017-05-01-4.json",
                        //  "../2017-05-01-5.json",
                        //  "../2017-05-01-6.json",
                        //  "../2017-05-01-7.json",
                        //  "../2017-05-01-8.json",
                        //  "../2017-05-01-9.json",
                        //  "../2017-05-01-10.json",
                        //  "../2017-05-01-11.json",
                        //  "../2017-05-01-12.json",
                        //  "../2017-05-01-13.json",
                        //  "../2017-05-01-14.json",
                        //  "../2017-05-01-15.json",
                        //  "../2017-05-01-16.json",
                        //  "../2017-05-01-17.json",
                        //  "../2017-05-01-18.json",
                        //  "../2017-05-01-19.json",
                        //  "../2017-05-01-20.json",
                        //  "../2017-05-01-21.json",
                        //  "../2017-05-01-22.json",
                        //  "../2017-05-01-23.json",
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

fn print_committers_per_repo(events: Vec<Event>) {
    let mut sw = Stopwatch::start_new();
    let pr_events: Vec<Event> = events
        .into_par_iter()
        .filter(|event| event.event_type == "PullRequestEvent" || event.event_type == "PushEvent")
        .collect();

    println!("finding PR events took {}ms", sw.elapsed_ms());
    sw.restart();

    // naive dumping data into a vec then sort+dedup is faster than checking in each iteration
    let mut commits_accepted_to_repo: Vec<PrByActor> = pr_events
        .par_iter()
        .filter(|event| {
            // This is evaluating both: we should see about refactoring to avoid it
            // and the check in each of the below functions.
            is_accepted_pr(&event) || is_direct_push_event(&event)
        })
        .map(|event| PrByActor { repo: event.repo.clone(), actor: event.actor.clone(), } )
        .collect();

    commits_accepted_to_repo.sort();
    commits_accepted_to_repo.dedup();

    println!("Combining PRs and actors took {}ms", sw.elapsed_ms());
    sw.restart();

    // for each repo, count accepted PRs made to it
    // TODO: this may not correctly correlate renamed repos.
    // Switch to using the repoID instead of repo object, then do another pass
    // to tie the repo ID to the current name of the repo.
    use std::collections::BTreeMap;
    let mut repo_actors_count: BTreeMap<Repo, i32> = BTreeMap::new();
    for pr in commits_accepted_to_repo {
        *repo_actors_count.entry(pr.repo).or_insert(0) += 1;
    }
    println!("Tying repos to actors took {}ms", sw.elapsed_ms());
    sw.restart();

    println!("\n repo_actors_count: {:#?}", repo_actors_count);
}

fn is_direct_push_event(event: &Event) -> bool {
    if event.event_type != "PushEvent" {
        return false;
    }
    match event.payload {
        Some(ref payload) => match payload.commits {
            Some(ref commits) => commits.len() > 0,
            None => false,
        },
        None => false,
    }
}

fn is_accepted_pr(event: &Event) -> bool {
    if event.event_type != "PullRequestEvent" {
        return false;
    }
    match event.payload {
        Some(ref payload) => match payload.pull_request {
            Some(ref pr) => match pr.merged {
                Some(merged) => merged,
                None => false,
            },
            None => false,
        },
        None => false,
    }
}

fn parse_ze_file(file_location: &str) -> Result<Vec<Event>, String> {
    let f = File::open(file_location).expect("file not found");

    let sw = Stopwatch::start_new();
    let events: Vec<Event> = BufReader::new(f)
        .lines()
        .map(|l| serde_json::from_str(&l.unwrap()).expect("Couldn't deserialize event file."))
        .collect();

    println!("file reading and deserialization took {}ms", sw.elapsed_ms());

    Ok(events)
}