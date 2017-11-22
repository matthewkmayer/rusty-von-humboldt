extern crate rusty_von_humboldt;

extern crate serde;
extern crate serde_json;
extern crate rayon;
extern crate stopwatch;

use stopwatch::Stopwatch;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::collections::BTreeMap;
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
                         "../2017-05-01-3.json",
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

    sw.restart();
    // This function should be refactored to walk the events list once instead of a whole bunch:
    let repo_id_name_map = calculate_up_to_date_name_for_repos(&events);
    println!("\ncalculate_up_to_date_name_for_repos took {}ms\n", sw.elapsed_ms());

    sw.restart();
    print_committers_per_repo(&events, &repo_id_name_map);
    println!("\nprint_committers_per_repo took {}ms\n", sw.elapsed_ms());
}

// Assumes the github repo ID doesn't change but the name field can:
fn calculate_up_to_date_name_for_repos(events: &Vec<Event>) -> BTreeMap<i64, String> {
    // get list of repo ids seen in events
    let mut repo_ids: Vec<i64> = events
        .into_par_iter()
        .map(|event| event.id_as_i64.expect("id should be populated"))
        .collect();

    repo_ids.sort();
    repo_ids.dedup();

    println!("\nFound {} unique repo IDs, populating the table with the latest name for them.", repo_ids.len());

    let mut id_to_latest_repo_name: BTreeMap<i64, String> = BTreeMap::new();    
    for (i, repo) in repo_ids.iter().enumerate() {
        id_to_latest_repo_name.insert(*repo, latest_name_for_repo_id(*repo, events));
        if i % 100 == 0 {
            println!("Did {} passes of finding latest repo names", i);
        }
    }

    id_to_latest_repo_name
}

// What's the most recent name for repo by github ID repo_id?
fn latest_name_for_repo_id(repo_id: i64, events: &Vec<Event>) -> String {
    let mut events_on_repo: Vec<&Event> = events
        .into_par_iter()
        .filter(|ref event| event.id_as_i64.expect("id should be populated") == repo_id)
        .collect();

    // Put most recent name at the end:
    events_on_repo.sort_by_key(|ref k| (k.id_as_i64.expect("Should be populated is i64")));
    if events_on_repo.len() == 0 {
        println!("why is repoid {:?} not in events?", repo_id);
    }
    match events_on_repo.last() {
        Some(ref latest_repo_event) => latest_repo_event.repo.name.to_string(),
        None => "unknown".to_string(),
    }
}

fn print_committers_per_repo(events: &Vec<Event>, repo_id_name_map: &BTreeMap<i64, String>) {
    let mut sw = Stopwatch::start_new();
    let pr_events: Vec<&Event> = events
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

    display_actor_count_per_repo(&commits_accepted_to_repo, repo_id_name_map);
    println!("Tying repos to actors took {}ms", sw.elapsed_ms());
}

fn display_actor_count_per_repo(commits_accepted_to_repo: &Vec<PrByActor>, repo_id_name_map: &BTreeMap<i64, String>) {
    // for each repo, count accepted PRs and direct commits made
    let mut repo_actors_count: BTreeMap<i64, i32> = BTreeMap::new();
    for pr in commits_accepted_to_repo {
        *repo_actors_count.entry(pr.repo.id).or_insert(0) += 1;
    }

    // match repo ids to their current names:
    let mut repo_name_and_actors: BTreeMap<Repo, i32>= BTreeMap::new();
    for (repo_id, actor_count) in repo_actors_count {
        repo_name_and_actors
            .insert(Repo {id: repo_id, name: repo_id_name_map.get(&repo_id).expect("repo name not available").to_string()}, actor_count);
    }

    println!("\nrepo_name_and_actors: {:#?}", repo_name_and_actors);
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
        .map(|l| {
            let mut event: Event = serde_json::from_str(&l.unwrap()).expect("Couldn't deserialize event file.");
            event.id_as_i64 = Some(event.id.parse::<i64>().expect("github ID should be an i64"));
            event
        })
        .collect();

    println!("file reading and deserialization took {}ms", sw.elapsed_ms());

    Ok(events)
}