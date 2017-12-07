extern crate rusty_von_humboldt;

extern crate serde;
extern crate serde_json;
extern crate rayon;
extern crate stopwatch;
extern crate rusoto_core;
extern crate rusoto_s3;
extern crate flate2;
extern crate rand;

use std::collections::BTreeMap;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufWriter;
use rayon::prelude::*;
use stopwatch::Stopwatch;

use rusty_von_humboldt::*;
use rand::{thread_rng, Rng};

fn main() {
    println!("Welcome to Rusty von Humboldt.");

    let file_list = make_list();

    let mut sw = Stopwatch::start_new();
    let mut repo_id_to_name: Vec<RepoIdToName> = Vec::new();
    let mut events: Vec<Event> = Vec::new();
    
    // split processing of file list here
    // handle pr_events in the same way: chunks at a time
    for chunk in file_list.chunks(5) {
        println!("My chunk is {:?}", chunk);
        let event_subset: Vec<Event> = chunk
            .par_iter()
            .flat_map(|file_name| download_and_parse_file(&file_name).expect("Issue with file ingest"))
            .collect();

        // Do repo mapping here (check if it's present, check if it's newer, etc...)
        for event in event_subset {
            let update_existing = match repo_id_to_name.par_iter_mut().find_any(|repo_item| repo_item.repo_id == event.repo.id && repo_item.event_id < event.id && repo_item.repo_name != event.repo.name) {
                Some(ref mut existing) => {
                    if existing.event_id < event.id {
                        println!("Found a renamed repo! repo id: {:?} old name: {:?} new name {:?}", existing.repo_id, existing.repo_name, event.repo.name);
                        existing.event_id = event.id;
                        existing.repo_name = event.repo.name.clone();
                    }
                    true
                    },
                None => false,
            };

            if !update_existing {
                repo_id_to_name
                        .push(RepoIdToName {
                            repo_id: event.repo.id,
                            repo_name: event.repo.name,
                            event_id: event.id
                        })
            }
        }
        println!("Items in repo_id_to_name: {:?}", repo_id_to_name.len());
    }

    let mut file = BufWriter::new(File::create("repo_mappings.txt").expect("Couldn't open file for writing"));
    file.write_all(format!("{:#?}", repo_id_to_name).as_bytes()).expect("Couldn't write to file");

    println!("\nGetting repo mapping took {}ms\n", sw.elapsed_ms());

    sw.restart();
    let repo_id_name_map = calculate_up_to_date_name_for_repos(&mut events);
    println!("\ncalculate_up_to_date_name_for_repos took {}ms\n", sw.elapsed_ms());

    sw.restart();
    print_committers_per_repo(&events, &repo_id_name_map);
    println!("\nprint_committers_per_repo took {}ms\n", sw.elapsed_ms());
}

// Assumes the github repo ID doesn't change but the name field can:
fn calculate_up_to_date_name_for_repos(events: &mut Vec<Event>) -> BTreeMap<i64, String> {
    // Don't assume it's ordered correctly from GHA:
    events.sort_by_key(|ref k| (k.id));
    let mut id_to_latest_repo_name: BTreeMap<i64, String> = BTreeMap::new();
    for event in events {
        id_to_latest_repo_name.
            insert(event.repo.id, event.repo.name.to_string());
    }

    id_to_latest_repo_name
}

fn print_committers_per_repo(events: &Vec<Event>, repo_id_name_map: &BTreeMap<i64, String>) {
    let mut sw = Stopwatch::start_new();
    let pr_events: Vec<&Event> = events
        .into_par_iter()
        .filter(|event| event.is_accepted_pr() || event.is_direct_push_event())
        .collect();

    println!("finding PR events took {}ms", sw.elapsed_ms());
    sw.restart();

    // naive dumping data into a vec then sort+dedup is faster than checking in each iteration
    let mut commits_accepted_to_repo: Vec<PrByActor> = pr_events
        .par_iter()
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
        repo_name_and_actors.insert(Repo {id: repo_id, name: repo_id_name_map.get(&repo_id).expect("repo name should be present").to_string()}, actor_count);
    }

    // println!("\nrepo_name_and_actors: {:#?}", repo_name_and_actors);
}

fn make_list() -> Vec<String> {
    let mut file_list = construct_list_of_ingest_files();
    println!("file list is {:#?}", file_list);
    println!("Shuffling input file list");
    let mut rng = thread_rng();
    rng.shuffle(&mut file_list);
    println!("file list is now {:#?}", file_list);
    file_list
}

// from list of S3 files

// for each chunk of 100
    // read and deserialize
    // update the repo name map (if what we have is newer, otherwise insert)