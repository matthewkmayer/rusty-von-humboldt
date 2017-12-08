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
    let mut repo_id_to_name: Vec<RepoIdToName> = Vec::with_capacity(1000000);
    let mut commits_accepted_to_repo: Vec<PrByActor> = Vec::new();

    // split processing of file list here
    // handle pr_events in the same way: chunks at a time
    for chunk in file_list.chunks(25) {
        println!("My chunk is {:?}", chunk);
        let event_subset: Vec<Event> = chunk
            .par_iter()
            .flat_map(|file_name| download_and_parse_file(&file_name).expect("Issue with file ingest"))
            .collect();

        let mut this_chunk_repo_names: Vec<RepoIdToName> = event_subset
            .par_iter()
            .map(|r| RepoIdToName {
                    repo_id: r.repo.id,
                    repo_name: r.repo.name.clone(),
                    event_id: r.id
                })
            .collect();
        repo_id_to_name.append(&mut this_chunk_repo_names);

        let mut this_chunk_commits_accepted_to_repo: Vec<PrByActor> = event_subset
            .par_iter()
            .map(|event| PrByActor { repo: event.repo.clone(), actor: event.actor.clone(), } )
            .collect();

        this_chunk_commits_accepted_to_repo.sort();
        this_chunk_commits_accepted_to_repo.dedup();

        commits_accepted_to_repo.append(&mut this_chunk_commits_accepted_to_repo);

        // TEST THIS (in this project not just playground)
        repo_id_to_name.sort_by_key(|r| r.repo_id);
        // a bit interesting since we can get all eventIDs mismashed.
        // see https://play.rust-lang.org/?gist=74aba1e331605ed3767e75cb99aa2e0d&version=stable
        repo_id_to_name.dedup_by(|a, b| a.repo_id == b.repo_id && a.event_id < b.event_id);
        repo_id_to_name.reverse();
        repo_id_to_name.dedup_by(|a, b| a.repo_id == b.repo_id && a.event_id < b.event_id);

        println!("Items in repo_id_to_name: {:?}", repo_id_to_name.len());
    }

    println!("Doing some crunching fun here");
    repo_id_to_name.sort_by_key(|r| r.event_id);
    
    let mut file = BufWriter::new(File::create("repo_mappings.txt").expect("Couldn't open file for writing"));
    file.write_all(format!("{:#?}", repo_id_to_name).as_bytes()).expect("Couldn't write to file");

    println!("\nGetting repo mapping took {}ms\n", sw.elapsed_ms());

    sw.restart();
    let repo_id_name_map = calculate_up_to_date_name_for_repos(&repo_id_to_name);
    println!("\ncalculate_up_to_date_name_for_repos took {}ms\n", sw.elapsed_ms());

    sw.restart();

    print_committers_per_repo(&commits_accepted_to_repo, &repo_id_name_map);
    println!("\nprint_committers_per_repo took {}ms\n", sw.elapsed_ms());
}

// Assumes the github repo ID doesn't change but the name field can:
fn calculate_up_to_date_name_for_repos(events: &Vec<RepoIdToName>) -> BTreeMap<i64, String> {
    let mut id_to_latest_repo_name: BTreeMap<i64, String> = BTreeMap::new();
    for event in events {
        id_to_latest_repo_name.
            insert(event.repo_id, event.repo_name.to_string());
    }

    id_to_latest_repo_name
}

fn print_committers_per_repo(commits_accepted_to_repo: &Vec<PrByActor>, repo_id_name_map: &BTreeMap<i64, String>) {
    let sw = Stopwatch::start_new();
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
    let mut file = BufWriter::new(File::create("commiter_count.txt").expect("Couldn't open file for writing"));
    file.write_all(format!("{:#?}", repo_name_and_actors).as_bytes()).expect("Couldn't write to file");
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