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
use rayon::prelude::*;
use stopwatch::Stopwatch;

use rusty_von_humboldt::*;
use rand::{thread_rng, Rng};

fn main() {
    println!("Welcome to Rusty von Humboldt.");

    let mut file_list = construct_list_of_ingest_files();
    println!("file list is {:#?}", file_list);
    println!("Shuffling input file list");
    let mut rng = thread_rng();
    rng.shuffle(&mut file_list);
    println!("file list is now {:#?}", file_list);

    let mut sw = Stopwatch::start_new();
    let mut events: Vec<EventForRepoNames> = file_list
        .par_iter()
        .flat_map(|file_name| download_and_parse_file(&file_name).expect("Issue with file ingest"))
        .collect();

    println!("\nGetting events took {}ms\n", sw.elapsed_ms());

    sw.restart();
    let repo_id_name_map = calculate_up_to_date_name_for_repos(&mut events);
    println!("\ncalculate_up_to_date_name_for_repos took {}ms\n", sw.elapsed_ms());

    // sw.restart();
    // print_committers_per_repo(&events, &repo_id_name_map);
    // println!("\nprint_committers_per_repo took {}ms\n", sw.elapsed_ms());
}

// Assumes the github repo ID doesn't change but the name field can:
fn calculate_up_to_date_name_for_repos(events: &mut Vec<EventForRepoNames>) -> BTreeMap<i64, String> {
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
