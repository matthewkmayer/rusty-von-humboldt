extern crate rusty_von_humboldt;

extern crate serde;
extern crate serde_json;
extern crate rayon;
extern crate stopwatch;
extern crate rusoto_core;
extern crate rusoto_s3;

use stopwatch::Stopwatch;
use std::io::prelude::*;
use std::io::BufReader;
use std::collections::BTreeMap;
use std::env;
use rayon::prelude::*;
use rusoto_core::{DefaultCredentialsProvider, Region, default_tls_client};
use rusoto_s3::{S3, S3Client, ListObjectsV2Request, GetObjectRequest};

use rusty_von_humboldt::*;

fn construct_list_of_ingest_files() -> Vec<String> {
    // Get file list from S3:
    let bucket = env::var("GHABUCKET").expect("Need GHABUCKET set to bucket name");
    let client = S3Client::new(default_tls_client().unwrap(),
                               DefaultCredentialsProvider::new().unwrap(),
                               Region::UsEast1);

    let list_obj_req = ListObjectsV2Request {
        bucket: bucket.to_owned(),
        start_after: Some("2016".to_owned()),
        max_keys: Some(5),
        ..Default::default()
    };
    let result = client.list_objects_v2(&list_obj_req).expect("Couldn't list items in bucket (v2)");
    let mut files: Vec<String> = Vec::new();
    for item in result.contents.expect("Should have list of items") {
        files.push(item.key.expect("Key should exist for S3 item."));
    }

    files
}

fn download_and_parse_file(file_on_s3: &str) -> Result<Vec<Event>, String> {
    // locate and download file from S3
    let bucket = env::var("GHABUCKET").expect("Need GHABUCKET set to bucket name");
    let client = S3Client::new(default_tls_client().unwrap(),
                               DefaultCredentialsProvider::new().unwrap(),
                               Region::UsEast1);

    // TODO: refactor to pass stream right into the parser
    let get_req = GetObjectRequest {
        bucket: bucket.to_owned(),
        key: file_on_s3.to_owned(),
        ..Default::default()
    };

    let result = client.get_object(&get_req).expect("Couldn't GET object");
    println!("get object result: {:#?}", result);

    let stream = BufReader::new(result.body.unwrap());
    let body = stream.bytes().collect::<Result<Vec<u8>, _>>().unwrap();

    parse_ze_file(body)
}

fn main() {
    let mut sw = Stopwatch::start_new();

    println!("Welcome to Rusty von Humboldt.");

    let file_list = construct_list_of_ingest_files();

    println!("file list is {:#?}", file_list);
    panic!("bailing");

    let mut events: Vec<Event> = file_list
        .par_iter()
        .flat_map(|file_name| download_and_parse_file(&file_name).expect("Issue with file ingest"))
        .collect();

    println!("\nGetting events took {}ms\n", sw.elapsed_ms());

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
    events.sort_by_key(|ref k| (k.id_as_i64.expect("Should be populated is i64")));
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
        repo_name_and_actors.insert(Repo {id: repo_id, name: repo_id_name_map.get(&repo_id).expect("repo name should be present").to_string()}, actor_count);
    }

    // println!("\nrepo_name_and_actors: {:#?}", repo_name_and_actors);
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

fn parse_ze_file<R: Read>(contents: R) -> Result<Vec<Event>, String> {
    let sw = Stopwatch::start_new();
    let events: Vec<Event> = BufReader::new(contents)
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