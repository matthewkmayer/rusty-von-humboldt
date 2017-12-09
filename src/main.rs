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
use std::env;
use rayon::prelude::*;
use stopwatch::Stopwatch;
use flate2::Compression;
use flate2::write::GzEncoder;

use rusty_von_humboldt::*;
use rand::{thread_rng, Rng};
use rusoto_core::{DefaultCredentialsProvider, Region, default_tls_client};
use rusoto_s3::{S3, S3Client, PutObjectRequest};

// Chunk size controls size of output files and roughly the amount of parallelism
// when downloading and deserializing files.
const CHUNK_SIZE: i64 = 300;

fn main() {
    println!("Welcome to Rusty von Humboldt.");
    
    env::var("DESTBUCKET").expect("Need DESTBUCKET set to bucket name");
    let year_to_process: i32 = env::var("GHAYEAR").expect("Need GHAYEAR set to year to process").parse::<i32>().expect("Please set GHAYEAR to an integer value");;

    let do_committer_counts = false;
    let file_list = make_list();

    let mut sw = Stopwatch::start_new();
    let mut commits_accepted_to_repo: Vec<PrByActor> = Vec::new();

    let mut approx_files_seen: i64 = 0;
    let mut i: i64 = 0;
    // split processing of file list here
    for chunk in file_list.chunks(CHUNK_SIZE as usize) {
        println!("My chunk is {:#?} and approx_files_seen is {:?}", chunk, approx_files_seen);
        let event_subset = get_event_subset(chunk);

        repo_mappings_as_sql_to_s3(&repo_id_to_name_mappings(&event_subset), &i, &year_to_process);

        if do_committer_counts {
            let mut this_chunk_commits_accepted_to_repo: Vec<PrByActor> = committers_to_repo(&event_subset);
            this_chunk_commits_accepted_to_repo.sort();
            this_chunk_commits_accepted_to_repo.dedup();
            commits_accepted_to_repo.append(&mut this_chunk_commits_accepted_to_repo);
        }
        approx_files_seen += CHUNK_SIZE;
        i += 1;
    }

    // In a bit of a half-baked state while we move repo ids to a database
    if do_committer_counts {
        sw.restart();
        commits_accepted_to_repo.sort();
        commits_accepted_to_repo.dedup();

        let repo_id_name_map: BTreeMap<i64, String> = BTreeMap::new();
        print_committers_per_repo(&commits_accepted_to_repo, &repo_id_name_map);
        println!("\nprint_committers_per_repo took {}ms\n", sw.elapsed_ms());
    }

    println!("This is Rusty von Humboldt, heading home.");
}

fn repo_mappings_as_sql_to_s3(repo_id_details: &Vec<RepoIdToName>, i: &i64, year: &i32) {
    let dest_bucket = env::var("DESTBUCKET").expect("Need DESTBUCKET set to bucket name");
    let client = S3Client::new(default_tls_client().unwrap(),
                               DefaultCredentialsProvider::new().unwrap(),
                               Region::UsEast1);


    let file_name = format!("rvh/{}/repo_mappings_{:010}.txt.gz", year, i);
    let sql_text: String = repo_id_details
        .par_iter()
        .map(|item| format!("{}\n", item.as_sql()))
        .collect::<Vec<String>>()
        .join("");

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write(sql_text.as_bytes()).expect("encoding failed");
    let compressed_results = encoder.finish().expect("Couldn't compress file, sad.");

    let upload_request = PutObjectRequest {
        bucket: dest_bucket.to_owned(),
        key: file_name.to_owned(),
        body: Some(compressed_results),
        ..Default::default()
    };

    match client.put_object(&upload_request) {
        Ok(_) => println!("uploaded {} to {}", file_name, dest_bucket),
        Err(e) => {
            println!("Failed to upload {} to {}: {:?}", file_name, dest_bucket, e);
            // try again?
        },
    }
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
    let mut rng = thread_rng();
    rng.shuffle(&mut file_list);
    println!("file list is now {:#?}", file_list);
    file_list
}

fn get_event_subset(chunk: &[String]) -> Vec<Event> {
    chunk
        .par_iter()
        .flat_map(|file_name| download_and_parse_file(&file_name).expect("Issue with file ingest"))
        .collect()
}

fn repo_id_to_name_mappings(events: &Vec<Event>) -> Vec<RepoIdToName> {
    events
        .par_iter()
        .map(|r| RepoIdToName {
                repo_id: r.repo.id,
                repo_name: r.repo.name.clone(),
                event_id: r.id
            })
        .collect()
}

fn committers_to_repo(events: &Vec<Event>) -> Vec<PrByActor> {
    events
        .par_iter()
        .map(|event| PrByActor { repo: event.repo.clone(), actor: event.actor.clone(), } )
        .collect()
}