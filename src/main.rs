extern crate rusty_von_humboldt;

extern crate serde;
extern crate serde_json;
extern crate rayon;
extern crate stopwatch;
extern crate rusoto_core;
extern crate rusoto_s3;
extern crate flate2;
extern crate rand;

use std::io::prelude::*;
use std::env;
use rayon::prelude::*;
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

    let file_list = make_list();

    let mut approx_files_seen: i64 = 0;
    // split processing of file list here
    for (i, chunk) in file_list.chunks(CHUNK_SIZE as usize).enumerate() {
        println!("My chunk is {:#?} and approx_files_seen is {:?}", chunk, approx_files_seen);
        
        if year_to_process < 2015 {
            let event_subset = get_old_event_subset(chunk);
            repo_mappings_as_sql_to_s3(&repo_id_to_name_mappings_old(&event_subset), &i, &year_to_process);
        } else {
            let event_subset = get_event_subset(chunk);
            repo_mappings_as_sql_to_s3(&repo_id_to_name_mappings(&event_subset), &i, &year_to_process);
        }

        approx_files_seen += CHUNK_SIZE;
    }

    println!("This is Rusty von Humboldt, heading home.");
}

fn repo_mappings_as_sql_to_s3(repo_id_details: &[RepoIdToName], i: &usize, year: &i32) {
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
    encoder.write_all(sql_text.as_bytes()).expect("encoding failed");
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
        .flat_map(|file_name| download_and_parse_file(file_name).expect("Issue with file ingest"))
        .collect()
}

fn get_old_event_subset(chunk: &[String]) -> Vec<Pre2015Event> {
    chunk
        .par_iter()
        .flat_map(|file_name| download_and_parse_old_file(file_name).expect("Issue with file ingest"))
        .collect()
}

fn repo_id_to_name_mappings_old(events: &[Pre2015Event]) -> Vec<RepoIdToName> {
    events
        .par_iter()
        .map(|r| {
            let repo_id = match r.repo {
                Some(ref repo) => repo.id,
                None => match r.repository {
                    Some(ref repository) => repository.id,
                    None => -1, // TODO: somehow ignore this event, as we can't use it
                }
            };
            let repo_name = match r.repo {
                Some(ref repo) => repo.name.clone(),
                None => match r.repository {
                    Some(ref repository) => repository.name.clone(),
                    None => "".to_string(),
                }
            };

            RepoIdToName {
                    repo_id: repo_id,
                    repo_name: repo_name,
                    event_timestamp: r.created_at.clone(),
                }
            }
        )
        .collect()
        
}


fn repo_id_to_name_mappings(events: &[Event]) -> Vec<RepoIdToName> {
    events
        .par_iter()
        .map(|r| RepoIdToName {
                repo_id: r.repo.id,
                repo_name: r.repo.name.clone(),
                event_timestamp: r.created_at.clone(),
            })
        .collect()
}
