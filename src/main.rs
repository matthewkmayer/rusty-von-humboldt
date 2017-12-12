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
use std::thread;
use std::sync::mpsc::{sync_channel, Receiver};
use rayon::prelude::*;
use flate2::Compression;
use flate2::write::GzEncoder;

use rusty_von_humboldt::*;
use rand::{thread_rng, Rng};
use rusoto_core::{DefaultCredentialsProvider, Region, default_tls_client};
use rusoto_s3::{S3, S3Client, PutObjectRequest};

// Chunk size controls size of output files and roughly the amount of parallelism
// when downloading and deserializing files.
const CHUNK_SIZE: i64 = 24;

#[derive(Debug)]
struct WorkItem {
    sql: String,
    s3_bucket_name: String,
    s3_file_location: String,
    no_more_work: bool,
}

fn compressor_work(receiver: Receiver<WorkItem>) {
    println!("Compressor work thread fired up.");
    loop {
        let work_item: WorkItem = receiver.recv().unwrap();
        if work_item.no_more_work {
            break;
        }

        let client = S3Client::new(default_tls_client().unwrap(),
                                DefaultCredentialsProvider::new().unwrap(),
                                Region::UsEast1);

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(work_item.sql.as_bytes()).expect("encoding failed");
        let compressed_results = encoder.finish().expect("Couldn't compress file, sad.");

        let upload_request = PutObjectRequest {
            bucket: work_item.s3_bucket_name.to_owned(),
            key: work_item.s3_file_location.to_owned(),
            body: Some(compressed_results),
            ..Default::default()
        };

        match client.put_object(&upload_request) {
            Ok(_) => println!("uploaded {} to {}", work_item.s3_file_location, work_item.s3_bucket_name),
            Err(e) => {
                println!("Failed to upload {} to {}: {:?}", work_item.s3_file_location, work_item.s3_bucket_name, e);
                // try again?
            },
        }
    }
    println!("Compressor work thread all done.");
}

fn main() {
    println!("Welcome to Rusty von Humboldt.");

    let (sender, receiver) = sync_channel(2);
    let compressor_thread = thread::spawn(move|| {
        compressor_work(receiver);
    });
    
    let year_to_process: i32 = env::var("GHAYEAR").expect("Need GHAYEAR set to year to process").parse::<i32>().expect("Please set GHAYEAR to an integer value");;

    let file_list = make_list();

    let mut approx_files_seen: i64 = 0;
    let dest_bucket = env::var("DESTBUCKET").expect("Need DESTBUCKET set to bucket name");
    // split processing of file list here
    for (i, chunk) in file_list.chunks(CHUNK_SIZE as usize).enumerate() {
        println!("My chunk is {:#?} and approx_files_seen is {:?}", chunk, approx_files_seen);
        let file_name = format!("rvh/{}/repo_mappings_{:010}.txt.gz", year_to_process, i);
        if year_to_process < 2015 {
            let event_subset = get_old_event_subset(chunk);
            let sql = repo_id_to_name_mappings_old(&event_subset)
                .par_iter()
                .map(|item| format!("{}\n", item.as_sql()))
                .collect::<Vec<String>>()
                .join("");

            let workitem = WorkItem {
                sql: sql,
                s3_bucket_name: dest_bucket.clone(),
                s3_file_location: file_name,
                no_more_work: false,
            };
            sender.send(workitem).expect("Channel send no worky");
        } else {
            let event_subset = get_event_subset(chunk);
            let sql = repo_id_to_name_mappings(&event_subset)
                .par_iter()
                .map(|item| format!("{}\n", item.as_sql()))
                .collect::<Vec<String>>()
                .join("");

            let workitem = WorkItem {
                sql: sql,
                s3_bucket_name: dest_bucket.clone(),
                s3_file_location: file_name,
                no_more_work: false,
            };
            sender.send(workitem).expect("Channel send no worky");
        }

        approx_files_seen += CHUNK_SIZE;
    }
    println!("Waiting for compressor thread to wrap up");
    let final_workitem = WorkItem {
        sql: String::new(),
        s3_bucket_name: String::new(),
        s3_file_location: String::new(),
        no_more_work: true,
    };
    sender.send(final_workitem).expect("Channel wrapup send no worky");
    match compressor_thread.join() {
        Ok(_) => println!("Compressor thread all wrapped up."),
        Err(e) => println!("Compressor thread didn't want to quit: {:?}", e),
    }

    println!("This is Rusty von Humboldt, heading home.");
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
