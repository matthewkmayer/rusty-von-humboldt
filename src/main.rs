extern crate rusty_von_humboldt;

extern crate serde;
extern crate serde_json;
extern crate rayon;
extern crate stopwatch;
extern crate rusoto_core;
extern crate rusoto_s3;
extern crate flate2;
extern crate rand;
extern crate md5;
#[macro_use]
extern crate lazy_static;

use std::io::prelude::*;
use std::env;
use std::sync::mpsc::sync_channel;
use std::{thread, time};
use std::thread::JoinHandle;
use std::str::FromStr;
use rayon::prelude::*;
use flate2::Compression;
use flate2::write::GzEncoder;

use rusty_von_humboldt::*;
use rand::{thread_rng, Rng};
use rusoto_core::{DefaultCredentialsProviderSync, Region, default_tls_client, ProvideAwsCredentials, DispatchSignedRequest};
use rusoto_s3::{S3, S3Client, PutObjectRequest};


fn pipeline_main() {
    environment_check();

    let pipes = make_channels_and_threads();
    let file_list = make_list();

    // distribute file list equally into the pipeline channels
    send_ze_files(&pipes, &file_list);

    wait_for_threads(pipes);
}

fn wait_for_threads(pipes: Vec<PipelineTracker>) {
    for pipe in pipes {
        let done_signal = FileWorkItem {
            file: String::new(),
            no_more_work: true,
        };
        match pipe.transmit_channel.send(done_signal) {
            Ok(_) => (),
            Err(e) => println!("Couldn't send to channel: {}", e),
        }
        match pipe.thread.join() {
            Ok(_) => println!("Pipe thread all wrapped up."),
            Err(e) => println!("Pipe thread didn't want to quit: {:?}", e),
        }
    }
}

fn send_ze_files(pipes: &[PipelineTracker], file_list: &[String]) {
    for file in file_list {
        let mut file_sent = false;
        for pipe in pipes {
            if file_sent {
                break;
            }
            let item_to_send = FileWorkItem {
                file: file.clone(),
                no_more_work: false,
            };
            match pipe.transmit_channel.try_send(item_to_send.clone()) {
                Ok(_) => file_sent = true,
                Err(_) => (),
            }
        }
    }
}

// TODO: create more than one
fn make_channels_and_threads() -> Vec<PipelineTracker> {
    // assign a subset to each thread
    let (send, recv) = sync_channel(2);
    let thread = thread::spawn(move|| {
        let client = S3Client::new(default_tls_client().expect("Couldn't make TLS client"),
            DefaultCredentialsProviderSync::new().expect("Couldn't get new copy of DefaultCredentialsProviderSync"),
            Region::UsEast1);
        loop {
            let item: FileWorkItem = match recv.recv() {
                Ok(i) => i,
                Err(_) => continue,
            };
            if item.no_more_work {
                println!("No more work, hooray!");
                return;
            }
            single_function_of_doom(&client, &vec![item.file]);
        }
    });
    let pipe = PipelineTracker {
        thread: thread,
        transmit_channel: send,
    };

    vec![pipe]
}

fn compress_and_send
    <P: ProvideAwsCredentials + Sync + Send,
    D: DispatchSignedRequest + Sync + Send>
    (work_item: WorkItem, client: &S3Client<P, D>) {

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(work_item.sql.as_bytes()).expect("encoding failed");
    let compressed_results = encoder.finish().expect("Couldn't compress file, sad.");

    let upload_request = PutObjectRequest {
        bucket: work_item.s3_bucket_name.to_owned(),
        key: work_item.s3_file_location.to_owned(),
        body: Some(compressed_results.clone()),
        ..Default::default()
    };

    if MODE.dry_run {
        println!("Not uploading to S3, it's a dry run.");
        return;
    }

    match client.put_object(&upload_request) {
        Ok(_) => println!("uploaded {} to {}", work_item.s3_file_location, work_item.s3_bucket_name),
        Err(e) => {
            println!("Failed to upload {} to {}: {:?}. Retrying...", work_item.s3_file_location, work_item.s3_bucket_name, e);
            thread::sleep(time::Duration::from_millis(8000));
            match client.put_object(&upload_request) {
                Ok(_) => println!("uploaded {} to {}", work_item.s3_file_location, work_item.s3_bucket_name),
                Err(e) => {
                    println!("Failed to upload {} to {}, second attempt: {:?}", work_item.s3_file_location, work_item.s3_bucket_name, e);
                },
            };
        }
    }
}

fn generate_mode_string() -> String {
    if MODE.committer_count {
        return "committers".to_string();
    }
    "repomapping".to_string()
}

fn single_function_of_doom 
    <P: ProvideAwsCredentials + Sync + Send,
    D: DispatchSignedRequest + Sync + Send>
    (client: &S3Client<P, D>, chunk: &[String]) {
    let dest_bucket = env::var("DESTBUCKET").expect("Need DESTBUCKET set to bucket name");
    if MODE.committer_count {
        // TODO: extract to function
        if *YEAR < 2015 {
            let mut committer_events: Vec<CommitEvent> = get_old_event_subset_committers(chunk, &client)
                .par_iter()
                .map(|item| item.as_commit_event())
                .collect();
            committer_events.sort();
            committer_events.dedup();

            let sql = committer_events
                .par_iter()
                .map(|item| format!("{}\n", item.as_sql()))
                .collect::<Vec<String>>()
                .join("");

            let file_name = format!("rvh/{}/{:x}", generate_mode_string(), md5::compute(&sql));

            let workitem = WorkItem {
                sql: sql,
                s3_bucket_name: dest_bucket.clone(),
                s3_file_location: file_name,
                no_more_work: false,
            };

            compress_and_send(workitem, client);
        } else {
            let event_subset = get_event_subset_committers(chunk, &client);
            // println!("2015+ eventsubset is {:#?}", event_subset.first().unwrap());
            let mut committer_events: Vec<CommitEvent> = event_subset
                .par_iter()
                .map(|item| item.as_commit_event())
                .collect();
            committer_events.sort();
            committer_events.dedup();

            let sql = committer_events
                .par_iter()
                .map(|item| format!("{}\n", item.as_sql()))
                .collect::<Vec<String>>()
                .join("");

            let file_name = format!("rvh/{}/{:x}", generate_mode_string(), md5::compute(&sql));

            let workitem = WorkItem {
                sql: sql,
                s3_bucket_name: dest_bucket.clone(),
                s3_file_location: file_name,
                no_more_work: false,
            };
            compress_and_send(workitem, client);
        }
    } else if MODE.repo_mapping {
        // TODO: extract to function
        if *YEAR < 2015 {
            // change get_old_event_subset to only fetch x number of files?
            let event_subset = get_old_event_subset(chunk, &client);
            let sql = repo_id_to_name_mappings_old(&event_subset)
                .par_iter()
                .map(|item| format!("{}\n", item.as_sql()))
                .collect::<Vec<String>>()
                .join("");
            
            let file_name = format!("rvh/{}/{:x}", generate_mode_string(), md5::compute(&sql));

            let workitem = WorkItem {
                sql: sql,
                s3_bucket_name: dest_bucket.clone(),
                s3_file_location: file_name,
                no_more_work: false,
            };
            compress_and_send(workitem, client);
        } else {
            let event_subset = get_event_subset(chunk, &client);
            let sql = repo_id_to_name_mappings(&event_subset)
                .par_iter()
                .map(|item| format!("{}\n", item.as_sql()))
                .collect::<Vec<String>>()
                .join("");
            let file_name = format!("rvh/{}/{:x}", generate_mode_string(), md5::compute(&sql));
            let workitem = WorkItem {
                sql: sql,
                s3_bucket_name: dest_bucket.clone(),
                s3_file_location: file_name,
                no_more_work: false,
            };
            compress_and_send(workitem, client);
        }
    }
}

// check things like dryrun etc
fn environment_check() {
    let _ = env::var("DESTBUCKET").expect("Need DESTBUCKET set to bucket name");
    let _ = env::var("GHABUCKET").expect("Need GHABUCKET set to bucket name");
    let _ = env::var("GHAYEAR").expect("Need GHAYEAR set to year to process");
    let _ = env::var("GHAHOURS")
        .expect("Need GHAHOURS set to number of hours (files) to process")
        .parse::<i64>().expect("Please set GHAHOURS to an integer value");
}

fn main() {
    println!("Welcome to Rusty von Humboldt.");
    pipeline_main();
    println!("This is Rusty von Humboldt, heading home.");
}

fn make_list() -> Vec<String> {
    let mut file_list = construct_list_of_ingest_files();
    let mut rng = thread_rng();
    rng.shuffle(&mut file_list);
    println!("file list is now {:#?}", file_list);
    file_list
}

fn get_event_subset<P: ProvideAwsCredentials + Sync + Send,
    D: DispatchSignedRequest + Sync + Send>(chunk: &[String], client: &S3Client<P, D>) -> Vec<Event> {
    chunk
        .par_iter()
        // todo: don't panic here
        .flat_map(|file_name| download_and_parse_file(file_name, &client).expect("Issue with file ingest"))
        .collect()
}

fn get_event_subset_committers<P: ProvideAwsCredentials + Sync + Send,
    D: DispatchSignedRequest + Sync + Send>(chunk: &[String], client: &S3Client<P, D>) -> Vec<Event> {
    
    let commit_events: Vec<Event> = chunk
        .par_iter()
        // todo: don't panic here
        .flat_map(|file_name| download_and_parse_file(file_name, &client).expect("Issue with file ingest"))
        .filter(|ref x| x.is_commit_event())
        .collect();
    commit_events
}

fn get_old_event_subset_committers<P: ProvideAwsCredentials + Sync + Send,
    D: DispatchSignedRequest + Sync + Send>(chunk: &[String], client: &S3Client<P, D>) -> Vec<Pre2015Event> {
    
    let commit_events: Vec<Pre2015Event> = chunk
        .par_iter()
        // todo: don't panic here
        .flat_map(|file_name| download_and_parse_old_file(file_name, &client).expect("Issue with file ingest"))
        .filter(|ref x| x.is_commit_event())
        .collect();
    commit_events
}

fn get_old_event_subset<P: ProvideAwsCredentials + Sync + Send,
    D: DispatchSignedRequest + Sync + Send>(chunk: &[String], client: &S3Client<P, D>) -> Vec<Pre2015Event> {
    chunk
        .par_iter()
        // todo: don't panic here
        .flat_map(|file_name| download_and_parse_old_file(file_name, &client).expect("Issue with file ingest"))
        .collect()
}

fn repo_id_to_name_mappings_old(events: &[Pre2015Event]) -> Vec<RepoIdToName> {
    events
        .par_iter()
        .map(|r| {
            // replace with r.repo_id():
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

#[derive(Debug, Clone)]
struct WorkItem {
    sql: String,
    s3_bucket_name: String,
    s3_file_location: String,
    no_more_work: bool,
}

#[derive(Debug, Clone)]
struct Mode {
    committer_count: bool,
    repo_mapping: bool,
    dry_run: bool,
}

#[derive(Debug)]
struct PipelineTracker {
    thread: JoinHandle<()>,
    transmit_channel: std::sync::mpsc::SyncSender<FileWorkItem>,
}

#[derive(Debug, Clone)]
struct FileWorkItem {
    file: String,
    no_more_work: bool,
}

lazy_static! {
    static ref MODE: Mode = Mode { 
        committer_count: true,
        repo_mapping: false,
        dry_run: {
            match env::var("DRYRUN"){
                Ok(dryrun) => match bool::from_str(&dryrun) {
                    Ok(should_dryrun) => should_dryrun,
                    Err(_) => false,
                },
                Err(_) => false,  
            }
        },
    };
}

lazy_static! {
    static ref YEAR: i32 = {
        env::var("GHAYEAR").expect("Please set GHAYEAR env var").parse::<i32>().expect("Please set GHAYEAR env var to an integer value.")
    };
}