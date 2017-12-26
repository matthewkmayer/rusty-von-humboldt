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
extern crate chrono;

use std::io::prelude::*;
use std::env;
use std::sync::mpsc::sync_channel;
use std::{thread, time};
use std::str::FromStr;
use rayon::prelude::*;
use flate2::Compression;
use flate2::write::GzEncoder;
use chrono::{DateTime, Utc};

use rusty_von_humboldt::*;
use rand::{thread_rng, Rng};
use rusoto_core::{DefaultCredentialsProviderSync, Region, default_tls_client, ProvideAwsCredentials, DispatchSignedRequest};
use rusoto_s3::{S3, S3Client, PutObjectRequest};

/// MODE contains what mode to do: committer count or repo mappings as well as if it should
/// upload results to s3 or not (dry run).
lazy_static! {
    static ref MODE: Mode = Mode {
        committer_count: false,
        repo_mapping: true,
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

fn main() {
    println!("Welcome to Rusty von Humboldt.");
    environment_check();
    sinker();
    println!("This is Rusty von Humboldt, heading home.");
}

/// Using channels to synchronize between sending threads and receiving thread.
///
/// Spin up a receiving thread that takes Events from the channel. It consolidates/dedupes them, converts
/// them to SQL then uploads to S3 when it has enough items collected. Behavior of committer count or
/// repository ID mapping is controlled by the MODE lazy static.
///
/// Sending threads (two threads) take the to-process file list and downloads, deserializes and sends
/// to the channel.
fn sinker() {
    let dest_bucket = env::var("DESTBUCKET").expect("Need DESTBUCKET set to bucket name");
    // take the receive channel for file locations
    let mut file_list = make_list();
    let (send, recv) = sync_channel(500000);

    // The receiving thread that accepts Events and converts them to the type needed.
    let thread = thread::spawn(move|| { 
        let thread_client = S3Client::new(default_tls_client().expect("Couldn't make TLS client"),
                DefaultCredentialsProviderSync::new().expect("Couldn't get new copy of DefaultCredentialsProviderSync"),
                Region::UsEast1);
        
        match MODE.committer_count {
            true => do_work_son(recv, thread_client, dest_bucket),
            false => do_repo_work_son(recv, thread_client, dest_bucket),
        }
    });

    // send things all threaded like
    let send_a = send.clone();
    let send_b = send.clone();
    let middle_of_file_list: usize = file_list.len()/2;
    let second_file_list = file_list.split_off(middle_of_file_list);

    let send_thread_a = thread::spawn(move|| { 
        let client = S3Client::new(default_tls_client().expect("Couldn't make TLS client"),
                DefaultCredentialsProviderSync::new().expect("Couldn't get new copy of DefaultCredentialsProviderSync"),
                Region::UsEast1);
        for file in file_list.chunks(10) {
            let event_subset = match MODE.committer_count {
                true => get_event_subset_committers(&file, &client),
                false => get_event_subset(&file, &client),
            };
            for event in event_subset {
                let event_item = EventWorkItem {
                    event: event,
                    no_more_work: false,
                };
                send_a.send(event_item).expect("Should have sent event.");
            }
        }
    });

    let send_thread_b = thread::spawn(move|| {
        let client = S3Client::new(default_tls_client().expect("Couldn't make TLS client"),
                DefaultCredentialsProviderSync::new().expect("Couldn't get new copy of DefaultCredentialsProviderSync"),
                Region::UsEast1);
        for file in second_file_list.chunks(10) {
            let event_subset = match MODE.committer_count {
                true => get_event_subset_committers(&file, &client),
                false => get_event_subset(&file, &client),
            };
            for event in event_subset {
                let event_item = EventWorkItem {
                    event: event,
                    no_more_work: false,
                };
                send_b.send(event_item).expect("Couldn't send event to channel b");
            }
        }
    });

    // These join calls will block until the sending threads have completed all their work.
    match send_thread_a.join() {
        Ok(_) => println!("Thread all wrapped up."),
        Err(e) => println!("Thread didn't want to quit: {:?}", e),
    }
    match send_thread_b.join() {
        Ok(_) => println!("Thread all wrapped up."),
        Err(e) => println!("Thread didn't want to quit: {:?}", e),
    }

    println!("We're done sending items.");
    let event_item = EventWorkItem {
        event: Event::new(),
        no_more_work: true,
    };
    send.send(event_item).expect("Couldn't send stop work item.");

    // Wait for the worker thread to wrap up.
    match thread.join() {
        Ok(_) => println!("Thread all wrapped up."),
        Err(e) => println!("Thread didn't want to quit: {:?}", e),
    }
    println!("all wrapped up.");
}

// dudupe RepoIdToName: if repo_id and repo_name are the same we can ditch one
fn do_repo_work_son
    <P: ProvideAwsCredentials + Sync + Send,
    D: DispatchSignedRequest + Sync + Send>
    (recv: std::sync::mpsc::Receiver<EventWorkItem>, client: S3Client<P, D>, dest_bucket: String) {

    let events_to_hold = 15000000;
    let mut wrap_things_up = false;
    let mut repo_mappings: Vec<RepoIdToName> = Vec::with_capacity(events_to_hold);
    let mut sql_collector: Vec<String> = Vec::new();
    let mut sql_bytes: Vec<u8> = Vec::new();
    let mut index = 0;
    // TODO: handle pre-2015 events
    loop {
        index += 1;
        repo_mappings.clear();
        sql_collector.clear();
        sql_bytes.clear();
        if wrap_things_up {
            println!("wrapping thread up.");
            break;
        }
        // fetch work loop: 
        loop {
            let item: EventWorkItem = match recv.recv() {
                Ok(i) => i,
                Err(_) => {
                    panic!("receiving error");
                },
            };
            if repo_mappings.len() % 2000000 == 0 {
                println!("Repo mapping size: {}", repo_mappings.len());
                // println!("number of work items: {}", repo_mappings.len());
                let old_size = repo_mappings.len();
                repo_mappings.sort();
                // println!("before: {:#?}", repo_mappings);
                repo_mappings.dedup_by(|a, b| a.repo_id == b.repo_id && a.repo_name == b.repo_name);
                // println!("after: {:#?}", repo_mappings);
                // println!("{:?}: Inner loop: we shrunk the repo events from {} to {}", thread::current().id(), old_size, repo_mappings.len());
            }
            if item.no_more_work {
                wrap_things_up = true;
                break;
            } else {
                repo_mappings.push(item.event.as_repo_id_mapping());
            }
            if repo_mappings.len() == events_to_hold {
                println!("\n\n\nWe got enough work to do!\n\n");
                break;
            }
        }

        let old_size = repo_mappings.len();
        repo_mappings.sort();
        repo_mappings.dedup();
        println!("{:?}: We shrunk the repo events from {} to {}", thread::current().id(), old_size, repo_mappings.len());
        println!("Converting to sql");
        let mut inner_index = 1;

        repo_mappings
            .chunks(1000000)
            .for_each(|chunk| {
                chunk
                    .par_iter()
                    .map(|item| format!("{}\n", item.as_sql()))
                    .collect_into(&mut sql_collector);

                sql_bytes = sql_collector
                    .join("")
                    .as_bytes()
                    .to_vec();

                let file_name = format!("rvh/{}/{}/{:03}_{:03}.txt.gz", generate_mode_string(), *YEAR, index, inner_index);
                inner_index += 1;
                println!("compressing and uploading to s3");

                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(&sql_bytes).expect("encoding failed");
                let compressed_results = encoder.finish().expect("Couldn't compress file, sad.");
                println!("Compression done.");


                let upload_request = PutObjectRequest {
                    bucket: dest_bucket.clone(),
                    key: file_name.to_owned(),
                    body: Some(compressed_results),
                    ..Default::default()
                };

                if MODE.dry_run {
                    println!("Not uploading to S3, it's a dry run.  Would have uploaded to bucket {} and key {}.", upload_request.bucket, upload_request.key);
                } else {
                    println!("Uploading to S3.");
                    // We create a new client every time since the underlying connection pool can
                    // deadlock if all the connections were closed by the receiving end (S3).
                    // This bypasses that issue by creating a new pool every time.
                    let client = S3Client::new(default_tls_client().expect("Couldn't make TLS client"),
                                                DefaultCredentialsProviderSync::new().expect("Couldn't get new copy of DefaultCredentialsProviderSync"),
                                                Region::UsEast1);
                    match client.put_object(&upload_request) {
                        Ok(_) => println!("uploaded {} to {}", upload_request.key, upload_request.bucket),
                        Err(_) => println!("Whoops, couldn't upload {}", upload_request.key),
                    }
                }
            }) 
    }
}

/// Committer count
fn do_work_son
    <P: ProvideAwsCredentials + Sync + Send,
    D: DispatchSignedRequest + Sync + Send>
    (recv: std::sync::mpsc::Receiver<EventWorkItem>, client: S3Client<P, D>, dest_bucket: String) {

    // bump this higher
    let events_to_hold = 6000000;
    let mut wrap_things_up = false;
    let mut committer_events: Vec<CommitEvent> = Vec::new();
    let mut sql_collector: Vec<String> = Vec::new();
    let mut sql_bytes: Vec<u8> = Vec::new();
    let mut index = 0;

    loop {
        index += 1;
        committer_events.clear();
        sql_collector.clear();
        sql_bytes.clear();
        if wrap_things_up {
            println!("wrapping thread up.");
            break;
        }

        loop {
            let item: EventWorkItem = match recv.recv() {
                Ok(i) => i,
                Err(_) => {
                    panic!("receiving error");
                },
            };
            // convert to something like 1/10 of the max amount
            if committer_events.len() % 200000 == 0 {
                println!("number of work items: {}", committer_events.len());
                let old_size = committer_events.len();
                committer_events.sort();
                committer_events.dedup();
                println!("{:?}: Inner loop: we shrunk the committer events from {} to {}", thread::current().id(), old_size, committer_events.len());
            }
            if item.no_more_work {
                wrap_things_up = true;
                break;
            } else {
                committer_events.push(item.event.as_commit_event()); 
            }
            if committer_events.len() == events_to_hold {
                println!("\n\n\nWe got enough work to do!\n\n");
                break;
            }
        }

        let old_size = committer_events.len();
        committer_events.sort();
        committer_events.dedup();
        println!("{:?}: We shrunk the committer events from {} to {}", thread::current().id(), old_size, committer_events.len());

        println!("Converting to sql");
        committer_events
            .par_iter()
            .map(|item| format!("{}\n", item.as_sql()))
            .collect_into(&mut sql_collector);
        
        sql_bytes = sql_collector
            .join("")
            .as_bytes()
            .to_vec();

        let file_name = format!("rvh/{}/{}/{:03}.txt.gz", generate_mode_string(), *YEAR, index);
        
        println!("compressing and uploading to s3");

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&sql_bytes).expect("encoding failed");
        let compressed_results = encoder.finish().expect("Couldn't compress file, sad.");
        println!("Compression done.");


        let upload_request = PutObjectRequest {
            bucket: dest_bucket.clone(),
            key: file_name.to_owned(),
            body: Some(compressed_results),
            ..Default::default()
        };

        {
            if MODE.dry_run {
                println!("Not uploading to S3, it's a dry run.  Would have uploaded to bucket {} and key {}.", upload_request.bucket, upload_request.key);
                continue;
            }
            println!("Uploading to S3.");
            // We create a new client every time since the underlying connection pool can
            // deadlock if all the connections were closed by the receiving end (S3).
            // This bypasses that issue by creating a new pool every time.
            match client.put_object(&upload_request) {
                Ok(_) => println!("uploaded {} to {}", upload_request.key, upload_request.bucket),
                Err(_) => {
                    thread::sleep(time::Duration::from_millis(100));
                    match client.put_object(&upload_request) {
                        Ok(_) => println!("uploaded {} to {}", upload_request.key, upload_request.bucket),
                        Err(_) => {
                            thread::sleep(time::Duration::from_millis(1000));
                            match client.put_object(&upload_request) {
                                Ok(_) => println!("uploaded {} to {}", upload_request.key, upload_request.bucket),
                                Err(_) => {
                                    let client = S3Client::new(default_tls_client().expect("Couldn't make TLS client"),
                                        DefaultCredentialsProviderSync::new().expect("Couldn't get new copy of DefaultCredentialsProviderSync"),
                                        Region::UsEast1);
                                    match client.put_object(&upload_request) {
                                        Ok(_) => println!("uploaded {} to {} with new client", upload_request.key, upload_request.bucket),
                                        Err(e) => println!("FOURTH ATTEMPT TO UPLOAD FAILED SO SAD. {:?}", e),
                                    }
                                },
                            };
                        },
                    };
                }
            }

        }
    }
}

fn generate_mode_string() -> String {
    if MODE.committer_count {
        return "committers".to_string();
    }
    "repomapping".to_string()
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

/// Make the list of GHA input files.
fn make_list() -> Vec<String> {
    let mut file_list = construct_list_of_ingest_files();
    let mut rng = thread_rng();
    // Shuffling the list prevents hotspot reads from S3, boosting download performance.
    rng.shuffle(&mut file_list);
    println!("file list is now {:#?}", file_list);
    file_list
}

/// Get all events from the file specified on S3
fn get_event_subset<P: ProvideAwsCredentials + Sync + Send,
    D: DispatchSignedRequest + Sync + Send>(chunk: &[String], client: &S3Client<P, D>) -> Vec<Event> {
    chunk
        .par_iter()
        // todo: don't panic here (issue only when S3 kicks back errors)
        .flat_map(|file_name| download_and_parse_file(file_name, &client).expect("Issue with file ingest"))
        .collect()
}

/// Get commit/PR events from the file specified on S3
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

/// Get commit/PR events for pre-2015 events from the file specified on S3
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

// Get all pre-2015 events from file specified
fn get_old_event_subset<P: ProvideAwsCredentials + Sync + Send,
    D: DispatchSignedRequest + Sync + Send>(chunk: &[String], client: &S3Client<P, D>) -> Vec<Pre2015Event> {
    chunk
        .par_iter()
        // todo: don't panic here
        .flat_map(|file_name| download_and_parse_old_file(file_name, &client).expect("Issue with file ingest"))
        .collect()
}

/// Take collection of pre-2015 events and convert to vector of RepoIdToName.
/// Deduplicates by not inserting older events if a newer one is present for that
/// repository ID.
fn repo_id_to_name_mappings_old(events: &[Pre2015Event]) -> Vec<RepoIdToName> {
    let mut repo_mappings: Vec<RepoIdToName> = events
        .par_iter()
        .map(|r| {
            // replace with r.repo_id():
            let repo_id = match r.repo {
                Some(ref repo) => repo.id,
                None => match r.repository {
                    Some(ref repository) => repository.id,
                    None => -1,
                }
            };
            let repo_name = match r.repo {
                Some(ref repo) => repo.name.clone(),
                None => match r.repository {
                    Some(ref repository) => repository.name.clone(),
                    None => "".to_string(),
                }
            };

            let timestamp = match DateTime::parse_from_rfc3339(&r.created_at) {
                Ok(time) => time,
                Err(_) => DateTime::parse_from_rfc3339("2011-01-01T21:00:09+09:00").unwrap(), // Make ourselves low priority
            };

            let utc_timestamp = DateTime::<Utc>::from_utc(timestamp.naive_utc(), Utc);

            RepoIdToName {
                    repo_id: repo_id,
                    repo_name: repo_name,
                    event_timestamp: utc_timestamp,
                }
            }
        )
        .filter(|x| x.repo_id >= 0)
        .filter(|x| x.repo_name != "")
        .collect();

    // get unique list of repo ids
    repo_mappings.sort_by_key(|x| x.repo_id);
    let mut list_of_repo_ids: Vec<i64> = repo_mappings.iter().map(|x| x.repo_id).collect();
    list_of_repo_ids.sort();
    list_of_repo_ids.dedup();
    // for each repo id, find the entry with the most recent timestamp
    let a: Vec<RepoIdToName> = list_of_repo_ids
        .iter()
        .map(|repo_id| {
            // find most up to date entry for this one
            let mut all_entries_for_repo_id: Vec<RepoIdToName> = repo_mappings
                .iter()
                .filter(|x| x.repo_id == *repo_id)
                .map(|x| x.clone())
                .collect();
            all_entries_for_repo_id.sort_by_key(|x| x.event_timestamp);
            // println!("sorted: {:#?}", all_entries_for_repo_id);
            all_entries_for_repo_id.last().unwrap().clone()
        })
        .collect();

    // collect and return those most recent timestamp ones
    // println!("repo mappings after dedupin': {:#?}", a);
    println!("pre-2015 len difference: {:?} to {:?}", repo_mappings.len(), a.len());
    a
}

/// Take collection of 2015 and later events and convert to vector of RepoIdToName.
/// Deduplicates by not inserting older events if a newer one is present for that
/// repository ID.
fn repo_id_to_name_mappings(events: &[Event]) -> Vec<RepoIdToName> {
    let mut repo_mappings: Vec<RepoIdToName> = events
        .par_iter()
        .map(|r| RepoIdToName {
                repo_id: r.repo.id,
                repo_name: r.repo.name.clone(),
                event_timestamp: r.created_at.clone(),
            })
        .collect();

    // get unique list of repo ids
    repo_mappings.sort_by_key(|x| x.repo_id);
    let mut list_of_repo_ids: Vec<i64> = repo_mappings.par_iter().map(|x| x.repo_id).collect();
    list_of_repo_ids.sort();
    list_of_repo_ids.dedup();
    // for each repo id, find the entry with the most recent timestamp
    let a: Vec<RepoIdToName> = list_of_repo_ids
        .par_iter()
        .map(|repo_id| {
            // find most up to date entry for this one
            let mut all_entries_for_repo_id: Vec<RepoIdToName> = repo_mappings
                .iter()
                .filter(|x| x.repo_id == *repo_id)
                .map(|x| x.clone())
                .collect();
            all_entries_for_repo_id.sort_by_key(|x| x.event_timestamp);
            // println!("sorted: {:#?}", all_entries_for_repo_id);
            all_entries_for_repo_id.last().unwrap().clone()
        })
        .collect();

    println!("len difference: {:?} to {:?}", repo_mappings.len(), a.len());
    a
}

/// Struct representing a completed item of work to upload to S3.
/// Also used as a "no more items" signal.
#[derive(Debug, Clone)]
struct WorkItem {
    sql: String,
    s3_bucket_name: String,
    s3_file_location: String,
    no_more_work: bool,
}

/// Struct for what mode we're in.
#[derive(Debug, Clone)]
struct Mode {
    committer_count: bool,
    repo_mapping: bool,
    dry_run: bool,
}

/// Struct representing a file to download and parse.
/// Also allows a "no more work" signal to be passed.
#[derive(Debug, Clone)]
struct FileWorkItem {
    file: String,
    no_more_work: bool,
}

/// Struct representing a 2015 and later event.
/// Also allows a "no more work" signal to be passed.
#[derive(Debug, Clone)]
struct EventWorkItem {
    event: Event,
    no_more_work: bool,
}

lazy_static! {
    static ref YEAR: i32 = {
        env::var("GHAYEAR").expect("Please set GHAYEAR env var").parse::<i32>().expect("Please set GHAYEAR env var to an integer value.")
    };
}

#[cfg(test)]
mod tests {

    // Only really used to see what the memory usage of a big ol' vector is.
    #[test]
    fn max_event_vec_size() {
        use rusty_von_humboldt::types::Event;
        use chrono::{TimeZone, Utc};

        let mut collector: Vec<Event> = Vec::new();
        // 9 million items is ~1.6 GB of RAM
        // 55 million items was ~8 GB
        for i in 0..95000 {
            let mut event = Event::new();
            event.repo.id = i;
            event.repo.name = "hi".to_string();
            event.created_at = Utc.ymd(2014, 7, 8).and_hms(9, 10, 11);

            collector.push(event);
        }
        println!("len is {:?}", collector.len());
    }

    // Remove older entries from a RepoIdToName collection
    #[test]
    fn reduce_works() {
        use repo_id_to_name_mappings;
        use rusty_von_humboldt::RepoIdToName;
        use rusty_von_humboldt::types::Event;
        use chrono::{TimeZone, Utc};

        let most_newest_timestamp = Utc.ymd(2014, 7, 8).and_hms(9, 10, 11);
        let an_older_timestamp = Utc.ymd(2014, 7, 8).and_hms(0, 10, 11);

        let mut expected: Vec<RepoIdToName> = Vec::new();
        expected.push(RepoIdToName {
            repo_id: 5,
            repo_name: "new".to_string(),
            event_timestamp: most_newest_timestamp,
        });

        let mut input = Vec::new();
        
        let mut foo = Event::new();
        foo.repo.id = 5;
        foo.repo.name = "old".to_string();
        foo.created_at = an_older_timestamp;
        input.push(foo);

        foo = Event::new();
        foo.repo.id = 5;
        foo.repo.name = "new".to_string();
        foo.created_at = most_newest_timestamp;
        input.push(foo);

        assert_eq!(expected, repo_id_to_name_mappings(&input));
    }

    // mostly a test for playing with the different timestamps in pre-2015 events
    #[test]
    fn timestamp_parsing() {
        use chrono::{DateTime, Utc};
        let style_one = "2013-01-01T12:00:24-08:00";
        let style_two = "2011-05-01T15:59:59Z";

        match DateTime::parse_from_rfc3339(style_one) {
            Ok(time) => println!("got {:?} from {:?}", time, style_one),
            Err(e) => println!("Failed to get anything from {:?}. Error: {:?}", style_one, e),
        }

        match DateTime::parse_from_rfc3339(style_two) {
            Ok(time) => println!("got {:?} from {:?}", time, style_two),
            Err(e) => println!("Failed to get anything from {:?}. Error: {:?}", style_two, e),
        }

        let localtime = DateTime::parse_from_rfc3339(style_two).unwrap();
        let _utc: DateTime<Utc> = DateTime::<Utc>::from_utc(localtime.naive_utc(), Utc);
    }
}