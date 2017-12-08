extern crate serde;
extern crate serde_json;
extern crate rusoto_core;
extern crate rusoto_s3;
extern crate rayon;
extern crate flate2;

use std::io::{BufReader, BufRead};
use std::env;
use rusoto_core::{DefaultCredentialsProvider, Region, default_tls_client};
use rusoto_s3::{S3, S3Client, ListObjectsV2Request, GetObjectRequest};
use self::flate2::read::GzDecoder;
use types::*;

const MAX_PAGE_SIZE: i64 = 500;

pub fn construct_list_of_ingest_files() -> Vec<String> {
    // Get file list from S3:
    let bucket = env::var("GHABUCKET").expect("Need GHABUCKET set to bucket name");
    let year_to_process = env::var("GHAYEAR").expect("Need GHAYEAR set to year to process");
    let hours_to_process = env::var("GHAHOURS")
        .expect("Need GHAHOURS set to number of hours (files) to process")
        .parse::<i64>().expect("Please set GHAHOURS to an integer value");;
    let client = S3Client::new(default_tls_client().unwrap(),
                               DefaultCredentialsProvider::new().unwrap(),
                               Region::UsEast1);

    let mut key_count_to_request = 10;
    // single page if we want less than 1,000 items:
    if hours_to_process as i64 <= MAX_PAGE_SIZE {
        key_count_to_request = hours_to_process;
    }

    let list_obj_req = ListObjectsV2Request {
        bucket: bucket.to_owned(),
        start_after: Some(year_to_process.to_owned()),
        max_keys: Some(key_count_to_request),
        ..Default::default()
    };
    let result = client.list_objects_v2(&list_obj_req).expect("Couldn't list items in bucket (v2)");
    let mut files: Vec<String> = Vec::new();

    for item in result.contents.expect("Should have list of items") {
        files.push(item.key.expect("Key should exist for S3 item."));
    }

    let mut more_to_go = result.next_continuation_token.is_some() || result.continuation_token.is_some();
    if files.len() >= hours_to_process as usize {
        more_to_go = false;
    }
    let mut continue_token = String::new();
    match result.next_continuation_token {
        Some(ref token) => continue_token = token.to_owned(),
        None => (),
    }

    match result.continuation_token {
        Some(ref token) => continue_token = token.to_owned(),
        None => (),
    }

    while more_to_go {
        // less than MAX_PAGE_SIZE items to request? Just request what we need.
        if (files.len() - (hours_to_process as usize)) <= MAX_PAGE_SIZE as usize {
            key_count_to_request = (files.len() - (hours_to_process as usize)) as i64;
        } else {
            key_count_to_request = MAX_PAGE_SIZE;
        }
        let list_obj_req = ListObjectsV2Request {
            bucket: bucket.to_owned(),
            start_after: Some(year_to_process.to_owned()),
            max_keys: Some(key_count_to_request),
            continuation_token: Some(continue_token.clone()),
            ..Default::default()
        };
        let inner_result = client.list_objects_v2(&list_obj_req).expect("Couldn't list items in bucket (v2)");

        for item in inner_result.contents.expect("Should have list of items") {
            files.push(item.key.expect("Key should exist for S3 item."));
        }
        more_to_go = inner_result.next_continuation_token.is_some() && files.len() <= hours_to_process as usize;
        match inner_result.next_continuation_token {
            Some(ref token) => continue_token = token.to_owned(),
            None => (),
        }
    }

    files
}

pub fn download_and_parse_file(file_on_s3: &str) -> Result<Vec<Event>, String> {
    let bucket = env::var("GHABUCKET").expect("Need GHABUCKET set to bucket name");
    let client = S3Client::new(default_tls_client().unwrap(),
                               DefaultCredentialsProvider::new().unwrap(),
                               Region::UsEast1);

    let get_req = GetObjectRequest {
        bucket: bucket.to_owned(),
        key: file_on_s3.to_owned(),
        ..Default::default()
    };

    let result = match client.get_object(&get_req) {
        Ok(s3_result) => s3_result,
        Err(err) => {
            println!("Failed to get {:?} from S3: {:?}.  Retrying.", file_on_s3, err);
            match client.get_object(&get_req) {
                Ok(s3_result) => s3_result,
                Err(err) => {
                    println!("Failed to get {:?} from S3, second attempt.", file_on_s3);
                    return Err(format!("{:?}", err));
                },
            }
        }
    };
    let decoder = GzDecoder::new(result.body.expect("body should be preset")).unwrap();
    parse_ze_file(BufReader::new(decoder))
}

fn parse_ze_file<R: BufRead>(contents: R) -> Result<Vec<Event>, String> {
    let mut events: Vec<Event> = contents
        .lines()
        .map(|l| {
            let event_found: Event = match serde_json::from_str(&l.unwrap()) {
                Ok(event) => event,
                Err(err) => {
                    println!("Found a weird line of json, got this error: {:?}", err);
                    // make a fake one to toss out later
                    return Event::new();
                }
            };
            event_found
        })
        .collect();

    // We tossed in the fake events, don't pass them back up
    events.retain(|event| !event.is_temp_one());

    Ok(events)
}
