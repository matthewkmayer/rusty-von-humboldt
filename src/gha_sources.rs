extern crate flate2;
extern crate futures;
extern crate rayon;
extern crate rusoto_core;
extern crate rusoto_s3;
extern crate serde;
extern crate serde_json;

use self::flate2::bufread::GzDecoder;
use self::futures::{Future, Stream};
use rusoto_core::{DispatchSignedRequest, ProvideAwsCredentials, Region};
use rusoto_s3::{GetObjectRequest, ListObjectsV2Request, S3Client, S3};
use std::env;
use std::io::{BufRead, BufReader};
use std::{thread, time};
use crate::types::*;

const MAX_PAGE_SIZE: i64 = 500;

/// Get list of files in the bucket, starting with the specified year and up to the number of hours specified.
pub fn construct_list_of_ingest_files() -> Vec<String> {
    // Get file list from S3:
    let bucket = env::var("GHABUCKET").expect("Need GHABUCKET set to bucket name");
    let year_to_process = env::var("GHAYEAR").expect("Need GHAYEAR set to year to process");
    let hours_to_process = env::var("GHAHOURS")
        .expect("Need GHAHOURS set to number of hours (files) to process")
        .parse::<i64>()
        .expect("Please set GHAHOURS to an integer value");
    let client = S3Client::new(Region::UsEast1);

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
    let result = client
        .list_objects_v2(list_obj_req)
        .sync()
        .expect("Couldn't list items in bucket (v2)");
    let mut files: Vec<String> = Vec::new();

    for item in result.contents.expect("Should have list of items") {
        files.push(item.key.expect("Key should exist for S3 item."));
    }

    let mut more_to_go =
        result.next_continuation_token.is_some() || result.continuation_token.is_some();
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
        if (files.len() as i64 - hours_to_process) <= MAX_PAGE_SIZE
            && (files.len() as i64 - hours_to_process) > 0
        {
            key_count_to_request = (files.len() as i64 - hours_to_process) as i64;
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
        let inner_result = client
            .list_objects_v2(list_obj_req)
            .sync()
            .expect("Couldn't list items in bucket (v2)");

        for item in inner_result.contents.expect("Should have list of items") {
            files.push(item.key.expect("Key should exist for S3 item."));
        }
        more_to_go = inner_result.next_continuation_token.is_some()
            && files.len() <= hours_to_process as usize;
        match inner_result.next_continuation_token {
            Some(ref token) => continue_token = token.to_owned(),
            None => (),
        }
    }

    println!("Parsing these files: {:#?}", files);

    files
}

/// Download the specified file and parse into pre-2015 events.
pub fn download_and_parse_old_file<
    P: ProvideAwsCredentials + Sync + Send + 'static,
    D: DispatchSignedRequest + Sync + Send + 'static,
>(
    file_on_s3: &str,
    client: &S3Client,
) -> Result<Vec<Pre2015Event>, String> {
    let bucket = env::var("GHABUCKET").expect("Need GHABUCKET set to bucket name");

    let get_req = GetObjectRequest {
        bucket: bucket.to_owned(),
        key: file_on_s3.to_owned(),
        ..Default::default()
    };

    let result = match client.get_object(get_req.clone()).sync() {
        Ok(s3_result) => s3_result,
        Err(_) => {
            thread::sleep(time::Duration::from_millis(50));
            match client.get_object(get_req.clone()).sync() {
                Ok(s3_result) => s3_result,
                Err(err) => return Err(format!("{:?}", err)),
            }
        }
    };

    let read_body: Vec<_> = result
        .body
        .expect("body should be preset")
        .concat2()
        .wait()
        .unwrap()
        .to_vec();

    // convert the Vec<u8> into a slice for the GzDecoder:
    let decoder = GzDecoder::new(&read_body[..]);
    parse_ze_file_2014_older(BufReader::new(decoder))
}

/// Download the specified file and parse into 2015 and later events.
pub fn download_and_parse_file(file_on_s3: &str, client: &S3Client) -> Result<Vec<Event>, String> {
    let bucket = env::var("GHABUCKET").expect("Need GHABUCKET set to bucket name");

    let get_req = GetObjectRequest {
        bucket: bucket.to_owned(),
        key: file_on_s3.to_owned(),
        ..Default::default()
    };

    let result = match client.get_object(get_req.clone()).sync() {
        Ok(s3_result) => s3_result,
        Err(_) => {
            thread::sleep(time::Duration::from_millis(50));
            match client.get_object(get_req.clone()).sync() {
                Ok(s3_result) => s3_result,
                Err(err) => return Err(format!("{:?}", err)),
            }
        }
    };

    let read_body: Vec<_> = result
        .body
        .expect("body should be preset")
        .concat2()
        .wait()
        .unwrap()
        .to_vec();

    // conver the Vec<u8> into a slice for GzDecoder:
    let decoder = GzDecoder::new(&read_body[..]);
    parse_ze_file_2015_newer(BufReader::new(decoder))
}

/// Deserialize pre-2015 events
fn parse_ze_file_2014_older<R: BufRead>(mut contents: R) -> Result<Vec<Pre2015Event>, String> {
    let mut events: Vec<Pre2015Event> = Vec::new();
    let mut line = String::new();
    while contents.read_line(&mut line).unwrap() > 0 {
        match serde_json::from_str(&line) {
            Ok(event) => events.push(event),
            Err(err) => println!("Found a weird line of json, got this error: {:?}.", err),
        };
        line.clear();
    }

    Ok(events)
}

/// Deserialize 2015 and later events
fn parse_ze_file_2015_newer<R: BufRead>(mut contents: R) -> Result<Vec<Event>, String> {
    let mut events: Vec<Event> = Vec::new();
    let mut line = String::new();
    while contents.read_line(&mut line).unwrap() > 0 {
        match serde_json::from_str(&line) {
            Ok(event) => events.push(event),
            Err(err) => println!("Found a weird line of json, got this error: {:?}.", err),
        };
        line.clear();
    }

    Ok(events)
}
