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

    let list_obj_req = ListObjectsV2Request {
        bucket: bucket.to_owned(),
        start_after: Some(year_to_process.to_owned()),
        max_keys: Some(hours_to_process),
        ..Default::default()
    };
    let result = client.list_objects_v2(&list_obj_req).expect("Couldn't list items in bucket (v2)");
    let mut files: Vec<String> = Vec::new();
    for item in result.contents.expect("Should have list of items") {
        files.push(item.key.expect("Key should exist for S3 item."));
    }

    files
}

pub fn download_and_parse_file(file_on_s3: &str) -> Result<Vec<Event>, String> {
    println!("Downloading {} from S3", file_on_s3);
    let bucket = env::var("GHABUCKET").expect("Need GHABUCKET set to bucket name");
    let client = S3Client::new(default_tls_client().unwrap(),
                               DefaultCredentialsProvider::new().unwrap(),
                               Region::UsEast1);

    let get_req = GetObjectRequest {
        bucket: bucket.to_owned(),
        key: file_on_s3.to_owned(),
        ..Default::default()
    };

    let result = client.get_object(&get_req).expect("Couldn't GET object");
    let decoder = GzDecoder::new(result.body.expect("body should be preset")).unwrap();
    println!("Parsing {}", file_on_s3);
    parse_ze_file(BufReader::new(decoder))
}

fn parse_ze_file<R: BufRead>(contents: R) -> Result<Vec<Event>, String> {
    let events: Vec<Event> = contents
        .lines()
        .map(|l| {
            serde_json::from_str(&l.unwrap()).expect("Couldn't deserialize event file.")
        })
        .collect();

    Ok(events)
}
