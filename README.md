# rusty-von-humboldt
Exploring GitHub Archive with Rust

https://www.githubarchive.org/

## Overview

Rusty von Humboldt downloads, parses and extracts information from GitHub Archive (GHA) events.

Assumptions:
* GHA events are mirrored on an S3 bucket
* The destination bucket is writable by the AWS credentials available
* A Postgres database is available to load the results into

### Running

Dry run of parsing one hour from 2016.  Doesn't upload the results to S3:

`DRYRUN=true GHABUCKET=sourcebucketname DESTBUCKET=destbucketname GHAYEAR=2016 GHAHOURS=1 cargo run --release`

Upload results to S3:

`DRYRUN=false GHABUCKET=sourcebucketname DESTBUCKET=destbucketname GHAYEAR=2016 GHAHOURS=1 cargo run --release`

## Implemented behavior

#### Committer count

Count events where a GitHub account has either had a pull request (PR) accepted or a direct push event of commits to
the repository.

#### Repository ID/name mapping

Since repositories can be renamed on GitHub, we follow the repo ID. By using Postgres' upsert functionality we keep the
most up to date name of the repository.  An example: repo ID of 1 is called `foo/bar` and is renamed to `foo/baz`. All
committer counts to the repository are tracked and applied to the most recent name.