# rusty-von-humboldt

[![Build Status](https://travis-ci.com/matthewkmayer/rusty-von-humboldt.svg?branch=master)](https://travis-ci.com/matthewkmayer/rusty-von-humboldt)

Exploring GitHub Archive with Rust: https://www.githubarchive.org/

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

### Logging

`RUST_LOG=rusty_von_humboldt=info` or `RUST_LOG=rusty_von_humboldt=debug` as an environment variable. Full example:

`RUST_LOG=rusty_von_humboldt=debug DRYRUN=true GHABUCKET=sourcebucketname DESTBUCKET=destbucketname GHAYEAR=2016 GHAHOURS=1 cargo run --release`

If `RUST_LOG=debug` is set without the `rusty_von_humboldt`, other libraries will also log. Rusoto currently logs lots of information about HTTP requests/responses with this flag, so limiting the info to `RUST_LOG=rusty_von_humboldt=debug` is probably the most useful.

## Implemented behavior

#### Committer count

Count events where a GitHub account has either had a pull request (PR) accepted or a direct push event of commits to
the repository.

#### Repository ID/name mapping

Since repositories can be renamed on GitHub, we follow the repo ID. By using Postgres' upsert functionality we keep the
most up to date name of the repository.  An example: repo ID of 1 is called `foo/bar` and is renamed to `foo/baz`. All
committer counts to the repository are tracked and applied to the most recent name.

### Disabling progress bar

Use the `--no-default-features` flag to compile without progress bar output.
