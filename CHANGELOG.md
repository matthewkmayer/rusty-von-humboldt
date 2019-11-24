## Changelog

### Unreleased

(entries go here)

* Switched to a BTreeMap instead of Vec to get automatic deduplication of entries
* Updated Rusoto to 0.42
* Check for destination bucket access before running the analysis

### 0.2.0 - 11/15/2019

* Use env vars to switch between modes
* Checked in Cargo.lock for reproducible builds
* Added multi-line progress bar for file downloads
* Switch to crossbeam channels from stdlib ones
* Print hours of GHA processed and total run time at exit

### 0.1.0

* Project started