#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;
extern crate rusoto_core;
extern crate rusoto_s3;

pub mod types;
pub use types::*;