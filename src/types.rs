use std::fmt::Display;
use std::str::FromStr;

use serde::de::{self, Deserialize, Deserializer};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct Actor {
    #[serde(default = "id_not_specified")]
    pub id: i64,
    pub login: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct Repo {
    #[serde(default = "id_not_specified")]
    pub id: i64,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct PullRequest {
    pub merged: Option<bool>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct Commit {
    pub sha: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct Payload {
    pub action: Option<String>,
    #[serde(rename = "pull_request")]
    pub pull_request: Option<PullRequest>,
    pub commits: Option<Vec<Commit>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Event {
    #[serde(deserialize_with = "from_str")]
    pub id: i64,
    // Actually a datetime, may need to adjust later
    // EG: "created_at": "2013-01-01T12:00:17-08:00" for pre-2015 (rfc3339)
    // "created_at": "2017-05-01T00:59:44Z" for post-2015 (UTC)
    pub created_at: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub actor: Actor,
    pub repo: Repo,
    pub payload: Option<Payload>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ActorAttributes {
    pub login: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Pre2015Event {
    // sometimes called repository because why not?
    pub repository: Option<Repo>,
    pub repo: Option<Repo>,
    #[serde(rename = "type")]
    pub event_type: String,
    // sometimes this is a struct, sometimes it's a string
    // pub actor: Actor,
    // pub actor_attributes: ActorAttributes
    // Actually a datetime, may need to adjust later
    pub created_at: String,
}

impl Event {
    pub fn new() -> Event {
        Event {
            id: -1,
            event_type: "n/a".to_string(),
            actor: Actor {
                id: -1,
                login: None,
            },
            repo: Repo {
                id: -1,
                name: "n/a".to_string(),
            },
            payload: None,
            created_at: "".to_string(),
        }
    }

    // Also covers placeholder Events made in the constructor above
    pub fn is_missing_data(&self) -> bool{
        if self.id == -1 || self.repo.id == -1 || self.actor.id == -1 {
            return true;
        }
        false
    }

    pub fn is_accepted_pr(&self) -> bool {
        if self.event_type != "PullRequestEvent" {
            return false;
        }
        match self.payload {
            Some(ref payload) => match payload.pull_request {
                Some(ref pr) => match pr.merged {
                    Some(merged) => merged,
                    None => false,
                },
                None => false,
            },
            None => false,
        }
    }

    pub fn is_direct_push_event(&self) -> bool {
        if self.event_type != "PushEvent" {
            return false;
        }
        match self.payload {
            Some(ref payload) => match payload.commits {
                Some(ref commits) => commits.len() > 0,
                None => false,
            },
            None => false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct PrByActor {
    pub repo: Repo,
    pub actor: Actor,
}

// Let us figure out if there is a new name for the repo
// TODO: pre-2015 events don't have event_id, switch that to the created_at timestamp
#[derive(Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct RepoIdToName {
    pub repo_id: i64,
    pub repo_name: String,
    pub event_timestamp: String,
}

// TODO: check the new timestamp way works
impl RepoIdToName {
    pub fn as_sql(&self) -> String {
        format!("INSERT INTO repo_mapping (repo_id, repo_name, event_id)
            VALUES ({repo_id}, '{repo_name}', {event_id})
            ON CONFLICT (repo_id) DO UPDATE SET (repo_name, event_id) = ('{repo_name}', {event_id})
            WHERE repo_mapping.repo_id = EXCLUDED.repo_id AND repo_mapping.event_id < EXCLUDED.event_id;",
            repo_id = self.repo_id,
            repo_name = self.repo_name,
            event_id = self.event_timestamp).replace("\n", "")
    }
}

fn id_not_specified() -> i64 {
    -1
}

fn from_str<'de, T, D>(deserializer: D) -> Result<T, D::Error>
    where T: FromStr,
          T::Err: Display,
          D: Deserializer<'de>
{
    let s = String::deserialize(deserializer)?;
    T::from_str(&s).map_err(de::Error::custom)
}