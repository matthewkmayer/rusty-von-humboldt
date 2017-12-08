use std::fmt::Display;
use std::str::FromStr;

use serde::de::{self, Deserialize, Deserializer};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct Actor {
    pub id: i64,
    pub display_login: Option<String>,
    pub login: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct Repo {
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
    #[serde(rename = "type")]
    pub event_type: String,
    pub actor: Actor,
    pub repo: Repo,
    pub payload: Option<Payload>,
}

impl Event {
    pub fn new() -> Event {
        Event {
            id: -1,
            event_type: "n/a".to_string(),
            actor: Actor {
                id: -1,
                display_login: None,
                login: None,
            },
            repo: Repo {
                id: -1,
                name: "n/a".to_string(),
            },
            payload: None,
        }
    }

    pub fn is_temp_one(&self) -> bool{
        if self.id == -1 && self.repo.id == -1 && self.actor.id == -1 {
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
#[derive(Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct RepoIdToName {
    pub repo_id: i64,
    pub repo_name: String,
    pub event_id: i64,
}

impl RepoIdToName {
    pub fn as_sql(&self) -> String {
        // TODO: upsert: if ours is newer, we win!
        format!("INSERT INTO foo (repo_id, repo_name, event_id)
            VALUES ({repo_id}, {repo_name}, {event_id})
            ON CONFLICT (repo_id) DO UPDATE SET (repo_name, event_id) = ({repo_name}, {event_id})
            WHERE ",
            repo_id = self.repo_id,
            repo_name = self.repo_name,
            event_id = self.event_id).replace("\n", "")
    }
}

fn from_str<'de, T, D>(deserializer: D) -> Result<T, D::Error>
    where T: FromStr,
          T::Err: Display,
          D: Deserializer<'de>
{
    let s = String::deserialize(deserializer)?;
    T::from_str(&s).map_err(de::Error::custom)
}