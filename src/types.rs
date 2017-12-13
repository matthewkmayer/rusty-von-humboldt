use std::fmt::Display;
use std::str::FromStr;
use serde::de::{self, Deserialize, Deserializer};
use serde_json::Value;

// TODO: sort file as source event types then mapped types.

#[derive(Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct CommitEvent {
    pub actor: String,
    pub repo_id: i64,
}

impl CommitEvent {
    pub fn as_sql(&self) -> String {
        // Sometimes bad data can still get to here, skip if we don't have all the data required.
        if self.repo_id == -1 || self.actor == "" {
            return "".to_string();
        }
        let sql = format!("INSERT INTO committer_repo_id_names (repo_id, actor_name)
            VALUES ({repo_id}, '{actor_name}')
            ON CONFLICT DO NOTHING;",
            repo_id = self.repo_id,
            actor_name = self.actor);

        sql
    }
}

#[derive(Deserialize, Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct Actor {
    #[serde(default = "id_not_specified")]
    pub id: i64,
    pub login: Option<String>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct Repo {
    #[serde(default = "id_not_specified")]
    pub id: i64,
    pub name: String,
}

#[derive(Deserialize, Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct PullRequest {
    pub merged: Option<bool>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct Commit {
    pub sha: Option<String>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct Payload {
    pub action: Option<String>,
    #[serde(rename = "pull_request")]
    pub pull_request: Option<PullRequest>,
    pub commits: Option<Vec<Commit>>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Event {
    #[serde(deserialize_with = "from_str")]
    pub id: i64,
    pub created_at: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub actor: Actor,
    pub repo: Repo,
    pub payload: Option<Payload>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ActorAttributes {
    pub login: String,
}

#[derive(Deserialize, Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct OldPullRequest {
    pub merged: Option<bool>,
}

#[derive(Deserialize, Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct OldPayload {
    pub size: Option<i32>,
    pub pull_request: Option<OldPullRequest>,
}

#[derive(Debug, Clone)]
pub struct Pre2015Actor {
    actor: String,
}

impl<'de> Deserialize<'de> for Pre2015Actor
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        #[derive(Deserialize, Debug)]
        struct ActorHelper {
            login: String,
        }

        let v = Value::deserialize(deserializer)?;
        // println!("v is {}", v);
        if v.to_string().contains("{") {
            let helper = ActorHelper::deserialize(&v).map_err(de::Error::custom)?;
            // println!("all good, helper is {:?}", helper);
            Ok(Pre2015Actor{
                actor: helper.login,
            })
        } else {
            Ok(Pre2015Actor{
                actor: v.to_string().replace("\"", ""), // don't pass along the value of `"foo"`, make it `foo`
            })
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Pre2015Event {
    pub repository: Option<Repo>,
    pub repo: Option<Repo>,
    #[serde(rename = "type")]
    pub event_type: String,
    pub actor: Pre2015Actor,
    pub created_at: String,
    pub payload: Option<OldPayload>,
}

impl Pre2015Event {
    pub fn is_commit_event(&self) -> bool {
        self.is_accepted_pr() || self.is_direct_push_event()
    }

    pub fn as_commit_event(&self) -> CommitEvent {
        CommitEvent {
            actor: self.actor_name().to_string(),
            repo_id: self.repo_id(),
        }
    }

    pub fn actor_name(&self) -> String {
        self.actor.actor.to_string()
    }

    pub fn repo_id(&self) -> i64 {
        let repo_id = match self.repo {
            Some(ref repo) => repo.id,
            None => match self.repository {
                Some(ref repository) => repository.id,
                None => -1, // TODO: somehow ignore this event, as we can't use it
            }
        };
        repo_id
    }

    // TODO: if the event is old enough it just says "closed" for status, assume closed ones are accepted.
    pub fn is_accepted_pr(&self) -> bool {
        if self.event_type != "PullRequestEvent" {
            return false;
        }
        match self.payload {
            Some(ref payload) => match payload.pull_request {
                Some(ref pr) => match pr.merged {
                    Some(merged) => merged,
                    None => false, // sometimes merged isn't there, instead of ignoring should we assume it was accepted?
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
            Some(ref payload) => match payload.size {
                Some(x) => x > 0,
                None => false,
            },
            None => false,
        }
    }
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

    pub fn as_commit_event(&self) -> CommitEvent {
        CommitEvent {
            actor: match self.actor.login {
                Some(ref actor_login) => actor_login.clone(),
                None => "".to_string(),
            },
            repo_id: self.repo.id,
        }
    }

    // Also covers placeholder Events made in the constructor above
    pub fn is_missing_data(&self) -> bool{
        if self.id == -1 || self.repo.id == -1 || self.actor.id == -1 {
            return true;
        }
        false
    }

    pub fn is_commit_event(&self) -> bool {
        self.is_accepted_pr() || self.is_direct_push_event()
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
    pub event_timestamp: String,
}

impl RepoIdToName {
    pub fn as_sql(&self) -> String {
        // Sometimes bad data can still get to here, skip if we don't have all the data required.
        if self.repo_id == -1 || self.repo_name == "" {
            return "".to_string();
        }
        let sql = format!("INSERT INTO repo_mapping (repo_id, repo_name, event_timestamp)
            VALUES ({repo_id}, '{repo_name}', '{event_timestamp}')
            ON CONFLICT (repo_id) DO UPDATE SET (repo_name, event_timestamp) = ('{repo_name}', '{event_timestamp}')
            WHERE repo_mapping.repo_id = EXCLUDED.repo_id AND repo_mapping.event_timestamp < EXCLUDED.event_timestamp;",
            repo_id = self.repo_id,
            repo_name = self.repo_name,
            event_timestamp = self.event_timestamp).replace("\n", "");

        sql
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