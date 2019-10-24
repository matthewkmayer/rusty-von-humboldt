extern crate sha1;

use chrono::{DateTime, TimeZone, Utc};
use serde::de::{self, Deserialize, Deserializer};
use serde_json::Value;
use std::fmt::Display;
use std::str::FromStr;

// source events from github archive

/// An actor is a GitHub account.
/// This one represents a 2015 and later event.
/// If the ID isn't included we use a placeholder value.
#[derive(Deserialize, Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct Actor {
    #[serde(default = "id_not_specified")]
    pub id: i64,
    pub login: Option<String>,
}

/// GitHub repository.  Assuming the ID stays constant but the name can change.
#[derive(Deserialize, Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct Repo {
    #[serde(default = "id_not_specified")]
    pub id: i64,
    pub name: String,
}

/// Pull request in an event.  We only care if it was merged or not.
#[derive(Deserialize, Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct PullRequest {
    pub merged: Option<bool>,
    #[serde(rename = "user")]
    pub actor: Option<Actor>,
}

/// A git commit.
#[derive(Deserialize, Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct Commit {
    pub sha: Option<String>,
}

/// Type containing if it's a push event or pull request event.
#[derive(Deserialize, Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct Payload {
    pub action: Option<String>,
    #[serde(rename = "pull_request")]
    pub pull_request: Option<PullRequest>,
    pub commits: Option<Vec<Commit>>,
}

/// 2015 and later github archive event.
#[derive(Deserialize, Debug, Clone)]
pub struct Event {
    #[serde(deserialize_with = "from_str")]
    pub id: i64,
    pub created_at: DateTime<Utc>,
    #[serde(rename = "type")]
    pub event_type: String,
    pub actor: Actor,
    pub repo: Repo,
    pub payload: Option<Payload>,
}

impl Event {
    /// Constructor for a placeholder event.
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
            created_at: Utc.ymd(2010, 1, 1).and_hms(0, 0, 0),
        }
    }

    pub fn as_repo_id_mapping(&self) -> RepoIdToName {
        RepoIdToName {
            repo_id: self.repo.id,
            repo_name: self.repo.name.clone(),
            event_timestamp: self.created_at.clone(),
        }
    }

    pub fn as_commit_event(&self) -> CommitEvent {
        if self.event_type == "PullRequestEvent" {
            CommitEvent {
                actor: match self.payload {
                    Some(ref payload) => match payload.pull_request {
                        Some(ref pull_request) => match pull_request.actor {
                            Some(ref actor) => match actor.login {
                                Some(ref login) => login.clone(),
                                None => "".to_string(),
                            },
                            None => "".to_string(),
                        },
                        None => "".to_string(),
                    },
                    None => "".to_string(),
                },
                repo_id: self.repo.id,
            }
        } else {
            CommitEvent {
                actor: match self.actor.login {
                    Some(ref actor_login) => actor_login.clone(),
                    None => "".to_string(),
                },
                repo_id: self.repo.id,
            }
        }
    }

    // Also covers placeholder Events made in the constructor above
    pub fn is_missing_data(&self) -> bool {
        if self.id == -1 || self.repo.id == -1 || self.actor.id == -1 {
            return true;
        }
        false
    }

    pub fn is_commit_event(&self) -> bool {
        self.is_accepted_pr() || self.is_direct_push_event()
    }

    // This needs some testing
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

#[cfg(test)]
mod tests {
    extern crate serde_json;

    // Direct push to the repo counts as a commit
    #[test]
    fn direct_push_committer_gets_counted() {
        use types::Event;
        let commit_text = r#"
        {
  "id": "5785865382",
  "type": "PushEvent",
  "actor": {
    "id": 1234,
    "login": "direct_committer",
    "display_login": "direct_committer",
    "url": "https://api.github.com/users/direct_committer"
    },
  "repo": {
    "id": 255,
    "name": "foo/bar",
    "url": "https://api.github.com/repos/foo/bar"
  },
  "payload": {
    "push_id": 1234567,
    "size": 1,
    "distinct_size": 1
  },
  "created_at": "2017-05-01T07:00:00Z"
}
"#;
        let event: Event = match serde_json::from_str(&commit_text) {
            Ok(event) => event,
            Err(err) => panic!("Found a weird line of json, got this error: {:?}.", err),
        };
        let commit_event = event.as_commit_event();

        assert_eq!("direct_committer", commit_event.actor);
        assert_eq!(255, commit_event.repo_id);
    }

    // Ensure we count the person who made the PR as a committer, not the person who accepted it:
    #[test]
    fn pull_request_committer_gets_counted() {
        use types::Event;
        let pr_text = r#"{
  "id": "12345",
  "type": "PullRequestEvent",
  "actor": {
    "id": 1,
    "login": "owner-login",
    "display_login": "owner-login"
    },
    "repo": {
"id": 155,
"name": "foo/reponame",
"url": "https://api.github.com/repos/foo/reponame"
},
"payload": {
"action": "closed",
"pull_request": {
"state": "closed",
"user": {
"id": 5,
"login": "committer-login"
},
"created_at": "2017-04-30T13:14:51Z",
"updated_at": "2017-05-01T07:01:53Z",
"closed_at": "2017-05-01T07:01:53Z",
"merged_at": "2017-05-01T07:01:53Z",
"head": {
"repo": {
"id": 155,
"name": "reponame"
}
},
"base": {
"label": "foo:master",
"ref": "master",
"sha": "a829c2e22381a1ff55824602127b9a7e440d7dc5",
"repo": {
"id": 1234,
"name": "reponame",
"full_name": "foo/reponame",
"created_at": "2014-12-03T22:47:01Z",
"updated_at": "2017-04-27T09:13:53Z",
"pushed_at": "2017-05-01T07:01:53Z"
}
},
"merged": true
}
},
"public": true,
"created_at": "2017-05-01T07:01:53Z"
}"#;
        let event: Event = match serde_json::from_str(&pr_text) {
            Ok(event) => event,
            Err(err) => panic!("Found a weird line of json, got this error: {:?}.", err),
        };
        let commit_event = event.as_commit_event();

        assert_eq!("committer-login", commit_event.actor);
        assert_eq!(155, commit_event.repo_id);
    }
}

/// Get the login for the user/actor
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

/// Custom deserializer to handle getting the actor login for pre-2015 events.
/// What we're looking for can be in two different places.
impl<'de> Deserialize<'de> for Pre2015Actor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize, Debug)]
        struct ActorHelper {
            login: String,
        }

        let v = Value::deserialize(deserializer)?;
        if v.to_string().contains("{") {
            // sometimes it's just missing, we'll deal with it by ignoring it.
            let helper = ActorHelper::deserialize(&v).map_err(de::Error::custom)?;
            // println!("all good, helper is {:?}", helper);
            Ok(Pre2015Actor {
                actor: helper.login,
            })
        } else {
            Ok(Pre2015Actor {
                actor: v.to_string().replace("\"", ""), // don't pass along the value of `"foo"`, make it `foo`
            })
        }
    }
}

/// A github archive event before 2015.
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
            None => {
                match self.repository {
                    Some(ref repository) => repository.id,
                    None => -1, // TODO: somehow ignore this event, as we can't use it
                }
            }
        };
        repo_id
    }

    // TODO: if the event is old enough it just says "closed" for status, assume closed ones are accepted.
    // Right now this is conservative and may mark accepted PRs as not accepted if the event
    // doesn't specifically state it was accepted.
    pub fn is_accepted_pr(&self) -> bool {
        if self.event_type != "PullRequestEvent" {
            return false;
        }
        match self.payload {
            Some(ref payload) => {
                match payload.pull_request {
                    Some(ref pr) => {
                        match pr.merged {
                            Some(merged) => merged,
                            None => false, // sometimes merged isn't there, instead of ignoring should we assume it was accepted?
                        }
                    }
                    None => false,
                }
            }
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

// -----------------------------------------------
// events trimmed down to the fields we care about
#[derive(Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct CommitEvent {
    pub actor: String,
    pub repo_id: i64,
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
    pub event_timestamp: DateTime<Utc>,
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

/// Allows us to convert "1234" to 1234 integer type
fn from_str<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    T: FromStr,
    T::Err: Display,
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    T::from_str(&s).map_err(de::Error::custom)
}
