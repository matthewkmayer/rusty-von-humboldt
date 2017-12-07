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
    pub id: String,
    pub id_as_i64: Option<i64>,
    #[serde(rename = "type")]
    pub event_type: String,
    pub actor: Actor,
    pub repo: Repo,
    pub payload: Option<Payload>,
}

impl Event {
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