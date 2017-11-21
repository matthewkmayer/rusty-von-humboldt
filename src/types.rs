#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct Actor {
    pub id: i64,
    pub display_login: String,
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
pub struct Payload {
    pub action: Option<String>,
    #[serde(rename = "pull_request")]
    pub pull_request: Option<PullRequest>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Event {
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub actor: Actor,
    pub repo: Repo,
    pub payload: Option<Payload>,
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct PrByActor {
    pub repo: Repo,
    pub actor: Actor,
}