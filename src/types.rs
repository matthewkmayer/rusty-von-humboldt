#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct Actor {
    pub id: i64,
    pub display_login: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd, Ord, Eq)]
pub struct Repo {
    pub id: i64,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Event {
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub actor: Actor,
    pub repo: Repo,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pr_by_actor {
    pub repo: Repo,
    pub actor: Actor,
}