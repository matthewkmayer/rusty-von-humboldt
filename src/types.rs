#[derive(Serialize, Deserialize, Debug)]
pub struct Actor {
    pub id: i64,
    pub display_login: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Repo {
    pub id: i64,
    pub name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Event {
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    pub actor: Actor,
    pub repo: Repo,
}