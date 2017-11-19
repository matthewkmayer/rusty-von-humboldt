#[derive(Serialize, Deserialize, Debug)]
pub struct Actor {
    id: i64,
    display_login: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Repo {
    id: i64,
    name: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Event {
    id: String,
    #[serde(rename = "type")]
    event_type: String,
    actor: Actor,
    repo: Repo,
}