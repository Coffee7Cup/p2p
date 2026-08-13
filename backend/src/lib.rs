uniffi::setup_scaffolding!();

mod app_manager;
mod chats;
mod errors;
mod message_queue;
mod models;
mod tor;

#[derive(uniffi::Object)]
struct Greeter {
    name: String,
}

pub type Result<T> = Result<T, errors::P2PError>;

#[uniffi::export]
impl Greeter {
    #[uniffi::constructor]
    pub fn new(name: &str) -> Self {
        Greeter {
            name: name.to_string(),
        }
    }

    pub fn say_hi(&self) -> String {
        format!("Hello : {}", self.name)
    }

    pub fn ask_if_he_ate(&self) -> String {
        format!("Hey {} have you ate?", self.name)
    }
}
