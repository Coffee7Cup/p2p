uniffi::setup_scaffolding!();

// WARN: This is the worst error handled code Do Something

mod app_manager;
mod errors;
mod message_queue;
mod tor;

#[derive(uniffi::Object)]
struct Greeter {
    name: String,
}

pub type Result<T> = std::result::Result<T, errors::P2PError>;

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
