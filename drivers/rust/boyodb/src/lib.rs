//! # boyodb
//!
//! A Rust client driver for boyodb-server.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use boyodb::{Client, Config};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), boyodb::Error> {
//!     let client = Client::connect("localhost:8765", Config::default()).await?;
//!
//!     let result = client.query("SELECT * FROM mydb.users LIMIT 10").await?;
//!     for row in result.rows {
//!         println!("{:?}", row);
//!     }
//!
//!     client.close().await?;
//!     Ok(())
//! }
//! ```

mod client;
mod config;
mod error;
mod result;

pub use client::Client;
pub use config::Config;
pub use error::Error;
pub use result::{QueryResult, TableInfo};
