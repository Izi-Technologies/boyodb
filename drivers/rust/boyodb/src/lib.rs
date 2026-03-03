//! # boyodb
//!
//! A native Rust client driver for boyodb-server with connection pooling,
//! Arrow support, and transaction management.
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
//!     println!("Got {} rows", result.row_count);
//!
//!     client.close().await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Connection Pooling
//!
//! ```rust,no_run
//! use boyodb::{Pool, PoolConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), boyodb::Error> {
//!     let pool = Pool::new("localhost:8765", PoolConfig::new(10)).await?;
//!
//!     let conn = pool.get().await?;
//!     conn.query("SELECT 1").await?;
//!
//!     // Connection automatically returned to pool when dropped
//!     Ok(())
//! }
//! ```
//!
//! ## Batch Inserts
//!
//! ```rust,no_run
//! use boyodb::{Client, Config, batch::{BatchInserter, Value}};
//! use arrow_schema::{Schema, Field, DataType};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), boyodb::Error> {
//!     let client = Client::connect("localhost:8765", Config::default()).await?;
//!
//!     let schema = Arc::new(Schema::new(vec![
//!         Field::new("id", DataType::Int64, false),
//!         Field::new("name", DataType::Utf8, true),
//!     ]));
//!
//!     let mut inserter = BatchInserter::new(&client, "mydb", "users", schema, 1000);
//!
//!     for i in 0..10000 {
//!         inserter.add_row_and_maybe_flush(vec![
//!             Value::Int64(i),
//!             Value::String(format!("user_{}", i)),
//!         ]).await?;
//!     }
//!     inserter.flush().await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Transactions
//!
//! ```rust,no_run
//! use boyodb::{Client, Config, transaction::Transaction};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), boyodb::Error> {
//!     let client = Client::connect("localhost:8765", Config::default()).await?;
//!
//!     let txn = Transaction::begin(&client).await?;
//!     txn.exec("INSERT INTO mydb.users (id, name) VALUES (1, 'Alice')").await?;
//!     txn.exec("INSERT INTO mydb.users (id, name) VALUES (2, 'Bob')").await?;
//!     txn.commit().await?;
//!
//!     Ok(())
//! }
//! ```

mod client;
mod config;
mod error;
mod result;

pub mod batch;
pub mod pool;
pub mod transaction;

pub use client::Client;
pub use config::Config;
pub use error::Error;
pub use pool::{Pool, PoolConfig, PoolStats, PooledClient};
pub use result::{QueryResult, TableInfo};
pub use transaction::{IsolationLevel, Transaction, TransactionExt};

// Re-export commonly used arrow types for convenience
pub use arrow_array::RecordBatch;
pub use arrow_schema::{DataType, Field, Schema};
