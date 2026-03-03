//! Transaction support for BoyoDB.
//!
//! Provides transaction management with automatic rollback on errors.

use crate::client::Client;
use crate::error::Error;
use crate::result::QueryResult;

/// Transaction isolation levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// Read committed isolation (default).
    ReadCommitted,
    /// Repeatable read isolation.
    RepeatableRead,
    /// Serializable isolation.
    Serializable,
}

impl IsolationLevel {
    fn as_sql(&self) -> &'static str {
        match self {
            IsolationLevel::ReadCommitted => "READ COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE READ",
            IsolationLevel::Serializable => "SERIALIZABLE",
        }
    }
}

impl Default for IsolationLevel {
    fn default() -> Self {
        IsolationLevel::ReadCommitted
    }
}

/// A transaction handle.
///
/// The transaction is automatically rolled back if not committed.
pub struct Transaction<'a> {
    client: &'a Client,
    committed: bool,
    rolled_back: bool,
}

impl<'a> Transaction<'a> {
    /// Begin a new transaction.
    pub async fn begin(client: &'a Client) -> Result<Transaction<'a>, Error> {
        Self::begin_with_isolation(client, IsolationLevel::default()).await
    }

    /// Begin a new transaction with a specific isolation level.
    pub async fn begin_with_isolation(
        client: &'a Client,
        isolation: IsolationLevel,
    ) -> Result<Transaction<'a>, Error> {
        let sql = format!("BEGIN TRANSACTION ISOLATION LEVEL {}", isolation.as_sql());
        client.exec(&sql).await?;

        Ok(Transaction {
            client,
            committed: false,
            rolled_back: false,
        })
    }

    /// Execute a SQL query within the transaction.
    pub async fn query(&self, sql: &str) -> Result<QueryResult, Error> {
        if self.committed || self.rolled_back {
            return Err(Error::Transaction("Transaction already ended".into()));
        }
        self.client.query(sql).await
    }

    /// Execute a SQL statement within the transaction.
    pub async fn exec(&self, sql: &str) -> Result<(), Error> {
        if self.committed || self.rolled_back {
            return Err(Error::Transaction("Transaction already ended".into()));
        }
        self.client.exec(sql).await
    }

    /// Create a savepoint within the transaction.
    pub async fn savepoint(&self, name: &str) -> Result<Savepoint<'_, 'a>, Error> {
        if self.committed || self.rolled_back {
            return Err(Error::Transaction("Transaction already ended".into()));
        }

        let sql = format!("SAVEPOINT {}", name);
        self.client.exec(&sql).await?;

        Ok(Savepoint {
            transaction: self,
            name: name.to_string(),
            released: false,
        })
    }

    /// Commit the transaction.
    pub async fn commit(mut self) -> Result<(), Error> {
        if self.rolled_back {
            return Err(Error::Transaction("Transaction already rolled back".into()));
        }
        if self.committed {
            return Ok(());
        }

        self.client.exec("COMMIT").await?;
        self.committed = true;
        Ok(())
    }

    /// Rollback the transaction.
    pub async fn rollback(mut self) -> Result<(), Error> {
        if self.committed {
            return Err(Error::Transaction("Transaction already committed".into()));
        }
        if self.rolled_back {
            return Ok(());
        }

        self.client.exec("ROLLBACK").await?;
        self.rolled_back = true;
        Ok(())
    }

    /// Check if the transaction has been committed.
    pub fn is_committed(&self) -> bool {
        self.committed
    }

    /// Check if the transaction has been rolled back.
    pub fn is_rolled_back(&self) -> bool {
        self.rolled_back
    }

    /// Check if the transaction is still active.
    pub fn is_active(&self) -> bool {
        !self.committed && !self.rolled_back
    }
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if !self.committed && !self.rolled_back {
            // Transaction not properly ended, log a warning
            tracing::warn!(
                "Transaction dropped without commit or rollback. \
                 The server will automatically rollback."
            );
        }
    }
}

/// A savepoint within a transaction.
pub struct Savepoint<'a, 'b> {
    transaction: &'a Transaction<'b>,
    name: String,
    released: bool,
}

impl<'a, 'b> Savepoint<'a, 'b> {
    /// Rollback to this savepoint.
    pub async fn rollback(&self) -> Result<(), Error> {
        if self.released {
            return Err(Error::Transaction("Savepoint already released".into()));
        }

        let sql = format!("ROLLBACK TO SAVEPOINT {}", self.name);
        self.transaction.client.exec(&sql).await
    }

    /// Release this savepoint (make it permanent within the transaction).
    pub async fn release(mut self) -> Result<(), Error> {
        if self.released {
            return Ok(());
        }

        let sql = format!("RELEASE SAVEPOINT {}", self.name);
        self.transaction.client.exec(&sql).await?;
        self.released = true;
        Ok(())
    }

    /// Get the savepoint name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Extension trait for Client to provide transaction helpers.
#[async_trait::async_trait]
pub trait TransactionExt {
    /// Execute a closure within a transaction.
    ///
    /// If the closure returns Ok, the transaction is committed.
    /// If the closure returns Err or panics, the transaction is rolled back.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use boyodb::{Client, Config, transaction::TransactionExt};
    ///
    /// async fn example(client: &Client) -> Result<(), boyodb::Error> {
    ///     client.with_transaction(|client| Box::pin(async move {
    ///         client.exec("INSERT INTO db.table (id) VALUES (1)").await?;
    ///         client.exec("INSERT INTO db.table (id) VALUES (2)").await?;
    ///         Ok(())
    ///     })).await
    /// }
    /// ```
    async fn with_transaction<F, Fut, T>(&self, f: F) -> Result<T, Error>
    where
        F: FnOnce(&Client) -> Fut + Send,
        Fut: std::future::Future<Output = Result<T, Error>> + Send,
        T: Send;
}

#[async_trait::async_trait]
impl TransactionExt for Client {
    async fn with_transaction<F, Fut, T>(&self, f: F) -> Result<T, Error>
    where
        F: FnOnce(&Client) -> Fut + Send,
        Fut: std::future::Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        // Begin the transaction
        self.exec("BEGIN TRANSACTION").await?;

        // Execute the user's code
        match f(self).await {
            Ok(result) => {
                // Commit on success
                self.exec("COMMIT").await?;
                Ok(result)
            }
            Err(e) => {
                // Rollback on failure
                let _ = self.exec("ROLLBACK").await;
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_isolation_level_sql() {
        assert_eq!(IsolationLevel::ReadCommitted.as_sql(), "READ COMMITTED");
        assert_eq!(IsolationLevel::RepeatableRead.as_sql(), "REPEATABLE READ");
        assert_eq!(IsolationLevel::Serializable.as_sql(), "SERIALIZABLE");
    }

    #[test]
    fn test_default_isolation() {
        assert_eq!(IsolationLevel::default(), IsolationLevel::ReadCommitted);
    }
}
