//! Blockchain Ledger Module
//!
//! Provides immutable audit logging with cryptographic verification for BoyoDB.
//! Features:
//! - Immutable append-only ledger
//! - Cryptographic hash chains (SHA-256)
//! - Merkle tree verification
//! - Digital signatures
//! - Tamper detection
//! - Audit trail queries

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

/// Block in the blockchain ledger
#[derive(Debug, Clone)]
pub struct Block {
    /// Block index in the chain
    pub index: u64,
    /// Timestamp when block was created
    pub timestamp: u64,
    /// Hash of the previous block
    pub previous_hash: String,
    /// Hash of this block
    pub hash: String,
    /// Merkle root of transactions
    pub merkle_root: String,
    /// Transactions in this block
    pub transactions: Vec<Transaction>,
    /// Nonce for proof-of-work (optional)
    pub nonce: u64,
}

impl Block {
    /// Create genesis block
    pub fn genesis() -> Self {
        let timestamp = current_timestamp();
        let mut block = Block {
            index: 0,
            timestamp,
            previous_hash: "0".repeat(64),
            hash: String::new(),
            merkle_root: "0".repeat(64),
            transactions: Vec::new(),
            nonce: 0,
        };
        block.hash = block.calculate_hash();
        block
    }

    /// Create new block
    pub fn new(index: u64, previous_hash: String, transactions: Vec<Transaction>) -> Self {
        let timestamp = current_timestamp();
        let merkle_root = calculate_merkle_root(&transactions);
        let mut block = Block {
            index,
            timestamp,
            previous_hash,
            hash: String::new(),
            merkle_root,
            transactions,
            nonce: 0,
        };
        block.hash = block.calculate_hash();
        block
    }

    /// Calculate block hash
    pub fn calculate_hash(&self) -> String {
        let data = format!(
            "{}{}{}{}{}",
            self.index, self.timestamp, self.previous_hash, self.merkle_root, self.nonce
        );
        sha256_hash(&data)
    }

    /// Verify block integrity
    pub fn verify(&self) -> bool {
        // Verify hash
        if self.hash != self.calculate_hash() {
            return false;
        }

        // Verify merkle root
        let calculated_merkle = calculate_merkle_root(&self.transactions);
        if self.merkle_root != calculated_merkle {
            return false;
        }

        true
    }
}

/// Transaction in the ledger
#[derive(Debug, Clone)]
pub struct Transaction {
    /// Transaction ID
    pub id: String,
    /// Timestamp
    pub timestamp: u64,
    /// Transaction type
    pub tx_type: TransactionType,
    /// Actor who performed the action
    pub actor: String,
    /// Target entity (table, database, etc.)
    pub target: String,
    /// Operation details
    pub details: String,
    /// Transaction hash
    pub hash: String,
    /// Digital signature (optional)
    pub signature: Option<String>,
}

impl Transaction {
    /// Create new transaction
    pub fn new(tx_type: TransactionType, actor: String, target: String, details: String) -> Self {
        let timestamp = current_timestamp();
        let id = generate_tx_id(&actor, timestamp);
        let hash = calculate_tx_hash(&id, timestamp, &tx_type, &actor, &target, &details);

        Transaction {
            id,
            timestamp,
            tx_type,
            actor,
            target,
            details,
            hash,
            signature: None,
        }
    }

    /// Sign transaction
    pub fn sign(&mut self, private_key: &str) {
        // Simplified signing - in production would use proper crypto
        let data = format!("{}{}", self.hash, private_key);
        self.signature = Some(sha256_hash(&data));
    }

    /// Verify transaction hash
    pub fn verify(&self) -> bool {
        let calculated = calculate_tx_hash(
            &self.id,
            self.timestamp,
            &self.tx_type,
            &self.actor,
            &self.target,
            &self.details,
        );
        self.hash == calculated
    }
}

/// Transaction types
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionType {
    /// Data insertion
    Insert,
    /// Data update
    Update,
    /// Data deletion
    Delete,
    /// Schema creation
    CreateSchema,
    /// Schema modification
    AlterSchema,
    /// Schema deletion
    DropSchema,
    /// Access grant
    GrantAccess,
    /// Access revocation
    RevokeAccess,
    /// Configuration change
    ConfigChange,
    /// Custom event
    Custom(String),
}

impl std::fmt::Display for TransactionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionType::Insert => write!(f, "INSERT"),
            TransactionType::Update => write!(f, "UPDATE"),
            TransactionType::Delete => write!(f, "DELETE"),
            TransactionType::CreateSchema => write!(f, "CREATE_SCHEMA"),
            TransactionType::AlterSchema => write!(f, "ALTER_SCHEMA"),
            TransactionType::DropSchema => write!(f, "DROP_SCHEMA"),
            TransactionType::GrantAccess => write!(f, "GRANT_ACCESS"),
            TransactionType::RevokeAccess => write!(f, "REVOKE_ACCESS"),
            TransactionType::ConfigChange => write!(f, "CONFIG_CHANGE"),
            TransactionType::Custom(s) => write!(f, "CUSTOM:{}", s),
        }
    }
}

/// Blockchain ledger
pub struct BlockchainLedger {
    /// Chain of blocks
    blocks: Vec<Block>,
    /// Pending transactions
    pending_transactions: Vec<Transaction>,
    /// Block size limit
    block_size: usize,
    /// Index by transaction ID
    tx_index: HashMap<String, (u64, usize)>, // (block_index, tx_index)
    /// Index by actor
    actor_index: HashMap<String, Vec<String>>, // actor -> tx_ids
    /// Index by target
    target_index: HashMap<String, Vec<String>>, // target -> tx_ids
}

impl BlockchainLedger {
    /// Create new blockchain ledger
    pub fn new(block_size: usize) -> Self {
        let genesis = Block::genesis();
        BlockchainLedger {
            blocks: vec![genesis],
            pending_transactions: Vec::new(),
            block_size,
            tx_index: HashMap::new(),
            actor_index: HashMap::new(),
            target_index: HashMap::new(),
        }
    }

    /// Add transaction to pending pool
    pub fn add_transaction(&mut self, tx: Transaction) -> String {
        let tx_id = tx.id.clone();

        // Update indexes
        self.actor_index
            .entry(tx.actor.clone())
            .or_default()
            .push(tx_id.clone());
        self.target_index
            .entry(tx.target.clone())
            .or_default()
            .push(tx_id.clone());

        self.pending_transactions.push(tx);

        // Auto-mine if block is full
        if self.pending_transactions.len() >= self.block_size {
            self.mine_block();
        }

        tx_id
    }

    /// Mine new block with pending transactions
    pub fn mine_block(&mut self) -> Option<u64> {
        if self.pending_transactions.is_empty() {
            return None;
        }

        let previous_block = self.blocks.last().unwrap();
        let new_index = previous_block.index + 1;

        let transactions: Vec<Transaction> = self.pending_transactions.drain(..).collect();

        // Update tx_index
        for (i, tx) in transactions.iter().enumerate() {
            self.tx_index.insert(tx.id.clone(), (new_index, i));
        }

        let block = Block::new(new_index, previous_block.hash.clone(), transactions);
        self.blocks.push(block);

        Some(new_index)
    }

    /// Get block by index
    pub fn get_block(&self, index: u64) -> Option<&Block> {
        self.blocks.get(index as usize)
    }

    /// Get latest block
    pub fn get_latest_block(&self) -> &Block {
        self.blocks.last().unwrap()
    }

    /// Get transaction by ID
    pub fn get_transaction(&self, tx_id: &str) -> Option<&Transaction> {
        if let Some(&(block_idx, tx_idx)) = self.tx_index.get(tx_id) {
            if let Some(block) = self.blocks.get(block_idx as usize) {
                return block.transactions.get(tx_idx);
            }
        }
        // Check pending
        self.pending_transactions.iter().find(|tx| tx.id == tx_id)
    }

    /// Get transactions by actor
    pub fn get_transactions_by_actor(&self, actor: &str) -> Vec<&Transaction> {
        let mut result = Vec::new();
        if let Some(tx_ids) = self.actor_index.get(actor) {
            for tx_id in tx_ids {
                if let Some(tx) = self.get_transaction(tx_id) {
                    result.push(tx);
                }
            }
        }
        result
    }

    /// Get transactions by target
    pub fn get_transactions_by_target(&self, target: &str) -> Vec<&Transaction> {
        let mut result = Vec::new();
        if let Some(tx_ids) = self.target_index.get(target) {
            for tx_id in tx_ids {
                if let Some(tx) = self.get_transaction(tx_id) {
                    result.push(tx);
                }
            }
        }
        result
    }

    /// Verify entire chain integrity
    pub fn verify_chain(&self) -> ChainVerification {
        let mut result = ChainVerification {
            is_valid: true,
            blocks_verified: 0,
            transactions_verified: 0,
            errors: Vec::new(),
        };

        for (i, block) in self.blocks.iter().enumerate() {
            // Verify block hash
            if !block.verify() {
                result.is_valid = false;
                result
                    .errors
                    .push(format!("Block {} hash verification failed", i));
            }

            // Verify chain linkage (except genesis)
            if i > 0 {
                let prev_block = &self.blocks[i - 1];
                if block.previous_hash != prev_block.hash {
                    result.is_valid = false;
                    result.errors.push(format!(
                        "Block {} previous_hash mismatch with block {}",
                        i,
                        i - 1
                    ));
                }
            }

            // Verify transactions
            for (j, tx) in block.transactions.iter().enumerate() {
                if !tx.verify() {
                    result.is_valid = false;
                    result.errors.push(format!(
                        "Transaction {} in block {} verification failed",
                        j, i
                    ));
                }
                result.transactions_verified += 1;
            }

            result.blocks_verified += 1;
        }

        result
    }

    /// Get chain statistics
    pub fn get_stats(&self) -> LedgerStats {
        let total_transactions: usize = self.blocks.iter().map(|b| b.transactions.len()).sum();

        LedgerStats {
            total_blocks: self.blocks.len(),
            total_transactions,
            pending_transactions: self.pending_transactions.len(),
            unique_actors: self.actor_index.len(),
            unique_targets: self.target_index.len(),
            latest_block_hash: self.get_latest_block().hash.clone(),
        }
    }

    /// Get audit trail for a target
    pub fn get_audit_trail(&self, target: &str) -> AuditTrail {
        let transactions = self.get_transactions_by_target(target);
        let entries: Vec<AuditEntry> = transactions
            .into_iter()
            .map(|tx| AuditEntry {
                timestamp: tx.timestamp,
                actor: tx.actor.clone(),
                action: tx.tx_type.to_string(),
                details: tx.details.clone(),
                tx_hash: tx.hash.clone(),
            })
            .collect();

        AuditTrail {
            target: target.to_string(),
            entries,
        }
    }

    /// Export chain as JSON
    pub fn export_json(&self) -> String {
        let mut blocks_json = Vec::new();
        for block in &self.blocks {
            let txs: Vec<String> = block
                .transactions
                .iter()
                .map(|tx| {
                    format!(
                        r#"{{"id":"{}","timestamp":{},"type":"{}","actor":"{}","target":"{}","hash":"{}"}}"#,
                        tx.id, tx.timestamp, tx.tx_type, tx.actor, tx.target, tx.hash
                    )
                })
                .collect();

            blocks_json.push(format!(
                r#"{{"index":{},"timestamp":{},"hash":"{}","previous_hash":"{}","merkle_root":"{}","transactions":[{}]}}"#,
                block.index,
                block.timestamp,
                block.hash,
                block.previous_hash,
                block.merkle_root,
                txs.join(",")
            ));
        }
        format!("[{}]", blocks_json.join(","))
    }
}

/// Chain verification result
#[derive(Debug, Clone)]
pub struct ChainVerification {
    /// Whether chain is valid
    pub is_valid: bool,
    /// Number of blocks verified
    pub blocks_verified: usize,
    /// Number of transactions verified
    pub transactions_verified: usize,
    /// List of errors found
    pub errors: Vec<String>,
}

/// Ledger statistics
#[derive(Debug, Clone)]
pub struct LedgerStats {
    /// Total number of blocks
    pub total_blocks: usize,
    /// Total number of transactions
    pub total_transactions: usize,
    /// Pending transactions count
    pub pending_transactions: usize,
    /// Number of unique actors
    pub unique_actors: usize,
    /// Number of unique targets
    pub unique_targets: usize,
    /// Hash of latest block
    pub latest_block_hash: String,
}

/// Audit trail for a target
#[derive(Debug, Clone)]
pub struct AuditTrail {
    /// Target entity
    pub target: String,
    /// Audit entries
    pub entries: Vec<AuditEntry>,
}

/// Single audit entry
#[derive(Debug, Clone)]
pub struct AuditEntry {
    /// Timestamp
    pub timestamp: u64,
    /// Actor who performed action
    pub actor: String,
    /// Action performed
    pub action: String,
    /// Details
    pub details: String,
    /// Transaction hash
    pub tx_hash: String,
}

/// Merkle tree node
#[derive(Debug, Clone)]
pub struct MerkleNode {
    /// Node hash
    pub hash: String,
    /// Left child
    pub left: Option<Box<MerkleNode>>,
    /// Right child
    pub right: Option<Box<MerkleNode>>,
}

impl MerkleNode {
    /// Create leaf node
    pub fn leaf(data: &str) -> Self {
        MerkleNode {
            hash: sha256_hash(data),
            left: None,
            right: None,
        }
    }

    /// Create internal node
    pub fn internal(left: MerkleNode, right: MerkleNode) -> Self {
        let combined = format!("{}{}", left.hash, right.hash);
        MerkleNode {
            hash: sha256_hash(&combined),
            left: Some(Box::new(left)),
            right: Some(Box::new(right)),
        }
    }
}

/// Merkle tree for transaction verification
pub struct MerkleTree {
    /// Root node
    pub root: Option<MerkleNode>,
    /// Number of leaves
    pub leaf_count: usize,
}

impl MerkleTree {
    /// Build merkle tree from data
    pub fn build(data: &[String]) -> Self {
        if data.is_empty() {
            return MerkleTree {
                root: None,
                leaf_count: 0,
            };
        }

        let mut nodes: Vec<MerkleNode> = data.iter().map(|d| MerkleNode::leaf(d)).collect();

        // Pad to power of 2
        while nodes.len() > 1 && !nodes.len().is_power_of_two() {
            nodes.push(nodes.last().unwrap().clone());
        }

        let leaf_count = nodes.len();

        // Build tree bottom-up
        while nodes.len() > 1 {
            let mut parent_level = Vec::new();
            for chunk in nodes.chunks(2) {
                if chunk.len() == 2 {
                    parent_level.push(MerkleNode::internal(chunk[0].clone(), chunk[1].clone()));
                } else {
                    parent_level.push(chunk[0].clone());
                }
            }
            nodes = parent_level;
        }

        MerkleTree {
            root: nodes.into_iter().next(),
            leaf_count,
        }
    }

    /// Get root hash
    pub fn root_hash(&self) -> String {
        self.root
            .as_ref()
            .map(|n| n.hash.clone())
            .unwrap_or_else(|| "0".repeat(64))
    }

    /// Verify data exists in tree (simplified)
    pub fn verify(&self, data: &str) -> bool {
        let hash = sha256_hash(data);
        self.contains_hash(&self.root, &hash)
    }

    fn contains_hash(&self, node: &Option<MerkleNode>, hash: &str) -> bool {
        match node {
            None => false,
            Some(n) => {
                if n.hash == *hash {
                    return true;
                }
                let left = n.left.as_ref().map(|b| b.as_ref().clone());
                let right = n.right.as_ref().map(|b| b.as_ref().clone());
                self.contains_hash(&left, hash) || self.contains_hash(&right, hash)
            }
        }
    }
}

/// Thread-safe ledger registry
pub struct LedgerRegistry {
    ledgers: Arc<RwLock<HashMap<String, BlockchainLedger>>>,
}

impl LedgerRegistry {
    /// Create new registry
    pub fn new() -> Self {
        LedgerRegistry {
            ledgers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create new ledger
    pub fn create_ledger(&self, name: &str, block_size: usize) -> Result<(), String> {
        let mut ledgers = self.ledgers.write();
        if ledgers.contains_key(name) {
            return Err(format!("Ledger '{}' already exists", name));
        }
        ledgers.insert(name.to_string(), BlockchainLedger::new(block_size));
        Ok(())
    }

    /// Add transaction to ledger
    pub fn add_transaction(&self, ledger_name: &str, tx: Transaction) -> Result<String, String> {
        let mut ledgers = self.ledgers.write();
        let ledger = ledgers
            .get_mut(ledger_name)
            .ok_or_else(|| format!("Ledger '{}' not found", ledger_name))?;
        Ok(ledger.add_transaction(tx))
    }

    /// Mine block in ledger
    pub fn mine_block(&self, ledger_name: &str) -> Result<Option<u64>, String> {
        let mut ledgers = self.ledgers.write();
        let ledger = ledgers
            .get_mut(ledger_name)
            .ok_or_else(|| format!("Ledger '{}' not found", ledger_name))?;
        Ok(ledger.mine_block())
    }

    /// Verify ledger chain
    pub fn verify_chain(&self, ledger_name: &str) -> Result<ChainVerification, String> {
        let ledgers = self.ledgers.read();
        let ledger = ledgers
            .get(ledger_name)
            .ok_or_else(|| format!("Ledger '{}' not found", ledger_name))?;
        Ok(ledger.verify_chain())
    }

    /// Get ledger stats
    pub fn get_stats(&self, ledger_name: &str) -> Result<LedgerStats, String> {
        let ledgers = self.ledgers.read();
        let ledger = ledgers
            .get(ledger_name)
            .ok_or_else(|| format!("Ledger '{}' not found", ledger_name))?;
        Ok(ledger.get_stats())
    }

    /// Get audit trail
    pub fn get_audit_trail(&self, ledger_name: &str, target: &str) -> Result<AuditTrail, String> {
        let ledgers = self.ledgers.read();
        let ledger = ledgers
            .get(ledger_name)
            .ok_or_else(|| format!("Ledger '{}' not found", ledger_name))?;
        Ok(ledger.get_audit_trail(target))
    }

    /// List all ledgers
    pub fn list_ledgers(&self) -> Result<Vec<String>, String> {
        let ledgers = self.ledgers.read();
        Ok(ledgers.keys().cloned().collect())
    }
}

impl Default for LedgerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// Helper functions

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn sha256_hash(data: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn generate_tx_id(actor: &str, timestamp: u64) -> String {
    let data = format!("{}{}{}", actor, timestamp, rand_u64());
    sha256_hash(&data)[..16].to_string()
}

fn rand_u64() -> u64 {
    // Simple pseudo-random using timestamp
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    now.as_nanos() as u64
}

fn calculate_tx_hash(
    id: &str,
    timestamp: u64,
    tx_type: &TransactionType,
    actor: &str,
    target: &str,
    details: &str,
) -> String {
    let data = format!(
        "{}{}{}{}{}{}",
        id, timestamp, tx_type, actor, target, details
    );
    sha256_hash(&data)
}

fn calculate_merkle_root(transactions: &[Transaction]) -> String {
    if transactions.is_empty() {
        return "0".repeat(64);
    }

    let hashes: Vec<String> = transactions.iter().map(|tx| tx.hash.clone()).collect();
    let tree = MerkleTree::build(&hashes);
    tree.root_hash()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_genesis_block() {
        let block = Block::genesis();
        assert_eq!(block.index, 0);
        assert!(block.verify());
    }

    #[test]
    fn test_transaction_creation() {
        let tx = Transaction::new(
            TransactionType::Insert,
            "user1".to_string(),
            "table1".to_string(),
            "Inserted 100 rows".to_string(),
        );
        assert!(tx.verify());
    }

    #[test]
    fn test_blockchain_operations() {
        let mut ledger = BlockchainLedger::new(2);

        // Add transactions
        let tx1 = Transaction::new(
            TransactionType::Insert,
            "alice".to_string(),
            "users".to_string(),
            "Added user".to_string(),
        );
        ledger.add_transaction(tx1);

        let tx2 = Transaction::new(
            TransactionType::Update,
            "bob".to_string(),
            "users".to_string(),
            "Updated profile".to_string(),
        );
        ledger.add_transaction(tx2); // This should trigger mining

        // Verify chain
        let verification = ledger.verify_chain();
        assert!(verification.is_valid);
        assert_eq!(verification.blocks_verified, 2);
    }

    #[test]
    fn test_merkle_tree() {
        let data = vec![
            "tx1".to_string(),
            "tx2".to_string(),
            "tx3".to_string(),
            "tx4".to_string(),
        ];
        let tree = MerkleTree::build(&data);
        assert!(tree.root.is_some());
        assert!(!tree.root_hash().is_empty());
    }

    #[test]
    fn test_audit_trail() {
        let mut ledger = BlockchainLedger::new(10);

        ledger.add_transaction(Transaction::new(
            TransactionType::CreateSchema,
            "admin".to_string(),
            "orders".to_string(),
            "Created table".to_string(),
        ));

        ledger.add_transaction(Transaction::new(
            TransactionType::Insert,
            "app".to_string(),
            "orders".to_string(),
            "Inserted order #123".to_string(),
        ));

        ledger.mine_block();

        let trail = ledger.get_audit_trail("orders");
        assert_eq!(trail.entries.len(), 2);
    }

    #[test]
    fn test_ledger_registry() {
        let registry = LedgerRegistry::new();

        registry.create_ledger("audit", 5).unwrap();

        let tx = Transaction::new(
            TransactionType::ConfigChange,
            "admin".to_string(),
            "system".to_string(),
            "Changed setting".to_string(),
        );

        registry.add_transaction("audit", tx).unwrap();

        let stats = registry.get_stats("audit").unwrap();
        assert_eq!(stats.total_blocks, 1); // Just genesis
        assert_eq!(stats.pending_transactions, 1);
    }
}
