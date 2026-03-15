//! Full-Text Search (FTS) module for BoyoDB
//!
//! Provides full-text search capabilities including:
//! - Text tokenization with multiple tokenizers
//! - Inverted index for fast term lookups
//! - TF-IDF and BM25 relevance scoring
//! - Boolean and phrase search
//! - Highlighting and snippets
//! - N-gram based fulltext indexes for substring search (LIKE '%pattern%')

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use parking_lot::RwLock;

use crate::engine::EngineError;

/// Full-text search configuration
#[derive(Debug, Clone)]
pub struct FtsConfig {
    /// Tokenizer to use (standard, simple, whitespace, ngram)
    pub tokenizer: TokenizerType,
    /// Minimum token length to index
    pub min_token_length: usize,
    /// Maximum token length to index
    pub max_token_length: usize,
    /// List of stop words to filter
    pub stop_words: HashSet<String>,
    /// Enable stemming (language-dependent)
    pub enable_stemming: bool,
    /// Stemmer language
    pub stemmer_language: String,
    /// Enable case folding (lowercase)
    pub case_insensitive: bool,
}

impl Default for FtsConfig {
    fn default() -> Self {
        Self {
            tokenizer: TokenizerType::Standard,
            min_token_length: 2,
            max_token_length: 100,
            stop_words: default_stop_words(),
            enable_stemming: true,
            stemmer_language: "english".to_string(),
            case_insensitive: true,
        }
    }
}

/// Tokenizer types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenizerType {
    /// Standard tokenizer: splits on whitespace and punctuation
    Standard,
    /// Simple tokenizer: splits on non-letter characters
    Simple,
    /// Whitespace tokenizer: splits only on whitespace
    Whitespace,
    /// N-gram tokenizer: generates n-grams of specified size
    Ngram { min_gram: usize, max_gram: usize },
    /// Edge n-gram: n-grams from the beginning of tokens
    EdgeNgram { min_gram: usize, max_gram: usize },
}

/// A token from text analysis
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FtsToken {
    /// The token text (possibly normalized)
    pub text: String,
    /// Position in the original text
    pub position: usize,
    /// Character offset start
    pub start_offset: usize,
    /// Character offset end
    pub end_offset: usize,
}

/// Text analyzer that tokenizes and normalizes text
pub struct TextAnalyzer {
    config: FtsConfig,
}

impl TextAnalyzer {
    pub fn new(config: FtsConfig) -> Self {
        Self { config }
    }

    /// Analyze text into tokens
    pub fn analyze(&self, text: &str) -> Vec<FtsToken> {
        let mut tokens = Vec::new();
        let text_to_process = if self.config.case_insensitive {
            text.to_lowercase()
        } else {
            text.to_string()
        };

        match self.config.tokenizer {
            TokenizerType::Standard => {
                self.tokenize_standard(&text_to_process, &mut tokens);
            }
            TokenizerType::Simple => {
                self.tokenize_simple(&text_to_process, &mut tokens);
            }
            TokenizerType::Whitespace => {
                self.tokenize_whitespace(&text_to_process, &mut tokens);
            }
            TokenizerType::Ngram { min_gram, max_gram } => {
                self.tokenize_ngram(&text_to_process, min_gram, max_gram, &mut tokens);
            }
            TokenizerType::EdgeNgram { min_gram, max_gram } => {
                self.tokenize_edge_ngram(&text_to_process, min_gram, max_gram, &mut tokens);
            }
        }

        // Filter by length and stop words
        tokens.retain(|t| {
            t.text.len() >= self.config.min_token_length
                && t.text.len() <= self.config.max_token_length
                && !self.config.stop_words.contains(&t.text)
        });

        // Apply stemming if enabled
        if self.config.enable_stemming {
            for token in &mut tokens {
                token.text = stem_word(&token.text, &self.config.stemmer_language);
            }
        }

        tokens
    }

    fn tokenize_standard(&self, text: &str, tokens: &mut Vec<FtsToken>) {
        let mut position = 0;
        let mut start = 0;
        let mut in_token = false;

        for (i, c) in text.char_indices() {
            let is_word_char = c.is_alphanumeric() || c == '_';

            if is_word_char {
                if !in_token {
                    start = i;
                    in_token = true;
                }
            } else if in_token {
                tokens.push(FtsToken {
                    text: text[start..i].to_string(),
                    position,
                    start_offset: start,
                    end_offset: i,
                });
                position += 1;
                in_token = false;
            }
        }

        // Handle last token
        if in_token {
            tokens.push(FtsToken {
                text: text[start..].to_string(),
                position,
                start_offset: start,
                end_offset: text.len(),
            });
        }
    }

    fn tokenize_simple(&self, text: &str, tokens: &mut Vec<FtsToken>) {
        let mut position = 0;
        let mut start = 0;
        let mut in_token = false;

        for (i, c) in text.char_indices() {
            let is_letter = c.is_alphabetic();

            if is_letter {
                if !in_token {
                    start = i;
                    in_token = true;
                }
            } else if in_token {
                tokens.push(FtsToken {
                    text: text[start..i].to_string(),
                    position,
                    start_offset: start,
                    end_offset: i,
                });
                position += 1;
                in_token = false;
            }
        }

        if in_token {
            tokens.push(FtsToken {
                text: text[start..].to_string(),
                position,
                start_offset: start,
                end_offset: text.len(),
            });
        }
    }

    fn tokenize_whitespace(&self, text: &str, tokens: &mut Vec<FtsToken>) {
        let mut position = 0;
        for (i, word) in text.split_whitespace().enumerate() {
            let start = text.find(word).unwrap_or(0);
            tokens.push(FtsToken {
                text: word.to_string(),
                position,
                start_offset: start,
                end_offset: start + word.len(),
            });
            position += 1;
        }
    }

    fn tokenize_ngram(&self, text: &str, min_gram: usize, max_gram: usize, tokens: &mut Vec<FtsToken>) {
        let chars: Vec<char> = text.chars().collect();
        let mut position = 0;

        for n in min_gram..=max_gram {
            for i in 0..=chars.len().saturating_sub(n) {
                let ngram: String = chars[i..i + n].iter().collect();
                tokens.push(FtsToken {
                    text: ngram,
                    position,
                    start_offset: i,
                    end_offset: i + n,
                });
                position += 1;
            }
        }
    }

    fn tokenize_edge_ngram(&self, text: &str, min_gram: usize, max_gram: usize, tokens: &mut Vec<FtsToken>) {
        // First tokenize normally, then generate edge ngrams for each token
        let mut base_tokens = Vec::new();
        self.tokenize_standard(text, &mut base_tokens);

        let mut position = 0;
        for base in base_tokens {
            let chars: Vec<char> = base.text.chars().collect();
            for n in min_gram..=max_gram.min(chars.len()) {
                let ngram: String = chars[..n].iter().collect();
                tokens.push(FtsToken {
                    text: ngram,
                    position,
                    start_offset: base.start_offset,
                    end_offset: base.start_offset + n,
                });
                position += 1;
            }
        }
    }
}

/// Inverted index for full-text search
pub struct InvertedFtsIndex {
    /// Term -> List of (document_id, positions)
    index: BTreeMap<String, Vec<Posting>>,
    /// Document ID -> document length (in tokens)
    doc_lengths: HashMap<u64, usize>,
    /// Total number of documents
    doc_count: u64,
    /// Average document length
    avg_doc_length: f64,
    /// Configuration
    config: FtsConfig,
    /// Analyzer instance
    analyzer: TextAnalyzer,
}

/// A posting entry in the inverted index
#[derive(Debug, Clone)]
pub struct Posting {
    /// Document ID
    pub doc_id: u64,
    /// Positions of the term in the document
    pub positions: Vec<usize>,
    /// Term frequency in this document
    pub term_freq: usize,
}

impl InvertedFtsIndex {
    pub fn new(config: FtsConfig) -> Self {
        let analyzer = TextAnalyzer::new(config.clone());
        Self {
            index: BTreeMap::new(),
            doc_lengths: HashMap::new(),
            doc_count: 0,
            avg_doc_length: 0.0,
            config,
            analyzer,
        }
    }

    /// Index a document
    pub fn index_document(&mut self, doc_id: u64, text: &str) {
        let tokens = self.analyzer.analyze(text);
        let doc_length = tokens.len();

        // Group tokens by term
        let mut term_positions: HashMap<String, Vec<usize>> = HashMap::new();
        for token in &tokens {
            term_positions
                .entry(token.text.clone())
                .or_default()
                .push(token.position);
        }

        // Add to inverted index
        for (term, positions) in term_positions {
            let posting = Posting {
                doc_id,
                term_freq: positions.len(),
                positions,
            };

            self.index
                .entry(term)
                .or_default()
                .push(posting);
        }

        // Update document stats
        self.doc_lengths.insert(doc_id, doc_length);
        self.doc_count += 1;
        self.update_avg_doc_length();
    }

    /// Remove a document from the index
    pub fn remove_document(&mut self, doc_id: u64) {
        for postings in self.index.values_mut() {
            postings.retain(|p| p.doc_id != doc_id);
        }
        self.index.retain(|_, v| !v.is_empty());

        if self.doc_lengths.remove(&doc_id).is_some() {
            self.doc_count = self.doc_count.saturating_sub(1);
            self.update_avg_doc_length();
        }
    }

    fn update_avg_doc_length(&mut self) {
        if self.doc_count == 0 {
            self.avg_doc_length = 0.0;
        } else {
            let total: usize = self.doc_lengths.values().sum();
            self.avg_doc_length = total as f64 / self.doc_count as f64;
        }
    }

    /// Search with BM25 scoring
    pub fn search(&self, query: &str, limit: usize) -> Vec<SearchResult> {
        let query_tokens = self.analyzer.analyze(query);
        let query_terms: HashSet<String> = query_tokens.iter().map(|t| t.text.clone()).collect();

        // Calculate BM25 scores
        let mut scores: HashMap<u64, f64> = HashMap::new();

        for term in &query_terms {
            if let Some(postings) = self.index.get(term) {
                let idf = self.calculate_idf(postings.len());

                for posting in postings {
                    let doc_length = self.doc_lengths.get(&posting.doc_id).copied().unwrap_or(0);
                    let tf_score = self.calculate_bm25_tf(posting.term_freq, doc_length);
                    let score = idf * tf_score;

                    *scores.entry(posting.doc_id).or_default() += score;
                }
            }
        }

        // Sort by score
        let mut results: Vec<SearchResult> = scores
            .into_iter()
            .map(|(doc_id, score)| SearchResult { doc_id, score })
            .collect();

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(limit);
        results
    }

    /// Search for exact phrase
    pub fn search_phrase(&self, phrase: &str, limit: usize) -> Vec<SearchResult> {
        let tokens = self.analyzer.analyze(phrase);
        if tokens.is_empty() {
            return Vec::new();
        }

        // Get postings for first term
        let first_term = &tokens[0].text;
        let Some(first_postings) = self.index.get(first_term) else {
            return Vec::new();
        };

        let mut results: Vec<SearchResult> = Vec::new();

        // For each document containing the first term
        for posting in first_postings {
            // Check if the document contains all subsequent terms in order
            let mut matches = Vec::new();

            for &first_pos in &posting.positions {
                let mut _current_pos = first_pos;
                let mut all_match = true;

                for (i, token) in tokens.iter().enumerate().skip(1) {
                    let expected_pos = first_pos + i;

                    if let Some(term_postings) = self.index.get(&token.text) {
                        let doc_posting = term_postings.iter().find(|p| p.doc_id == posting.doc_id);
                        if let Some(dp) = doc_posting {
                            if !dp.positions.contains(&expected_pos) {
                                all_match = false;
                                break;
                            }
                        } else {
                            all_match = false;
                            break;
                        }
                    } else {
                        all_match = false;
                        break;
                    }
                }

                if all_match {
                    matches.push(first_pos);
                }
            }

            if !matches.is_empty() {
                // Score based on number of phrase matches
                let score = matches.len() as f64;
                results.push(SearchResult {
                    doc_id: posting.doc_id,
                    score,
                });
            }
        }

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(limit);
        results
    }

    /// Boolean search with AND/OR/NOT operators
    pub fn search_boolean(&self, query: &BooleanQuery, limit: usize) -> Vec<SearchResult> {
        let matching_docs = self.evaluate_boolean(query);

        // Score matched documents
        let mut results: Vec<SearchResult> = matching_docs
            .into_iter()
            .map(|doc_id| {
                let score = self.score_document(doc_id, &query.extract_terms());
                SearchResult { doc_id, score }
            })
            .collect();

        results.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(limit);
        results
    }

    fn evaluate_boolean(&self, query: &BooleanQuery) -> HashSet<u64> {
        match query {
            BooleanQuery::Term(term) => {
                let tokens = self.analyzer.analyze(term);
                let mut docs = HashSet::new();
                for token in tokens {
                    if let Some(postings) = self.index.get(&token.text) {
                        for posting in postings {
                            docs.insert(posting.doc_id);
                        }
                    }
                }
                docs
            }
            BooleanQuery::And(queries) => {
                let mut result: Option<HashSet<u64>> = None;
                for q in queries {
                    let docs = self.evaluate_boolean(q);
                    result = Some(match result {
                        Some(r) => r.intersection(&docs).copied().collect(),
                        None => docs,
                    });
                }
                result.unwrap_or_default()
            }
            BooleanQuery::Or(queries) => {
                let mut result = HashSet::new();
                for q in queries {
                    result.extend(self.evaluate_boolean(q));
                }
                result
            }
            BooleanQuery::Not(query) => {
                let excluded = self.evaluate_boolean(query);
                self.doc_lengths
                    .keys()
                    .filter(|id| !excluded.contains(id))
                    .copied()
                    .collect()
            }
            BooleanQuery::Phrase(phrase) => {
                self.search_phrase(phrase, usize::MAX)
                    .into_iter()
                    .map(|r| r.doc_id)
                    .collect()
            }
        }
    }

    fn score_document(&self, doc_id: u64, terms: &HashSet<String>) -> f64 {
        let doc_length = self.doc_lengths.get(&doc_id).copied().unwrap_or(0);
        let mut score = 0.0;

        for term in terms {
            if let Some(postings) = self.index.get(term) {
                if let Some(posting) = postings.iter().find(|p| p.doc_id == doc_id) {
                    let idf = self.calculate_idf(postings.len());
                    let tf_score = self.calculate_bm25_tf(posting.term_freq, doc_length);
                    score += idf * tf_score;
                }
            }
        }

        score
    }

    /// Calculate IDF (Inverse Document Frequency)
    fn calculate_idf(&self, doc_freq: usize) -> f64 {
        let n = self.doc_count as f64;
        let df = doc_freq as f64;
        ((n - df + 0.5) / (df + 0.5) + 1.0).ln()
    }

    /// Calculate BM25 term frequency component
    fn calculate_bm25_tf(&self, term_freq: usize, doc_length: usize) -> f64 {
        const K1: f64 = 1.2;
        const B: f64 = 0.75;

        let tf = term_freq as f64;
        let dl = doc_length as f64;
        let avgdl = self.avg_doc_length;

        (tf * (K1 + 1.0)) / (tf + K1 * (1.0 - B + B * dl / avgdl))
    }

    /// Get highlighted snippet from document
    pub fn highlight(&self, text: &str, query: &str, start_tag: &str, end_tag: &str) -> String {
        let query_tokens = self.analyzer.analyze(query);
        let query_terms: HashSet<String> = query_tokens.iter().map(|t| t.text.clone()).collect();

        let tokens = self.analyzer.analyze(text);
        let mut result = text.to_string();
        let mut offset: i64 = 0;

        // Sort tokens by position to process in order
        let mut matching_tokens: Vec<&FtsToken> = tokens
            .iter()
            .filter(|t| query_terms.contains(&t.text))
            .collect();
        matching_tokens.sort_by_key(|t| t.start_offset);

        for token in matching_tokens {
            let start = (token.start_offset as i64 + offset) as usize;
            let end = (token.end_offset as i64 + offset) as usize;

            if start <= result.len() && end <= result.len() {
                let original = &result[start..end];
                let highlighted = format!("{}{}{}", start_tag, original, end_tag);
                result.replace_range(start..end, &highlighted);
                offset += (start_tag.len() + end_tag.len()) as i64;
            }
        }

        result
    }

    /// Get snippet with context around matches
    pub fn snippet(&self, text: &str, query: &str, max_length: usize) -> String {
        let query_tokens = self.analyzer.analyze(query);
        let query_terms: HashSet<String> = query_tokens.iter().map(|t| t.text.clone()).collect();

        let tokens = self.analyzer.analyze(text);

        // Find first matching token
        let first_match = tokens.iter().find(|t| query_terms.contains(&t.text));

        match first_match {
            Some(token) => {
                let center = token.start_offset;
                let half_len = max_length / 2;

                let start = center.saturating_sub(half_len);
                let end = (center + half_len).min(text.len());

                let mut snippet = String::new();
                if start > 0 {
                    snippet.push_str("...");
                }
                snippet.push_str(&text[start..end]);
                if end < text.len() {
                    snippet.push_str("...");
                }
                snippet
            }
            None => {
                if text.len() <= max_length {
                    text.to_string()
                } else {
                    format!("{}...", &text[..max_length])
                }
            }
        }
    }
}

/// Search result with document ID and relevance score
#[derive(Debug, Clone)]
pub struct SearchResult {
    pub doc_id: u64,
    pub score: f64,
}

/// Boolean query expression
#[derive(Debug, Clone)]
pub enum BooleanQuery {
    /// Single term search
    Term(String),
    /// AND: all subqueries must match
    And(Vec<BooleanQuery>),
    /// OR: any subquery may match
    Or(Vec<BooleanQuery>),
    /// NOT: exclude matches
    Not(Box<BooleanQuery>),
    /// Exact phrase match
    Phrase(String),
}

impl BooleanQuery {
    /// Extract all search terms from the query
    pub fn extract_terms(&self) -> HashSet<String> {
        let mut terms = HashSet::new();
        self.collect_terms(&mut terms);
        terms
    }

    fn collect_terms(&self, terms: &mut HashSet<String>) {
        match self {
            BooleanQuery::Term(t) => {
                terms.insert(t.clone());
            }
            BooleanQuery::And(queries) | BooleanQuery::Or(queries) => {
                for q in queries {
                    q.collect_terms(terms);
                }
            }
            BooleanQuery::Not(query) => {
                query.collect_terms(terms);
            }
            BooleanQuery::Phrase(phrase) => {
                for word in phrase.split_whitespace() {
                    terms.insert(word.to_lowercase());
                }
            }
        }
    }

    /// Parse a simple boolean query string
    /// Supports: term, "phrase", term AND term, term OR term, -term (NOT)
    pub fn parse(query: &str) -> Result<Self, EngineError> {
        let query = query.trim();
        if query.is_empty() {
            return Err(EngineError::InvalidArgument("Empty query".into()));
        }

        // Check for phrase (quoted)
        if query.starts_with('"') && query.ends_with('"') && query.len() > 2 {
            return Ok(BooleanQuery::Phrase(query[1..query.len()-1].to_string()));
        }

        // Check for AND
        if let Some(pos) = query.find(" AND ") {
            let left = Self::parse(&query[..pos])?;
            let right = Self::parse(&query[pos + 5..])?;
            return Ok(BooleanQuery::And(vec![left, right]));
        }

        // Check for OR
        if let Some(pos) = query.find(" OR ") {
            let left = Self::parse(&query[..pos])?;
            let right = Self::parse(&query[pos + 4..])?;
            return Ok(BooleanQuery::Or(vec![left, right]));
        }

        // Check for NOT (prefix -)
        if query.starts_with('-') && query.len() > 1 {
            let inner = Self::parse(&query[1..])?;
            return Ok(BooleanQuery::Not(Box::new(inner)));
        }

        // Simple term
        Ok(BooleanQuery::Term(query.to_string()))
    }
}

/// Simple Porter stemmer for English
fn stem_word(word: &str, _language: &str) -> String {
    // Simplified stemming - remove common suffixes
    let word = word.to_lowercase();

    if word.len() <= 3 {
        return word;
    }

    // Common suffix removals
    let suffixes = [
        ("ational", "ate"),
        ("tional", "tion"),
        ("enci", "ence"),
        ("anci", "ance"),
        ("izer", "ize"),
        ("ation", "ate"),
        ("ator", "ate"),
        ("alism", "al"),
        ("iveness", "ive"),
        ("fulness", "ful"),
        ("ousness", "ous"),
        ("aliti", "al"),
        ("iviti", "ive"),
        ("biliti", "ble"),
        ("alli", "al"),
        ("entli", "ent"),
        ("eli", "e"),
        ("ousli", "ous"),
        ("ization", "ize"),
        ("ation", "ate"),
        ("ing", ""),
        ("ied", "y"),
        ("ies", "y"),
        ("ed", ""),
        ("ly", ""),
        ("es", ""),
        ("s", ""),
    ];

    for (suffix, replacement) in suffixes {
        if word.ends_with(suffix) {
            let stem = &word[..word.len() - suffix.len()];
            if stem.len() >= 2 {
                return format!("{}{}", stem, replacement);
            }
        }
    }

    word
}

/// Default English stop words
fn default_stop_words() -> HashSet<String> {
    [
        "a", "an", "and", "are", "as", "at", "be", "by", "for", "from",
        "has", "he", "in", "is", "it", "its", "of", "on", "that", "the",
        "to", "was", "were", "will", "with", "the", "this", "but", "they",
        "have", "had", "what", "when", "where", "who", "which", "why", "how",
        "all", "each", "every", "both", "few", "more", "most", "other",
        "some", "such", "no", "nor", "not", "only", "own", "same", "so",
        "than", "too", "very", "just", "can", "could", "may", "might",
        "must", "shall", "should", "would", "now", "or", "if", "then",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

/// Thread-safe FTS index manager
pub struct FtsManager {
    /// Index per (database, table, column)
    indexes: RwLock<HashMap<(String, String, String), Arc<RwLock<InvertedFtsIndex>>>>,
}

impl FtsManager {
    pub fn new() -> Self {
        Self {
            indexes: RwLock::new(HashMap::new()),
        }
    }

    /// Create or get an FTS index for a column
    pub fn get_or_create_index(
        &self,
        database: &str,
        table: &str,
        column: &str,
        config: FtsConfig,
    ) -> Arc<RwLock<InvertedFtsIndex>> {
        let key = (database.to_string(), table.to_string(), column.to_string());

        {
            let indexes = self.indexes.read();
            if let Some(idx) = indexes.get(&key) {
                return idx.clone();
            }
        }

        let mut indexes = self.indexes.write();
        indexes
            .entry(key)
            .or_insert_with(|| Arc::new(RwLock::new(InvertedFtsIndex::new(config))))
            .clone()
    }

    /// Drop an FTS index
    pub fn drop_index(&self, database: &str, table: &str, column: &str) -> bool {
        let key = (database.to_string(), table.to_string(), column.to_string());
        let mut indexes = self.indexes.write();
        indexes.remove(&key).is_some()
    }
}

impl Default for FtsManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// N-gram Based Fulltext Index for Substring Search (LIKE '%pattern%')
// ============================================================================

/// Magic bytes for fulltext index file format
pub const FULLTEXT_INDEX_MAGIC: &[u8; 8] = b"BOYOFTS\0";

/// Current version of the fulltext index format
pub const FULLTEXT_INDEX_VERSION: u32 = 1;

/// Configuration for fulltext index
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FulltextConfig {
    /// Minimum n-gram size (default: 3)
    pub ngram_min: u8,
    /// Maximum n-gram size (default: 3)
    pub ngram_max: u8,
    /// Whether to be case-sensitive (default: false)
    pub case_sensitive: bool,
}

impl Default for FulltextConfig {
    fn default() -> Self {
        Self {
            ngram_min: 3,
            ngram_max: 3,
            case_sensitive: false,
        }
    }
}

/// Generate n-grams from a string
///
/// # Arguments
/// * `text` - The input string
/// * `n` - The n-gram size
/// * `case_sensitive` - Whether to preserve case
///
/// # Returns
/// A HashSet of n-grams
///
/// # Example
/// ```
/// use boyodb_core::fts::generate_ngrams;
/// let ngrams = generate_ngrams("254712345678", 3, false);
/// assert!(ngrams.contains("254"));
/// assert!(ngrams.contains("712"));
/// ```
pub fn generate_ngrams(text: &str, n: usize, case_sensitive: bool) -> HashSet<String> {
    let text = if case_sensitive {
        text.to_string()
    } else {
        text.to_lowercase()
    };

    let chars: Vec<char> = text.chars().collect();
    let mut ngrams = HashSet::new();

    if chars.len() >= n {
        for i in 0..=chars.len() - n {
            let ngram: String = chars[i..i + n].iter().collect();
            ngrams.insert(ngram);
        }
    }

    ngrams
}

/// Generate n-grams for a range of sizes
pub fn generate_ngrams_range(
    text: &str,
    min_n: usize,
    max_n: usize,
    case_sensitive: bool,
) -> HashSet<String> {
    let mut all_ngrams = HashSet::new();
    for n in min_n..=max_n {
        all_ngrams.extend(generate_ngrams(text, n, case_sensitive));
    }
    all_ngrams
}

/// Builder for constructing a fulltext index
///
/// This builds an inverted index mapping n-grams to row indices,
/// enabling efficient segment pruning for LIKE '%pattern%' queries.
pub struct FulltextIndexBuilder {
    /// Map from n-gram to list of row indices containing that n-gram
    terms: BTreeMap<String, Vec<u32>>,
    /// N-gram size
    ngram_size: usize,
    /// Total row count
    row_count: u32,
    /// Configuration
    config: FulltextConfig,
}

impl FulltextIndexBuilder {
    /// Create a new fulltext index builder
    pub fn new(config: FulltextConfig) -> Self {
        Self {
            terms: BTreeMap::new(),
            ngram_size: config.ngram_min as usize,
            row_count: 0,
            config,
        }
    }

    /// Add a value to the index at the given row index
    pub fn add_value(&mut self, row_idx: u32, value: &str) {
        let ngrams = generate_ngrams_range(
            value,
            self.config.ngram_min as usize,
            self.config.ngram_max as usize,
            self.config.case_sensitive,
        );

        for ngram in ngrams {
            self.terms.entry(ngram).or_default().push(row_idx);
        }

        self.row_count = self.row_count.max(row_idx + 1);
    }

    /// Get the number of unique n-grams in the index
    pub fn term_count(&self) -> usize {
        self.terms.len()
    }

    /// Get the total number of rows indexed
    pub fn row_count(&self) -> u32 {
        self.row_count
    }

    /// Build and serialize the index to bytes
    pub fn build(self) -> Result<Vec<u8>, EngineError> {
        serialize_fulltext_index(&self.terms, &self.config, self.row_count)
    }
}

/// Serialized fulltext index that can be loaded and queried
pub struct FulltextIndex {
    /// Map from n-gram to list of row indices
    terms: BTreeMap<String, Vec<u32>>,
    /// Configuration
    config: FulltextConfig,
    /// Total document/row count
    doc_count: u32,
}

impl FulltextIndex {
    /// Check if the index might contain rows matching the given substring pattern
    ///
    /// This is used for segment pruning: if this returns false, the segment
    /// definitely has no matches and can be skipped entirely.
    ///
    /// # Arguments
    /// * `pattern` - The substring to search for (without % wildcards)
    ///
    /// # Returns
    /// * `true` if the segment might contain matches (must scan)
    /// * `false` if the segment definitely has no matches (can skip)
    pub fn might_contain(&self, pattern: &str) -> bool {
        let pattern = if self.config.case_sensitive {
            pattern.to_string()
        } else {
            pattern.to_lowercase()
        };

        // Generate n-grams from the search pattern
        let ngrams = generate_ngrams_range(
            &pattern,
            self.config.ngram_min as usize,
            self.config.ngram_max as usize,
            self.config.case_sensitive,
        );

        // If we can't generate any n-grams (pattern too short), we must scan
        if ngrams.is_empty() {
            return true;
        }

        // All n-grams from the pattern must exist in the index
        // If any n-gram is missing, there can be no matches
        for ngram in ngrams {
            if !self.terms.contains_key(&ngram) {
                return false;
            }
        }

        true
    }

    /// Get candidate row indices that might match the pattern
    ///
    /// Returns the intersection of row sets for all n-grams in the pattern.
    /// This can be used to limit scanning to specific rows.
    pub fn candidate_rows(&self, pattern: &str) -> Option<Vec<u32>> {
        let pattern = if self.config.case_sensitive {
            pattern.to_string()
        } else {
            pattern.to_lowercase()
        };

        let ngrams = generate_ngrams_range(
            &pattern,
            self.config.ngram_min as usize,
            self.config.ngram_max as usize,
            self.config.case_sensitive,
        );

        if ngrams.is_empty() {
            return None; // Must scan all rows
        }

        let mut result: Option<HashSet<u32>> = None;

        for ngram in ngrams {
            match self.terms.get(&ngram) {
                Some(rows) => {
                    let row_set: HashSet<u32> = rows.iter().copied().collect();
                    result = Some(match result {
                        Some(current) => current.intersection(&row_set).copied().collect(),
                        None => row_set,
                    });
                }
                None => return Some(Vec::new()), // No matches possible
            }
        }

        result.map(|s| {
            let mut v: Vec<u32> = s.into_iter().collect();
            v.sort_unstable();
            v
        })
    }

    /// Get the configuration
    pub fn config(&self) -> &FulltextConfig {
        &self.config
    }

    /// Get the document/row count
    pub fn doc_count(&self) -> u32 {
        self.doc_count
    }

    /// Get the number of unique terms (n-grams)
    pub fn term_count(&self) -> usize {
        self.terms.len()
    }
}

/// Serialize a fulltext index to binary format
///
/// File format:
/// - Header (64 bytes):
///   - [8 bytes]  Magic: "BOYOFTS\0"
///   - [4 bytes]  Version: 1
///   - [1 byte]   ngram_min
///   - [1 byte]   ngram_max
///   - [1 byte]   case_sensitive (0 or 1)
///   - [1 byte]   reserved
///   - [8 bytes]  term_count
///   - [8 bytes]  doc_count
///   - [8 bytes]  data_offset
///   - [8 bytes]  data_length
///   - [16 bytes] reserved
/// - Data section: bincode-serialized BTreeMap<String, Vec<u32>>
pub fn serialize_fulltext_index(
    terms: &BTreeMap<String, Vec<u32>>,
    config: &FulltextConfig,
    doc_count: u32,
) -> Result<Vec<u8>, EngineError> {
    // Serialize the terms map using bincode
    let data = bincode::serialize(terms)
        .map_err(|e| EngineError::Internal(format!("failed to serialize fulltext index: {e}")))?;

    let term_count = terms.len() as u64;
    let data_offset: u64 = 64; // Header size
    let data_length = data.len() as u64;

    let mut buffer = Vec::with_capacity(64 + data.len());

    // Write header
    buffer.extend_from_slice(FULLTEXT_INDEX_MAGIC);              // 8 bytes
    buffer.extend_from_slice(&FULLTEXT_INDEX_VERSION.to_le_bytes()); // 4 bytes
    buffer.push(config.ngram_min);                               // 1 byte
    buffer.push(config.ngram_max);                               // 1 byte
    buffer.push(if config.case_sensitive { 1 } else { 0 });      // 1 byte
    buffer.push(0);                                              // 1 byte reserved
    buffer.extend_from_slice(&term_count.to_le_bytes());         // 8 bytes
    buffer.extend_from_slice(&(doc_count as u64).to_le_bytes()); // 8 bytes
    buffer.extend_from_slice(&data_offset.to_le_bytes());        // 8 bytes
    buffer.extend_from_slice(&data_length.to_le_bytes());        // 8 bytes
    buffer.extend_from_slice(&[0u8; 16]);                        // 16 bytes reserved

    // Write data
    buffer.extend_from_slice(&data);

    Ok(buffer)
}

/// Load a fulltext index from binary data
pub fn load_fulltext_index(data: &[u8]) -> Result<FulltextIndex, EngineError> {
    if data.len() < 64 {
        return Err(EngineError::InvalidArgument(
            "fulltext index too small".into(),
        ));
    }

    // Verify magic
    if &data[0..8] != FULLTEXT_INDEX_MAGIC {
        return Err(EngineError::InvalidArgument(
            "invalid fulltext index magic".into(),
        ));
    }

    // Read header
    let version = u32::from_le_bytes(data[8..12].try_into().unwrap());
    if version != FULLTEXT_INDEX_VERSION {
        return Err(EngineError::InvalidArgument(format!(
            "unsupported fulltext index version: {version}"
        )));
    }

    let ngram_min = data[12];
    let ngram_max = data[13];
    let case_sensitive = data[14] != 0;
    // data[15] is reserved

    let _term_count = u64::from_le_bytes(data[16..24].try_into().unwrap());
    let doc_count = u64::from_le_bytes(data[24..32].try_into().unwrap()) as u32;
    let data_offset = u64::from_le_bytes(data[32..40].try_into().unwrap()) as usize;
    let data_length = u64::from_le_bytes(data[40..48].try_into().unwrap()) as usize;

    if data.len() < data_offset + data_length {
        return Err(EngineError::InvalidArgument(
            "fulltext index data truncated".into(),
        ));
    }

    let terms: BTreeMap<String, Vec<u32>> =
        bincode::deserialize(&data[data_offset..data_offset + data_length])
            .map_err(|e| EngineError::Internal(format!("failed to deserialize fulltext index: {e}")))?;

    Ok(FulltextIndex {
        terms,
        config: FulltextConfig {
            ngram_min,
            ngram_max,
            case_sensitive,
        },
        doc_count,
    })
}

/// Load a fulltext index from a file path
pub fn load_fulltext_index_from_path(path: &Path) -> Result<FulltextIndex, EngineError> {
    let data = std::fs::read(path)
        .map_err(|e| EngineError::Io(format!("failed to read fulltext index: {e}")))?;
    load_fulltext_index(&data)
}

/// Extract the substring from a LIKE pattern of the form '%value%'
///
/// Returns `Some(value)` if the pattern is `%value%` where value contains no wildcards.
/// Returns `None` for other patterns like 'prefix%', '%suffix', or patterns with internal wildcards.
pub fn extract_like_substring(pattern: &str) -> Option<String> {
    // Must start and end with %
    if !pattern.starts_with('%') || !pattern.ends_with('%') {
        return None;
    }

    // Remove the surrounding %
    let inner = &pattern[1..pattern.len() - 1];

    // Inner part must not contain any wildcards
    if inner.contains('%') || inner.contains('_') {
        return None;
    }

    // Must have actual content
    if inner.is_empty() {
        return None;
    }

    Some(inner.to_string())
}

/// Check if a fulltext index file might contain matches for a LIKE pattern
///
/// This is a convenience function that loads the index and checks for matches.
pub fn fulltext_index_might_contain(index_path: &Path, pattern: &str) -> Result<bool, EngineError> {
    // Extract substring from LIKE pattern
    let substring = match extract_like_substring(pattern) {
        Some(s) => s,
        None => return Ok(true), // Can't optimize, must scan
    };

    // Load and check the index
    let index = load_fulltext_index_from_path(index_path)?;
    Ok(index.might_contain(&substring))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenizer_standard() {
        let analyzer = TextAnalyzer::new(FtsConfig {
            stop_words: HashSet::new(),
            enable_stemming: false,
            ..Default::default()
        });

        let tokens = analyzer.analyze("Hello, World! This is a test.");
        let texts: Vec<&str> = tokens.iter().map(|t| t.text.as_str()).collect();
        assert_eq!(texts, vec!["hello", "world", "this", "is", "test"]);
    }

    #[test]
    fn test_inverted_index() {
        let mut index = InvertedFtsIndex::new(FtsConfig::default());

        index.index_document(1, "The quick brown fox");
        index.index_document(2, "The lazy brown dog");
        index.index_document(3, "Quick foxes are fast");

        let results = index.search("quick fox", 10);
        assert!(!results.is_empty());
        assert_eq!(results[0].doc_id, 1); // Best match for "quick" and "fox"
    }

    #[test]
    fn test_phrase_search() {
        let mut index = InvertedFtsIndex::new(FtsConfig::default());

        index.index_document(1, "the quick brown fox jumps");
        index.index_document(2, "brown quick fox");

        let results = index.search_phrase("quick brown", 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, 1);
    }

    #[test]
    fn test_boolean_query() {
        let mut index = InvertedFtsIndex::new(FtsConfig::default());

        index.index_document(1, "apple banana");
        index.index_document(2, "apple orange");
        index.index_document(3, "banana orange");

        let query = BooleanQuery::And(vec![
            BooleanQuery::Term("apple".into()),
            BooleanQuery::Term("banana".into()),
        ]);

        let results = index.search_boolean(&query, 10);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, 1);
    }

    #[test]
    fn test_highlight() {
        let mut index = InvertedFtsIndex::new(FtsConfig::default());
        index.index_document(1, "test document");

        let highlighted = index.highlight(
            "This is a test document for highlighting",
            "test document",
            "<b>",
            "</b>",
        );

        assert!(highlighted.contains("<b>test</b>"));
        assert!(highlighted.contains("<b>document</b>"));
    }

    // ========================================================================
    // Fulltext Index Tests (N-gram based for LIKE '%pattern%')
    // ========================================================================

    #[test]
    fn test_generate_ngrams() {
        let ngrams = generate_ngrams("254712345678", 3, false);
        assert!(ngrams.contains("254"));
        assert!(ngrams.contains("547"));
        assert!(ngrams.contains("471"));
        assert!(ngrams.contains("712"));
        assert!(ngrams.contains("123"));
        assert!(ngrams.contains("234"));
        assert!(ngrams.contains("345"));
        assert!(ngrams.contains("456"));
        assert!(ngrams.contains("567"));
        assert!(ngrams.contains("678"));
        assert_eq!(ngrams.len(), 10);
    }

    #[test]
    fn test_generate_ngrams_case_insensitive() {
        let ngrams = generate_ngrams("AbCdEf", 3, false);
        assert!(ngrams.contains("abc"));
        assert!(ngrams.contains("bcd"));
        assert!(ngrams.contains("cde"));
        assert!(ngrams.contains("def"));
    }

    #[test]
    fn test_generate_ngrams_case_sensitive() {
        let ngrams = generate_ngrams("AbCdEf", 3, true);
        assert!(ngrams.contains("AbC"));
        assert!(ngrams.contains("bCd"));
        assert!(ngrams.contains("CdE"));
        assert!(ngrams.contains("dEf"));
    }

    #[test]
    fn test_generate_ngrams_short_string() {
        let ngrams = generate_ngrams("ab", 3, false);
        assert!(ngrams.is_empty());
    }

    #[test]
    fn test_fulltext_index_builder() {
        let config = FulltextConfig::default();
        let mut builder = FulltextIndexBuilder::new(config);

        builder.add_value(0, "254712345678");
        builder.add_value(1, "254799887766");
        builder.add_value(2, "123456789012");

        assert_eq!(builder.row_count(), 3);
        assert!(builder.term_count() > 0);
    }

    #[test]
    fn test_fulltext_index_serialization() {
        let config = FulltextConfig::default();
        let mut builder = FulltextIndexBuilder::new(config);

        builder.add_value(0, "254712345678");
        builder.add_value(1, "254799887766");
        builder.add_value(2, "123456789012");

        let data = builder.build().unwrap();

        // Verify header
        assert_eq!(&data[0..8], FULLTEXT_INDEX_MAGIC);

        // Deserialize and verify
        let index = load_fulltext_index(&data).unwrap();
        assert_eq!(index.doc_count(), 3);
        assert!(index.term_count() > 0);
    }

    #[test]
    fn test_fulltext_index_might_contain() {
        let config = FulltextConfig::default();
        let mut builder = FulltextIndexBuilder::new(config);

        builder.add_value(0, "254712345678");
        builder.add_value(1, "254799887766");

        let data = builder.build().unwrap();
        let index = load_fulltext_index(&data).unwrap();

        // Should find "712" - it's in "254712345678"
        assert!(index.might_contain("712"));

        // Should find "254" - it's in both numbers
        assert!(index.might_contain("254"));

        // Should not find "999" - not in any number
        assert!(!index.might_contain("999"));

        // Should not find "111" - not in any number
        assert!(!index.might_contain("111"));
    }

    #[test]
    fn test_fulltext_index_candidate_rows() {
        let config = FulltextConfig::default();
        let mut builder = FulltextIndexBuilder::new(config);

        builder.add_value(0, "254712345678");
        builder.add_value(1, "254799887766");
        builder.add_value(2, "123456789012");

        let data = builder.build().unwrap();
        let index = load_fulltext_index(&data).unwrap();

        // "712" only in row 0
        let candidates = index.candidate_rows("712").unwrap();
        assert_eq!(candidates, vec![0]);

        // "254" in rows 0 and 1
        let candidates = index.candidate_rows("254").unwrap();
        assert!(candidates.contains(&0));
        assert!(candidates.contains(&1));
        assert!(!candidates.contains(&2));

        // "999" in no rows
        let candidates = index.candidate_rows("999").unwrap();
        assert!(candidates.is_empty());
    }

    #[test]
    fn test_extract_like_substring() {
        // Valid patterns
        assert_eq!(extract_like_substring("%712%"), Some("712".to_string()));
        assert_eq!(extract_like_substring("%abc%"), Some("abc".to_string()));
        assert_eq!(extract_like_substring("%Hello World%"), Some("Hello World".to_string()));

        // Invalid patterns
        assert_eq!(extract_like_substring("prefix%"), None);
        assert_eq!(extract_like_substring("%suffix"), None);
        assert_eq!(extract_like_substring("no_wildcards"), None);
        assert_eq!(extract_like_substring("%a%b%"), None); // Internal wildcard
        assert_eq!(extract_like_substring("%a_b%"), None); // Internal single-char wildcard
        assert_eq!(extract_like_substring("%%"), None);    // Empty inner
    }
}
