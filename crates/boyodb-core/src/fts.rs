//! Full-Text Search (FTS) module for BoyoDB
//!
//! Provides full-text search capabilities including:
//! - Text tokenization with multiple tokenizers
//! - Inverted index for fast term lookups
//! - TF-IDF and BM25 relevance scoring
//! - Boolean and phrase search
//! - Highlighting and snippets

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, RwLock};

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
            let indexes = self.indexes.read().unwrap();
            if let Some(idx) = indexes.get(&key) {
                return idx.clone();
            }
        }

        let mut indexes = self.indexes.write().unwrap();
        indexes
            .entry(key)
            .or_insert_with(|| Arc::new(RwLock::new(InvertedFtsIndex::new(config))))
            .clone()
    }

    /// Drop an FTS index
    pub fn drop_index(&self, database: &str, table: &str, column: &str) -> bool {
        let key = (database.to_string(), table.to_string(), column.to_string());
        let mut indexes = self.indexes.write().unwrap();
        indexes.remove(&key).is_some()
    }
}

impl Default for FtsManager {
    fn default() -> Self {
        Self::new()
    }
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
}
