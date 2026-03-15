// Full Text Search (Enhanced) - tsvector/tsquery with GIN indexes for BoyoDB
//
// Provides PostgreSQL-style full-text search:
// - tsvector type for tokenized documents
// - tsquery type for search queries
// - Text search configurations (language support)
// - Ranking functions (ts_rank, ts_rank_cd)
// - Highlighting (ts_headline)
// - GIN index support

use std::collections::{HashMap, HashSet, BTreeMap};
use std::sync::Arc;
use parking_lot::RwLock;

// ============================================================================
// Basic Types
// ============================================================================

/// Position of a lexeme in a document
pub type Position = u16;

/// Weight of a lexeme (A=highest, D=lowest)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum Weight {
    A = 3,
    B = 2,
    C = 1,
    #[default]
    D = 0,
}

impl Weight {
    /// Parse weight from character
    pub fn from_char(c: char) -> Option<Self> {
        match c.to_ascii_uppercase() {
            'A' => Some(Weight::A),
            'B' => Some(Weight::B),
            'C' => Some(Weight::C),
            'D' => Some(Weight::D),
            _ => None,
        }
    }

    /// Convert to character
    pub fn to_char(self) -> char {
        match self {
            Weight::A => 'A',
            Weight::B => 'B',
            Weight::C => 'C',
            Weight::D => 'D',
        }
    }

    /// Numeric value for ranking
    pub fn value(self) -> f32 {
        match self {
            Weight::A => 1.0,
            Weight::B => 0.4,
            Weight::C => 0.2,
            Weight::D => 0.1,
        }
    }
}

/// A weighted position for a lexeme
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WeightedPosition {
    pub position: Position,
    pub weight: Weight,
}

impl WeightedPosition {
    pub fn new(position: Position, weight: Weight) -> Self {
        Self { position, weight }
    }
}

// ============================================================================
// tsvector Type
// ============================================================================

/// A tsvector - tokenized document representation
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TsVector {
    /// Lexemes with their positions and weights
    /// Sorted by lexeme for efficient comparison
    lexemes: BTreeMap<String, Vec<WeightedPosition>>,
}

impl TsVector {
    /// Create an empty tsvector
    pub fn new() -> Self {
        Self {
            lexemes: BTreeMap::new(),
        }
    }

    /// Create from a list of (lexeme, positions) pairs
    pub fn from_lexemes(lexemes: Vec<(String, Vec<WeightedPosition>)>) -> Self {
        let mut ts = Self::new();
        for (lexeme, positions) in lexemes {
            ts.add_lexeme(lexeme, positions);
        }
        ts
    }

    /// Parse from PostgreSQL string format
    pub fn parse(s: &str) -> Result<Self, FtsError> {
        let mut ts = Self::new();
        let s = s.trim();

        if s.is_empty() {
            return Ok(ts);
        }

        for part in s.split_whitespace() {
            let (lexeme, positions) = Self::parse_lexeme_entry(part)?;
            ts.add_lexeme(lexeme, positions);
        }

        Ok(ts)
    }

    fn parse_lexeme_entry(s: &str) -> Result<(String, Vec<WeightedPosition>), FtsError> {
        // Format: 'lexeme':pos1A,pos2B or just 'lexeme'
        let parts: Vec<&str> = s.splitn(2, ':').collect();
        let lexeme = parts[0].trim_matches('\'').to_string();

        let positions = if parts.len() > 1 {
            Self::parse_positions(parts[1])?
        } else {
            Vec::new()
        };

        Ok((lexeme, positions))
    }

    fn parse_positions(s: &str) -> Result<Vec<WeightedPosition>, FtsError> {
        let mut positions = Vec::new();

        for pos_str in s.split(',') {
            let pos_str = pos_str.trim();
            if pos_str.is_empty() {
                continue;
            }

            // Extract position number and optional weight
            let (num_str, weight_char) = if let Some(c) = pos_str.chars().last() {
                if c.is_ascii_alphabetic() {
                    (&pos_str[..pos_str.len()-1], Some(c))
                } else {
                    (pos_str, None)
                }
            } else {
                (pos_str, None)
            };

            let position: Position = num_str.parse()
                .map_err(|_| FtsError::ParseError(format!("Invalid position: {}", num_str)))?;

            let weight = weight_char
                .and_then(Weight::from_char)
                .unwrap_or(Weight::D);

            positions.push(WeightedPosition::new(position, weight));
        }

        Ok(positions)
    }

    /// Add a lexeme with positions
    pub fn add_lexeme(&mut self, lexeme: String, mut positions: Vec<WeightedPosition>) {
        let existing = self.lexemes.entry(lexeme).or_default();
        existing.append(&mut positions);
        existing.sort();
        existing.dedup();
    }

    /// Add a single lexeme at a position
    pub fn add(&mut self, lexeme: &str, position: Position, weight: Weight) {
        self.add_lexeme(
            lexeme.to_string(),
            vec![WeightedPosition::new(position, weight)],
        );
    }

    /// Check if tsvector contains a lexeme
    pub fn contains(&self, lexeme: &str) -> bool {
        self.lexemes.contains_key(lexeme)
    }

    /// Get positions for a lexeme
    pub fn positions(&self, lexeme: &str) -> Option<&Vec<WeightedPosition>> {
        self.lexemes.get(lexeme)
    }

    /// Get all lexemes
    pub fn lexemes(&self) -> impl Iterator<Item = &String> {
        self.lexemes.keys()
    }

    /// Number of unique lexemes
    pub fn len(&self) -> usize {
        self.lexemes.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.lexemes.is_empty()
    }

    /// Concatenate two tsvectors
    pub fn concat(&self, other: &TsVector) -> TsVector {
        let mut result = self.clone();
        for (lexeme, positions) in &other.lexemes {
            result.add_lexeme(lexeme.clone(), positions.clone());
        }
        result
    }

    /// Set weight for all lexemes
    pub fn setweight(&self, weight: Weight) -> TsVector {
        let lexemes = self.lexemes.iter()
            .map(|(l, positions)| {
                let new_pos: Vec<_> = positions.iter()
                    .map(|p| WeightedPosition::new(p.position, weight))
                    .collect();
                (l.clone(), new_pos)
            })
            .collect();
        TsVector { lexemes }
    }

    /// Remove positions/weights, keeping only lexemes
    pub fn strip(&self) -> TsVector {
        let lexemes = self.lexemes.keys()
            .map(|l| (l.clone(), Vec::new()))
            .collect();
        TsVector { lexemes }
    }

    /// Convert to PostgreSQL string format
    pub fn to_string(&self) -> String {
        let parts: Vec<String> = self.lexemes.iter()
            .map(|(lexeme, positions)| {
                if positions.is_empty() {
                    format!("'{}'", lexeme)
                } else {
                    let pos_str: Vec<String> = positions.iter()
                        .map(|wp| {
                            if wp.weight == Weight::D {
                                format!("{}", wp.position)
                            } else {
                                format!("{}{}", wp.position, wp.weight.to_char())
                            }
                        })
                        .collect();
                    format!("'{}':{}",lexeme, pos_str.join(","))
                }
            })
            .collect();
        parts.join(" ")
    }
}

impl Default for TsVector {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// tsquery Type
// ============================================================================

/// Query operator
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryOp {
    And,
    Or,
    Not,
    Phrase(u16), // Distance for phrase search
}

/// A node in the tsquery tree
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TsQueryNode {
    /// A lexeme to match
    Lexeme {
        word: String,
        prefix: bool,
        weights: Option<Vec<Weight>>,
    },
    /// Binary operator
    Binary {
        op: QueryOp,
        left: Box<TsQueryNode>,
        right: Box<TsQueryNode>,
    },
    /// Unary NOT operator
    Not(Box<TsQueryNode>),
}

/// A tsquery - search query representation
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TsQuery {
    pub root: Option<TsQueryNode>,
}

impl TsQuery {
    /// Create an empty query
    pub fn new() -> Self {
        Self { root: None }
    }

    /// Create a simple lexeme query
    pub fn lexeme(word: &str) -> Self {
        Self {
            root: Some(TsQueryNode::Lexeme {
                word: word.to_string(),
                prefix: false,
                weights: None,
            }),
        }
    }

    /// Create a prefix query
    pub fn prefix(word: &str) -> Self {
        Self {
            root: Some(TsQueryNode::Lexeme {
                word: word.to_string(),
                prefix: true,
                weights: None,
            }),
        }
    }

    /// Parse from PostgreSQL string format
    pub fn parse(s: &str) -> Result<Self, FtsError> {
        let s = s.trim();
        if s.is_empty() {
            return Ok(Self::new());
        }

        let tokens = Self::tokenize(s)?;
        let root = Self::parse_expr(&tokens, 0)?.0;

        Ok(Self { root: Some(root) })
    }

    fn tokenize(s: &str) -> Result<Vec<String>, FtsError> {
        let mut tokens = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut chars = s.chars().peekable();

        while let Some(c) = chars.next() {
            match c {
                '\'' => {
                    if in_quotes {
                        tokens.push(current.clone());
                        current.clear();
                    }
                    in_quotes = !in_quotes;
                }
                '&' if !in_quotes => {
                    if !current.is_empty() {
                        tokens.push(current.clone());
                        current.clear();
                    }
                    tokens.push("&".to_string());
                }
                '|' if !in_quotes => {
                    if !current.is_empty() {
                        tokens.push(current.clone());
                        current.clear();
                    }
                    tokens.push("|".to_string());
                }
                '!' if !in_quotes => {
                    if !current.is_empty() {
                        tokens.push(current.clone());
                        current.clear();
                    }
                    tokens.push("!".to_string());
                }
                '(' if !in_quotes => {
                    if !current.is_empty() {
                        tokens.push(current.clone());
                        current.clear();
                    }
                    tokens.push("(".to_string());
                }
                ')' if !in_quotes => {
                    if !current.is_empty() {
                        tokens.push(current.clone());
                        current.clear();
                    }
                    tokens.push(")".to_string());
                }
                '<' if !in_quotes => {
                    if !current.is_empty() {
                        tokens.push(current.clone());
                        current.clear();
                    }
                    // Parse <-> or <N>
                    let mut phrase = String::from("<");
                    while let Some(&nc) = chars.peek() {
                        phrase.push(chars.next().unwrap());
                        if nc == '>' {
                            break;
                        }
                    }
                    tokens.push(phrase);
                }
                ' ' | '\t' | '\n' if !in_quotes => {
                    if !current.is_empty() {
                        tokens.push(current.clone());
                        current.clear();
                    }
                }
                _ => {
                    current.push(c);
                }
            }
        }

        if !current.is_empty() {
            tokens.push(current);
        }

        Ok(tokens)
    }

    fn parse_expr(tokens: &[String], pos: usize) -> Result<(TsQueryNode, usize), FtsError> {
        let (mut left, mut pos) = Self::parse_term(tokens, pos)?;

        while pos < tokens.len() {
            match tokens[pos].as_str() {
                "&" => {
                    let (right, new_pos) = Self::parse_term(tokens, pos + 1)?;
                    left = TsQueryNode::Binary {
                        op: QueryOp::And,
                        left: Box::new(left),
                        right: Box::new(right),
                    };
                    pos = new_pos;
                }
                "|" => {
                    let (right, new_pos) = Self::parse_term(tokens, pos + 1)?;
                    left = TsQueryNode::Binary {
                        op: QueryOp::Or,
                        left: Box::new(left),
                        right: Box::new(right),
                    };
                    pos = new_pos;
                }
                s if s.starts_with('<') && s.ends_with('>') => {
                    // Phrase operator <-> or <N>
                    let dist_str = &s[1..s.len()-1];
                    let distance = if dist_str == "-" {
                        1
                    } else {
                        dist_str.parse().unwrap_or(1)
                    };
                    let (right, new_pos) = Self::parse_term(tokens, pos + 1)?;
                    left = TsQueryNode::Binary {
                        op: QueryOp::Phrase(distance),
                        left: Box::new(left),
                        right: Box::new(right),
                    };
                    pos = new_pos;
                }
                ")" => break,
                _ => break,
            }
        }

        Ok((left, pos))
    }

    fn parse_term(tokens: &[String], pos: usize) -> Result<(TsQueryNode, usize), FtsError> {
        if pos >= tokens.len() {
            return Err(FtsError::ParseError("Unexpected end of query".to_string()));
        }

        match tokens[pos].as_str() {
            "!" => {
                let (node, new_pos) = Self::parse_term(tokens, pos + 1)?;
                Ok((TsQueryNode::Not(Box::new(node)), new_pos))
            }
            "(" => {
                let (node, mut new_pos) = Self::parse_expr(tokens, pos + 1)?;
                if new_pos < tokens.len() && tokens[new_pos] == ")" {
                    new_pos += 1;
                }
                Ok((node, new_pos))
            }
            word => {
                let (word, prefix) = if word.ends_with(":*") {
                    (&word[..word.len()-2], true)
                } else {
                    (word, false)
                };

                // Parse optional weight filter :ABC
                let (word, weights) = if let Some(colon_pos) = word.rfind(':') {
                    let (w, weight_str) = word.split_at(colon_pos);
                    let weights: Vec<Weight> = weight_str[1..].chars()
                        .filter_map(Weight::from_char)
                        .collect();
                    if weights.is_empty() {
                        (word.to_string(), None)
                    } else {
                        (w.to_string(), Some(weights))
                    }
                } else {
                    (word.to_string(), None)
                };

                Ok((
                    TsQueryNode::Lexeme { word, prefix, weights },
                    pos + 1,
                ))
            }
        }
    }

    /// Check if a tsvector matches this query
    pub fn matches(&self, ts: &TsVector) -> bool {
        match &self.root {
            None => true,
            Some(node) => Self::node_matches(node, ts),
        }
    }

    fn node_matches(node: &TsQueryNode, ts: &TsVector) -> bool {
        match node {
            TsQueryNode::Lexeme { word, prefix, weights } => {
                if *prefix {
                    // Prefix match
                    ts.lexemes.keys().any(|l| {
                        if l.starts_with(word) {
                            Self::check_weights(ts.positions(l), weights)
                        } else {
                            false
                        }
                    })
                } else {
                    // Exact match
                    if let Some(positions) = ts.positions(word) {
                        Self::check_weights(Some(positions), weights)
                    } else {
                        false
                    }
                }
            }
            TsQueryNode::Binary { op, left, right } => {
                match op {
                    QueryOp::And => {
                        Self::node_matches(left, ts) && Self::node_matches(right, ts)
                    }
                    QueryOp::Or => {
                        Self::node_matches(left, ts) || Self::node_matches(right, ts)
                    }
                    QueryOp::Phrase(distance) => {
                        Self::phrase_matches(left, right, *distance, ts)
                    }
                    QueryOp::Not => {
                        Self::node_matches(left, ts) && !Self::node_matches(right, ts)
                    }
                }
            }
            TsQueryNode::Not(inner) => !Self::node_matches(inner, ts),
        }
    }

    fn check_weights(
        positions: Option<&Vec<WeightedPosition>>,
        required_weights: &Option<Vec<Weight>>,
    ) -> bool {
        match (positions, required_weights) {
            (None, _) => false,
            (Some(_), None) => true,
            (Some(positions), Some(required)) => {
                positions.iter().any(|p| required.contains(&p.weight))
            }
        }
    }

    fn phrase_matches(
        left: &TsQueryNode,
        right: &TsQueryNode,
        distance: u16,
        ts: &TsVector,
    ) -> bool {
        // Get positions for left and right lexemes
        let left_positions = Self::get_positions(left, ts);
        let right_positions = Self::get_positions(right, ts);

        // Check if any positions match the distance requirement
        for lp in &left_positions {
            for rp in &right_positions {
                let diff = if rp.position > lp.position {
                    rp.position - lp.position
                } else {
                    continue;
                };
                if diff == distance {
                    return true;
                }
            }
        }

        false
    }

    fn get_positions(node: &TsQueryNode, ts: &TsVector) -> Vec<WeightedPosition> {
        match node {
            TsQueryNode::Lexeme { word, prefix: _, weights: _ } => {
                ts.positions(word).cloned().unwrap_or_default()
            }
            _ => Vec::new(),
        }
    }

    /// Convert to PostgreSQL string format
    pub fn to_string(&self) -> String {
        match &self.root {
            None => String::new(),
            Some(node) => Self::node_to_string(node),
        }
    }

    fn node_to_string(node: &TsQueryNode) -> String {
        match node {
            TsQueryNode::Lexeme { word, prefix, weights } => {
                let mut s = format!("'{}'", word);
                if let Some(w) = weights {
                    s.push(':');
                    for weight in w {
                        s.push(weight.to_char());
                    }
                }
                if *prefix {
                    s.push_str(":*");
                }
                s
            }
            TsQueryNode::Binary { op, left, right } => {
                let op_str = match op {
                    QueryOp::And => " & ",
                    QueryOp::Or => " | ",
                    QueryOp::Not => " & !",
                    QueryOp::Phrase(d) => {
                        if *d == 1 {
                            " <-> "
                        } else {
                            return format!(
                                "{} <{}> {}",
                                Self::node_to_string(left),
                                d,
                                Self::node_to_string(right)
                            );
                        }
                    }
                };
                format!(
                    "{}{}{}",
                    Self::node_to_string(left),
                    op_str,
                    Self::node_to_string(right)
                )
            }
            TsQueryNode::Not(inner) => format!("!{}", Self::node_to_string(inner)),
        }
    }
}

impl Default for TsQuery {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Text Search Configuration
// ============================================================================

/// Configuration for text search
#[derive(Debug, Clone)]
pub struct TsConfig {
    /// Configuration name
    pub name: String,
    /// Parser name
    pub parser: String,
    /// Dictionary mappings (token type -> dictionaries)
    pub dictionaries: HashMap<String, Vec<String>>,
    /// Stop words
    pub stop_words: HashSet<String>,
    /// Language code
    pub language: String,
}

impl Default for TsConfig {
    fn default() -> Self {
        Self::english()
    }
}

impl TsConfig {
    /// English configuration
    pub fn english() -> Self {
        Self {
            name: "english".to_string(),
            parser: "default".to_string(),
            dictionaries: HashMap::new(),
            stop_words: Self::english_stop_words(),
            language: "en".to_string(),
        }
    }

    /// Simple configuration (no stemming)
    pub fn simple() -> Self {
        Self {
            name: "simple".to_string(),
            parser: "default".to_string(),
            dictionaries: HashMap::new(),
            stop_words: HashSet::new(),
            language: "".to_string(),
        }
    }

    fn english_stop_words() -> HashSet<String> {
        [
            "a", "an", "and", "are", "as", "at", "be", "but", "by", "for",
            "if", "in", "into", "is", "it", "no", "not", "of", "on", "or",
            "such", "that", "the", "their", "then", "there", "these", "they",
            "this", "to", "was", "will", "with",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }

    /// Check if a word is a stop word
    pub fn is_stop_word(&self, word: &str) -> bool {
        self.stop_words.contains(&word.to_lowercase())
    }

    /// Tokenize and process text
    pub fn to_tsvector(&self, text: &str) -> TsVector {
        let mut ts = TsVector::new();
        let mut position: Position = 0;

        for word in self.tokenize(text) {
            position += 1;

            // Skip stop words
            if self.is_stop_word(&word) {
                continue;
            }

            // Normalize (stem)
            let normalized = self.normalize(&word);

            ts.add(&normalized, position, Weight::D);
        }

        ts
    }

    /// Parse text to tsquery
    pub fn to_tsquery(&self, text: &str) -> TsQuery {
        let words: Vec<String> = self.tokenize(text)
            .into_iter()
            .filter(|w| !self.is_stop_word(w))
            .map(|w| self.normalize(&w))
            .collect();

        if words.is_empty() {
            return TsQuery::new();
        }

        // Build AND query
        let mut root = TsQueryNode::Lexeme {
            word: words[0].clone(),
            prefix: false,
            weights: None,
        };

        for word in words.into_iter().skip(1) {
            root = TsQueryNode::Binary {
                op: QueryOp::And,
                left: Box::new(root),
                right: Box::new(TsQueryNode::Lexeme {
                    word,
                    prefix: false,
                    weights: None,
                }),
            };
        }

        TsQuery { root: Some(root) }
    }

    /// Plainto_tsquery (simple query from plain text)
    pub fn plainto_tsquery(&self, text: &str) -> TsQuery {
        self.to_tsquery(text)
    }

    /// Phraseto_tsquery (phrase query from plain text)
    pub fn phraseto_tsquery(&self, text: &str) -> TsQuery {
        let words: Vec<String> = self.tokenize(text)
            .into_iter()
            .filter(|w| !self.is_stop_word(w))
            .map(|w| self.normalize(&w))
            .collect();

        if words.is_empty() {
            return TsQuery::new();
        }

        // Build phrase query with <->
        let mut root = TsQueryNode::Lexeme {
            word: words[0].clone(),
            prefix: false,
            weights: None,
        };

        for word in words.into_iter().skip(1) {
            root = TsQueryNode::Binary {
                op: QueryOp::Phrase(1),
                left: Box::new(root),
                right: Box::new(TsQueryNode::Lexeme {
                    word,
                    prefix: false,
                    weights: None,
                }),
            };
        }

        TsQuery { root: Some(root) }
    }

    /// Websearch_to_tsquery (web-style query)
    pub fn websearch_to_tsquery(&self, text: &str) -> TsQuery {
        // Handle quoted phrases, +required, -excluded
        let mut required = Vec::new();
        let mut excluded = Vec::new();
        let mut in_quotes = false;
        let mut current_phrase = String::new();
        let mut is_excluded = false;

        for word in text.split_whitespace() {
            if word.starts_with('"') {
                in_quotes = true;
                current_phrase = word[1..].to_string();
                if word.ends_with('"') && word.len() > 1 {
                    in_quotes = false;
                    current_phrase = current_phrase[..current_phrase.len()-1].to_string();
                    required.push(current_phrase.clone());
                }
            } else if in_quotes {
                if word.ends_with('"') {
                    current_phrase.push(' ');
                    current_phrase.push_str(&word[..word.len()-1]);
                    in_quotes = false;
                    required.push(current_phrase.clone());
                } else {
                    current_phrase.push(' ');
                    current_phrase.push_str(word);
                }
            } else if word.starts_with('-') {
                is_excluded = true;
                let w = &word[1..];
                if !w.is_empty() {
                    excluded.push(self.normalize(w));
                }
            } else if word.starts_with('+') {
                is_excluded = false;
                let w = &word[1..];
                if !w.is_empty() && !self.is_stop_word(w) {
                    required.push(self.normalize(w));
                }
            } else if !self.is_stop_word(word) {
                if is_excluded {
                    excluded.push(self.normalize(word));
                } else {
                    required.push(self.normalize(word));
                }
                is_excluded = false;
            }
        }

        if required.is_empty() {
            return TsQuery::new();
        }

        // Build query
        let mut root = TsQueryNode::Lexeme {
            word: required[0].clone(),
            prefix: false,
            weights: None,
        };

        for word in required.into_iter().skip(1) {
            root = TsQueryNode::Binary {
                op: QueryOp::And,
                left: Box::new(root),
                right: Box::new(TsQueryNode::Lexeme {
                    word,
                    prefix: false,
                    weights: None,
                }),
            };
        }

        // Add exclusions
        for word in excluded {
            root = TsQueryNode::Binary {
                op: QueryOp::Not,
                left: Box::new(root),
                right: Box::new(TsQueryNode::Lexeme {
                    word,
                    prefix: false,
                    weights: None,
                }),
            };
        }

        TsQuery { root: Some(root) }
    }

    /// Tokenize text into words
    fn tokenize(&self, text: &str) -> Vec<String> {
        text.split(|c: char| !c.is_alphanumeric())
            .filter(|w| !w.is_empty())
            .map(|w| w.to_lowercase())
            .collect()
    }

    /// Normalize a word (simplified stemming)
    fn normalize(&self, word: &str) -> String {
        let word = word.to_lowercase();

        // Very basic English stemming
        if self.language == "en" {
            // Remove common suffixes
            let suffixes = ["ing", "ed", "er", "est", "ly", "ies", "ness"];
            for suffix in suffixes {
                if word.len() > suffix.len() + 2 && word.ends_with(suffix) {
                    return word[..word.len() - suffix.len()].to_string();
                }
            }

            // Handle -s plural
            if word.len() > 3 && word.ends_with('s') && !word.ends_with("ss") {
                return word[..word.len() - 1].to_string();
            }
        }

        word
    }
}

// ============================================================================
// Ranking Functions
// ============================================================================

/// Ranking normalization options
#[derive(Debug, Clone, Copy, Default)]
pub struct RankNormalization {
    /// Divide rank by document length
    pub divide_by_length: bool,
    /// Log of document length + 1
    pub log_length: bool,
    /// Divide by number of unique words
    pub divide_by_unique_words: bool,
    /// Divide by log of unique words + 1
    pub log_unique_words: bool,
}

/// Calculate ts_rank score
pub fn ts_rank(ts: &TsVector, query: &TsQuery, weights: Option<[f32; 4]>) -> f32 {
    let weights = weights.unwrap_or([0.1, 0.2, 0.4, 1.0]); // D, C, B, A

    match &query.root {
        None => 0.0,
        Some(node) => rank_node(node, ts, &weights),
    }
}

fn rank_node(node: &TsQueryNode, ts: &TsVector, weights: &[f32; 4]) -> f32 {
    match node {
        TsQueryNode::Lexeme { word, prefix, weights: required_weights } => {
            if *prefix {
                // Sum ranks for all matching prefixes
                ts.lexemes.iter()
                    .filter(|(l, _)| l.starts_with(word))
                    .map(|(_, positions)| {
                        rank_positions(positions, weights, required_weights)
                    })
                    .sum()
            } else {
                ts.positions(word)
                    .map(|pos| rank_positions(pos, weights, required_weights))
                    .unwrap_or(0.0)
            }
        }
        TsQueryNode::Binary { op, left, right } => {
            let left_rank = rank_node(left, ts, weights);
            let right_rank = rank_node(right, ts, weights);

            match op {
                QueryOp::And => (left_rank + right_rank) / 2.0,
                QueryOp::Or => left_rank.max(right_rank),
                QueryOp::Phrase(_) => (left_rank + right_rank) / 2.0,
                QueryOp::Not => left_rank,
            }
        }
        TsQueryNode::Not(_) => 0.0,
    }
}

fn rank_positions(
    positions: &[WeightedPosition],
    weights: &[f32; 4],
    required_weights: &Option<Vec<Weight>>,
) -> f32 {
    positions.iter()
        .filter(|p| {
            required_weights.as_ref()
                .map(|rw| rw.contains(&p.weight))
                .unwrap_or(true)
        })
        .map(|p| weights[p.weight as usize])
        .sum()
}

/// Calculate ts_rank_cd (cover density ranking)
pub fn ts_rank_cd(ts: &TsVector, query: &TsQuery, weights: Option<[f32; 4]>) -> f32 {
    // Simplified cover density - just use regular ranking with a boost for phrases
    ts_rank(ts, query, weights) * 1.5
}

// ============================================================================
// Headline Generation
// ============================================================================

/// Options for ts_headline
#[derive(Debug, Clone)]
pub struct HeadlineOptions {
    /// Maximum length of headline
    pub max_words: usize,
    /// Minimum words per fragment
    pub min_words: usize,
    /// String to highlight start
    pub start_sel: String,
    /// String to highlight end
    pub stop_sel: String,
    /// Fragment delimiter
    pub fragment_delimiter: String,
}

impl Default for HeadlineOptions {
    fn default() -> Self {
        Self {
            max_words: 35,
            min_words: 15,
            start_sel: "<b>".to_string(),
            stop_sel: "</b>".to_string(),
            fragment_delimiter: " ... ".to_string(),
        }
    }
}

/// Generate a headline with highlighted matches
pub fn ts_headline(
    config: &TsConfig,
    document: &str,
    query: &TsQuery,
    options: Option<HeadlineOptions>,
) -> String {
    let options = options.unwrap_or_default();

    // Get query lexemes
    let query_words = get_query_lexemes(query);

    // Tokenize document
    let words: Vec<&str> = document.split_whitespace().collect();

    if words.is_empty() {
        return String::new();
    }

    // Find matching positions
    let mut matches: Vec<usize> = Vec::new();
    for (i, word) in words.iter().enumerate() {
        let normalized = config.normalize(word);
        if query_words.contains(&normalized) {
            matches.push(i);
        }
    }

    if matches.is_empty() {
        // No matches, return first max_words words
        let end = words.len().min(options.max_words);
        return words[..end].join(" ");
    }

    // Build headline around first match
    let first_match = matches[0];
    let start = first_match.saturating_sub(options.min_words / 2);
    let end = (first_match + options.min_words / 2).min(words.len());

    let mut result = String::new();

    for (i, word) in words[start..end].iter().enumerate() {
        let actual_i = start + i;

        if i > 0 {
            result.push(' ');
        }

        if matches.contains(&actual_i) {
            result.push_str(&options.start_sel);
            result.push_str(word);
            result.push_str(&options.stop_sel);
        } else {
            result.push_str(word);
        }
    }

    result
}

fn get_query_lexemes(query: &TsQuery) -> HashSet<String> {
    let mut lexemes = HashSet::new();
    if let Some(ref node) = query.root {
        collect_lexemes(node, &mut lexemes);
    }
    lexemes
}

fn collect_lexemes(node: &TsQueryNode, lexemes: &mut HashSet<String>) {
    match node {
        TsQueryNode::Lexeme { word, .. } => {
            lexemes.insert(word.clone());
        }
        TsQueryNode::Binary { left, right, .. } => {
            collect_lexemes(left, lexemes);
            collect_lexemes(right, lexemes);
        }
        TsQueryNode::Not(inner) => {
            collect_lexemes(inner, lexemes);
        }
    }
}

// ============================================================================
// GIN Index Support
// ============================================================================

/// GIN index for full-text search
pub struct GinIndex {
    /// Posting lists: lexeme -> document IDs
    postings: RwLock<HashMap<String, Vec<u64>>>,
    /// Document count
    doc_count: RwLock<u64>,
}

impl GinIndex {
    /// Create a new GIN index
    pub fn new() -> Self {
        Self {
            postings: RwLock::new(HashMap::new()),
            doc_count: RwLock::new(0),
        }
    }

    /// Insert a document
    pub fn insert(&self, doc_id: u64, ts: &TsVector) {
        let mut postings = self.postings.write();

        for lexeme in ts.lexemes() {
            postings.entry(lexeme.clone())
                .or_default()
                .push(doc_id);
        }

        *self.doc_count.write() += 1;
    }

    /// Delete a document
    pub fn delete(&self, doc_id: u64, ts: &TsVector) {
        let mut postings = self.postings.write();

        for lexeme in ts.lexemes() {
            if let Some(docs) = postings.get_mut(lexeme) {
                docs.retain(|&id| id != doc_id);
            }
        }

        let mut count = self.doc_count.write();
        if *count > 0 {
            *count -= 1;
        }
    }

    /// Search for documents matching a query
    pub fn search(&self, query: &TsQuery) -> Vec<u64> {
        let postings = self.postings.read();

        match &query.root {
            None => Vec::new(),
            Some(node) => self.search_node(node, &postings),
        }
    }

    fn search_node(&self, node: &TsQueryNode, postings: &HashMap<String, Vec<u64>>) -> Vec<u64> {
        match node {
            TsQueryNode::Lexeme { word, prefix, .. } => {
                if *prefix {
                    // Union of all matching prefixes
                    let mut result: HashSet<u64> = HashSet::new();
                    for (lexeme, docs) in postings.iter() {
                        if lexeme.starts_with(word) {
                            result.extend(docs);
                        }
                    }
                    result.into_iter().collect()
                } else {
                    postings.get(word).cloned().unwrap_or_default()
                }
            }
            TsQueryNode::Binary { op, left, right } => {
                let left_docs: HashSet<u64> = self.search_node(left, postings).into_iter().collect();
                let right_docs: HashSet<u64> = self.search_node(right, postings).into_iter().collect();

                match op {
                    QueryOp::And | QueryOp::Phrase(_) => {
                        left_docs.intersection(&right_docs).copied().collect()
                    }
                    QueryOp::Or => {
                        left_docs.union(&right_docs).copied().collect()
                    }
                    QueryOp::Not => {
                        left_docs.difference(&right_docs).copied().collect()
                    }
                }
            }
            TsQueryNode::Not(_) => {
                // Standalone NOT requires full scan
                Vec::new()
            }
        }
    }

    /// Get index statistics
    pub fn stats(&self) -> GinIndexStats {
        let postings = self.postings.read();
        GinIndexStats {
            lexeme_count: postings.len(),
            doc_count: *self.doc_count.read(),
            avg_postings: if postings.is_empty() {
                0.0
            } else {
                postings.values().map(|v| v.len()).sum::<usize>() as f64 / postings.len() as f64
            },
        }
    }
}

impl Default for GinIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// GIN index statistics
#[derive(Debug, Clone)]
pub struct GinIndexStats {
    pub lexeme_count: usize,
    pub doc_count: u64,
    pub avg_postings: f64,
}

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug)]
pub enum FtsError {
    ParseError(String),
    InvalidWeight(char),
    ConfigNotFound(String),
}

impl std::fmt::Display for FtsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ParseError(msg) => write!(f, "Parse error: {}", msg),
            Self::InvalidWeight(c) => write!(f, "Invalid weight: {}", c),
            Self::ConfigNotFound(name) => write!(f, "Configuration '{}' not found", name),
        }
    }
}

impl std::error::Error for FtsError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_weight() {
        assert_eq!(Weight::from_char('A'), Some(Weight::A));
        assert_eq!(Weight::from_char('d'), Some(Weight::D));
        assert_eq!(Weight::from_char('x'), None);
        assert_eq!(Weight::A.to_char(), 'A');
    }

    #[test]
    fn test_tsvector_empty() {
        let ts = TsVector::new();
        assert!(ts.is_empty());
        assert_eq!(ts.len(), 0);
    }

    #[test]
    fn test_tsvector_add() {
        let mut ts = TsVector::new();
        ts.add("hello", 1, Weight::A);
        ts.add("world", 2, Weight::B);

        assert_eq!(ts.len(), 2);
        assert!(ts.contains("hello"));
        assert!(ts.contains("world"));
        assert!(!ts.contains("foo"));
    }

    #[test]
    fn test_tsvector_parse() {
        let ts = TsVector::parse("'cat':1A 'dog':2B,3 'bird':4").unwrap();

        assert_eq!(ts.len(), 3);
        assert!(ts.contains("cat"));
        assert!(ts.contains("dog"));
        assert!(ts.contains("bird"));

        let cat_pos = ts.positions("cat").unwrap();
        assert_eq!(cat_pos[0].position, 1);
        assert_eq!(cat_pos[0].weight, Weight::A);
    }

    #[test]
    fn test_tsvector_concat() {
        let ts1 = TsVector::parse("'hello':1A").unwrap();
        let ts2 = TsVector::parse("'world':2B").unwrap();

        let combined = ts1.concat(&ts2);
        assert_eq!(combined.len(), 2);
        assert!(combined.contains("hello"));
        assert!(combined.contains("world"));
    }

    #[test]
    fn test_tsvector_setweight() {
        let ts = TsVector::parse("'test':1A,2B").unwrap();
        let weighted = ts.setweight(Weight::C);

        let positions = weighted.positions("test").unwrap();
        assert!(positions.iter().all(|p| p.weight == Weight::C));
    }

    #[test]
    fn test_tsquery_lexeme() {
        let query = TsQuery::lexeme("hello");
        let ts = TsVector::parse("'hello':1").unwrap();

        assert!(query.matches(&ts));
    }

    #[test]
    fn test_tsquery_prefix() {
        let query = TsQuery::prefix("hel");
        let ts = TsVector::parse("'hello':1 'help':2").unwrap();

        assert!(query.matches(&ts));
    }

    #[test]
    fn test_tsquery_parse_and() {
        let query = TsQuery::parse("'cat' & 'dog'").unwrap();

        let ts_both = TsVector::parse("'cat':1 'dog':2").unwrap();
        let ts_cat = TsVector::parse("'cat':1").unwrap();

        assert!(query.matches(&ts_both));
        assert!(!query.matches(&ts_cat));
    }

    #[test]
    fn test_tsquery_parse_or() {
        let query = TsQuery::parse("'cat' | 'dog'").unwrap();

        let ts_cat = TsVector::parse("'cat':1").unwrap();
        let ts_dog = TsVector::parse("'dog':1").unwrap();
        let ts_bird = TsVector::parse("'bird':1").unwrap();

        assert!(query.matches(&ts_cat));
        assert!(query.matches(&ts_dog));
        assert!(!query.matches(&ts_bird));
    }

    #[test]
    fn test_tsquery_parse_not() {
        let query = TsQuery::parse("!'cat'").unwrap();

        let ts_cat = TsVector::parse("'cat':1").unwrap();
        let ts_dog = TsVector::parse("'dog':1").unwrap();

        assert!(!query.matches(&ts_cat));
        assert!(query.matches(&ts_dog));
    }

    #[test]
    fn test_tsquery_phrase() {
        let query = TsQuery::parse("'quick' <-> 'brown'").unwrap();

        let ts_adjacent = TsVector::parse("'quick':1 'brown':2").unwrap();
        let ts_separated = TsVector::parse("'quick':1 'brown':5").unwrap();

        assert!(query.matches(&ts_adjacent));
        assert!(!query.matches(&ts_separated));
    }

    #[test]
    fn test_ts_config_english() {
        let config = TsConfig::english();

        assert!(config.is_stop_word("the"));
        assert!(config.is_stop_word("and"));
        assert!(!config.is_stop_word("cat"));
    }

    #[test]
    fn test_ts_config_to_tsvector() {
        let config = TsConfig::english();
        let ts = config.to_tsvector("The quick brown fox jumps over the lazy dog");

        // "the" is a stop word, should not be included
        assert!(!ts.contains("the"));
        assert!(ts.contains("quick"));
        assert!(ts.contains("fox"));
    }

    #[test]
    fn test_ts_config_to_tsquery() {
        let config = TsConfig::english();
        let query = config.to_tsquery("quick brown fox");

        let ts = config.to_tsvector("The quick brown fox");
        assert!(query.matches(&ts));
    }

    #[test]
    fn test_plainto_tsquery() {
        let config = TsConfig::english();
        let query = config.plainto_tsquery("cats and dogs");

        let ts = config.to_tsvector("I have cats and dogs at home");
        assert!(query.matches(&ts));
    }

    #[test]
    fn test_phraseto_tsquery() {
        let config = TsConfig::english();
        let query = config.phraseto_tsquery("quick brown");

        // Creates phrase query with <->
        assert!(query.root.is_some());
    }

    #[test]
    fn test_websearch_to_tsquery() {
        let config = TsConfig::english();

        // Simple terms
        let query = config.websearch_to_tsquery("cats dogs");
        let ts = config.to_tsvector("I love cats and dogs");
        assert!(query.matches(&ts));

        // Exclusion
        let query = config.websearch_to_tsquery("cats -dogs");
        let ts_cats_only = config.to_tsvector("I love cats");
        assert!(query.matches(&ts_cats_only));
    }

    #[test]
    fn test_ts_rank() {
        let ts = TsVector::parse("'important':1A 'less':2C 'minor':3D").unwrap();
        let query = TsQuery::lexeme("important");

        let rank = ts_rank(&ts, &query, None);
        assert!(rank > 0.0);
    }

    #[test]
    fn test_ts_headline() {
        let config = TsConfig::english();
        let query = config.to_tsquery("fox");
        let headline = ts_headline(
            &config,
            "The quick brown fox jumps over the lazy dog",
            &query,
            None,
        );

        assert!(headline.contains("<b>"));
        assert!(headline.contains("fox"));
    }

    #[test]
    fn test_gin_index_insert_search() {
        let index = GinIndex::new();
        let config = TsConfig::english();

        // Insert documents
        let doc1 = config.to_tsvector("The quick brown fox");
        let doc2 = config.to_tsvector("The lazy brown dog");
        let doc3 = config.to_tsvector("A red fox and blue bird");

        index.insert(1, &doc1);
        index.insert(2, &doc2);
        index.insert(3, &doc3);

        // Search
        let query = config.to_tsquery("fox");
        let results = index.search(&query);
        assert!(results.contains(&1));
        assert!(results.contains(&3));
        assert!(!results.contains(&2));
    }

    #[test]
    fn test_gin_index_stats() {
        let index = GinIndex::new();
        let config = TsConfig::english();

        index.insert(1, &config.to_tsvector("hello world"));
        index.insert(2, &config.to_tsvector("world peace"));

        let stats = index.stats();
        assert_eq!(stats.doc_count, 2);
        assert!(stats.lexeme_count >= 2);
    }

    #[test]
    fn test_gin_index_delete() {
        let index = GinIndex::new();
        let config = TsConfig::english();

        let doc = config.to_tsvector("hello world");
        index.insert(1, &doc);

        let query = config.to_tsquery("hello");
        assert!(!index.search(&query).is_empty());

        index.delete(1, &doc);
        assert!(index.search(&query).is_empty());
    }

    #[test]
    fn test_error_display() {
        let errors = vec![
            FtsError::ParseError("bad input".to_string()),
            FtsError::InvalidWeight('X'),
            FtsError::ConfigNotFound("german".to_string()),
        ];

        for error in errors {
            let msg = format!("{}", error);
            assert!(!msg.is_empty());
        }
    }

    #[test]
    fn test_tsvector_to_string() {
        let mut ts = TsVector::new();
        ts.add("hello", 1, Weight::A);
        ts.add("world", 2, Weight::D);

        let s = ts.to_string();
        assert!(s.contains("hello"));
        assert!(s.contains("world"));
    }

    #[test]
    fn test_tsquery_to_string() {
        let query = TsQuery::parse("'cat' & 'dog'").unwrap();
        let s = query.to_string();
        assert!(s.contains("cat"));
        assert!(s.contains("dog"));
        assert!(s.contains("&"));
    }
}
