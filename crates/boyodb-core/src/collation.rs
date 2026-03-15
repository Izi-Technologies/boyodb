// Collation - Multi-locale string comparison for BoyoDB
//
// Provides PostgreSQL-style collation support:
// - Locale-aware string comparison
// - Case sensitivity options
// - Accent sensitivity options
// - Custom collation definitions
// - Built-in collations (C, POSIX, en_US, etc.)

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

// ============================================================================
// Collation Types
// ============================================================================

/// Collation provider type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CollationProvider {
    /// libc-based collation
    Libc,
    /// ICU-based collation
    Icu,
    /// Built-in simple collation
    Builtin,
}

impl Default for CollationProvider {
    fn default() -> Self {
        CollationProvider::Builtin
    }
}

/// Case sensitivity option
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CaseSensitivity {
    /// Case-sensitive comparison (default)
    #[default]
    Sensitive,
    /// Case-insensitive comparison
    Insensitive,
}

/// Accent sensitivity option
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AccentSensitivity {
    /// Accent-sensitive comparison (default)
    #[default]
    Sensitive,
    /// Accent-insensitive comparison
    Insensitive,
}

/// Collation strength (ICU-style)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CollationStrength {
    /// Primary: base characters only
    Primary,
    /// Secondary: base + accents
    Secondary,
    /// Tertiary: base + accents + case (default)
    #[default]
    Tertiary,
    /// Quaternary: base + accents + case + punctuation
    Quaternary,
    /// Identical: exact byte comparison
    Identical,
}

/// Collation configuration
#[derive(Debug, Clone)]
pub struct CollationConfig {
    /// Collation name
    pub name: String,
    /// Locale identifier (e.g., "en_US", "de_DE")
    pub locale: String,
    /// Collation provider
    pub provider: CollationProvider,
    /// Case sensitivity
    pub case_sensitivity: CaseSensitivity,
    /// Accent sensitivity
    pub accent_sensitivity: AccentSensitivity,
    /// Collation strength
    pub strength: CollationStrength,
    /// Is deterministic (same result for same input)
    pub deterministic: bool,
    /// Version string for ICU
    pub version: Option<String>,
}

impl Default for CollationConfig {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            locale: "en_US".to_string(),
            provider: CollationProvider::default(),
            case_sensitivity: CaseSensitivity::default(),
            accent_sensitivity: AccentSensitivity::default(),
            strength: CollationStrength::default(),
            deterministic: true,
            version: None,
        }
    }
}

// ============================================================================
// Collation Implementation
// ============================================================================

/// A collation for string comparison
#[derive(Debug, Clone)]
pub struct Collation {
    /// Collation OID
    pub oid: u32,
    /// Configuration
    pub config: CollationConfig,
}

impl Collation {
    /// Create a new collation
    pub fn new(oid: u32, config: CollationConfig) -> Self {
        Self { oid, config }
    }

    /// Get the collation name
    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Compare two strings using this collation
    pub fn compare(&self, a: &str, b: &str) -> Ordering {
        match self.config.provider {
            CollationProvider::Builtin => self.compare_builtin(a, b),
            CollationProvider::Libc => self.compare_libc(a, b),
            CollationProvider::Icu => self.compare_icu(a, b),
        }
    }

    /// Compare using built-in logic
    fn compare_builtin(&self, a: &str, b: &str) -> Ordering {
        // Normalize based on case sensitivity
        let (a_norm, b_norm) = match self.config.case_sensitivity {
            CaseSensitivity::Sensitive => (a.to_string(), b.to_string()),
            CaseSensitivity::Insensitive => (a.to_lowercase(), b.to_lowercase()),
        };

        // Normalize based on accent sensitivity
        let (a_final, b_final) = match self.config.accent_sensitivity {
            AccentSensitivity::Sensitive => (a_norm, b_norm),
            AccentSensitivity::Insensitive => (
                remove_accents(&a_norm),
                remove_accents(&b_norm),
            ),
        };

        // Handle different locales
        if self.config.locale == "C" || self.config.locale == "POSIX" {
            // Binary comparison
            a_final.cmp(&b_final)
        } else {
            // Locale-aware comparison (simplified)
            self.locale_compare(&a_final, &b_final)
        }
    }

    /// Locale-aware comparison (simplified)
    fn locale_compare(&self, a: &str, b: &str) -> Ordering {
        // Get sort keys based on locale
        let a_key = self.sort_key(a);
        let b_key = self.sort_key(b);
        a_key.cmp(&b_key)
    }

    /// Generate a sort key for a string
    fn sort_key(&self, s: &str) -> Vec<u32> {
        // Simplified sort key generation
        // In a real implementation, this would use ICU or locale-specific rules
        s.chars()
            .map(|c| {
                let base = self.base_weight(c);
                let accent = self.accent_weight(c);
                let case = self.case_weight(c);

                match self.config.strength {
                    CollationStrength::Primary => base,
                    CollationStrength::Secondary => (base << 8) | accent,
                    CollationStrength::Tertiary => (base << 16) | (accent << 8) | case,
                    CollationStrength::Quaternary | CollationStrength::Identical => {
                        (base << 16) | (accent << 8) | case
                    }
                }
            })
            .collect()
    }

    /// Get base weight for a character
    fn base_weight(&self, c: char) -> u32 {
        // Simplified base weight - uses Unicode code point
        // Real implementation would use DUCET or locale-specific weights
        let lower = c.to_lowercase().next().unwrap_or(c);
        let base = match lower {
            'a'..='z' => (lower as u32) - ('a' as u32) + 100,
            '0'..='9' => (lower as u32) - ('0' as u32) + 50,
            _ => lower as u32,
        };

        // Handle locale-specific ordering
        match self.config.locale.as_str() {
            "de_DE" | "de_AT" => {
                // German: ä after a, ö after o, ü after u, ß after ss
                match lower {
                    'ä' => 101,  // After 'a'
                    'ö' => 115,  // After 'o'
                    'ü' => 121,  // After 'u'
                    'ß' => 200,  // After 's'
                    _ => base,
                }
            }
            "sv_SE" | "fi_FI" => {
                // Swedish/Finnish: å, ä, ö at end of alphabet
                match lower {
                    'å' => 200,
                    'ä' => 201,
                    'ö' => 202,
                    _ => base,
                }
            }
            "es_ES" => {
                // Spanish: ñ after n
                match lower {
                    'ñ' => 114,  // After 'n'
                    _ => base,
                }
            }
            _ => base,
        }
    }

    /// Get accent weight for a character
    fn accent_weight(&self, c: char) -> u32 {
        // Simplified accent weight
        match c {
            'á' | 'à' | 'â' | 'ã' | 'ä' | 'å' => 1,
            'é' | 'è' | 'ê' | 'ë' => 2,
            'í' | 'ì' | 'î' | 'ï' => 3,
            'ó' | 'ò' | 'ô' | 'õ' | 'ö' => 4,
            'ú' | 'ù' | 'û' | 'ü' => 5,
            'ñ' => 6,
            'ç' => 7,
            _ => 0,
        }
    }

    /// Get case weight for a character
    fn case_weight(&self, c: char) -> u32 {
        if c.is_uppercase() { 1 } else { 0 }
    }

    /// Compare using libc (fallback to builtin)
    fn compare_libc(&self, a: &str, b: &str) -> Ordering {
        // In a real implementation, would use libc::strcoll
        // For now, fallback to builtin
        self.compare_builtin(a, b)
    }

    /// Compare using ICU (fallback to builtin)
    fn compare_icu(&self, a: &str, b: &str) -> Ordering {
        // In a real implementation, would use ICU collator
        // For now, fallback to builtin
        self.compare_builtin(a, b)
    }

    /// Check if a string matches a pattern (with collation)
    pub fn like(&self, text: &str, pattern: &str) -> bool {
        self.like_internal(text, pattern, false)
    }

    /// Case-insensitive LIKE (ILIKE)
    pub fn ilike(&self, text: &str, pattern: &str) -> bool {
        self.like_internal(text, pattern, true)
    }

    fn like_internal(&self, text: &str, pattern: &str, case_insensitive: bool) -> bool {
        let (text, pattern) = if case_insensitive {
            (text.to_lowercase(), pattern.to_lowercase())
        } else {
            (text.to_string(), pattern.to_string())
        };

        like_match(&text, &pattern)
    }

    /// Get collation key (sort key as bytes)
    pub fn collation_key(&self, s: &str) -> Vec<u8> {
        let sort_key = self.sort_key(s);
        let mut bytes = Vec::with_capacity(sort_key.len() * 4);
        for k in sort_key {
            bytes.extend(k.to_be_bytes());
        }
        bytes
    }
}

/// LIKE pattern matching
fn like_match(text: &str, pattern: &str) -> bool {
    let text_chars: Vec<char> = text.chars().collect();
    let pattern_chars: Vec<char> = pattern.chars().collect();
    like_match_recursive(&text_chars, &pattern_chars, 0, 0)
}

fn like_match_recursive(text: &[char], pattern: &[char], ti: usize, pi: usize) -> bool {
    if pi >= pattern.len() {
        return ti >= text.len();
    }

    match pattern[pi] {
        '%' => {
            // Match zero or more characters
            for i in ti..=text.len() {
                if like_match_recursive(text, pattern, i, pi + 1) {
                    return true;
                }
            }
            false
        }
        '_' => {
            // Match exactly one character
            if ti < text.len() {
                like_match_recursive(text, pattern, ti + 1, pi + 1)
            } else {
                false
            }
        }
        c => {
            // Match literal character
            if ti < text.len() && text[ti] == c {
                like_match_recursive(text, pattern, ti + 1, pi + 1)
            } else {
                false
            }
        }
    }
}

/// Remove accents from a string (simplified)
fn remove_accents(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'á' | 'à' | 'â' | 'ã' | 'ä' | 'å' | 'Á' | 'À' | 'Â' | 'Ã' | 'Ä' | 'Å' => 'a',
            'é' | 'è' | 'ê' | 'ë' | 'É' | 'È' | 'Ê' | 'Ë' => 'e',
            'í' | 'ì' | 'î' | 'ï' | 'Í' | 'Ì' | 'Î' | 'Ï' => 'i',
            'ó' | 'ò' | 'ô' | 'õ' | 'ö' | 'Ó' | 'Ò' | 'Ô' | 'Õ' | 'Ö' => 'o',
            'ú' | 'ù' | 'û' | 'ü' | 'Ú' | 'Ù' | 'Û' | 'Ü' => 'u',
            'ñ' | 'Ñ' => 'n',
            'ç' | 'Ç' => 'c',
            _ => c,
        })
        .collect()
}

// ============================================================================
// Collation Manager
// ============================================================================

/// Manages all collations in the system
pub struct CollationManager {
    /// Collations by OID
    collations: RwLock<HashMap<u32, Arc<Collation>>>,
    /// Name to OID mapping
    name_index: RwLock<HashMap<String, u32>>,
    /// Next available OID
    next_oid: RwLock<u32>,
}

impl CollationManager {
    /// Create a new collation manager
    pub fn new() -> Self {
        let manager = Self {
            collations: RwLock::new(HashMap::new()),
            name_index: RwLock::new(HashMap::new()),
            next_oid: RwLock::new(16384),
        };

        // Initialize built-in collations
        manager.initialize_builtins();
        manager
    }

    /// Initialize built-in collations
    fn initialize_builtins(&self) {
        let builtins = vec![
            // Default collation
            (100, CollationConfig {
                name: "default".to_string(),
                locale: "en_US".to_string(),
                ..Default::default()
            }),
            // C collation (binary)
            (950, CollationConfig {
                name: "C".to_string(),
                locale: "C".to_string(),
                provider: CollationProvider::Builtin,
                deterministic: true,
                ..Default::default()
            }),
            // POSIX collation (same as C)
            (951, CollationConfig {
                name: "POSIX".to_string(),
                locale: "POSIX".to_string(),
                provider: CollationProvider::Builtin,
                deterministic: true,
                ..Default::default()
            }),
            // Unicode (UCS_BASIC)
            (952, CollationConfig {
                name: "ucs_basic".to_string(),
                locale: "C".to_string(),
                provider: CollationProvider::Builtin,
                deterministic: true,
                ..Default::default()
            }),
            // English (US)
            (1000, CollationConfig {
                name: "en_US".to_string(),
                locale: "en_US".to_string(),
                provider: CollationProvider::Builtin,
                ..Default::default()
            }),
            // German
            (1001, CollationConfig {
                name: "de_DE".to_string(),
                locale: "de_DE".to_string(),
                provider: CollationProvider::Builtin,
                ..Default::default()
            }),
            // French
            (1002, CollationConfig {
                name: "fr_FR".to_string(),
                locale: "fr_FR".to_string(),
                provider: CollationProvider::Builtin,
                ..Default::default()
            }),
            // Spanish
            (1003, CollationConfig {
                name: "es_ES".to_string(),
                locale: "es_ES".to_string(),
                provider: CollationProvider::Builtin,
                ..Default::default()
            }),
            // Swedish
            (1004, CollationConfig {
                name: "sv_SE".to_string(),
                locale: "sv_SE".to_string(),
                provider: CollationProvider::Builtin,
                ..Default::default()
            }),
            // Case-insensitive
            (2000, CollationConfig {
                name: "en_US_ci".to_string(),
                locale: "en_US".to_string(),
                case_sensitivity: CaseSensitivity::Insensitive,
                provider: CollationProvider::Builtin,
                deterministic: false,
                ..Default::default()
            }),
            // Case and accent insensitive
            (2001, CollationConfig {
                name: "en_US_ci_ai".to_string(),
                locale: "en_US".to_string(),
                case_sensitivity: CaseSensitivity::Insensitive,
                accent_sensitivity: AccentSensitivity::Insensitive,
                provider: CollationProvider::Builtin,
                deterministic: false,
                ..Default::default()
            }),
        ];

        for (oid, config) in builtins {
            let name = config.name.clone();
            let collation = Arc::new(Collation::new(oid, config));
            self.collations.write().insert(oid, collation);
            self.name_index.write().insert(name, oid);
        }
    }

    /// Create a new collation
    pub fn create_collation(&self, config: CollationConfig) -> Result<Arc<Collation>, CollationError> {
        // Validate name
        if config.name.is_empty() {
            return Err(CollationError::InvalidName("empty name".to_string()));
        }

        // Check for duplicates
        {
            let name_index = self.name_index.read();
            if name_index.contains_key(&config.name) {
                return Err(CollationError::AlreadyExists(config.name.clone()));
            }
        }

        // Allocate OID
        let oid = {
            let mut next_oid = self.next_oid.write();
            let oid = *next_oid;
            *next_oid += 1;
            oid
        };

        let collation = Arc::new(Collation::new(oid, config.clone()));

        self.collations.write().insert(oid, collation.clone());
        self.name_index.write().insert(config.name, oid);

        Ok(collation)
    }

    /// Drop a collation
    pub fn drop_collation(&self, name: &str) -> Result<(), CollationError> {
        let oid = {
            let name_index = self.name_index.read();
            *name_index.get(name)
                .ok_or_else(|| CollationError::NotFound(name.to_string()))?
        };

        // Check if it's a built-in collation
        if oid < 16384 {
            return Err(CollationError::CannotDropBuiltin(name.to_string()));
        }

        self.collations.write().remove(&oid);
        self.name_index.write().remove(name);

        Ok(())
    }

    /// Get a collation by name
    pub fn get_collation(&self, name: &str) -> Option<Arc<Collation>> {
        let name_index = self.name_index.read();
        let oid = name_index.get(name)?;
        self.collations.read().get(oid).cloned()
    }

    /// Get a collation by OID
    pub fn get_collation_by_oid(&self, oid: u32) -> Option<Arc<Collation>> {
        self.collations.read().get(&oid).cloned()
    }

    /// Get the default collation
    pub fn default_collation(&self) -> Arc<Collation> {
        self.get_collation("default")
            .expect("default collation must exist")
    }

    /// Get the C collation
    pub fn c_collation(&self) -> Arc<Collation> {
        self.get_collation("C")
            .expect("C collation must exist")
    }

    /// List all collations
    pub fn list_collations(&self) -> Vec<Arc<Collation>> {
        self.collations.read().values().cloned().collect()
    }

    /// Find collations matching a locale pattern
    pub fn find_by_locale(&self, locale_pattern: &str) -> Vec<Arc<Collation>> {
        self.collations.read()
            .values()
            .filter(|c| c.config.locale.starts_with(locale_pattern))
            .cloned()
            .collect()
    }
}

impl Default for CollationManager {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Collation-aware string operations
// ============================================================================

/// Compare strings with collation
pub fn collate_compare(a: &str, b: &str, collation: &Collation) -> Ordering {
    collation.compare(a, b)
}

/// Sort strings with collation
pub fn collate_sort(strings: &mut [String], collation: &Collation) {
    strings.sort_by(|a, b| collation.compare(a, b));
}

/// Find minimum with collation
pub fn collate_min<'a>(strings: &'a [&'a str], collation: &Collation) -> Option<&'a str> {
    strings.iter()
        .min_by(|a, b| collation.compare(a, b))
        .copied()
}

/// Find maximum with collation
pub fn collate_max<'a>(strings: &'a [&'a str], collation: &Collation) -> Option<&'a str> {
    strings.iter()
        .max_by(|a, b| collation.compare(a, b))
        .copied()
}

/// Binary search with collation
pub fn collate_binary_search(sorted: &[&str], target: &str, collation: &Collation) -> Option<usize> {
    sorted.binary_search_by(|s| collation.compare(s, target)).ok()
}

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug)]
pub enum CollationError {
    InvalidName(String),
    AlreadyExists(String),
    NotFound(String),
    CannotDropBuiltin(String),
    InvalidLocale(String),
    ProviderNotAvailable(CollationProvider),
}

impl std::fmt::Display for CollationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidName(msg) => write!(f, "Invalid collation name: {}", msg),
            Self::AlreadyExists(name) => write!(f, "Collation '{}' already exists", name),
            Self::NotFound(name) => write!(f, "Collation '{}' not found", name),
            Self::CannotDropBuiltin(name) => write!(f, "Cannot drop built-in collation '{}'", name),
            Self::InvalidLocale(locale) => write!(f, "Invalid locale: {}", locale),
            Self::ProviderNotAvailable(provider) => write!(f, "Collation provider not available: {:?}", provider),
        }
    }
}

impl std::error::Error for CollationError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collation_config_default() {
        let config = CollationConfig::default();
        assert_eq!(config.name, "default");
        assert_eq!(config.locale, "en_US");
        assert!(config.deterministic);
    }

    #[test]
    fn test_basic_comparison() {
        let config = CollationConfig {
            name: "test".to_string(),
            locale: "en_US".to_string(),
            ..Default::default()
        };
        let collation = Collation::new(1, config);

        assert_eq!(collation.compare("a", "b"), Ordering::Less);
        assert_eq!(collation.compare("b", "a"), Ordering::Greater);
        assert_eq!(collation.compare("abc", "abc"), Ordering::Equal);
    }

    #[test]
    fn test_case_insensitive() {
        let config = CollationConfig {
            name: "test_ci".to_string(),
            locale: "en_US".to_string(),
            case_sensitivity: CaseSensitivity::Insensitive,
            ..Default::default()
        };
        let collation = Collation::new(1, config);

        assert_eq!(collation.compare("ABC", "abc"), Ordering::Equal);
        assert_eq!(collation.compare("Hello", "hello"), Ordering::Equal);
    }

    #[test]
    fn test_accent_insensitive() {
        let config = CollationConfig {
            name: "test_ai".to_string(),
            locale: "en_US".to_string(),
            accent_sensitivity: AccentSensitivity::Insensitive,
            ..Default::default()
        };
        let collation = Collation::new(1, config);

        assert_eq!(collation.compare("café", "cafe"), Ordering::Equal);
        assert_eq!(collation.compare("résumé", "resume"), Ordering::Equal);
    }

    #[test]
    fn test_c_collation() {
        let config = CollationConfig {
            name: "C".to_string(),
            locale: "C".to_string(),
            ..Default::default()
        };
        let collation = Collation::new(1, config);

        // C collation uses byte order
        assert_eq!(collation.compare("A", "a"), Ordering::Less); // 'A' < 'a' in ASCII
        assert_eq!(collation.compare("Z", "a"), Ordering::Less); // 'Z' < 'a' in ASCII
    }

    #[test]
    fn test_german_collation() {
        let config = CollationConfig {
            name: "de_DE".to_string(),
            locale: "de_DE".to_string(),
            ..Default::default()
        };
        let collation = Collation::new(1, config);

        // ä should sort after a
        assert_eq!(collation.compare("a", "ä"), Ordering::Less);
        // ö should sort after o
        assert_eq!(collation.compare("o", "ö"), Ordering::Less);
    }

    #[test]
    fn test_swedish_collation() {
        let config = CollationConfig {
            name: "sv_SE".to_string(),
            locale: "sv_SE".to_string(),
            ..Default::default()
        };
        let collation = Collation::new(1, config);

        // In Swedish, å, ä, ö come at the end of the alphabet
        assert_eq!(collation.compare("z", "å"), Ordering::Less);
        assert_eq!(collation.compare("z", "ä"), Ordering::Less);
        assert_eq!(collation.compare("z", "ö"), Ordering::Less);
    }

    #[test]
    fn test_like_matching() {
        let collation = Collation::new(1, CollationConfig::default());

        assert!(collation.like("hello", "hello"));
        assert!(collation.like("hello", "h%"));
        assert!(collation.like("hello", "%o"));
        assert!(collation.like("hello", "h%o"));
        assert!(collation.like("hello", "%ell%"));
        assert!(collation.like("hello", "h_llo"));
        assert!(collation.like("hello", "_____"));

        assert!(!collation.like("hello", "world"));
        assert!(!collation.like("hello", "h%x"));
        assert!(!collation.like("hello", "____")); // Too few characters
    }

    #[test]
    fn test_ilike_matching() {
        let collation = Collation::new(1, CollationConfig::default());

        assert!(collation.ilike("HELLO", "hello"));
        assert!(collation.ilike("Hello", "HELLO"));
        assert!(collation.ilike("HeLLo", "h%o"));
    }

    #[test]
    fn test_collation_key() {
        let collation = Collation::new(1, CollationConfig::default());

        let key1 = collation.collation_key("abc");
        let key2 = collation.collation_key("abd");

        assert!(key1 < key2);

        let key3 = collation.collation_key("abc");
        assert_eq!(key1, key3);
    }

    #[test]
    fn test_collation_manager_builtins() {
        let manager = CollationManager::new();

        assert!(manager.get_collation("default").is_some());
        assert!(manager.get_collation("C").is_some());
        assert!(manager.get_collation("POSIX").is_some());
        assert!(manager.get_collation("en_US").is_some());
        assert!(manager.get_collation("de_DE").is_some());
    }

    #[test]
    fn test_collation_manager_create() {
        let manager = CollationManager::new();

        let config = CollationConfig {
            name: "custom".to_string(),
            locale: "fr_CA".to_string(),
            ..Default::default()
        };

        let collation = manager.create_collation(config).unwrap();
        assert_eq!(collation.name(), "custom");

        let retrieved = manager.get_collation("custom").unwrap();
        assert_eq!(retrieved.config.locale, "fr_CA");
    }

    #[test]
    fn test_collation_manager_drop() {
        let manager = CollationManager::new();

        let config = CollationConfig {
            name: "droppable".to_string(),
            locale: "en_US".to_string(),
            ..Default::default()
        };

        manager.create_collation(config).unwrap();
        assert!(manager.get_collation("droppable").is_some());

        manager.drop_collation("droppable").unwrap();
        assert!(manager.get_collation("droppable").is_none());
    }

    #[test]
    fn test_cannot_drop_builtin() {
        let manager = CollationManager::new();

        let result = manager.drop_collation("C");
        assert!(matches!(result, Err(CollationError::CannotDropBuiltin(_))));
    }

    #[test]
    fn test_collate_sort() {
        let manager = CollationManager::new();
        let collation = manager.get_collation("en_US_ci").unwrap();

        let mut strings = vec![
            "Banana".to_string(),
            "apple".to_string(),
            "Cherry".to_string(),
        ];

        collate_sort(&mut strings, &collation);

        // Case-insensitive sort
        assert_eq!(strings[0], "apple");
        assert_eq!(strings[1], "Banana");
        assert_eq!(strings[2], "Cherry");
    }

    #[test]
    fn test_collate_min_max() {
        let manager = CollationManager::new();
        let collation = manager.default_collation();

        let strings = vec!["cherry", "apple", "banana"];

        let min = collate_min(&strings, &collation);
        assert_eq!(min, Some("apple"));

        let max = collate_max(&strings, &collation);
        assert_eq!(max, Some("cherry"));
    }

    #[test]
    fn test_collation_strength() {
        let config_primary = CollationConfig {
            name: "primary".to_string(),
            strength: CollationStrength::Primary,
            ..Default::default()
        };
        let primary = Collation::new(1, config_primary);

        let config_tertiary = CollationConfig {
            name: "tertiary".to_string(),
            strength: CollationStrength::Tertiary,
            ..Default::default()
        };
        let tertiary = Collation::new(2, config_tertiary);

        // Primary ignores case and accents
        let key_a = primary.sort_key("a");
        let key_A = primary.sort_key("A");
        // Both should have same primary weight
        assert_eq!(key_a[0] >> 16, key_A[0] >> 16);

        // Tertiary distinguishes case
        let key_a_t = tertiary.sort_key("a");
        let key_A_t = tertiary.sort_key("A");
        assert_ne!(key_a_t[0], key_A_t[0]);
    }

    #[test]
    fn test_remove_accents() {
        assert_eq!(remove_accents("café"), "cafe");
        assert_eq!(remove_accents("résumé"), "resume");
        assert_eq!(remove_accents("naïve"), "naive");
        assert_eq!(remove_accents("hello"), "hello");
    }

    #[test]
    fn test_find_by_locale() {
        let manager = CollationManager::new();

        let en_collations = manager.find_by_locale("en_");
        assert!(!en_collations.is_empty());

        for coll in en_collations {
            assert!(coll.config.locale.starts_with("en_"));
        }
    }

    #[test]
    fn test_list_collations() {
        let manager = CollationManager::new();

        let all = manager.list_collations();
        assert!(all.len() >= 10); // At least 10 built-in collations
    }

    #[test]
    fn test_error_display() {
        let errors = vec![
            CollationError::InvalidName("bad".to_string()),
            CollationError::AlreadyExists("dup".to_string()),
            CollationError::NotFound("missing".to_string()),
            CollationError::CannotDropBuiltin("C".to_string()),
            CollationError::InvalidLocale("xx_XX".to_string()),
            CollationError::ProviderNotAvailable(CollationProvider::Icu),
        ];

        for error in errors {
            let msg = format!("{}", error);
            assert!(!msg.is_empty());
        }
    }

    #[test]
    fn test_binary_search() {
        let manager = CollationManager::new();
        let collation = manager.c_collation();

        let sorted = vec!["apple", "banana", "cherry", "date"];

        assert_eq!(collate_binary_search(&sorted, "banana", &collation), Some(1));
        assert_eq!(collate_binary_search(&sorted, "cherry", &collation), Some(2));
        assert_eq!(collate_binary_search(&sorted, "grape", &collation), None);
    }
}
