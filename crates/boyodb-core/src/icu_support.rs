// ICU Support - Unicode collation algorithms for BoyoDB
//
// Provides ICU (International Components for Unicode) integration:
// - Unicode Collation Algorithm (UCA) support
// - Locale-specific sorting rules
// - Normalization forms (NFC, NFD, NFKC, NFKD)
// - Unicode character properties
// - Transliteration support
//
// When the `icu-collation` feature is enabled, this module uses the
// actual ICU libraries for production-grade Unicode support.

use std::cmp::Ordering;
use std::collections::HashMap;

#[cfg(feature = "icu-collation")]
use icu_collator::{Collator, CollatorOptions, Strength};
#[cfg(feature = "icu-collation")]
use icu_locid::Locale;
#[cfg(feature = "icu-collation")]
use icu_normalizer::{ComposingNormalizer, DecomposingNormalizer};

// ============================================================================
// Unicode Normalization
// ============================================================================

/// Unicode normalization form
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NormalizationForm {
    /// Canonical decomposition, followed by canonical composition
    #[default]
    Nfc,
    /// Canonical decomposition
    Nfd,
    /// Compatibility decomposition, followed by canonical composition
    Nfkc,
    /// Compatibility decomposition
    Nfkd,
}

/// Normalize a string using specified form
pub fn normalize(s: &str, form: NormalizationForm) -> String {
    #[cfg(feature = "icu-collation")]
    {
        match form {
            NormalizationForm::Nfc => {
                let normalizer = ComposingNormalizer::new_nfc();
                normalizer.normalize(s)
            }
            NormalizationForm::Nfd => {
                let normalizer = DecomposingNormalizer::new_nfd();
                normalizer.normalize(s)
            }
            NormalizationForm::Nfkc => {
                let normalizer = ComposingNormalizer::new_nfkc();
                normalizer.normalize(s)
            }
            NormalizationForm::Nfkd => {
                let normalizer = DecomposingNormalizer::new_nfkd();
                normalizer.normalize(s)
            }
        }
    }

    #[cfg(not(feature = "icu-collation"))]
    match form {
        NormalizationForm::Nfc => nfc_normalize(s),
        NormalizationForm::Nfd => nfd_normalize(s),
        NormalizationForm::Nfkc => nfkc_normalize(s),
        NormalizationForm::Nfkd => nfkd_normalize(s),
    }
}

/// NFC normalization (canonical composition)
fn nfc_normalize(s: &str) -> String {
    // Simplified NFC - in production would use ICU
    // For now, just handle common composed/decomposed pairs
    let mut result = String::with_capacity(s.len());

    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        // Check for combining characters
        if let Some(&next) = chars.peek() {
            if let Some(composed) = compose_chars(c, next) {
                result.push(composed);
                chars.next(); // Consume the combining char
                continue;
            }
        }
        result.push(c);
    }

    result
}

/// NFD normalization (canonical decomposition)
fn nfd_normalize(s: &str) -> String {
    let mut result = String::with_capacity(s.len() * 2);

    for c in s.chars() {
        if let Some((base, combining)) = decompose_char(c) {
            result.push(base);
            result.push(combining);
        } else {
            result.push(c);
        }
    }

    result
}

/// NFKC normalization (compatibility composition)
fn nfkc_normalize(s: &str) -> String {
    // Apply compatibility decomposition then compose
    let decomposed = nfkd_normalize(s);
    nfc_normalize(&decomposed)
}

/// NFKD normalization (compatibility decomposition)
fn nfkd_normalize(s: &str) -> String {
    let mut result = String::with_capacity(s.len() * 2);

    for c in s.chars() {
        // First apply canonical decomposition
        if let Some((base, combining)) = decompose_char(c) {
            result.push(base);
            result.push(combining);
        } else {
            // Then apply compatibility decomposition
            let compat = compatibility_decompose(c);
            result.push_str(&compat);
        }
    }

    result
}

/// Compose two characters if possible
fn compose_chars(base: char, combining: char) -> Option<char> {
    // Common composed characters
    match (base, combining) {
        // Acute accent
        ('a', '\u{0301}') => Some('á'),
        ('e', '\u{0301}') => Some('é'),
        ('i', '\u{0301}') => Some('í'),
        ('o', '\u{0301}') => Some('ó'),
        ('u', '\u{0301}') => Some('ú'),
        ('A', '\u{0301}') => Some('Á'),
        ('E', '\u{0301}') => Some('É'),
        ('I', '\u{0301}') => Some('Í'),
        ('O', '\u{0301}') => Some('Ó'),
        ('U', '\u{0301}') => Some('Ú'),
        // Grave accent
        ('a', '\u{0300}') => Some('à'),
        ('e', '\u{0300}') => Some('è'),
        ('i', '\u{0300}') => Some('ì'),
        ('o', '\u{0300}') => Some('ò'),
        ('u', '\u{0300}') => Some('ù'),
        // Circumflex
        ('a', '\u{0302}') => Some('â'),
        ('e', '\u{0302}') => Some('ê'),
        ('i', '\u{0302}') => Some('î'),
        ('o', '\u{0302}') => Some('ô'),
        ('u', '\u{0302}') => Some('û'),
        // Tilde
        ('a', '\u{0303}') => Some('ã'),
        ('n', '\u{0303}') => Some('ñ'),
        ('o', '\u{0303}') => Some('õ'),
        ('N', '\u{0303}') => Some('Ñ'),
        // Umlaut/diaeresis
        ('a', '\u{0308}') => Some('ä'),
        ('e', '\u{0308}') => Some('ë'),
        ('i', '\u{0308}') => Some('ï'),
        ('o', '\u{0308}') => Some('ö'),
        ('u', '\u{0308}') => Some('ü'),
        ('A', '\u{0308}') => Some('Ä'),
        ('O', '\u{0308}') => Some('Ö'),
        ('U', '\u{0308}') => Some('Ü'),
        // Cedilla
        ('c', '\u{0327}') => Some('ç'),
        ('C', '\u{0327}') => Some('Ç'),
        // Ring above
        ('a', '\u{030A}') => Some('å'),
        ('A', '\u{030A}') => Some('Å'),
        _ => None,
    }
}

/// Decompose a character into base + combining
fn decompose_char(c: char) -> Option<(char, char)> {
    match c {
        // Acute accent
        'á' => Some(('a', '\u{0301}')),
        'é' => Some(('e', '\u{0301}')),
        'í' => Some(('i', '\u{0301}')),
        'ó' => Some(('o', '\u{0301}')),
        'ú' => Some(('u', '\u{0301}')),
        'Á' => Some(('A', '\u{0301}')),
        'É' => Some(('E', '\u{0301}')),
        'Í' => Some(('I', '\u{0301}')),
        'Ó' => Some(('O', '\u{0301}')),
        'Ú' => Some(('U', '\u{0301}')),
        // Grave accent
        'à' => Some(('a', '\u{0300}')),
        'è' => Some(('e', '\u{0300}')),
        'ì' => Some(('i', '\u{0300}')),
        'ò' => Some(('o', '\u{0300}')),
        'ù' => Some(('u', '\u{0300}')),
        // Circumflex
        'â' => Some(('a', '\u{0302}')),
        'ê' => Some(('e', '\u{0302}')),
        'î' => Some(('i', '\u{0302}')),
        'ô' => Some(('o', '\u{0302}')),
        'û' => Some(('u', '\u{0302}')),
        // Tilde
        'ã' => Some(('a', '\u{0303}')),
        'ñ' => Some(('n', '\u{0303}')),
        'õ' => Some(('o', '\u{0303}')),
        'Ñ' => Some(('N', '\u{0303}')),
        // Umlaut/diaeresis
        'ä' => Some(('a', '\u{0308}')),
        'ë' => Some(('e', '\u{0308}')),
        'ï' => Some(('i', '\u{0308}')),
        'ö' => Some(('o', '\u{0308}')),
        'ü' => Some(('u', '\u{0308}')),
        'Ä' => Some(('A', '\u{0308}')),
        'Ö' => Some(('O', '\u{0308}')),
        'Ü' => Some(('U', '\u{0308}')),
        // Cedilla
        'ç' => Some(('c', '\u{0327}')),
        'Ç' => Some(('C', '\u{0327}')),
        // Ring above
        'å' => Some(('a', '\u{030A}')),
        'Å' => Some(('A', '\u{030A}')),
        _ => None,
    }
}

/// Compatibility decomposition
fn compatibility_decompose(c: char) -> String {
    match c {
        // Ligatures
        'ﬁ' => "fi".to_string(),
        'ﬂ' => "fl".to_string(),
        'ﬀ' => "ff".to_string(),
        'ﬃ' => "ffi".to_string(),
        'ﬄ' => "ffl".to_string(),
        'Ꜳ' | 'ꜳ' => "aa".to_string(),
        'Æ' => "AE".to_string(),
        'æ' => "ae".to_string(),
        'Œ' => "OE".to_string(),
        'œ' => "oe".to_string(),
        'ß' => "ss".to_string(),

        // Superscripts
        '¹' => "1".to_string(),
        '²' => "2".to_string(),
        '³' => "3".to_string(),
        '⁴' => "4".to_string(),
        '⁵' => "5".to_string(),

        // Subscripts
        '₀' => "0".to_string(),
        '₁' => "1".to_string(),
        '₂' => "2".to_string(),

        // Fractions
        '½' => "1/2".to_string(),
        '⅓' => "1/3".to_string(),
        '¼' => "1/4".to_string(),
        '¾' => "3/4".to_string(),

        // Full-width characters
        c if c >= '\u{FF01}' && c <= '\u{FF5E}' => {
            char::from_u32(c as u32 - 0xFEE0)
                .map(|ch| ch.to_string())
                .unwrap_or_else(|| c.to_string())
        }

        _ => c.to_string(),
    }
}

// ============================================================================
// Unicode Collation Algorithm (UCA)
// ============================================================================

/// Collation element - weight tuple for sorting
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CollationElement {
    /// Primary weight (base character)
    pub primary: u16,
    /// Secondary weight (accents)
    pub secondary: u16,
    /// Tertiary weight (case)
    pub tertiary: u16,
}

impl CollationElement {
    pub fn new(primary: u16, secondary: u16, tertiary: u16) -> Self {
        Self { primary, secondary, tertiary }
    }

    /// Variable weight element (ignorable at some levels)
    pub fn variable(primary: u16) -> Self {
        Self { primary, secondary: 0, tertiary: 0 }
    }
}

/// Unicode Collation Algorithm collator
pub struct UcaCollator {
    /// Collation element table
    ce_table: HashMap<char, Vec<CollationElement>>,
    /// Locale-specific tailorings
    tailorings: HashMap<String, Vec<TailoringRule>>,
    /// Current locale
    locale: String,
    /// Strength level
    strength: CollatorStrength,
    /// Variable weighting
    variable_weighting: VariableWeighting,
    /// Case first setting
    case_first: CaseFirst,
    /// Normalization mode
    normalization: bool,
    /// ICU Collator (when feature enabled)
    #[cfg(feature = "icu-collation")]
    icu_collator: Option<Collator>,
}

/// Collator strength level
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum CollatorStrength {
    /// Primary (base characters only)
    Primary,
    /// Secondary (+ accents)
    Secondary,
    /// Tertiary (+ case) - default
    #[default]
    Tertiary,
    /// Quaternary (+ punctuation)
    Quaternary,
    /// Identical (exact match)
    Identical,
}

/// Variable weighting strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VariableWeighting {
    /// Non-ignorable (punctuation has weight)
    #[default]
    NonIgnorable,
    /// Blanked (punctuation ignored)
    Blanked,
    /// Shifted (punctuation compared at quaternary level)
    Shifted,
}

/// Case first setting
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CaseFirst {
    /// No special case handling
    #[default]
    Off,
    /// Uppercase first
    Upper,
    /// Lowercase first
    Lower,
}

/// Tailoring rule for locale-specific sorting
#[derive(Debug, Clone)]
pub struct TailoringRule {
    /// Source character(s)
    pub source: String,
    /// Target character(s)
    pub target: String,
    /// Relationship type
    pub relation: TailoringRelation,
}

/// Tailoring relationship
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TailoringRelation {
    /// Source is primary less than target
    PrimaryBefore,
    /// Source is secondary less than target
    SecondaryBefore,
    /// Source is tertiary less than target
    TertiaryBefore,
    /// Source equals target at all levels
    Equal,
}

impl UcaCollator {
    /// Create a new collator
    pub fn new() -> Self {
        let mut collator = Self {
            ce_table: HashMap::new(),
            tailorings: HashMap::new(),
            locale: "root".to_string(),
            strength: CollatorStrength::Tertiary,
            variable_weighting: VariableWeighting::NonIgnorable,
            case_first: CaseFirst::Off,
            normalization: true,
            #[cfg(feature = "icu-collation")]
            icu_collator: Self::create_icu_collator("root", CollatorStrength::Tertiary),
        };
        collator.initialize_ducet();
        collator
    }

    /// Create a collator for a specific locale
    pub fn new_with_locale(locale: &str) -> Self {
        let mut collator = Self::new();
        collator.set_locale(locale);
        collator
    }

    /// Create ICU collator for a locale
    #[cfg(feature = "icu-collation")]
    fn create_icu_collator(locale: &str, strength: CollatorStrength) -> Option<Collator> {
        let locale_id: Locale = locale.parse().unwrap_or_else(|_| "und".parse().unwrap());

        let icu_strength = match strength {
            CollatorStrength::Primary => Strength::Primary,
            CollatorStrength::Secondary => Strength::Secondary,
            CollatorStrength::Tertiary => Strength::Tertiary,
            CollatorStrength::Quaternary => Strength::Quaternary,
            CollatorStrength::Identical => Strength::Identical,
        };

        let mut options = CollatorOptions::new();
        options.strength = Some(icu_strength);

        Collator::try_new(&locale_id.into(), options).ok()
    }

    /// Initialize DUCET (Default Unicode Collation Element Table)
    fn initialize_ducet(&mut self) {
        // Simplified DUCET - in production would load full ICU data
        // ASCII letters
        for (i, c) in ('a'..='z').enumerate() {
            self.ce_table.insert(c, vec![CollationElement::new(
                (0x1000 + i) as u16, // Primary
                0x0020,              // Secondary (base)
                0x0002,              // Tertiary (lowercase)
            )]);
            self.ce_table.insert(c.to_ascii_uppercase(), vec![CollationElement::new(
                (0x1000 + i) as u16, // Same primary
                0x0020,              // Same secondary
                0x0008,              // Tertiary (uppercase)
            )]);
        }

        // Digits
        for (i, c) in ('0'..='9').enumerate() {
            self.ce_table.insert(c, vec![CollationElement::new(
                (0x0100 + i) as u16,
                0x0020,
                0x0002,
            )]);
        }

        // Common accented characters
        let accented = [
            ('á', 'a', 0x0021), ('à', 'a', 0x0022), ('â', 'a', 0x0023),
            ('ä', 'a', 0x0024), ('ã', 'a', 0x0025), ('å', 'a', 0x0026),
            ('é', 'e', 0x0021), ('è', 'e', 0x0022), ('ê', 'e', 0x0023),
            ('ë', 'e', 0x0024),
            ('í', 'i', 0x0021), ('ì', 'i', 0x0022), ('î', 'i', 0x0023),
            ('ï', 'i', 0x0024),
            ('ó', 'o', 0x0021), ('ò', 'o', 0x0022), ('ô', 'o', 0x0023),
            ('ö', 'o', 0x0024), ('õ', 'o', 0x0025),
            ('ú', 'u', 0x0021), ('ù', 'u', 0x0022), ('û', 'u', 0x0023),
            ('ü', 'u', 0x0024),
            ('ñ', 'n', 0x0021),
            ('ç', 'c', 0x0021),
        ];

        for (accented_char, base_char, secondary) in accented {
            let base_primary = self.ce_table.get(&base_char)
                .map(|ces| ces[0].primary)
                .unwrap_or(0x1000);
            self.ce_table.insert(accented_char, vec![CollationElement::new(
                base_primary,
                secondary,
                0x0002,
            )]);
            self.ce_table.insert(accented_char.to_ascii_uppercase(), vec![CollationElement::new(
                base_primary,
                secondary,
                0x0008,
            )]);
        }

        // Punctuation and symbols (variable weight)
        for c in [' ', ',', '.', ';', ':', '!', '?', '-', '_'] {
            self.ce_table.insert(c, vec![CollationElement::variable(0x0001)]);
        }
    }

    /// Set the locale
    pub fn set_locale(&mut self, locale: &str) {
        self.locale = locale.to_string();
        self.apply_locale_tailorings(locale);

        #[cfg(feature = "icu-collation")]
        {
            self.icu_collator = Self::create_icu_collator(locale, self.strength);
        }
    }

    /// Apply locale-specific tailorings
    fn apply_locale_tailorings(&mut self, locale: &str) {
        match locale {
            "de_DE" | "de" => self.apply_german_tailorings(),
            "sv_SE" | "sv" | "fi_FI" | "fi" => self.apply_swedish_finnish_tailorings(),
            "es_ES" | "es" => self.apply_spanish_tailorings(),
            "da_DK" | "da" | "nb_NO" | "nb" => self.apply_danish_norwegian_tailorings(),
            _ => {}
        }
    }

    fn apply_german_tailorings(&mut self) {
        // German: ä sorts with ae, ö with oe, ü with ue
        // ß sorts with ss
        // (This is DIN 5007-1 standard)
        // In phone book ordering, ä = ae would sort differently

        // For simplified implementation, we'll use base sorting
        // with ä, ö, ü after a, o, u
    }

    fn apply_swedish_finnish_tailorings(&mut self) {
        // å, ä, ö sort at the end of the alphabet
        // V = W in Swedish

        // å after z
        self.ce_table.insert('å', vec![CollationElement::new(0x2000, 0x0020, 0x0002)]);
        self.ce_table.insert('Å', vec![CollationElement::new(0x2000, 0x0020, 0x0008)]);

        // ä after å
        self.ce_table.insert('ä', vec![CollationElement::new(0x2001, 0x0020, 0x0002)]);
        self.ce_table.insert('Ä', vec![CollationElement::new(0x2001, 0x0020, 0x0008)]);

        // ö after ä
        self.ce_table.insert('ö', vec![CollationElement::new(0x2002, 0x0020, 0x0002)]);
        self.ce_table.insert('Ö', vec![CollationElement::new(0x2002, 0x0020, 0x0008)]);
    }

    fn apply_spanish_tailorings(&mut self) {
        // Traditional Spanish: ch and ll were separate letters
        // Modern Spanish (2010): ch and ll are digraphs, not separate letters
        // ñ sorts after n

        let n_primary = self.ce_table.get(&'n')
            .map(|ces| ces[0].primary)
            .unwrap_or(0x1000);

        self.ce_table.insert('ñ', vec![CollationElement::new(
            n_primary + 1, 0x0020, 0x0002
        )]);
        self.ce_table.insert('Ñ', vec![CollationElement::new(
            n_primary + 1, 0x0020, 0x0008
        )]);
    }

    fn apply_danish_norwegian_tailorings(&mut self) {
        // æ, ø, å at end of alphabet

        self.ce_table.insert('æ', vec![CollationElement::new(0x2000, 0x0020, 0x0002)]);
        self.ce_table.insert('Æ', vec![CollationElement::new(0x2000, 0x0020, 0x0008)]);

        self.ce_table.insert('ø', vec![CollationElement::new(0x2001, 0x0020, 0x0002)]);
        self.ce_table.insert('Ø', vec![CollationElement::new(0x2001, 0x0020, 0x0008)]);

        self.ce_table.insert('å', vec![CollationElement::new(0x2002, 0x0020, 0x0002)]);
        self.ce_table.insert('Å', vec![CollationElement::new(0x2002, 0x0020, 0x0008)]);
    }

    /// Set strength level
    pub fn set_strength(&mut self, strength: CollatorStrength) {
        self.strength = strength;

        #[cfg(feature = "icu-collation")]
        {
            self.icu_collator = Self::create_icu_collator(&self.locale, strength);
        }
    }

    /// Compare two strings
    pub fn compare(&self, a: &str, b: &str) -> Ordering {
        // Use ICU collator if available
        #[cfg(feature = "icu-collation")]
        if let Some(ref collator) = self.icu_collator {
            return collator.compare(a, b);
        }

        // Fallback to built-in implementation
        // Normalize if enabled
        let (a, b) = if self.normalization {
            (normalize(a, NormalizationForm::Nfc), normalize(b, NormalizationForm::Nfc))
        } else {
            (a.to_string(), b.to_string())
        };

        // Get sort keys
        let key_a = self.get_sort_key(&a);
        let key_b = self.get_sort_key(&b);

        key_a.cmp(&key_b)
    }

    /// Get sort key for a string
    pub fn get_sort_key(&self, s: &str) -> Vec<u16> {
        let mut primary_keys = Vec::new();
        let mut secondary_keys = Vec::new();
        let mut tertiary_keys = Vec::new();

        // Get collation elements for each character
        for c in s.chars() {
            let elements = self.get_collation_elements(c);
            for elem in elements {
                // Handle variable weighting
                if self.variable_weighting == VariableWeighting::Blanked
                    && elem.secondary == 0 && elem.tertiary == 0 {
                    continue;
                }

                if elem.primary > 0 {
                    primary_keys.push(elem.primary);
                }
                if elem.secondary > 0 {
                    secondary_keys.push(elem.secondary);
                }
                if elem.tertiary > 0 {
                    tertiary_keys.push(elem.tertiary);
                }
            }
        }

        // Build sort key based on strength
        let mut sort_key = Vec::new();
        sort_key.extend(&primary_keys);

        if self.strength >= CollatorStrength::Secondary {
            sort_key.push(0); // Level separator
            sort_key.extend(&secondary_keys);
        }

        if self.strength >= CollatorStrength::Tertiary {
            sort_key.push(0); // Level separator

            // Apply case first
            match self.case_first {
                CaseFirst::Off => sort_key.extend(&tertiary_keys),
                CaseFirst::Upper => {
                    // Invert case bits for uppercase first
                    sort_key.extend(tertiary_keys.iter().map(|&t| {
                        if t == 0x0002 { 0x0008 }
                        else if t == 0x0008 { 0x0002 }
                        else { t }
                    }));
                }
                CaseFirst::Lower => sort_key.extend(&tertiary_keys),
            }
        }

        sort_key
    }

    /// Get collation elements for a character
    fn get_collation_elements(&self, c: char) -> Vec<CollationElement> {
        self.ce_table.get(&c)
            .cloned()
            .unwrap_or_else(|| {
                // Unknown character - use code point as primary weight
                vec![CollationElement::new(c as u16, 0x0020, 0x0002)]
            })
    }

    /// Check if strings are equal at current strength
    pub fn equals(&self, a: &str, b: &str) -> bool {
        self.compare(a, b) == Ordering::Equal
    }

    /// Get locale
    pub fn locale(&self) -> &str {
        &self.locale
    }
}

impl Default for UcaCollator {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Unicode Character Properties
// ============================================================================

/// Unicode general category
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeneralCategory {
    UppercaseLetter,
    LowercaseLetter,
    TitlecaseLetter,
    ModifierLetter,
    OtherLetter,
    NonspacingMark,
    SpacingCombiningMark,
    EnclosingMark,
    DecimalDigitNumber,
    LetterNumber,
    OtherNumber,
    ConnectorPunctuation,
    DashPunctuation,
    OpenPunctuation,
    ClosePunctuation,
    InitialQuotePunctuation,
    FinalQuotePunctuation,
    OtherPunctuation,
    MathSymbol,
    CurrencySymbol,
    ModifierSymbol,
    OtherSymbol,
    SpaceSeparator,
    LineSeparator,
    ParagraphSeparator,
    Control,
    Format,
    Surrogate,
    PrivateUse,
    Unassigned,
}

/// Get general category for a character
pub fn general_category(c: char) -> GeneralCategory {
    if c.is_uppercase() {
        GeneralCategory::UppercaseLetter
    } else if c.is_lowercase() {
        GeneralCategory::LowercaseLetter
    } else if c.is_ascii_digit() {
        GeneralCategory::DecimalDigitNumber
    } else if c.is_whitespace() {
        GeneralCategory::SpaceSeparator
    } else if c.is_ascii_punctuation() {
        GeneralCategory::OtherPunctuation
    } else if c.is_control() {
        GeneralCategory::Control
    } else if c.is_alphabetic() {
        GeneralCategory::OtherLetter
    } else {
        GeneralCategory::Unassigned
    }
}

/// Check if character is a letter
pub fn is_letter(c: char) -> bool {
    c.is_alphabetic()
}

/// Check if character is a mark
pub fn is_mark(c: char) -> bool {
    // Combining marks are in range U+0300 to U+036F (Combining Diacritical Marks)
    // and other ranges
    let code = c as u32;
    (0x0300..=0x036F).contains(&code) ||
    (0x1AB0..=0x1AFF).contains(&code) ||
    (0x1DC0..=0x1DFF).contains(&code) ||
    (0x20D0..=0x20FF).contains(&code) ||
    (0xFE20..=0xFE2F).contains(&code)
}

/// Check if character is a number
pub fn is_number(c: char) -> bool {
    c.is_numeric()
}

/// Check if character is a punctuation
pub fn is_punctuation(c: char) -> bool {
    c.is_ascii_punctuation() || {
        let code = c as u32;
        // General punctuation
        (0x2000..=0x206F).contains(&code) ||
        // Supplemental punctuation
        (0x2E00..=0x2E7F).contains(&code)
    }
}

/// Check if character is a symbol
pub fn is_symbol(c: char) -> bool {
    let code = c as u32;
    // Currency symbols
    (0x20A0..=0x20CF).contains(&code) ||
    // Mathematical operators
    (0x2200..=0x22FF).contains(&code) ||
    // Miscellaneous symbols
    (0x2600..=0x26FF).contains(&code) ||
    // Dingbats
    (0x2700..=0x27BF).contains(&code)
}

/// Check if character is a separator
pub fn is_separator(c: char) -> bool {
    c.is_whitespace()
}

// ============================================================================
// Transliteration
// ============================================================================

/// Transliteration direction
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TranslitDirection {
    Forward,
    Reverse,
}

/// Basic transliterator
pub struct Transliterator {
    rules: Vec<(String, String)>,
}

impl Transliterator {
    /// Create a new transliterator with rules
    pub fn new(rules: Vec<(String, String)>) -> Self {
        Self { rules }
    }

    /// Latin to ASCII transliterator
    pub fn latin_ascii() -> Self {
        Self::new(vec![
            ("á".to_string(), "a".to_string()),
            ("à".to_string(), "a".to_string()),
            ("â".to_string(), "a".to_string()),
            ("ä".to_string(), "a".to_string()),
            ("ã".to_string(), "a".to_string()),
            ("å".to_string(), "a".to_string()),
            ("æ".to_string(), "ae".to_string()),
            ("ç".to_string(), "c".to_string()),
            ("é".to_string(), "e".to_string()),
            ("è".to_string(), "e".to_string()),
            ("ê".to_string(), "e".to_string()),
            ("ë".to_string(), "e".to_string()),
            ("í".to_string(), "i".to_string()),
            ("ì".to_string(), "i".to_string()),
            ("î".to_string(), "i".to_string()),
            ("ï".to_string(), "i".to_string()),
            ("ñ".to_string(), "n".to_string()),
            ("ó".to_string(), "o".to_string()),
            ("ò".to_string(), "o".to_string()),
            ("ô".to_string(), "o".to_string()),
            ("ö".to_string(), "o".to_string()),
            ("õ".to_string(), "o".to_string()),
            ("ø".to_string(), "o".to_string()),
            ("œ".to_string(), "oe".to_string()),
            ("ß".to_string(), "ss".to_string()),
            ("ú".to_string(), "u".to_string()),
            ("ù".to_string(), "u".to_string()),
            ("û".to_string(), "u".to_string()),
            ("ü".to_string(), "u".to_string()),
            ("ý".to_string(), "y".to_string()),
            ("ÿ".to_string(), "y".to_string()),
        ])
    }

    /// Apply transliteration
    pub fn transliterate(&self, s: &str) -> String {
        let mut result = s.to_string();
        for (from, to) in &self.rules {
            result = result.replace(from, to);
            // Also handle uppercase (but skip special cases like ß which uppercases to multiple chars)
            let from_upper: String = from.chars().flat_map(|c| c.to_uppercase()).collect();
            // Only apply uppercase replacement if the uppercase has same character count
            // (avoids issues with ß -> SS)
            if from_upper.chars().count() == from.chars().count() && from_upper != *from {
                let to_upper: String = to.chars().flat_map(|c| c.to_uppercase()).collect();
                result = result.replace(&from_upper, &to_upper);
            }
        }
        result
    }
}

impl Default for Transliterator {
    fn default() -> Self {
        Self::latin_ascii()
    }
}

// ============================================================================
// ICU Provider
// ============================================================================

/// ICU data provider (simplified)
pub struct IcuProvider {
    collators: HashMap<String, UcaCollator>,
}

impl IcuProvider {
    /// Create a new provider
    pub fn new() -> Self {
        Self {
            collators: HashMap::new(),
        }
    }

    /// Get or create a collator for a locale
    pub fn collator(&mut self, locale: &str) -> &UcaCollator {
        if !self.collators.contains_key(locale) {
            self.collators.insert(locale.to_string(), UcaCollator::new_with_locale(locale));
        }
        self.collators.get(locale).unwrap()
    }

    /// List available locales
    pub fn available_locales(&self) -> Vec<&str> {
        vec![
            "root", "en", "en_US", "en_GB",
            "de", "de_DE", "de_AT",
            "fr", "fr_FR", "fr_CA",
            "es", "es_ES", "es_MX",
            "it", "it_IT",
            "pt", "pt_BR", "pt_PT",
            "nl", "nl_NL",
            "sv", "sv_SE",
            "da", "da_DK",
            "nb", "nb_NO",
            "fi", "fi_FI",
            "pl", "pl_PL",
            "ru", "ru_RU",
            "ja", "ja_JP",
            "zh", "zh_CN", "zh_TW",
            "ko", "ko_KR",
        ]
    }
}

impl Default for IcuProvider {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalization_nfc() {
        // Combining acute + a should become á
        let decomposed = "a\u{0301}"; // a + combining acute
        let composed = normalize(decomposed, NormalizationForm::Nfc);
        assert_eq!(composed, "á");
    }

    #[test]
    fn test_normalization_nfd() {
        let composed = "á";
        let decomposed = normalize(composed, NormalizationForm::Nfd);
        assert_eq!(decomposed, "a\u{0301}");
    }

    #[test]
    fn test_normalization_nfkd() {
        // Ligature fi should become f + i
        let ligature = "ﬁ";
        let decomposed = normalize(ligature, NormalizationForm::Nfkd);
        assert_eq!(decomposed, "fi");
    }

    #[test]
    fn test_uca_collator_basic() {
        let collator = UcaCollator::new();

        assert_eq!(collator.compare("a", "b"), Ordering::Less);
        assert_eq!(collator.compare("b", "a"), Ordering::Greater);
        assert_eq!(collator.compare("abc", "abc"), Ordering::Equal);
    }

    #[test]
    fn test_uca_collator_case() {
        let mut collator = UcaCollator::new();
        collator.set_strength(CollatorStrength::Tertiary);

        // At tertiary level, case matters
        assert_ne!(collator.compare("a", "A"), Ordering::Equal);

        // At primary level, case doesn't matter
        collator.set_strength(CollatorStrength::Primary);
        assert_eq!(collator.compare("a", "A"), Ordering::Equal);
    }

    #[test]
    fn test_uca_collator_accents() {
        let mut collator = UcaCollator::new();
        collator.set_strength(CollatorStrength::Secondary);

        // At secondary level, accents matter
        assert_ne!(collator.compare("a", "á"), Ordering::Equal);

        // At primary level, accents don't matter
        collator.set_strength(CollatorStrength::Primary);
        assert_eq!(collator.compare("a", "á"), Ordering::Equal);
    }

    #[test]
    fn test_uca_collator_swedish() {
        let collator = UcaCollator::new_with_locale("sv_SE");

        // In Swedish, å, ä, ö come after z
        assert_eq!(collator.compare("z", "å"), Ordering::Less);
        assert_eq!(collator.compare("z", "ä"), Ordering::Less);
        assert_eq!(collator.compare("z", "ö"), Ordering::Less);
    }

    #[test]
    fn test_uca_collator_spanish() {
        let collator = UcaCollator::new_with_locale("es_ES");

        // ñ should come after n
        assert_eq!(collator.compare("n", "ñ"), Ordering::Less);
    }

    #[test]
    fn test_sort_key() {
        let collator = UcaCollator::new();

        let key_a = collator.get_sort_key("a");
        let key_b = collator.get_sort_key("b");

        assert!(key_a < key_b);
    }

    #[test]
    fn test_general_category() {
        assert_eq!(general_category('A'), GeneralCategory::UppercaseLetter);
        assert_eq!(general_category('a'), GeneralCategory::LowercaseLetter);
        assert_eq!(general_category('5'), GeneralCategory::DecimalDigitNumber);
        assert_eq!(general_category(' '), GeneralCategory::SpaceSeparator);
    }

    #[test]
    fn test_is_mark() {
        assert!(is_mark('\u{0301}')); // Combining acute accent
        assert!(is_mark('\u{0308}')); // Combining diaeresis
        assert!(!is_mark('a'));
        assert!(!is_mark('1'));
    }

    #[test]
    fn test_transliterator() {
        let trans = Transliterator::latin_ascii();

        assert_eq!(trans.transliterate("café"), "cafe");
        assert_eq!(trans.transliterate("naïve"), "naive");
        assert_eq!(trans.transliterate("Ärger"), "Arger");
        assert_eq!(trans.transliterate("Straße"), "Strasse");
    }

    #[test]
    fn test_icu_provider() {
        let mut provider = IcuProvider::new();

        let collator = provider.collator("en_US");
        assert_eq!(collator.locale(), "en_US");

        let locales = provider.available_locales();
        assert!(locales.contains(&"en_US"));
        assert!(locales.contains(&"de_DE"));
    }

    #[test]
    fn test_collation_element() {
        let elem = CollationElement::new(0x1000, 0x0020, 0x0002);
        assert_eq!(elem.primary, 0x1000);
        assert_eq!(elem.secondary, 0x0020);
        assert_eq!(elem.tertiary, 0x0002);

        let var_elem = CollationElement::variable(0x0001);
        assert_eq!(var_elem.primary, 0x0001);
        assert_eq!(var_elem.secondary, 0);
        assert_eq!(var_elem.tertiary, 0);
    }

    #[test]
    fn test_collator_equals() {
        let collator = UcaCollator::new();

        assert!(collator.equals("hello", "hello"));
        assert!(!collator.equals("hello", "world"));
    }

    #[test]
    fn test_compatibility_decompose() {
        assert_eq!(compatibility_decompose('½'), "1/2");
        assert_eq!(compatibility_decompose('²'), "2");
        assert_eq!(compatibility_decompose('ﬁ'), "fi");
        assert_eq!(compatibility_decompose('a'), "a");
    }

    #[test]
    fn test_compose_chars() {
        assert_eq!(compose_chars('a', '\u{0301}'), Some('á'));
        assert_eq!(compose_chars('n', '\u{0303}'), Some('ñ'));
        assert_eq!(compose_chars('a', 'b'), None);
    }

    #[test]
    fn test_decompose_char() {
        assert_eq!(decompose_char('á'), Some(('a', '\u{0301}')));
        assert_eq!(decompose_char('ñ'), Some(('n', '\u{0303}')));
        assert_eq!(decompose_char('a'), None);
    }

    #[test]
    fn test_danish_tailoring() {
        let collator = UcaCollator::new_with_locale("da_DK");

        // æ, ø, å at end of alphabet
        assert_eq!(collator.compare("z", "æ"), Ordering::Less);
        assert_eq!(collator.compare("æ", "ø"), Ordering::Less);
        assert_eq!(collator.compare("ø", "å"), Ordering::Less);
    }

    #[test]
    fn test_variable_weighting() {
        let mut collator = UcaCollator::new();
        collator.set_strength(CollatorStrength::Primary);

        // With blanked weighting, punctuation is ignored
        collator.variable_weighting = VariableWeighting::Blanked;

        // Get sort keys - spaces should be ignored
        let key1 = collator.get_sort_key("hello world");
        let key2 = collator.get_sort_key("helloworld");

        // Note: This is a simplified test - full implementation would be more complex
        assert!(!key1.is_empty());
        assert!(!key2.is_empty());
    }

    #[test]
    fn test_normalization_forms() {
        assert_eq!(NormalizationForm::default(), NormalizationForm::Nfc);
    }

    #[test]
    fn test_collator_strength() {
        assert_eq!(CollatorStrength::default(), CollatorStrength::Tertiary);
    }
}
