//! Range Types
//!
//! PostgreSQL-compatible range types for representing ranges of values.
//! Supports int4range, int8range, numrange, tsrange, tstzrange, daterange.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

/// Range bound type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RangeBound<T> {
    /// Unbounded (extends to infinity)
    Unbounded,
    /// Inclusive bound [value
    Inclusive(T),
    /// Exclusive bound (value
    Exclusive(T),
}

impl<T: Clone> RangeBound<T> {
    pub fn value(&self) -> Option<&T> {
        match self {
            RangeBound::Unbounded => None,
            RangeBound::Inclusive(v) | RangeBound::Exclusive(v) => Some(v),
        }
    }

    pub fn is_inclusive(&self) -> bool {
        matches!(self, RangeBound::Inclusive(_))
    }

    pub fn is_unbounded(&self) -> bool {
        matches!(self, RangeBound::Unbounded)
    }
}

/// Generic range type
#[derive(Debug, Clone)]
pub struct Range<T> {
    /// Lower bound
    pub lower: RangeBound<T>,
    /// Upper bound
    pub upper: RangeBound<T>,
    /// Whether range is empty
    pub is_empty: bool,
}

impl<T: Clone + Ord> Range<T> {
    /// Create a new range
    pub fn new(lower: RangeBound<T>, upper: RangeBound<T>) -> Self {
        let is_empty = Self::check_empty(&lower, &upper);
        Self { lower, upper, is_empty }
    }

    /// Create an empty range
    pub fn empty() -> Self {
        Self {
            lower: RangeBound::Unbounded,
            upper: RangeBound::Unbounded,
            is_empty: true,
        }
    }

    /// Create inclusive range [lower, upper]
    pub fn inclusive(lower: T, upper: T) -> Self {
        Self::new(RangeBound::Inclusive(lower), RangeBound::Inclusive(upper))
    }

    /// Create exclusive range (lower, upper)
    pub fn exclusive(lower: T, upper: T) -> Self {
        Self::new(RangeBound::Exclusive(lower), RangeBound::Exclusive(upper))
    }

    /// Create half-open range [lower, upper)
    pub fn half_open(lower: T, upper: T) -> Self {
        Self::new(RangeBound::Inclusive(lower), RangeBound::Exclusive(upper))
    }

    /// Create range from lower bound to infinity
    pub fn from(lower: T, inclusive: bool) -> Self {
        let bound = if inclusive {
            RangeBound::Inclusive(lower)
        } else {
            RangeBound::Exclusive(lower)
        };
        Self::new(bound, RangeBound::Unbounded)
    }

    /// Create range from negative infinity to upper bound
    pub fn to(upper: T, inclusive: bool) -> Self {
        let bound = if inclusive {
            RangeBound::Inclusive(upper)
        } else {
            RangeBound::Exclusive(upper)
        };
        Self::new(RangeBound::Unbounded, bound)
    }

    /// Check if range is empty
    fn check_empty(lower: &RangeBound<T>, upper: &RangeBound<T>) -> bool {
        match (lower, upper) {
            (RangeBound::Unbounded, _) | (_, RangeBound::Unbounded) => false,
            (RangeBound::Inclusive(l), RangeBound::Inclusive(u)) => l > u,
            (RangeBound::Inclusive(l), RangeBound::Exclusive(u)) => l >= u,
            (RangeBound::Exclusive(l), RangeBound::Inclusive(u)) => l >= u,
            (RangeBound::Exclusive(l), RangeBound::Exclusive(u)) => l >= u,
        }
    }

    /// Check if value is contained in range
    pub fn contains(&self, value: &T) -> bool {
        if self.is_empty {
            return false;
        }

        let lower_ok = match &self.lower {
            RangeBound::Unbounded => true,
            RangeBound::Inclusive(l) => value >= l,
            RangeBound::Exclusive(l) => value > l,
        };

        let upper_ok = match &self.upper {
            RangeBound::Unbounded => true,
            RangeBound::Inclusive(u) => value <= u,
            RangeBound::Exclusive(u) => value < u,
        };

        lower_ok && upper_ok
    }

    /// Check if range contains another range
    pub fn contains_range(&self, other: &Range<T>) -> bool {
        if self.is_empty || other.is_empty {
            return other.is_empty;
        }

        let lower_ok = match (&self.lower, &other.lower) {
            (RangeBound::Unbounded, _) => true,
            (_, RangeBound::Unbounded) => false,
            (RangeBound::Inclusive(l1), RangeBound::Inclusive(l2)) => l1 <= l2,
            (RangeBound::Inclusive(l1), RangeBound::Exclusive(l2)) => l1 <= l2,
            (RangeBound::Exclusive(l1), RangeBound::Inclusive(l2)) => l1 < l2,
            (RangeBound::Exclusive(l1), RangeBound::Exclusive(l2)) => l1 <= l2,
        };

        let upper_ok = match (&self.upper, &other.upper) {
            (RangeBound::Unbounded, _) => true,
            (_, RangeBound::Unbounded) => false,
            (RangeBound::Inclusive(u1), RangeBound::Inclusive(u2)) => u1 >= u2,
            (RangeBound::Inclusive(u1), RangeBound::Exclusive(u2)) => u1 >= u2,
            (RangeBound::Exclusive(u1), RangeBound::Inclusive(u2)) => u1 > u2,
            (RangeBound::Exclusive(u1), RangeBound::Exclusive(u2)) => u1 >= u2,
        };

        lower_ok && upper_ok
    }

    /// Check if ranges overlap
    pub fn overlaps(&self, other: &Range<T>) -> bool {
        if self.is_empty || other.is_empty {
            return false;
        }

        // Check if self.lower < other.upper AND other.lower < self.upper
        let self_lower_before_other_upper = match (&self.lower, &other.upper) {
            (RangeBound::Unbounded, _) | (_, RangeBound::Unbounded) => true,
            (RangeBound::Inclusive(l), RangeBound::Inclusive(u)) => l <= u,
            (RangeBound::Inclusive(l), RangeBound::Exclusive(u)) => l < u,
            (RangeBound::Exclusive(l), RangeBound::Inclusive(u)) => l < u,
            (RangeBound::Exclusive(l), RangeBound::Exclusive(u)) => l < u,
        };

        let other_lower_before_self_upper = match (&other.lower, &self.upper) {
            (RangeBound::Unbounded, _) | (_, RangeBound::Unbounded) => true,
            (RangeBound::Inclusive(l), RangeBound::Inclusive(u)) => l <= u,
            (RangeBound::Inclusive(l), RangeBound::Exclusive(u)) => l < u,
            (RangeBound::Exclusive(l), RangeBound::Inclusive(u)) => l < u,
            (RangeBound::Exclusive(l), RangeBound::Exclusive(u)) => l < u,
        };

        self_lower_before_other_upper && other_lower_before_self_upper
    }

    /// Check if ranges are adjacent (share a bound)
    pub fn adjacent(&self, other: &Range<T>) -> bool
    where
        T: PartialEq,
    {
        if self.is_empty || other.is_empty {
            return false;
        }

        // Check if self.upper touches other.lower or other.upper touches self.lower
        let touch1 = match (&self.upper, &other.lower) {
            (RangeBound::Inclusive(u), RangeBound::Exclusive(l)) => u == l,
            (RangeBound::Exclusive(u), RangeBound::Inclusive(l)) => u == l,
            _ => false,
        };

        let touch2 = match (&other.upper, &self.lower) {
            (RangeBound::Inclusive(u), RangeBound::Exclusive(l)) => u == l,
            (RangeBound::Exclusive(u), RangeBound::Inclusive(l)) => u == l,
            _ => false,
        };

        touch1 || touch2
    }

    /// Compute union of two ranges (only if they overlap or are adjacent)
    pub fn union(&self, other: &Range<T>) -> Option<Range<T>>
    where
        T: PartialEq,
    {
        if self.is_empty {
            return Some(other.clone());
        }
        if other.is_empty {
            return Some(self.clone());
        }

        if !self.overlaps(other) && !self.adjacent(other) {
            return None;
        }

        let lower = Self::min_lower(&self.lower, &other.lower);
        let upper = Self::max_upper(&self.upper, &other.upper);

        Some(Range::new(lower, upper))
    }

    /// Compute intersection of two ranges
    pub fn intersection(&self, other: &Range<T>) -> Range<T> {
        if self.is_empty || other.is_empty || !self.overlaps(other) {
            return Range::empty();
        }

        let lower = Self::max_lower(&self.lower, &other.lower);
        let upper = Self::min_upper(&self.upper, &other.upper);

        Range::new(lower, upper)
    }

    /// Compute difference self - other
    pub fn difference(&self, other: &Range<T>) -> Vec<Range<T>>
    where
        T: PartialEq,
    {
        if self.is_empty || !self.overlaps(other) {
            return vec![self.clone()];
        }

        if other.contains_range(self) {
            return vec![];
        }

        let mut result = Vec::new();

        // Left part
        let self_before_other = match (&self.lower, &other.lower) {
            (RangeBound::Unbounded, RangeBound::Unbounded) => false,
            (_, RangeBound::Unbounded) => false,
            (RangeBound::Unbounded, _) => true,
            (RangeBound::Inclusive(l1), RangeBound::Inclusive(l2)) => l1 < l2,
            (RangeBound::Inclusive(l1), RangeBound::Exclusive(l2)) => l1 <= l2,
            (RangeBound::Exclusive(l1), RangeBound::Inclusive(l2)) => l1 < l2,
            (RangeBound::Exclusive(l1), RangeBound::Exclusive(l2)) => l1 < l2,
        };

        if self_before_other {
            let new_upper = match &other.lower {
                RangeBound::Inclusive(v) => RangeBound::Exclusive(v.clone()),
                RangeBound::Exclusive(v) => RangeBound::Inclusive(v.clone()),
                RangeBound::Unbounded => RangeBound::Unbounded,
            };
            result.push(Range::new(self.lower.clone(), new_upper));
        }

        // Right part
        let self_after_other = match (&self.upper, &other.upper) {
            (RangeBound::Unbounded, RangeBound::Unbounded) => false,
            (RangeBound::Unbounded, _) => true,
            (_, RangeBound::Unbounded) => false,
            (RangeBound::Inclusive(u1), RangeBound::Inclusive(u2)) => u1 > u2,
            (RangeBound::Inclusive(u1), RangeBound::Exclusive(u2)) => u1 >= u2,
            (RangeBound::Exclusive(u1), RangeBound::Inclusive(u2)) => u1 > u2,
            (RangeBound::Exclusive(u1), RangeBound::Exclusive(u2)) => u1 > u2,
        };

        if self_after_other {
            let new_lower = match &other.upper {
                RangeBound::Inclusive(v) => RangeBound::Exclusive(v.clone()),
                RangeBound::Exclusive(v) => RangeBound::Inclusive(v.clone()),
                RangeBound::Unbounded => RangeBound::Unbounded,
            };
            result.push(Range::new(new_lower, self.upper.clone()));
        }

        result
    }

    fn min_lower(a: &RangeBound<T>, b: &RangeBound<T>) -> RangeBound<T> {
        match (a, b) {
            (RangeBound::Unbounded, _) | (_, RangeBound::Unbounded) => RangeBound::Unbounded,
            (RangeBound::Inclusive(v1), RangeBound::Inclusive(v2)) => {
                if v1 <= v2 { RangeBound::Inclusive(v1.clone()) } else { RangeBound::Inclusive(v2.clone()) }
            }
            (RangeBound::Inclusive(v1), RangeBound::Exclusive(v2)) => {
                if v1 <= v2 { RangeBound::Inclusive(v1.clone()) } else { RangeBound::Exclusive(v2.clone()) }
            }
            (RangeBound::Exclusive(v1), RangeBound::Inclusive(v2)) => {
                if v1 < v2 { RangeBound::Exclusive(v1.clone()) } else { RangeBound::Inclusive(v2.clone()) }
            }
            (RangeBound::Exclusive(v1), RangeBound::Exclusive(v2)) => {
                if v1 <= v2 { RangeBound::Exclusive(v1.clone()) } else { RangeBound::Exclusive(v2.clone()) }
            }
        }
    }

    fn max_lower(a: &RangeBound<T>, b: &RangeBound<T>) -> RangeBound<T> {
        match (a, b) {
            (RangeBound::Unbounded, other) | (other, RangeBound::Unbounded) => other.clone(),
            (RangeBound::Inclusive(v1), RangeBound::Inclusive(v2)) => {
                if v1 >= v2 { RangeBound::Inclusive(v1.clone()) } else { RangeBound::Inclusive(v2.clone()) }
            }
            (RangeBound::Inclusive(v1), RangeBound::Exclusive(v2)) => {
                if v1 > v2 { RangeBound::Inclusive(v1.clone()) } else { RangeBound::Exclusive(v2.clone()) }
            }
            (RangeBound::Exclusive(v1), RangeBound::Inclusive(v2)) => {
                if v1 >= v2 { RangeBound::Exclusive(v1.clone()) } else { RangeBound::Inclusive(v2.clone()) }
            }
            (RangeBound::Exclusive(v1), RangeBound::Exclusive(v2)) => {
                if v1 >= v2 { RangeBound::Exclusive(v1.clone()) } else { RangeBound::Exclusive(v2.clone()) }
            }
        }
    }

    fn max_upper(a: &RangeBound<T>, b: &RangeBound<T>) -> RangeBound<T> {
        match (a, b) {
            (RangeBound::Unbounded, _) | (_, RangeBound::Unbounded) => RangeBound::Unbounded,
            (RangeBound::Inclusive(v1), RangeBound::Inclusive(v2)) => {
                if v1 >= v2 { RangeBound::Inclusive(v1.clone()) } else { RangeBound::Inclusive(v2.clone()) }
            }
            (RangeBound::Inclusive(v1), RangeBound::Exclusive(v2)) => {
                if v1 >= v2 { RangeBound::Inclusive(v1.clone()) } else { RangeBound::Exclusive(v2.clone()) }
            }
            (RangeBound::Exclusive(v1), RangeBound::Inclusive(v2)) => {
                if v1 > v2 { RangeBound::Exclusive(v1.clone()) } else { RangeBound::Inclusive(v2.clone()) }
            }
            (RangeBound::Exclusive(v1), RangeBound::Exclusive(v2)) => {
                if v1 >= v2 { RangeBound::Exclusive(v1.clone()) } else { RangeBound::Exclusive(v2.clone()) }
            }
        }
    }

    fn min_upper(a: &RangeBound<T>, b: &RangeBound<T>) -> RangeBound<T> {
        match (a, b) {
            (RangeBound::Unbounded, other) | (other, RangeBound::Unbounded) => other.clone(),
            (RangeBound::Inclusive(v1), RangeBound::Inclusive(v2)) => {
                if v1 <= v2 { RangeBound::Inclusive(v1.clone()) } else { RangeBound::Inclusive(v2.clone()) }
            }
            (RangeBound::Inclusive(v1), RangeBound::Exclusive(v2)) => {
                if v1 < v2 { RangeBound::Inclusive(v1.clone()) } else { RangeBound::Exclusive(v2.clone()) }
            }
            (RangeBound::Exclusive(v1), RangeBound::Inclusive(v2)) => {
                if v1 <= v2 { RangeBound::Exclusive(v1.clone()) } else { RangeBound::Inclusive(v2.clone()) }
            }
            (RangeBound::Exclusive(v1), RangeBound::Exclusive(v2)) => {
                if v1 <= v2 { RangeBound::Exclusive(v1.clone()) } else { RangeBound::Exclusive(v2.clone()) }
            }
        }
    }
}

impl<T: Clone + Ord + PartialEq> PartialEq for Range<T> {
    fn eq(&self, other: &Self) -> bool {
        if self.is_empty && other.is_empty {
            return true;
        }
        self.lower == other.lower && self.upper == other.upper
    }
}

impl<T: Clone + Ord + Eq> Eq for Range<T> {}

impl<T: Clone + Ord + Hash> Hash for Range<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.is_empty.hash(state);
        if !self.is_empty {
            self.lower.hash(state);
            self.upper.hash(state);
        }
    }
}

impl<T: fmt::Display + Clone> fmt::Display for Range<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty {
            return write!(f, "empty");
        }

        let lower = match &self.lower {
            RangeBound::Unbounded => "(".to_string(),
            RangeBound::Inclusive(v) => format!("[{}", v),
            RangeBound::Exclusive(v) => format!("({}", v),
        };

        let upper = match &self.upper {
            RangeBound::Unbounded => ")".to_string(),
            RangeBound::Inclusive(v) => format!("{}]", v),
            RangeBound::Exclusive(v) => format!("{})", v),
        };

        write!(f, "{},{}", lower, upper)
    }
}

// Type aliases for common range types
pub type Int4Range = Range<i32>;
pub type Int8Range = Range<i64>;
pub type NumRange = Range<f64>;

// Timestamp range (using i64 for microseconds since epoch)
pub type TsRange = Range<i64>;
pub type TsTzRange = Range<i64>;

// Date range (using i32 for days since epoch)
pub type DateRange = Range<i32>;

/// Parse range from string
pub fn parse_range<T: std::str::FromStr + Clone + Ord>(s: &str) -> Result<Range<T>, RangeParseError> {
    let s = s.trim();
    
    if s == "empty" {
        return Ok(Range::empty());
    }

    if s.len() < 3 {
        return Err(RangeParseError::InvalidFormat);
    }

    let lower_inclusive = match s.chars().next() {
        Some('[') => true,
        Some('(') => false,
        _ => return Err(RangeParseError::InvalidFormat),
    };

    let upper_inclusive = match s.chars().last() {
        Some(']') => true,
        Some(')') => false,
        _ => return Err(RangeParseError::InvalidFormat),
    };

    let inner = &s[1..s.len()-1];
    let parts: Vec<&str> = inner.split(',').collect();
    
    if parts.len() != 2 {
        return Err(RangeParseError::InvalidFormat);
    }

    let lower = if parts[0].trim().is_empty() {
        RangeBound::Unbounded
    } else {
        let val = parts[0].trim().parse::<T>()
            .map_err(|_| RangeParseError::InvalidValue)?;
        if lower_inclusive {
            RangeBound::Inclusive(val)
        } else {
            RangeBound::Exclusive(val)
        }
    };

    let upper = if parts[1].trim().is_empty() {
        RangeBound::Unbounded
    } else {
        let val = parts[1].trim().parse::<T>()
            .map_err(|_| RangeParseError::InvalidValue)?;
        if upper_inclusive {
            RangeBound::Inclusive(val)
        } else {
            RangeBound::Exclusive(val)
        }
    };

    Ok(Range::new(lower, upper))
}

#[derive(Debug, Clone)]
pub enum RangeParseError {
    InvalidFormat,
    InvalidValue,
}

impl fmt::Display for RangeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFormat => write!(f, "Invalid range format"),
            Self::InvalidValue => write!(f, "Invalid range value"),
        }
    }
}

impl std::error::Error for RangeParseError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_contains() {
        let range = Range::inclusive(1i32, 10);
        assert!(range.contains(&5));
        assert!(range.contains(&1));
        assert!(range.contains(&10));
        assert!(!range.contains(&0));
        assert!(!range.contains(&11));
    }

    #[test]
    fn test_range_half_open() {
        let range = Range::half_open(1i32, 10);
        assert!(range.contains(&1));
        assert!(range.contains(&9));
        assert!(!range.contains(&10));
    }

    #[test]
    fn test_range_overlaps() {
        let r1 = Range::inclusive(1i32, 5);
        let r2 = Range::inclusive(3i32, 8);
        let r3 = Range::inclusive(6i32, 10);

        assert!(r1.overlaps(&r2));
        assert!(r2.overlaps(&r3));
        assert!(!r1.overlaps(&r3));
    }

    #[test]
    fn test_range_contains_range() {
        let r1 = Range::inclusive(1i32, 10);
        let r2 = Range::inclusive(3i32, 7);
        let r3 = Range::inclusive(5i32, 15);

        assert!(r1.contains_range(&r2));
        assert!(!r1.contains_range(&r3));
        assert!(!r2.contains_range(&r1));
    }

    #[test]
    fn test_range_union() {
        let r1 = Range::inclusive(1i32, 5);
        let r2 = Range::inclusive(3i32, 8);
        
        let union = r1.union(&r2).unwrap();
        assert!(union.contains(&1));
        assert!(union.contains(&8));
    }

    #[test]
    fn test_range_intersection() {
        let r1 = Range::inclusive(1i32, 5);
        let r2 = Range::inclusive(3i32, 8);
        
        let intersection = r1.intersection(&r2);
        assert!(!intersection.contains(&2));
        assert!(intersection.contains(&3));
        assert!(intersection.contains(&5));
        assert!(!intersection.contains(&6));
    }

    #[test]
    fn test_parse_range() {
        let range: Int4Range = parse_range("[1,10]").unwrap();
        assert!(range.contains(&5));
        
        let range: Int4Range = parse_range("(1,10)").unwrap();
        assert!(!range.contains(&1));
        assert!(!range.contains(&10));
        
        let range: Int4Range = parse_range("[,10)").unwrap();
        assert!(range.contains(&-100));
        assert!(!range.contains(&10));
    }

    #[test]
    fn test_range_display() {
        let range = Range::inclusive(1i32, 10);
        assert_eq!(format!("{}", range), "[1,10]");
        
        let range = Range::half_open(1i32, 10);
        assert_eq!(format!("{}", range), "[1,10)");
        
        let range: Int4Range = Range::empty();
        assert_eq!(format!("{}", range), "empty");
    }
}
