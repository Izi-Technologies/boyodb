// Range Types - Date/numeric ranges with operators for BoyoDB
//
// Provides PostgreSQL-style range type support:
// - Integer ranges (int4range, int8range)
// - Numeric ranges (numrange)
// - Date/time ranges (daterange, tsrange, tstzrange)
// - Range operators (contains, overlaps, adjacent)
// - Range functions (lower, upper, isempty)
// - GiST index support for range queries

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

// ============================================================================
// Range Bounds
// ============================================================================

/// Bound type for range endpoints
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BoundType {
    /// Inclusive bound [
    Inclusive,
    /// Exclusive bound (
    Exclusive,
}

impl BoundType {
    pub fn is_inclusive(&self) -> bool {
        matches!(self, BoundType::Inclusive)
    }

    pub fn is_exclusive(&self) -> bool {
        matches!(self, BoundType::Exclusive)
    }

    pub fn symbol_lower(&self) -> char {
        match self {
            BoundType::Inclusive => '[',
            BoundType::Exclusive => '(',
        }
    }

    pub fn symbol_upper(&self) -> char {
        match self {
            BoundType::Inclusive => ']',
            BoundType::Exclusive => ')',
        }
    }
}

/// A bound (endpoint) of a range
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bound<T> {
    /// Unbounded (infinite)
    Unbounded,
    /// Bounded with a value and type
    Bounded(T, BoundType),
}

impl<T: Clone> Bound<T> {
    pub fn inclusive(value: T) -> Self {
        Bound::Bounded(value, BoundType::Inclusive)
    }

    pub fn exclusive(value: T) -> Self {
        Bound::Bounded(value, BoundType::Exclusive)
    }

    pub fn is_unbounded(&self) -> bool {
        matches!(self, Bound::Unbounded)
    }

    pub fn value(&self) -> Option<&T> {
        match self {
            Bound::Unbounded => None,
            Bound::Bounded(v, _) => Some(v),
        }
    }

    pub fn bound_type(&self) -> Option<BoundType> {
        match self {
            Bound::Unbounded => None,
            Bound::Bounded(_, bt) => Some(*bt),
        }
    }
}

impl<T: Hash> Hash for Bound<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Bound::Unbounded => 0u8.hash(state),
            Bound::Bounded(v, bt) => {
                1u8.hash(state);
                v.hash(state);
                bt.hash(state);
            }
        }
    }
}

// ============================================================================
// Generic Range Type
// ============================================================================

/// A generic range type
#[derive(Debug, Clone)]
pub struct Range<T> {
    /// Lower bound
    pub lower: Bound<T>,
    /// Upper bound
    pub upper: Bound<T>,
    /// Is empty range
    pub is_empty: bool,
}

impl<T: Clone + PartialOrd> Range<T> {
    /// Create a new range
    pub fn new(lower: Bound<T>, upper: Bound<T>) -> Self {
        let is_empty = Self::check_empty(&lower, &upper);
        Self { lower, upper, is_empty }
    }

    /// Create an empty range
    pub fn empty() -> Self {
        Self {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
            is_empty: true,
        }
    }

    /// Create a range [lower, upper]
    pub fn closed(lower: T, upper: T) -> Self {
        Self::new(
            Bound::inclusive(lower),
            Bound::inclusive(upper),
        )
    }

    /// Create a range (lower, upper)
    pub fn open(lower: T, upper: T) -> Self {
        Self::new(
            Bound::exclusive(lower),
            Bound::exclusive(upper),
        )
    }

    /// Create a range [lower, upper)
    pub fn closed_open(lower: T, upper: T) -> Self {
        Self::new(
            Bound::inclusive(lower),
            Bound::exclusive(upper),
        )
    }

    /// Create a range (lower, upper]
    pub fn open_closed(lower: T, upper: T) -> Self {
        Self::new(
            Bound::exclusive(lower),
            Bound::inclusive(upper),
        )
    }

    /// Create a range [lower, infinity)
    pub fn at_least(lower: T) -> Self {
        Self::new(
            Bound::inclusive(lower),
            Bound::Unbounded,
        )
    }

    /// Create a range (lower, infinity)
    pub fn greater_than(lower: T) -> Self {
        Self::new(
            Bound::exclusive(lower),
            Bound::Unbounded,
        )
    }

    /// Create a range (-infinity, upper]
    pub fn at_most(upper: T) -> Self {
        Self::new(
            Bound::Unbounded,
            Bound::inclusive(upper),
        )
    }

    /// Create a range (-infinity, upper)
    pub fn less_than(upper: T) -> Self {
        Self::new(
            Bound::Unbounded,
            Bound::exclusive(upper),
        )
    }

    /// Create a range (-infinity, infinity)
    pub fn all() -> Self {
        Self::new(Bound::Unbounded, Bound::Unbounded)
    }

    /// Check if range is empty
    fn check_empty(lower: &Bound<T>, upper: &Bound<T>) -> bool {
        match (lower, upper) {
            (Bound::Bounded(l, lt), Bound::Bounded(u, ut)) => {
                match l.partial_cmp(u) {
                    Some(Ordering::Greater) => true,
                    Some(Ordering::Equal) => {
                        // [x, x] is not empty, but (x, x], [x, x), (x, x) are empty
                        !lt.is_inclusive() || !ut.is_inclusive()
                    }
                    _ => false,
                }
            }
            _ => false, // Unbounded ranges are never empty
        }
    }

    /// Get the lower bound value
    pub fn lower_val(&self) -> Option<&T> {
        self.lower.value()
    }

    /// Get the upper bound value
    pub fn upper_val(&self) -> Option<&T> {
        self.upper.value()
    }

    /// Check if lower bound is inclusive
    pub fn lower_inc(&self) -> bool {
        self.lower.bound_type().map(|bt| bt.is_inclusive()).unwrap_or(false)
    }

    /// Check if upper bound is inclusive
    pub fn upper_inc(&self) -> bool {
        self.upper.bound_type().map(|bt| bt.is_inclusive()).unwrap_or(false)
    }

    /// Check if lower bound is infinite
    pub fn lower_inf(&self) -> bool {
        self.lower.is_unbounded()
    }

    /// Check if upper bound is infinite
    pub fn upper_inf(&self) -> bool {
        self.upper.is_unbounded()
    }
}

impl<T: Clone + PartialOrd> Range<T> {
    /// Check if range contains a value
    pub fn contains_value(&self, value: &T) -> bool {
        if self.is_empty {
            return false;
        }

        let lower_ok = match &self.lower {
            Bound::Unbounded => true,
            Bound::Bounded(l, BoundType::Inclusive) => value >= l,
            Bound::Bounded(l, BoundType::Exclusive) => value > l,
        };

        let upper_ok = match &self.upper {
            Bound::Unbounded => true,
            Bound::Bounded(u, BoundType::Inclusive) => value <= u,
            Bound::Bounded(u, BoundType::Exclusive) => value < u,
        };

        lower_ok && upper_ok
    }

    /// Check if this range contains another range (@>)
    pub fn contains_range(&self, other: &Range<T>) -> bool {
        if self.is_empty || other.is_empty {
            return other.is_empty;
        }

        // Check lower bound
        let lower_ok = match (&self.lower, &other.lower) {
            (Bound::Unbounded, _) => true,
            (_, Bound::Unbounded) => false,
            (Bound::Bounded(sl, slt), Bound::Bounded(ol, olt)) => {
                match sl.partial_cmp(ol) {
                    Some(Ordering::Less) => true,
                    Some(Ordering::Equal) => slt.is_inclusive() || olt.is_exclusive(),
                    _ => false,
                }
            }
        };

        // Check upper bound
        let upper_ok = match (&self.upper, &other.upper) {
            (Bound::Unbounded, _) => true,
            (_, Bound::Unbounded) => false,
            (Bound::Bounded(su, sut), Bound::Bounded(ou, out)) => {
                match su.partial_cmp(ou) {
                    Some(Ordering::Greater) => true,
                    Some(Ordering::Equal) => sut.is_inclusive() || out.is_exclusive(),
                    _ => false,
                }
            }
        };

        lower_ok && upper_ok
    }

    /// Check if this range is contained by another range (<@)
    pub fn contained_by(&self, other: &Range<T>) -> bool {
        other.contains_range(self)
    }

    /// Check if ranges overlap (&&)
    pub fn overlaps(&self, other: &Range<T>) -> bool {
        if self.is_empty || other.is_empty {
            return false;
        }

        // Check if self's lower is before other's upper
        let self_lower_before_other_upper = match (&self.lower, &other.upper) {
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
            (Bound::Bounded(sl, slt), Bound::Bounded(ou, out)) => {
                match sl.partial_cmp(ou) {
                    Some(Ordering::Less) => true,
                    Some(Ordering::Equal) => slt.is_inclusive() && out.is_inclusive(),
                    _ => false,
                }
            }
        };

        // Check if other's lower is before self's upper
        let other_lower_before_self_upper = match (&other.lower, &self.upper) {
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
            (Bound::Bounded(ol, olt), Bound::Bounded(su, sut)) => {
                match ol.partial_cmp(su) {
                    Some(Ordering::Less) => true,
                    Some(Ordering::Equal) => olt.is_inclusive() && sut.is_inclusive(),
                    _ => false,
                }
            }
        };

        self_lower_before_other_upper && other_lower_before_self_upper
    }

    /// Check if ranges are strictly left of (-<<)
    pub fn strictly_left_of(&self, other: &Range<T>) -> bool {
        if self.is_empty || other.is_empty {
            return false;
        }

        match (&self.upper, &other.lower) {
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => false,
            (Bound::Bounded(su, _), Bound::Bounded(ol, _)) => su < ol,
        }
    }

    /// Check if ranges are strictly right of (>>-)
    pub fn strictly_right_of(&self, other: &Range<T>) -> bool {
        other.strictly_left_of(self)
    }

    /// Check if ranges are adjacent (-|-)
    pub fn adjacent_to(&self, other: &Range<T>) -> bool {
        if self.is_empty || other.is_empty {
            return false;
        }

        // Check if self's upper is adjacent to other's lower
        let upper_adjacent = match (&self.upper, &other.lower) {
            (Bound::Bounded(su, sut), Bound::Bounded(ol, olt)) => {
                su == ol && (sut.is_exclusive() != olt.is_exclusive())
            }
            _ => false,
        };

        // Check if other's upper is adjacent to self's lower
        let lower_adjacent = match (&other.upper, &self.lower) {
            (Bound::Bounded(ou, out), Bound::Bounded(sl, slt)) => {
                ou == sl && (out.is_exclusive() != slt.is_exclusive())
            }
            _ => false,
        };

        upper_adjacent || lower_adjacent
    }

    /// Union of two ranges (+)
    pub fn union(&self, other: &Range<T>) -> Option<Range<T>> {
        if self.is_empty {
            return Some(other.clone());
        }
        if other.is_empty {
            return Some(self.clone());
        }

        // Ranges must overlap or be adjacent to union
        if !self.overlaps(other) && !self.adjacent_to(other) {
            return None;
        }

        // Take the lower of the lower bounds
        let new_lower = self.min_lower_bound(&self.lower, &other.lower);
        // Take the higher of the upper bounds
        let new_upper = self.max_upper_bound(&self.upper, &other.upper);

        Some(Range::new(new_lower, new_upper))
    }

    /// Intersection of two ranges (*)
    pub fn intersection(&self, other: &Range<T>) -> Range<T> {
        if self.is_empty || other.is_empty {
            return Range::empty();
        }

        if !self.overlaps(other) {
            return Range::empty();
        }

        // Take the higher of the lower bounds
        let new_lower = self.max_lower_bound(&self.lower, &other.lower);
        // Take the lower of the upper bounds
        let new_upper = self.min_upper_bound(&self.upper, &other.upper);

        Range::new(new_lower, new_upper)
    }

    /// Difference of two ranges (-)
    pub fn difference(&self, other: &Range<T>) -> Vec<Range<T>> {
        if self.is_empty || other.is_empty {
            return vec![self.clone()];
        }

        if !self.overlaps(other) {
            return vec![self.clone()];
        }

        let mut result = Vec::new();

        // Part before other
        if self.lower_starts_before(&other.lower) {
            let upper = self.invert_bound(&other.lower);
            let part = Range::new(self.lower.clone(), upper);
            if !part.is_empty {
                result.push(part);
            }
        }

        // Part after other
        if self.upper_ends_after(&other.upper) {
            let lower = self.invert_bound(&other.upper);
            let part = Range::new(lower, self.upper.clone());
            if !part.is_empty {
                result.push(part);
            }
        }

        result
    }

    fn min_lower_bound(&self, a: &Bound<T>, b: &Bound<T>) -> Bound<T> {
        match (a, b) {
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => Bound::Unbounded,
            (Bound::Bounded(av, at), Bound::Bounded(bv, bt)) => {
                match av.partial_cmp(bv) {
                    Some(Ordering::Less) => Bound::Bounded(av.clone(), *at),
                    Some(Ordering::Greater) => Bound::Bounded(bv.clone(), *bt),
                    _ => {
                        // Equal values - prefer inclusive
                        let bound_type = if at.is_inclusive() || bt.is_inclusive() {
                            BoundType::Inclusive
                        } else {
                            BoundType::Exclusive
                        };
                        Bound::Bounded(av.clone(), bound_type)
                    }
                }
            }
        }
    }

    fn max_lower_bound(&self, a: &Bound<T>, b: &Bound<T>) -> Bound<T> {
        match (a, b) {
            (Bound::Unbounded, other) | (other, Bound::Unbounded) => other.clone(),
            (Bound::Bounded(av, at), Bound::Bounded(bv, bt)) => {
                match av.partial_cmp(bv) {
                    Some(Ordering::Greater) => Bound::Bounded(av.clone(), *at),
                    Some(Ordering::Less) => Bound::Bounded(bv.clone(), *bt),
                    _ => {
                        // Equal values - prefer exclusive (more restrictive)
                        let bound_type = if at.is_exclusive() || bt.is_exclusive() {
                            BoundType::Exclusive
                        } else {
                            BoundType::Inclusive
                        };
                        Bound::Bounded(av.clone(), bound_type)
                    }
                }
            }
        }
    }

    fn min_upper_bound(&self, a: &Bound<T>, b: &Bound<T>) -> Bound<T> {
        match (a, b) {
            (Bound::Unbounded, other) | (other, Bound::Unbounded) => other.clone(),
            (Bound::Bounded(av, at), Bound::Bounded(bv, bt)) => {
                match av.partial_cmp(bv) {
                    Some(Ordering::Less) => Bound::Bounded(av.clone(), *at),
                    Some(Ordering::Greater) => Bound::Bounded(bv.clone(), *bt),
                    _ => {
                        let bound_type = if at.is_exclusive() || bt.is_exclusive() {
                            BoundType::Exclusive
                        } else {
                            BoundType::Inclusive
                        };
                        Bound::Bounded(av.clone(), bound_type)
                    }
                }
            }
        }
    }

    fn max_upper_bound(&self, a: &Bound<T>, b: &Bound<T>) -> Bound<T> {
        match (a, b) {
            (Bound::Unbounded, _) | (_, Bound::Unbounded) => Bound::Unbounded,
            (Bound::Bounded(av, at), Bound::Bounded(bv, bt)) => {
                match av.partial_cmp(bv) {
                    Some(Ordering::Greater) => Bound::Bounded(av.clone(), *at),
                    Some(Ordering::Less) => Bound::Bounded(bv.clone(), *bt),
                    _ => {
                        let bound_type = if at.is_inclusive() || bt.is_inclusive() {
                            BoundType::Inclusive
                        } else {
                            BoundType::Exclusive
                        };
                        Bound::Bounded(av.clone(), bound_type)
                    }
                }
            }
        }
    }

    fn invert_bound(&self, bound: &Bound<T>) -> Bound<T> {
        match bound {
            Bound::Unbounded => Bound::Unbounded,
            Bound::Bounded(v, BoundType::Inclusive) => Bound::Bounded(v.clone(), BoundType::Exclusive),
            Bound::Bounded(v, BoundType::Exclusive) => Bound::Bounded(v.clone(), BoundType::Inclusive),
        }
    }

    fn lower_starts_before(&self, other_lower: &Bound<T>) -> bool {
        match (&self.lower, other_lower) {
            (Bound::Unbounded, Bound::Bounded(_, _)) => true,
            (Bound::Unbounded, Bound::Unbounded) => false,
            (Bound::Bounded(_, _), Bound::Unbounded) => false,
            (Bound::Bounded(sl, slt), Bound::Bounded(ol, olt)) => {
                match sl.partial_cmp(ol) {
                    Some(Ordering::Less) => true,
                    Some(Ordering::Equal) => slt.is_inclusive() && olt.is_exclusive(),
                    _ => false,
                }
            }
        }
    }

    fn upper_ends_after(&self, other_upper: &Bound<T>) -> bool {
        match (&self.upper, other_upper) {
            (Bound::Unbounded, Bound::Bounded(_, _)) => true,
            (Bound::Unbounded, Bound::Unbounded) => false,
            (Bound::Bounded(_, _), Bound::Unbounded) => false,
            (Bound::Bounded(su, sut), Bound::Bounded(ou, out)) => {
                match su.partial_cmp(ou) {
                    Some(Ordering::Greater) => true,
                    Some(Ordering::Equal) => sut.is_inclusive() && out.is_exclusive(),
                    _ => false,
                }
            }
        }
    }
}

impl<T: Clone + PartialOrd + PartialEq> PartialEq for Range<T> {
    fn eq(&self, other: &Self) -> bool {
        if self.is_empty && other.is_empty {
            return true;
        }
        if self.is_empty != other.is_empty {
            return false;
        }
        self.lower == other.lower && self.upper == other.upper
    }
}

impl<T: Clone + PartialOrd + Eq> Eq for Range<T> {}

impl<T: Clone + PartialOrd + Hash> Hash for Range<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.is_empty.hash(state);
        if !self.is_empty {
            self.lower.hash(state);
            self.upper.hash(state);
        }
    }
}

impl<T: Clone + PartialOrd + fmt::Display> fmt::Display for Range<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty {
            return write!(f, "empty");
        }

        let lower_sym = self.lower.bound_type()
            .map(|bt| bt.symbol_lower())
            .unwrap_or('(');
        let upper_sym = self.upper.bound_type()
            .map(|bt| bt.symbol_upper())
            .unwrap_or(')');

        let lower_str = self.lower.value()
            .map(|v| v.to_string())
            .unwrap_or_default();
        let upper_str = self.upper.value()
            .map(|v| v.to_string())
            .unwrap_or_default();

        write!(f, "{}{},{}{}", lower_sym, lower_str, upper_str, upper_sym)
    }
}

// ============================================================================
// Concrete Range Types
// ============================================================================

/// 32-bit integer range
pub type Int4Range = Range<i32>;

/// 64-bit integer range
pub type Int8Range = Range<i64>;

/// Floating-point range
pub type NumRange = Range<f64>;

/// Date range (days since epoch)
pub type DateRange = Range<i32>;

/// Timestamp range (microseconds since epoch)
pub type TsRange = Range<i64>;

// ============================================================================
// Range Parsing
// ============================================================================

/// Parse error for ranges
#[derive(Debug)]
pub enum RangeParseError {
    InvalidFormat(String),
    InvalidBound(String),
    InvalidValue(String),
}

impl fmt::Display for RangeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFormat(s) => write!(f, "Invalid range format: {}", s),
            Self::InvalidBound(s) => write!(f, "Invalid bound: {}", s),
            Self::InvalidValue(s) => write!(f, "Invalid value: {}", s),
        }
    }
}

impl std::error::Error for RangeParseError {}

/// Parse an integer range from string
pub fn parse_int4_range(s: &str) -> Result<Int4Range, RangeParseError> {
    parse_range(s, |v| v.parse::<i32>().map_err(|_| RangeParseError::InvalidValue(v.to_string())))
}

/// Parse a bigint range from string
pub fn parse_int8_range(s: &str) -> Result<Int8Range, RangeParseError> {
    parse_range(s, |v| v.parse::<i64>().map_err(|_| RangeParseError::InvalidValue(v.to_string())))
}

/// Parse a numeric range from string
pub fn parse_num_range(s: &str) -> Result<NumRange, RangeParseError> {
    parse_range(s, |v| v.parse::<f64>().map_err(|_| RangeParseError::InvalidValue(v.to_string())))
}

fn parse_range<T, F>(s: &str, parse_value: F) -> Result<Range<T>, RangeParseError>
where
    T: Clone + PartialOrd,
    F: Fn(&str) -> Result<T, RangeParseError>,
{
    let s = s.trim();

    if s == "empty" {
        return Ok(Range::empty());
    }

    if s.len() < 3 {
        return Err(RangeParseError::InvalidFormat(s.to_string()));
    }

    let lower_type = match s.chars().next() {
        Some('[') => BoundType::Inclusive,
        Some('(') => BoundType::Exclusive,
        _ => return Err(RangeParseError::InvalidBound("invalid lower bound".to_string())),
    };

    let upper_type = match s.chars().last() {
        Some(']') => BoundType::Inclusive,
        Some(')') => BoundType::Exclusive,
        _ => return Err(RangeParseError::InvalidBound("invalid upper bound".to_string())),
    };

    let inner = &s[1..s.len()-1];
    let parts: Vec<&str> = inner.split(',').collect();
    if parts.len() != 2 {
        return Err(RangeParseError::InvalidFormat("expected two values separated by comma".to_string()));
    }

    let lower = if parts[0].trim().is_empty() {
        Bound::Unbounded
    } else {
        Bound::Bounded(parse_value(parts[0].trim())?, lower_type)
    };

    let upper = if parts[1].trim().is_empty() {
        Bound::Unbounded
    } else {
        Bound::Bounded(parse_value(parts[1].trim())?, upper_type)
    };

    Ok(Range::new(lower, upper))
}

// ============================================================================
// Range Aggregate Functions
// ============================================================================

/// Compute the union of multiple ranges
pub fn range_agg<T: Clone + PartialOrd>(ranges: &[Range<T>]) -> Vec<Range<T>> {
    if ranges.is_empty() {
        return vec![];
    }

    // Sort ranges by lower bound
    let mut sorted: Vec<_> = ranges.iter().filter(|r| !r.is_empty).cloned().collect();
    if sorted.is_empty() {
        return vec![];
    }

    sorted.sort_by(|a, b| {
        match (&a.lower, &b.lower) {
            (Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
            (Bound::Unbounded, _) => Ordering::Less,
            (_, Bound::Unbounded) => Ordering::Greater,
            (Bound::Bounded(av, _), Bound::Bounded(bv, _)) => {
                av.partial_cmp(bv).unwrap_or(Ordering::Equal)
            }
        }
    });

    let mut result = vec![sorted[0].clone()];

    for range in sorted.into_iter().skip(1) {
        let last = result.last_mut().unwrap();
        if let Some(merged) = last.union(&range) {
            *last = merged;
        } else {
            result.push(range);
        }
    }

    result
}

/// Compute the intersection of multiple ranges
pub fn range_intersect_agg<T: Clone + PartialOrd>(ranges: &[Range<T>]) -> Range<T> {
    if ranges.is_empty() {
        return Range::empty();
    }

    let mut result = ranges[0].clone();
    for range in ranges.iter().skip(1) {
        result = result.intersection(range);
        if result.is_empty {
            break;
        }
    }
    result
}

// ============================================================================
// GiST Index Support
// ============================================================================

/// GiST-compatible consistent function result
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GistConsistent {
    /// Definitely no match
    False,
    /// Definitely matches
    True,
    /// Maybe matches (need to check)
    Maybe,
}

/// GiST penalty for range insertion
pub fn gist_penalty<T: Clone + PartialOrd + Into<f64>>(
    orig: &Range<T>,
    new: &Range<T>,
) -> f64 {
    // Penalty is how much the bounding box would need to expand
    if orig.is_empty {
        return 0.0;
    }
    if new.is_empty {
        return 0.0;
    }

    // Calculate the union
    if let Some(union) = orig.union(new) {
        // If they're adjacent or overlapping, low penalty
        if orig.overlaps(new) || orig.adjacent_to(new) {
            return 0.0;
        }

        // Calculate expansion penalty
        let orig_size = range_size(orig);
        let union_size = range_size(&union);

        if orig_size == 0.0 {
            union_size
        } else {
            (union_size - orig_size) / orig_size
        }
    } else {
        // Disjoint ranges - high penalty
        1000.0
    }
}

fn range_size<T: Clone + PartialOrd + Into<f64>>(range: &Range<T>) -> f64 {
    if range.is_empty {
        return 0.0;
    }

    match (&range.lower, &range.upper) {
        (Bound::Bounded(l, _), Bound::Bounded(u, _)) => {
            let lower: f64 = l.clone().into();
            let upper: f64 = u.clone().into();
            (upper - lower).abs()
        }
        _ => f64::INFINITY,
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_creation() {
        let r = Int4Range::closed(1, 10);
        assert!(!r.is_empty);
        assert_eq!(r.lower_val(), Some(&1));
        assert_eq!(r.upper_val(), Some(&10));
        assert!(r.lower_inc());
        assert!(r.upper_inc());
    }

    #[test]
    fn test_empty_range() {
        let r = Int4Range::empty();
        assert!(r.is_empty);

        // [5, 3] is also empty
        let r2 = Int4Range::closed(5, 3);
        assert!(r2.is_empty);

        // (5, 5) is empty
        let r3 = Int4Range::open(5, 5);
        assert!(r3.is_empty);

        // [5, 5] is NOT empty
        let r4 = Int4Range::closed(5, 5);
        assert!(!r4.is_empty);
    }

    #[test]
    fn test_contains_value() {
        let r = Int4Range::closed(1, 10);
        assert!(r.contains_value(&1));
        assert!(r.contains_value(&5));
        assert!(r.contains_value(&10));
        assert!(!r.contains_value(&0));
        assert!(!r.contains_value(&11));

        let r2 = Int4Range::open(1, 10);
        assert!(!r2.contains_value(&1));
        assert!(r2.contains_value(&5));
        assert!(!r2.contains_value(&10));
    }

    #[test]
    fn test_contains_range() {
        let r1 = Int4Range::closed(1, 10);
        let r2 = Int4Range::closed(3, 7);

        assert!(r1.contains_range(&r2));
        assert!(!r2.contains_range(&r1));

        // Range contains itself
        assert!(r1.contains_range(&r1));

        // Empty range is contained by everything
        let empty = Int4Range::empty();
        assert!(r1.contains_range(&empty));
    }

    #[test]
    fn test_overlaps() {
        let r1 = Int4Range::closed(1, 5);
        let r2 = Int4Range::closed(3, 10);
        let r3 = Int4Range::closed(6, 10);

        assert!(r1.overlaps(&r2));
        assert!(r2.overlaps(&r1));
        assert!(!r1.overlaps(&r3));
        assert!(!r3.overlaps(&r1));

        // Adjacent ranges don't overlap
        let r4 = Int4Range::closed_open(1, 5);
        let r5 = Int4Range::closed(5, 10);
        assert!(!r4.overlaps(&r5));
    }

    #[test]
    fn test_adjacent() {
        let r1 = Int4Range::closed_open(1, 5);
        let r2 = Int4Range::closed(5, 10);

        assert!(r1.adjacent_to(&r2));
        assert!(r2.adjacent_to(&r1));

        // [1,5] and [5,10] are NOT adjacent (they overlap at 5)
        let r3 = Int4Range::closed(1, 5);
        let r4 = Int4Range::closed(5, 10);
        assert!(!r3.adjacent_to(&r4));
    }

    #[test]
    fn test_strictly_left_right() {
        let r1 = Int4Range::closed(1, 5);
        let r2 = Int4Range::closed(10, 20);

        assert!(r1.strictly_left_of(&r2));
        assert!(r2.strictly_right_of(&r1));
        assert!(!r2.strictly_left_of(&r1));
    }

    #[test]
    fn test_union() {
        let r1 = Int4Range::closed(1, 5);
        let r2 = Int4Range::closed(3, 10);

        let union = r1.union(&r2).unwrap();
        assert_eq!(union.lower_val(), Some(&1));
        assert_eq!(union.upper_val(), Some(&10));

        // Adjacent ranges can be unioned
        let r3 = Int4Range::closed_open(1, 5);
        let r4 = Int4Range::closed(5, 10);
        let union2 = r3.union(&r4).unwrap();
        assert_eq!(union2.lower_val(), Some(&1));
        assert_eq!(union2.upper_val(), Some(&10));

        // Disjoint ranges cannot be unioned
        let r5 = Int4Range::closed(1, 3);
        let r6 = Int4Range::closed(7, 10);
        assert!(r5.union(&r6).is_none());
    }

    #[test]
    fn test_intersection() {
        let r1 = Int4Range::closed(1, 10);
        let r2 = Int4Range::closed(5, 15);

        let intersection = r1.intersection(&r2);
        assert!(!intersection.is_empty);
        assert_eq!(intersection.lower_val(), Some(&5));
        assert_eq!(intersection.upper_val(), Some(&10));

        // Non-overlapping ranges have empty intersection
        let r3 = Int4Range::closed(1, 5);
        let r4 = Int4Range::closed(10, 15);
        let intersection2 = r3.intersection(&r4);
        assert!(intersection2.is_empty);
    }

    #[test]
    fn test_difference() {
        let r1 = Int4Range::closed(1, 10);
        let r2 = Int4Range::closed(3, 7);

        let diff = r1.difference(&r2);
        assert_eq!(diff.len(), 2);
    }

    #[test]
    fn test_unbounded_ranges() {
        let r1 = Int4Range::at_least(5);
        assert!(r1.contains_value(&5));
        assert!(r1.contains_value(&100));
        assert!(!r1.contains_value(&4));

        let r2 = Int4Range::at_most(5);
        assert!(r2.contains_value(&5));
        assert!(r2.contains_value(&-100));
        assert!(!r2.contains_value(&6));

        let r3 = Int4Range::all();
        assert!(r3.contains_value(&0));
        assert!(r3.contains_value(&i32::MAX));
        assert!(r3.contains_value(&i32::MIN));
    }

    #[test]
    fn test_parse_int4_range() {
        let r = parse_int4_range("[1,10]").unwrap();
        assert_eq!(r.lower_val(), Some(&1));
        assert_eq!(r.upper_val(), Some(&10));
        assert!(r.lower_inc());
        assert!(r.upper_inc());

        let r2 = parse_int4_range("(1,10)").unwrap();
        assert!(!r2.lower_inc());
        assert!(!r2.upper_inc());

        let r3 = parse_int4_range("[,10]").unwrap();
        assert!(r3.lower_inf());
        assert_eq!(r3.upper_val(), Some(&10));

        let r4 = parse_int4_range("empty").unwrap();
        assert!(r4.is_empty);
    }

    #[test]
    fn test_range_display() {
        let r1 = Int4Range::closed(1, 10);
        assert_eq!(format!("{}", r1), "[1,10]");

        let r2 = Int4Range::open(1, 10);
        assert_eq!(format!("{}", r2), "(1,10)");

        let r3 = Int4Range::empty();
        assert_eq!(format!("{}", r3), "empty");

        let r4 = Int4Range::at_least(5);
        assert_eq!(format!("{}", r4), "[5,)");
    }

    #[test]
    fn test_range_equality() {
        let r1 = Int4Range::closed(1, 10);
        let r2 = Int4Range::closed(1, 10);
        let r3 = Int4Range::closed(1, 11);

        assert_eq!(r1, r2);
        assert_ne!(r1, r3);

        // Empty ranges are equal
        let e1 = Int4Range::empty();
        let e2 = Int4Range::open(5, 5);
        assert_eq!(e1, e2);
    }

    #[test]
    fn test_range_agg() {
        let ranges = vec![
            Int4Range::closed(1, 5),
            Int4Range::closed(3, 8),
            Int4Range::closed(15, 20),
        ];

        let result = range_agg(&ranges);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].lower_val(), Some(&1));
        assert_eq!(result[0].upper_val(), Some(&8));
        assert_eq!(result[1].lower_val(), Some(&15));
        assert_eq!(result[1].upper_val(), Some(&20));
    }

    #[test]
    fn test_range_intersect_agg() {
        let ranges = vec![
            Int4Range::closed(1, 10),
            Int4Range::closed(5, 15),
            Int4Range::closed(3, 8),
        ];

        let result = range_intersect_agg(&ranges);
        assert!(!result.is_empty);
        assert_eq!(result.lower_val(), Some(&5));
        assert_eq!(result.upper_val(), Some(&8));
    }

    #[test]
    fn test_num_range() {
        let r = NumRange::closed(1.5, 10.5);
        assert!(r.contains_value(&5.0));
        assert!(r.contains_value(&1.5));
        assert!(r.contains_value(&10.5));
        assert!(!r.contains_value(&1.4));
        assert!(!r.contains_value(&10.6));
    }

    #[test]
    fn test_gist_penalty() {
        let r1 = Int4Range::closed(1, 10);
        let r2 = Int4Range::closed(5, 15);
        let r3 = Int4Range::closed(100, 200);

        // Overlapping ranges have low penalty
        let p1 = gist_penalty(&r1, &r2);
        assert!(p1 < 1.0);

        // Disjoint ranges have high penalty
        let p2 = gist_penalty(&r1, &r3);
        assert!(p2 > 100.0);
    }

    #[test]
    fn test_bound_type() {
        assert!(BoundType::Inclusive.is_inclusive());
        assert!(!BoundType::Inclusive.is_exclusive());
        assert!(BoundType::Exclusive.is_exclusive());
        assert!(!BoundType::Exclusive.is_inclusive());

        assert_eq!(BoundType::Inclusive.symbol_lower(), '[');
        assert_eq!(BoundType::Inclusive.symbol_upper(), ']');
        assert_eq!(BoundType::Exclusive.symbol_lower(), '(');
        assert_eq!(BoundType::Exclusive.symbol_upper(), ')');
    }
}
