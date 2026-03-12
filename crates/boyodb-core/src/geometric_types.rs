// Geometric Types - Points, lines, polygons, boxes for BoyoDB
//
// Provides PostgreSQL-style geometric type support:
// - Point (x, y coordinates)
// - Line (infinite line: Ax + By + C = 0)
// - Line segment (finite line between two points)
// - Box (rectangular box)
// - Path (connected sequence of points)
// - Polygon (closed path)
// - Circle (center point and radius)
// - Geometric operators and functions

use std::f64::consts::PI;
use std::fmt;
use std::str::FromStr;

// ============================================================================
// Point
// ============================================================================

/// A 2D point
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    pub x: f64,
    pub y: f64,
}

impl Point {
    /// Create a new point
    pub fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }

    /// Origin point (0, 0)
    pub fn origin() -> Self {
        Self { x: 0.0, y: 0.0 }
    }

    /// Distance to another point
    pub fn distance(&self, other: &Point) -> f64 {
        let dx = self.x - other.x;
        let dy = self.y - other.y;
        (dx * dx + dy * dy).sqrt()
    }

    /// Slope to another point
    pub fn slope(&self, other: &Point) -> Option<f64> {
        let dx = other.x - self.x;
        if dx.abs() < f64::EPSILON {
            None // Vertical line
        } else {
            Some((other.y - self.y) / dx)
        }
    }

    /// Midpoint between two points
    pub fn midpoint(&self, other: &Point) -> Point {
        Point::new(
            (self.x + other.x) / 2.0,
            (self.y + other.y) / 2.0,
        )
    }

    /// Translate by dx, dy
    pub fn translate(&self, dx: f64, dy: f64) -> Point {
        Point::new(self.x + dx, self.y + dy)
    }

    /// Scale from origin
    pub fn scale(&self, factor: f64) -> Point {
        Point::new(self.x * factor, self.y * factor)
    }

    /// Rotate around origin by angle (radians)
    pub fn rotate(&self, angle: f64) -> Point {
        let cos_a = angle.cos();
        let sin_a = angle.sin();
        Point::new(
            self.x * cos_a - self.y * sin_a,
            self.x * sin_a + self.y * cos_a,
        )
    }

    /// Rotate around a center point
    pub fn rotate_around(&self, center: &Point, angle: f64) -> Point {
        let translated = self.translate(-center.x, -center.y);
        let rotated = translated.rotate(angle);
        rotated.translate(center.x, center.y)
    }

    /// Check if points are approximately equal
    pub fn approx_eq(&self, other: &Point, epsilon: f64) -> bool {
        (self.x - other.x).abs() < epsilon && (self.y - other.y).abs() < epsilon
    }
}

impl Default for Point {
    fn default() -> Self {
        Self::origin()
    }
}

impl fmt::Display for Point {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({},{})", self.x, self.y)
    }
}

impl FromStr for Point {
    type Err = GeometricError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().trim_start_matches('(').trim_end_matches(')');
        let parts: Vec<&str> = s.split(',').collect();
        if parts.len() != 2 {
            return Err(GeometricError::ParseError("Invalid point format".to_string()));
        }
        let x = parts[0].trim().parse()
            .map_err(|_| GeometricError::ParseError("Invalid x coordinate".to_string()))?;
        let y = parts[1].trim().parse()
            .map_err(|_| GeometricError::ParseError("Invalid y coordinate".to_string()))?;
        Ok(Point::new(x, y))
    }
}

// ============================================================================
// Line (infinite)
// ============================================================================

/// An infinite line in the form Ax + By + C = 0
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Line {
    pub a: f64,
    pub b: f64,
    pub c: f64,
}

impl Line {
    /// Create a new line
    pub fn new(a: f64, b: f64, c: f64) -> Self {
        Self { a, b, c }
    }

    /// Create a line through two points
    pub fn through_points(p1: &Point, p2: &Point) -> Self {
        let a = p2.y - p1.y;
        let b = p1.x - p2.x;
        let c = -(a * p1.x + b * p1.y);
        Self { a, b, c }
    }

    /// Create a horizontal line at y = k
    pub fn horizontal(y: f64) -> Self {
        Self { a: 0.0, b: 1.0, c: -y }
    }

    /// Create a vertical line at x = k
    pub fn vertical(x: f64) -> Self {
        Self { a: 1.0, b: 0.0, c: -x }
    }

    /// Check if a point is on the line
    pub fn contains_point(&self, p: &Point) -> bool {
        (self.a * p.x + self.b * p.y + self.c).abs() < f64::EPSILON
    }

    /// Check if lines are parallel
    pub fn is_parallel(&self, other: &Line) -> bool {
        (self.a * other.b - self.b * other.a).abs() < f64::EPSILON
    }

    /// Check if lines are perpendicular
    pub fn is_perpendicular(&self, other: &Line) -> bool {
        (self.a * other.a + self.b * other.b).abs() < f64::EPSILON
    }

    /// Distance from a point to the line
    pub fn distance_to_point(&self, p: &Point) -> f64 {
        let numer = (self.a * p.x + self.b * p.y + self.c).abs();
        let denom = (self.a * self.a + self.b * self.b).sqrt();
        numer / denom
    }

    /// Intersection point with another line
    pub fn intersection(&self, other: &Line) -> Option<Point> {
        let det = self.a * other.b - other.a * self.b;
        if det.abs() < f64::EPSILON {
            return None; // Parallel lines
        }
        let x = (self.b * other.c - other.b * self.c) / det;
        let y = (other.a * self.c - self.a * other.c) / det;
        Some(Point::new(x, y))
    }

    /// Get the slope of the line
    pub fn slope(&self) -> Option<f64> {
        if self.b.abs() < f64::EPSILON {
            None // Vertical line
        } else {
            Some(-self.a / self.b)
        }
    }

    /// Perpendicular line through a point
    pub fn perpendicular_through(&self, p: &Point) -> Line {
        // Perpendicular has coefficients (b, -a, ...)
        let a = self.b;
        let b = -self.a;
        let c = -(a * p.x + b * p.y);
        Line { a, b, c }
    }

    /// Parallel line through a point
    pub fn parallel_through(&self, p: &Point) -> Line {
        let c = -(self.a * p.x + self.b * p.y);
        Line { a: self.a, b: self.b, c }
    }
}

impl fmt::Display for Line {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{{}x + {}y + {} = 0}}", self.a, self.b, self.c)
    }
}

// ============================================================================
// Line Segment
// ============================================================================

/// A finite line segment between two points
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LineSegment {
    pub start: Point,
    pub end: Point,
}

impl LineSegment {
    /// Create a new line segment
    pub fn new(start: Point, end: Point) -> Self {
        Self { start, end }
    }

    /// Length of the segment
    pub fn length(&self) -> f64 {
        self.start.distance(&self.end)
    }

    /// Midpoint of the segment
    pub fn midpoint(&self) -> Point {
        self.start.midpoint(&self.end)
    }

    /// Check if a point is on the segment
    pub fn contains_point(&self, p: &Point) -> bool {
        let d1 = self.start.distance(p);
        let d2 = self.end.distance(p);
        let len = self.length();
        (d1 + d2 - len).abs() < f64::EPSILON * 10.0
    }

    /// Convert to infinite line
    pub fn to_line(&self) -> Line {
        Line::through_points(&self.start, &self.end)
    }

    /// Check if segments intersect
    pub fn intersects(&self, other: &LineSegment) -> bool {
        self.intersection(other).is_some()
    }

    /// Find intersection point with another segment
    pub fn intersection(&self, other: &LineSegment) -> Option<Point> {
        let line1 = self.to_line();
        let line2 = other.to_line();

        let point = line1.intersection(&line2)?;

        // Check if point is within both segments
        if self.contains_point(&point) && other.contains_point(&point) {
            Some(point)
        } else {
            None
        }
    }

    /// Distance to a point
    pub fn distance_to_point(&self, p: &Point) -> f64 {
        // Project point onto the line
        let dx = self.end.x - self.start.x;
        let dy = self.end.y - self.start.y;
        let len_sq = dx * dx + dy * dy;

        if len_sq < f64::EPSILON {
            return self.start.distance(p);
        }

        // Parametric position of projection
        let t = ((p.x - self.start.x) * dx + (p.y - self.start.y) * dy) / len_sq;
        let t = t.clamp(0.0, 1.0);

        let proj = Point::new(
            self.start.x + t * dx,
            self.start.y + t * dy,
        );

        p.distance(&proj)
    }

    /// Translate the segment
    pub fn translate(&self, dx: f64, dy: f64) -> LineSegment {
        LineSegment {
            start: self.start.translate(dx, dy),
            end: self.end.translate(dx, dy),
        }
    }
}

impl fmt::Display for LineSegment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{},{}]", self.start, self.end)
    }
}

// ============================================================================
// Box (Rectangle)
// ============================================================================

/// An axis-aligned rectangular box
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Box {
    /// Lower-left corner
    pub low: Point,
    /// Upper-right corner
    pub high: Point,
}

impl Box {
    /// Create a new box from two corner points
    pub fn new(p1: Point, p2: Point) -> Self {
        Self {
            low: Point::new(p1.x.min(p2.x), p1.y.min(p2.y)),
            high: Point::new(p1.x.max(p2.x), p1.y.max(p2.y)),
        }
    }

    /// Create from center and dimensions
    pub fn from_center(center: Point, width: f64, height: f64) -> Self {
        let hw = width / 2.0;
        let hh = height / 2.0;
        Self {
            low: Point::new(center.x - hw, center.y - hh),
            high: Point::new(center.x + hw, center.y + hh),
        }
    }

    /// Width of the box
    pub fn width(&self) -> f64 {
        self.high.x - self.low.x
    }

    /// Height of the box
    pub fn height(&self) -> f64 {
        self.high.y - self.low.y
    }

    /// Area of the box
    pub fn area(&self) -> f64 {
        self.width() * self.height()
    }

    /// Perimeter of the box
    pub fn perimeter(&self) -> f64 {
        2.0 * (self.width() + self.height())
    }

    /// Center point
    pub fn center(&self) -> Point {
        self.low.midpoint(&self.high)
    }

    /// Diagonal length
    pub fn diagonal(&self) -> f64 {
        self.low.distance(&self.high)
    }

    /// Check if point is inside
    pub fn contains_point(&self, p: &Point) -> bool {
        p.x >= self.low.x && p.x <= self.high.x &&
        p.y >= self.low.y && p.y <= self.high.y
    }

    /// Check if box contains another box
    pub fn contains_box(&self, other: &Box) -> bool {
        self.contains_point(&other.low) && self.contains_point(&other.high)
    }

    /// Check if boxes overlap
    pub fn overlaps(&self, other: &Box) -> bool {
        self.low.x <= other.high.x && self.high.x >= other.low.x &&
        self.low.y <= other.high.y && self.high.y >= other.low.y
    }

    /// Intersection of two boxes
    pub fn intersection(&self, other: &Box) -> Option<Box> {
        if !self.overlaps(other) {
            return None;
        }
        Some(Box {
            low: Point::new(
                self.low.x.max(other.low.x),
                self.low.y.max(other.low.y),
            ),
            high: Point::new(
                self.high.x.min(other.high.x),
                self.high.y.min(other.high.y),
            ),
        })
    }

    /// Bounding box of two boxes
    pub fn bounding_box(&self, other: &Box) -> Box {
        Box {
            low: Point::new(
                self.low.x.min(other.low.x),
                self.low.y.min(other.low.y),
            ),
            high: Point::new(
                self.high.x.max(other.high.x),
                self.high.y.max(other.high.y),
            ),
        }
    }

    /// Translate the box
    pub fn translate(&self, dx: f64, dy: f64) -> Box {
        Box {
            low: self.low.translate(dx, dy),
            high: self.high.translate(dx, dy),
        }
    }

    /// Scale the box
    pub fn scale(&self, factor: f64) -> Box {
        let center = self.center();
        let half_w = self.width() * factor / 2.0;
        let half_h = self.height() * factor / 2.0;
        Box {
            low: Point::new(center.x - half_w, center.y - half_h),
            high: Point::new(center.x + half_w, center.y + half_h),
        }
    }
}

impl fmt::Display for Box {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(({},{}),({},{}))", self.low.x, self.low.y, self.high.x, self.high.y)
    }
}

// ============================================================================
// Path
// ============================================================================

/// A connected sequence of points
#[derive(Debug, Clone, PartialEq)]
pub struct Path {
    pub points: Vec<Point>,
    pub closed: bool,
}

impl Path {
    /// Create a new open path
    pub fn new(points: Vec<Point>) -> Self {
        Self { points, closed: false }
    }

    /// Create a closed path
    pub fn closed(points: Vec<Point>) -> Self {
        Self { points, closed: true }
    }

    /// Number of points
    pub fn len(&self) -> usize {
        self.points.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }

    /// Total length of the path
    pub fn length(&self) -> f64 {
        if self.points.len() < 2 {
            return 0.0;
        }

        let mut total = 0.0;
        for i in 1..self.points.len() {
            total += self.points[i - 1].distance(&self.points[i]);
        }

        if self.closed && self.points.len() > 2 {
            total += self.points.last().unwrap().distance(&self.points[0]);
        }

        total
    }

    /// Bounding box of the path
    pub fn bounding_box(&self) -> Option<Box> {
        if self.points.is_empty() {
            return None;
        }

        let mut min_x = f64::MAX;
        let mut min_y = f64::MAX;
        let mut max_x = f64::MIN;
        let mut max_y = f64::MIN;

        for p in &self.points {
            min_x = min_x.min(p.x);
            min_y = min_y.min(p.y);
            max_x = max_x.max(p.x);
            max_y = max_y.max(p.y);
        }

        Some(Box::new(Point::new(min_x, min_y), Point::new(max_x, max_y)))
    }

    /// Translate the path
    pub fn translate(&self, dx: f64, dy: f64) -> Path {
        Path {
            points: self.points.iter().map(|p| p.translate(dx, dy)).collect(),
            closed: self.closed,
        }
    }

    /// Scale the path from origin
    pub fn scale(&self, factor: f64) -> Path {
        Path {
            points: self.points.iter().map(|p| p.scale(factor)).collect(),
            closed: self.closed,
        }
    }

    /// Rotate the path around origin
    pub fn rotate(&self, angle: f64) -> Path {
        Path {
            points: self.points.iter().map(|p| p.rotate(angle)).collect(),
            closed: self.closed,
        }
    }
}

impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bracket = if self.closed { ('(', ')') } else { ('[', ']') };
        write!(f, "{}", bracket.0)?;
        for (i, p) in self.points.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, "{}", p)?;
        }
        write!(f, "{}", bracket.1)
    }
}

// ============================================================================
// Polygon
// ============================================================================

/// A closed polygon
#[derive(Debug, Clone, PartialEq)]
pub struct Polygon {
    pub points: Vec<Point>,
}

impl Polygon {
    /// Create a new polygon
    pub fn new(points: Vec<Point>) -> Result<Self, GeometricError> {
        if points.len() < 3 {
            return Err(GeometricError::InvalidPolygon("Need at least 3 points".to_string()));
        }
        Ok(Self { points })
    }

    /// Create a regular polygon with n sides
    pub fn regular(n: usize, center: Point, radius: f64) -> Result<Self, GeometricError> {
        if n < 3 {
            return Err(GeometricError::InvalidPolygon("Need at least 3 sides".to_string()));
        }

        let mut points = Vec::with_capacity(n);
        let angle_step = 2.0 * PI / n as f64;

        for i in 0..n {
            let angle = angle_step * i as f64 - PI / 2.0; // Start from top
            points.push(Point::new(
                center.x + radius * angle.cos(),
                center.y + radius * angle.sin(),
            ));
        }

        Ok(Self { points })
    }

    /// Number of vertices
    pub fn num_vertices(&self) -> usize {
        self.points.len()
    }

    /// Perimeter of the polygon
    pub fn perimeter(&self) -> f64 {
        if self.points.len() < 2 {
            return 0.0;
        }

        let mut total = 0.0;
        for i in 0..self.points.len() {
            let j = (i + 1) % self.points.len();
            total += self.points[i].distance(&self.points[j]);
        }
        total
    }

    /// Area of the polygon (shoelace formula)
    pub fn area(&self) -> f64 {
        if self.points.len() < 3 {
            return 0.0;
        }

        let mut sum = 0.0;
        for i in 0..self.points.len() {
            let j = (i + 1) % self.points.len();
            sum += self.points[i].x * self.points[j].y;
            sum -= self.points[j].x * self.points[i].y;
        }
        (sum / 2.0).abs()
    }

    /// Centroid of the polygon
    pub fn centroid(&self) -> Point {
        if self.points.is_empty() {
            return Point::origin();
        }

        let mut cx = 0.0;
        let mut cy = 0.0;
        let n = self.points.len() as f64;

        for p in &self.points {
            cx += p.x;
            cy += p.y;
        }

        Point::new(cx / n, cy / n)
    }

    /// Check if a point is inside the polygon (ray casting)
    pub fn contains_point(&self, p: &Point) -> bool {
        let mut inside = false;
        let n = self.points.len();

        let mut j = n - 1;
        for i in 0..n {
            let pi = &self.points[i];
            let pj = &self.points[j];

            if ((pi.y > p.y) != (pj.y > p.y)) &&
               (p.x < (pj.x - pi.x) * (p.y - pi.y) / (pj.y - pi.y) + pi.x)
            {
                inside = !inside;
            }
            j = i;
        }

        inside
    }

    /// Check if polygon is convex
    pub fn is_convex(&self) -> bool {
        if self.points.len() < 3 {
            return false;
        }

        let n = self.points.len();
        let mut sign = 0i32;

        for i in 0..n {
            let p0 = &self.points[i];
            let p1 = &self.points[(i + 1) % n];
            let p2 = &self.points[(i + 2) % n];

            let cross = (p1.x - p0.x) * (p2.y - p1.y) - (p1.y - p0.y) * (p2.x - p1.x);

            if cross.abs() > f64::EPSILON {
                let current_sign = if cross > 0.0 { 1 } else { -1 };
                if sign == 0 {
                    sign = current_sign;
                } else if sign != current_sign {
                    return false;
                }
            }
        }

        true
    }

    /// Bounding box of the polygon
    pub fn bounding_box(&self) -> Option<Box> {
        if self.points.is_empty() {
            return None;
        }

        let mut min_x = f64::MAX;
        let mut min_y = f64::MAX;
        let mut max_x = f64::MIN;
        let mut max_y = f64::MIN;

        for p in &self.points {
            min_x = min_x.min(p.x);
            min_y = min_y.min(p.y);
            max_x = max_x.max(p.x);
            max_y = max_y.max(p.y);
        }

        Some(Box::new(Point::new(min_x, min_y), Point::new(max_x, max_y)))
    }

    /// Translate the polygon
    pub fn translate(&self, dx: f64, dy: f64) -> Polygon {
        Polygon {
            points: self.points.iter().map(|p| p.translate(dx, dy)).collect(),
        }
    }
}

impl fmt::Display for Polygon {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "((")?;
        for (i, p) in self.points.iter().enumerate() {
            if i > 0 {
                write!(f, "),(")?;
            }
            write!(f, "{},{}", p.x, p.y)?;
        }
        write!(f, "))")
    }
}

// ============================================================================
// Circle
// ============================================================================

/// A circle defined by center and radius
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Circle {
    pub center: Point,
    pub radius: f64,
}

impl Circle {
    /// Create a new circle
    pub fn new(center: Point, radius: f64) -> Result<Self, GeometricError> {
        if radius < 0.0 {
            return Err(GeometricError::InvalidCircle("Radius must be non-negative".to_string()));
        }
        Ok(Self { center, radius })
    }

    /// Create from three points on the circle
    pub fn from_three_points(p1: &Point, p2: &Point, p3: &Point) -> Option<Self> {
        // Use perpendicular bisector method
        let mid1 = p1.midpoint(p2);
        let mid2 = p2.midpoint(p3);

        let line1 = Line::through_points(p1, p2);
        let line2 = Line::through_points(p2, p3);

        let perp1 = line1.perpendicular_through(&mid1);
        let perp2 = line2.perpendicular_through(&mid2);

        let center = perp1.intersection(&perp2)?;
        let radius = center.distance(p1);

        Circle::new(center, radius).ok()
    }

    /// Area of the circle
    pub fn area(&self) -> f64 {
        PI * self.radius * self.radius
    }

    /// Circumference of the circle
    pub fn circumference(&self) -> f64 {
        2.0 * PI * self.radius
    }

    /// Diameter of the circle
    pub fn diameter(&self) -> f64 {
        2.0 * self.radius
    }

    /// Check if a point is inside the circle
    pub fn contains_point(&self, p: &Point) -> bool {
        self.center.distance(p) <= self.radius + f64::EPSILON
    }

    /// Check if circle contains another circle
    pub fn contains_circle(&self, other: &Circle) -> bool {
        let center_dist = self.center.distance(&other.center);
        center_dist + other.radius <= self.radius + f64::EPSILON
    }

    /// Check if circles overlap
    pub fn overlaps(&self, other: &Circle) -> bool {
        let center_dist = self.center.distance(&other.center);
        center_dist < self.radius + other.radius + f64::EPSILON
    }

    /// Distance between circle edges
    pub fn distance_to(&self, other: &Circle) -> f64 {
        let center_dist = self.center.distance(&other.center);
        (center_dist - self.radius - other.radius).max(0.0)
    }

    /// Bounding box of the circle
    pub fn bounding_box(&self) -> Box {
        Box::new(
            Point::new(self.center.x - self.radius, self.center.y - self.radius),
            Point::new(self.center.x + self.radius, self.center.y + self.radius),
        )
    }

    /// Point on circle at given angle (radians)
    pub fn point_at_angle(&self, angle: f64) -> Point {
        Point::new(
            self.center.x + self.radius * angle.cos(),
            self.center.y + self.radius * angle.sin(),
        )
    }

    /// Translate the circle
    pub fn translate(&self, dx: f64, dy: f64) -> Circle {
        Circle {
            center: self.center.translate(dx, dy),
            radius: self.radius,
        }
    }

    /// Scale the circle
    pub fn scale(&self, factor: f64) -> Circle {
        Circle {
            center: self.center,
            radius: self.radius * factor.abs(),
        }
    }
}

impl fmt::Display for Circle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<{},{}>", self.center, self.radius)
    }
}

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug)]
pub enum GeometricError {
    ParseError(String),
    InvalidPolygon(String),
    InvalidCircle(String),
}

impl fmt::Display for GeometricError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ParseError(s) => write!(f, "Parse error: {}", s),
            Self::InvalidPolygon(s) => write!(f, "Invalid polygon: {}", s),
            Self::InvalidCircle(s) => write!(f, "Invalid circle: {}", s),
        }
    }
}

impl std::error::Error for GeometricError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_creation() {
        let p = Point::new(3.0, 4.0);
        assert_eq!(p.x, 3.0);
        assert_eq!(p.y, 4.0);
    }

    #[test]
    fn test_point_distance() {
        let p1 = Point::new(0.0, 0.0);
        let p2 = Point::new(3.0, 4.0);
        assert!((p1.distance(&p2) - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_point_midpoint() {
        let p1 = Point::new(0.0, 0.0);
        let p2 = Point::new(10.0, 10.0);
        let mid = p1.midpoint(&p2);
        assert_eq!(mid.x, 5.0);
        assert_eq!(mid.y, 5.0);
    }

    #[test]
    fn test_point_rotate() {
        let p = Point::new(1.0, 0.0);
        let rotated = p.rotate(PI / 2.0);
        assert!(rotated.approx_eq(&Point::new(0.0, 1.0), 1e-10));
    }

    #[test]
    fn test_point_parse() {
        let p: Point = "(3.5, 4.5)".parse().unwrap();
        assert_eq!(p.x, 3.5);
        assert_eq!(p.y, 4.5);
    }

    #[test]
    fn test_line_through_points() {
        let p1 = Point::new(0.0, 0.0);
        let p2 = Point::new(1.0, 1.0);
        let line = Line::through_points(&p1, &p2);

        assert!(line.contains_point(&p1));
        assert!(line.contains_point(&p2));
        assert!(line.contains_point(&Point::new(0.5, 0.5)));
    }

    #[test]
    fn test_line_intersection() {
        let l1 = Line::through_points(&Point::new(0.0, 0.0), &Point::new(1.0, 1.0));
        let l2 = Line::through_points(&Point::new(0.0, 1.0), &Point::new(1.0, 0.0));

        let intersection = l1.intersection(&l2).unwrap();
        assert!(intersection.approx_eq(&Point::new(0.5, 0.5), 1e-10));
    }

    #[test]
    fn test_line_parallel() {
        let l1 = Line::new(1.0, 1.0, 0.0);
        let l2 = Line::new(2.0, 2.0, 5.0);
        assert!(l1.is_parallel(&l2));
    }

    #[test]
    fn test_line_segment_length() {
        let seg = LineSegment::new(Point::new(0.0, 0.0), Point::new(3.0, 4.0));
        assert!((seg.length() - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_line_segment_intersection() {
        let seg1 = LineSegment::new(Point::new(0.0, 0.0), Point::new(2.0, 2.0));
        let seg2 = LineSegment::new(Point::new(0.0, 2.0), Point::new(2.0, 0.0));

        let intersection = seg1.intersection(&seg2).unwrap();
        assert!(intersection.approx_eq(&Point::new(1.0, 1.0), 1e-10));
    }

    #[test]
    fn test_box_creation() {
        let b = Box::new(Point::new(0.0, 0.0), Point::new(10.0, 5.0));
        assert_eq!(b.width(), 10.0);
        assert_eq!(b.height(), 5.0);
        assert_eq!(b.area(), 50.0);
    }

    #[test]
    fn test_box_contains() {
        let b = Box::new(Point::new(0.0, 0.0), Point::new(10.0, 10.0));

        assert!(b.contains_point(&Point::new(5.0, 5.0)));
        assert!(b.contains_point(&Point::new(0.0, 0.0)));
        assert!(!b.contains_point(&Point::new(15.0, 5.0)));
    }

    #[test]
    fn test_box_overlaps() {
        let b1 = Box::new(Point::new(0.0, 0.0), Point::new(10.0, 10.0));
        let b2 = Box::new(Point::new(5.0, 5.0), Point::new(15.0, 15.0));
        let b3 = Box::new(Point::new(20.0, 20.0), Point::new(30.0, 30.0));

        assert!(b1.overlaps(&b2));
        assert!(!b1.overlaps(&b3));
    }

    #[test]
    fn test_path_length() {
        let path = Path::new(vec![
            Point::new(0.0, 0.0),
            Point::new(3.0, 0.0),
            Point::new(3.0, 4.0),
        ]);
        assert!((path.length() - 7.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_polygon_area() {
        // Square with side 2
        let polygon = Polygon::new(vec![
            Point::new(0.0, 0.0),
            Point::new(2.0, 0.0),
            Point::new(2.0, 2.0),
            Point::new(0.0, 2.0),
        ]).unwrap();

        assert!((polygon.area() - 4.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_polygon_contains_point() {
        let polygon = Polygon::new(vec![
            Point::new(0.0, 0.0),
            Point::new(4.0, 0.0),
            Point::new(4.0, 4.0),
            Point::new(0.0, 4.0),
        ]).unwrap();

        assert!(polygon.contains_point(&Point::new(2.0, 2.0)));
        assert!(!polygon.contains_point(&Point::new(5.0, 5.0)));
    }

    #[test]
    fn test_polygon_convex() {
        // Square is convex
        let square = Polygon::new(vec![
            Point::new(0.0, 0.0),
            Point::new(2.0, 0.0),
            Point::new(2.0, 2.0),
            Point::new(0.0, 2.0),
        ]).unwrap();
        assert!(square.is_convex());

        // Concave polygon
        let concave = Polygon::new(vec![
            Point::new(0.0, 0.0),
            Point::new(2.0, 0.0),
            Point::new(1.0, 1.0), // Inward point
            Point::new(2.0, 2.0),
            Point::new(0.0, 2.0),
        ]).unwrap();
        assert!(!concave.is_convex());
    }

    #[test]
    fn test_regular_polygon() {
        let hexagon = Polygon::regular(6, Point::origin(), 1.0).unwrap();
        assert_eq!(hexagon.num_vertices(), 6);
    }

    #[test]
    fn test_circle_area() {
        let circle = Circle::new(Point::origin(), 1.0).unwrap();
        assert!((circle.area() - PI).abs() < 1e-10);
    }

    #[test]
    fn test_circle_contains() {
        let circle = Circle::new(Point::origin(), 5.0).unwrap();

        assert!(circle.contains_point(&Point::new(0.0, 0.0)));
        assert!(circle.contains_point(&Point::new(3.0, 4.0)));
        assert!(!circle.contains_point(&Point::new(10.0, 0.0)));
    }

    #[test]
    fn test_circle_overlaps() {
        let c1 = Circle::new(Point::new(0.0, 0.0), 5.0).unwrap();
        let c2 = Circle::new(Point::new(8.0, 0.0), 5.0).unwrap();
        let c3 = Circle::new(Point::new(20.0, 0.0), 5.0).unwrap();

        assert!(c1.overlaps(&c2));
        assert!(!c1.overlaps(&c3));
    }

    #[test]
    fn test_circle_from_three_points() {
        let p1 = Point::new(0.0, 0.0);
        let p2 = Point::new(2.0, 0.0);
        let p3 = Point::new(1.0, 1.0);

        let circle = Circle::from_three_points(&p1, &p2, &p3).unwrap();

        // All points should be on the circle
        assert!((circle.center.distance(&p1) - circle.radius).abs() < 1e-10);
        assert!((circle.center.distance(&p2) - circle.radius).abs() < 1e-10);
        assert!((circle.center.distance(&p3) - circle.radius).abs() < 1e-10);
    }

    #[test]
    fn test_error_display() {
        let errors = vec![
            GeometricError::ParseError("bad".to_string()),
            GeometricError::InvalidPolygon("too few".to_string()),
            GeometricError::InvalidCircle("negative".to_string()),
        ];

        for error in errors {
            let msg = format!("{}", error);
            assert!(!msg.is_empty());
        }
    }
}
