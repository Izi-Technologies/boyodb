//! Geospatial support for BoyoDB
//!
//! This module provides geospatial data types and functions compatible with PostGIS/OGC standards.
//! Supports Point, LineString, Polygon, and collection types with WKT/WKB encoding.

use std::f64::consts::PI;

/// Spatial Reference System ID (SRID)
/// 4326 = WGS84 (GPS coordinates)
/// 3857 = Web Mercator (Google Maps)
pub type Srid = i32;

/// Well-Known Binary (WKB) type codes
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WkbType {
    Point = 1,
    LineString = 2,
    Polygon = 3,
    MultiPoint = 4,
    MultiLineString = 5,
    MultiPolygon = 6,
    GeometryCollection = 7,
}

/// A 2D coordinate
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Coordinate {
    pub x: f64, // longitude for geographic
    pub y: f64, // latitude for geographic
}

impl Coordinate {
    pub fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }

    /// Calculate Euclidean distance to another coordinate
    pub fn distance_euclidean(&self, other: &Coordinate) -> f64 {
        ((self.x - other.x).powi(2) + (self.y - other.y).powi(2)).sqrt()
    }

    /// Calculate great-circle distance in meters (Haversine formula)
    pub fn distance_sphere(&self, other: &Coordinate) -> f64 {
        const EARTH_RADIUS_M: f64 = 6_371_000.0;

        let lat1 = self.y.to_radians();
        let lat2 = other.y.to_radians();
        let dlat = (other.y - self.y).to_radians();
        let dlon = (other.x - self.x).to_radians();

        let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

        EARTH_RADIUS_M * c
    }
}

/// Bounding box (envelope)
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BoundingBox {
    pub min_x: f64,
    pub min_y: f64,
    pub max_x: f64,
    pub max_y: f64,
}

impl BoundingBox {
    pub fn new(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> Self {
        Self {
            min_x,
            min_y,
            max_x,
            max_y,
        }
    }

    /// Check if this box contains a point
    pub fn contains_point(&self, x: f64, y: f64) -> bool {
        x >= self.min_x && x <= self.max_x && y >= self.min_y && y <= self.max_y
    }

    /// Check if this box intersects another
    pub fn intersects(&self, other: &BoundingBox) -> bool {
        self.min_x <= other.max_x
            && self.max_x >= other.min_x
            && self.min_y <= other.max_y
            && self.max_y >= other.min_y
    }

    /// Check if this box contains another
    pub fn contains(&self, other: &BoundingBox) -> bool {
        self.min_x <= other.min_x
            && self.max_x >= other.max_x
            && self.min_y <= other.min_y
            && self.max_y >= other.max_y
    }

    /// Expand to include a point
    pub fn expand_to_include(&mut self, x: f64, y: f64) {
        self.min_x = self.min_x.min(x);
        self.min_y = self.min_y.min(y);
        self.max_x = self.max_x.max(x);
        self.max_y = self.max_y.max(y);
    }

    /// Expand to include another box
    pub fn expand_to_include_box(&mut self, other: &BoundingBox) {
        self.min_x = self.min_x.min(other.min_x);
        self.min_y = self.min_y.min(other.min_y);
        self.max_x = self.max_x.max(other.max_x);
        self.max_y = self.max_y.max(other.max_y);
    }

    /// Calculate area
    pub fn area(&self) -> f64 {
        (self.max_x - self.min_x) * (self.max_y - self.min_y)
    }

    /// Get center point
    pub fn center(&self) -> Coordinate {
        Coordinate {
            x: (self.min_x + self.max_x) / 2.0,
            y: (self.min_y + self.max_y) / 2.0,
        }
    }
}

/// Geometry types
#[derive(Debug, Clone, PartialEq)]
pub enum Geometry {
    Point(Point),
    LineString(LineString),
    Polygon(Polygon),
    MultiPoint(MultiPoint),
    MultiLineString(MultiLineString),
    MultiPolygon(MultiPolygon),
    GeometryCollection(GeometryCollection),
}

impl Geometry {
    /// Get the geometry type
    pub fn geometry_type(&self) -> &'static str {
        match self {
            Geometry::Point(_) => "Point",
            Geometry::LineString(_) => "LineString",
            Geometry::Polygon(_) => "Polygon",
            Geometry::MultiPoint(_) => "MultiPoint",
            Geometry::MultiLineString(_) => "MultiLineString",
            Geometry::MultiPolygon(_) => "MultiPolygon",
            Geometry::GeometryCollection(_) => "GeometryCollection",
        }
    }

    /// Get bounding box
    pub fn bbox(&self) -> Option<BoundingBox> {
        match self {
            Geometry::Point(p) => Some(BoundingBox::new(p.x, p.y, p.x, p.y)),
            Geometry::LineString(ls) => ls.bbox(),
            Geometry::Polygon(poly) => poly.bbox(),
            Geometry::MultiPoint(mp) => mp.bbox(),
            Geometry::MultiLineString(mls) => mls.bbox(),
            Geometry::MultiPolygon(mp) => mp.bbox(),
            Geometry::GeometryCollection(gc) => gc.bbox(),
        }
    }

    /// Check if geometry is empty
    pub fn is_empty(&self) -> bool {
        match self {
            Geometry::Point(_) => false,
            Geometry::LineString(ls) => ls.points.is_empty(),
            Geometry::Polygon(poly) => poly.exterior.points.is_empty(),
            Geometry::MultiPoint(mp) => mp.points.is_empty(),
            Geometry::MultiLineString(mls) => mls.lines.is_empty(),
            Geometry::MultiPolygon(mp) => mp.polygons.is_empty(),
            Geometry::GeometryCollection(gc) => gc.geometries.is_empty(),
        }
    }

    /// Convert to WKT string
    pub fn to_wkt(&self) -> String {
        match self {
            Geometry::Point(p) => p.to_wkt(),
            Geometry::LineString(ls) => ls.to_wkt(),
            Geometry::Polygon(poly) => poly.to_wkt(),
            Geometry::MultiPoint(mp) => mp.to_wkt(),
            Geometry::MultiLineString(mls) => mls.to_wkt(),
            Geometry::MultiPolygon(mp) => mp.to_wkt(),
            Geometry::GeometryCollection(gc) => gc.to_wkt(),
        }
    }

    /// Parse from WKT string
    pub fn from_wkt(wkt: &str) -> Result<Self, GeoError> {
        let wkt = wkt.trim().to_uppercase();

        if wkt.starts_with("POINT") {
            Ok(Geometry::Point(Point::from_wkt(&wkt)?))
        } else if wkt.starts_with("LINESTRING") {
            Ok(Geometry::LineString(LineString::from_wkt(&wkt)?))
        } else if wkt.starts_with("POLYGON") {
            Ok(Geometry::Polygon(Polygon::from_wkt(&wkt)?))
        } else if wkt.starts_with("MULTIPOINT") {
            Ok(Geometry::MultiPoint(MultiPoint::from_wkt(&wkt)?))
        } else if wkt.starts_with("MULTILINESTRING") {
            Ok(Geometry::MultiLineString(MultiLineString::from_wkt(&wkt)?))
        } else if wkt.starts_with("MULTIPOLYGON") {
            Ok(Geometry::MultiPolygon(MultiPolygon::from_wkt(&wkt)?))
        } else {
            Err(GeoError::ParseError(format!(
                "Unknown geometry type: {}",
                wkt
            )))
        }
    }

    /// Calculate distance to another geometry
    pub fn distance(&self, other: &Geometry) -> f64 {
        match (self, other) {
            (Geometry::Point(p1), Geometry::Point(p2)) => {
                p1.coord().distance_euclidean(&p2.coord())
            }
            (Geometry::Point(p), Geometry::LineString(ls)) => ls.distance_to_point(&p.coord()),
            (Geometry::LineString(ls), Geometry::Point(p)) => ls.distance_to_point(&p.coord()),
            (Geometry::Point(p), Geometry::Polygon(poly)) => poly.distance_to_point(&p.coord()),
            (Geometry::Polygon(poly), Geometry::Point(p)) => poly.distance_to_point(&p.coord()),
            // For complex cases, use bounding box approximation
            _ => {
                let bb1 = self.bbox();
                let bb2 = other.bbox();
                match (bb1, bb2) {
                    (Some(b1), Some(b2)) => b1.center().distance_euclidean(&b2.center()),
                    _ => f64::MAX,
                }
            }
        }
    }

    /// Check if this geometry contains another
    pub fn contains(&self, other: &Geometry) -> bool {
        match (self, other) {
            (Geometry::Polygon(poly), Geometry::Point(p)) => poly.contains_point(&p.coord()),
            (Geometry::Polygon(poly1), Geometry::Polygon(poly2)) => {
                // Simplified: check if all vertices of poly2 are inside poly1
                poly2
                    .exterior
                    .points
                    .iter()
                    .all(|p| poly1.contains_point(p))
            }
            _ => {
                // Use bounding box containment as approximation
                match (self.bbox(), other.bbox()) {
                    (Some(b1), Some(b2)) => b1.contains(&b2),
                    _ => false,
                }
            }
        }
    }

    /// Check if geometries intersect
    pub fn intersects(&self, other: &Geometry) -> bool {
        // First check bounding boxes
        match (self.bbox(), other.bbox()) {
            (Some(b1), Some(b2)) => {
                if !b1.intersects(&b2) {
                    return false;
                }
            }
            _ => return false,
        }

        // More detailed check based on geometry types
        match (self, other) {
            (Geometry::Point(p1), Geometry::Point(p2)) => p1.x == p2.x && p1.y == p2.y,
            (Geometry::Point(p), Geometry::Polygon(poly))
            | (Geometry::Polygon(poly), Geometry::Point(p)) => poly.contains_point(&p.coord()),
            (Geometry::Polygon(p1), Geometry::Polygon(p2)) => {
                // Check if any vertex of one is inside the other
                p1.exterior.points.iter().any(|p| p2.contains_point(p))
                    || p2.exterior.points.iter().any(|p| p1.contains_point(p))
            }
            _ => true, // Bounding boxes intersect, assume intersection
        }
    }

    /// Calculate area (for polygons)
    pub fn area(&self) -> f64 {
        match self {
            Geometry::Polygon(poly) => poly.area(),
            Geometry::MultiPolygon(mp) => mp.polygons.iter().map(|p| p.area()).sum(),
            _ => 0.0,
        }
    }

    /// Calculate length (for lines)
    pub fn length(&self) -> f64 {
        match self {
            Geometry::LineString(ls) => ls.length(),
            Geometry::MultiLineString(mls) => mls.lines.iter().map(|l| l.length()).sum(),
            _ => 0.0,
        }
    }

    /// Get centroid
    pub fn centroid(&self) -> Option<Point> {
        match self {
            Geometry::Point(p) => Some(p.clone()),
            Geometry::LineString(ls) => ls.centroid(),
            Geometry::Polygon(poly) => poly.centroid(),
            _ => self
                .bbox()
                .map(|bb| Point::new(bb.center().x, bb.center().y)),
        }
    }

    /// Create a buffer around the geometry
    pub fn buffer(&self, distance: f64, segments: usize) -> Geometry {
        match self {
            Geometry::Point(p) => {
                // Create a circle approximation
                let mut points = Vec::with_capacity(segments + 1);
                for i in 0..segments {
                    let angle = 2.0 * PI * (i as f64) / (segments as f64);
                    points.push(Coordinate::new(
                        p.x + distance * angle.cos(),
                        p.y + distance * angle.sin(),
                    ));
                }
                points.push(points[0]); // Close the ring
                Geometry::Polygon(Polygon {
                    exterior: LineString {
                        points,
                        srid: p.srid,
                    },
                    holes: Vec::new(),
                    srid: p.srid,
                })
            }
            // For other geometries, use bounding box expansion as approximation
            _ => {
                if let Some(mut bb) = self.bbox() {
                    bb.min_x -= distance;
                    bb.min_y -= distance;
                    bb.max_x += distance;
                    bb.max_y += distance;
                    Geometry::Polygon(Polygon {
                        exterior: LineString {
                            points: vec![
                                Coordinate::new(bb.min_x, bb.min_y),
                                Coordinate::new(bb.max_x, bb.min_y),
                                Coordinate::new(bb.max_x, bb.max_y),
                                Coordinate::new(bb.min_x, bb.max_y),
                                Coordinate::new(bb.min_x, bb.min_y),
                            ],
                            srid: None,
                        },
                        holes: Vec::new(),
                        srid: None,
                    })
                } else {
                    self.clone()
                }
            }
        }
    }

    /// Set SRID
    pub fn set_srid(&mut self, srid: Srid) {
        match self {
            Geometry::Point(p) => p.srid = Some(srid),
            Geometry::LineString(ls) => ls.srid = Some(srid),
            Geometry::Polygon(poly) => poly.srid = Some(srid),
            Geometry::MultiPoint(mp) => mp.srid = Some(srid),
            Geometry::MultiLineString(mls) => mls.srid = Some(srid),
            Geometry::MultiPolygon(mp) => mp.srid = Some(srid),
            Geometry::GeometryCollection(gc) => gc.srid = Some(srid),
        }
    }

    /// Get SRID
    pub fn srid(&self) -> Option<Srid> {
        match self {
            Geometry::Point(p) => p.srid,
            Geometry::LineString(ls) => ls.srid,
            Geometry::Polygon(poly) => poly.srid,
            Geometry::MultiPoint(mp) => mp.srid,
            Geometry::MultiLineString(mls) => mls.srid,
            Geometry::MultiPolygon(mp) => mp.srid,
            Geometry::GeometryCollection(gc) => gc.srid,
        }
    }
}

/// Point geometry
#[derive(Debug, Clone, PartialEq)]
pub struct Point {
    pub x: f64,
    pub y: f64,
    pub srid: Option<Srid>,
}

impl Point {
    pub fn new(x: f64, y: f64) -> Self {
        Self { x, y, srid: None }
    }

    pub fn new_with_srid(x: f64, y: f64, srid: Srid) -> Self {
        Self {
            x,
            y,
            srid: Some(srid),
        }
    }

    pub fn coord(&self) -> Coordinate {
        Coordinate::new(self.x, self.y)
    }

    pub fn to_wkt(&self) -> String {
        format!("POINT({} {})", self.x, self.y)
    }

    pub fn from_wkt(wkt: &str) -> Result<Self, GeoError> {
        let content = extract_parens(wkt, "POINT")?;
        let coords = parse_coordinate(&content)?;
        Ok(Self {
            x: coords.x,
            y: coords.y,
            srid: None,
        })
    }
}

/// LineString geometry
#[derive(Debug, Clone, PartialEq)]
pub struct LineString {
    pub points: Vec<Coordinate>,
    pub srid: Option<Srid>,
}

impl LineString {
    pub fn new(points: Vec<Coordinate>) -> Self {
        Self { points, srid: None }
    }

    pub fn bbox(&self) -> Option<BoundingBox> {
        if self.points.is_empty() {
            return None;
        }
        let mut bb = BoundingBox::new(
            self.points[0].x,
            self.points[0].y,
            self.points[0].x,
            self.points[0].y,
        );
        for p in &self.points[1..] {
            bb.expand_to_include(p.x, p.y);
        }
        Some(bb)
    }

    pub fn length(&self) -> f64 {
        self.points
            .windows(2)
            .map(|w| w[0].distance_euclidean(&w[1]))
            .sum()
    }

    pub fn centroid(&self) -> Option<Point> {
        if self.points.is_empty() {
            return None;
        }
        let sum_x: f64 = self.points.iter().map(|p| p.x).sum();
        let sum_y: f64 = self.points.iter().map(|p| p.y).sum();
        let n = self.points.len() as f64;
        Some(Point::new(sum_x / n, sum_y / n))
    }

    pub fn distance_to_point(&self, point: &Coordinate) -> f64 {
        if self.points.is_empty() {
            return f64::MAX;
        }

        self.points
            .windows(2)
            .map(|w| point_to_segment_distance(point, &w[0], &w[1]))
            .fold(f64::MAX, |a, b| a.min(b))
    }

    pub fn to_wkt(&self) -> String {
        let coords: Vec<String> = self
            .points
            .iter()
            .map(|c| format!("{} {}", c.x, c.y))
            .collect();
        format!("LINESTRING({})", coords.join(", "))
    }

    pub fn from_wkt(wkt: &str) -> Result<Self, GeoError> {
        let content = extract_parens(wkt, "LINESTRING")?;
        let points = parse_coordinates(&content)?;
        Ok(Self { points, srid: None })
    }
}

/// Polygon geometry
#[derive(Debug, Clone, PartialEq)]
pub struct Polygon {
    pub exterior: LineString,
    pub holes: Vec<LineString>,
    pub srid: Option<Srid>,
}

impl Polygon {
    pub fn new(exterior: Vec<Coordinate>) -> Self {
        Self {
            exterior: LineString::new(exterior),
            holes: Vec::new(),
            srid: None,
        }
    }

    pub fn new_with_holes(exterior: Vec<Coordinate>, holes: Vec<Vec<Coordinate>>) -> Self {
        Self {
            exterior: LineString::new(exterior),
            holes: holes.into_iter().map(LineString::new).collect(),
            srid: None,
        }
    }

    pub fn bbox(&self) -> Option<BoundingBox> {
        self.exterior.bbox()
    }

    pub fn area(&self) -> f64 {
        let exterior_area = shoelace_area(&self.exterior.points);
        let holes_area: f64 = self.holes.iter().map(|h| shoelace_area(&h.points)).sum();
        (exterior_area - holes_area).abs()
    }

    pub fn centroid(&self) -> Option<Point> {
        if self.exterior.points.is_empty() {
            return None;
        }

        let mut sum_x = 0.0;
        let mut sum_y = 0.0;
        let mut area_sum = 0.0;

        for i in 0..self.exterior.points.len() - 1 {
            let p0 = &self.exterior.points[i];
            let p1 = &self.exterior.points[i + 1];
            let cross = p0.x * p1.y - p1.x * p0.y;
            area_sum += cross;
            sum_x += (p0.x + p1.x) * cross;
            sum_y += (p0.y + p1.y) * cross;
        }

        if area_sum.abs() < 1e-10 {
            return self.exterior.centroid();
        }

        Some(Point::new(
            sum_x / (3.0 * area_sum),
            sum_y / (3.0 * area_sum),
        ))
    }

    pub fn contains_point(&self, point: &Coordinate) -> bool {
        if !point_in_ring(point, &self.exterior.points) {
            return false;
        }
        // Check holes
        for hole in &self.holes {
            if point_in_ring(point, &hole.points) {
                return false;
            }
        }
        true
    }

    pub fn distance_to_point(&self, point: &Coordinate) -> f64 {
        if self.contains_point(point) {
            return 0.0;
        }
        self.exterior.distance_to_point(point)
    }

    pub fn to_wkt(&self) -> String {
        let mut rings = vec![ring_to_wkt(&self.exterior.points)];
        for hole in &self.holes {
            rings.push(ring_to_wkt(&hole.points));
        }
        format!("POLYGON({})", rings.join(", "))
    }

    pub fn from_wkt(wkt: &str) -> Result<Self, GeoError> {
        let content = extract_parens(wkt, "POLYGON")?;
        let rings = parse_rings(&content)?;
        if rings.is_empty() {
            return Err(GeoError::ParseError(
                "Polygon must have at least one ring".into(),
            ));
        }
        Ok(Self {
            exterior: LineString::new(rings[0].clone()),
            holes: rings[1..]
                .iter()
                .map(|r| LineString::new(r.clone()))
                .collect(),
            srid: None,
        })
    }
}

/// MultiPoint geometry
#[derive(Debug, Clone, PartialEq)]
pub struct MultiPoint {
    pub points: Vec<Point>,
    pub srid: Option<Srid>,
}

impl MultiPoint {
    pub fn bbox(&self) -> Option<BoundingBox> {
        if self.points.is_empty() {
            return None;
        }
        let mut bb = BoundingBox::new(
            self.points[0].x,
            self.points[0].y,
            self.points[0].x,
            self.points[0].y,
        );
        for p in &self.points[1..] {
            bb.expand_to_include(p.x, p.y);
        }
        Some(bb)
    }

    pub fn to_wkt(&self) -> String {
        let coords: Vec<String> = self
            .points
            .iter()
            .map(|p| format!("({} {})", p.x, p.y))
            .collect();
        format!("MULTIPOINT({})", coords.join(", "))
    }

    pub fn from_wkt(wkt: &str) -> Result<Self, GeoError> {
        let content = extract_parens(wkt, "MULTIPOINT")?;
        let mut points = Vec::new();
        for part in content.split("),") {
            let part = part.trim().trim_start_matches('(').trim_end_matches(')');
            let coord = parse_coordinate(part)?;
            points.push(Point::new(coord.x, coord.y));
        }
        Ok(Self { points, srid: None })
    }
}

/// MultiLineString geometry
#[derive(Debug, Clone, PartialEq)]
pub struct MultiLineString {
    pub lines: Vec<LineString>,
    pub srid: Option<Srid>,
}

impl MultiLineString {
    pub fn bbox(&self) -> Option<BoundingBox> {
        let mut result: Option<BoundingBox> = None;
        for line in &self.lines {
            if let Some(bb) = line.bbox() {
                result = Some(match result {
                    Some(mut r) => {
                        r.expand_to_include_box(&bb);
                        r
                    }
                    None => bb,
                });
            }
        }
        result
    }

    pub fn to_wkt(&self) -> String {
        let lines: Vec<String> = self
            .lines
            .iter()
            .map(|l| {
                let coords: Vec<String> = l
                    .points
                    .iter()
                    .map(|c| format!("{} {}", c.x, c.y))
                    .collect();
                format!("({})", coords.join(", "))
            })
            .collect();
        format!("MULTILINESTRING({})", lines.join(", "))
    }

    pub fn from_wkt(wkt: &str) -> Result<Self, GeoError> {
        let content = extract_parens(wkt, "MULTILINESTRING")?;
        let rings = parse_rings(&content)?;
        Ok(Self {
            lines: rings.into_iter().map(LineString::new).collect(),
            srid: None,
        })
    }
}

/// MultiPolygon geometry
#[derive(Debug, Clone, PartialEq)]
pub struct MultiPolygon {
    pub polygons: Vec<Polygon>,
    pub srid: Option<Srid>,
}

impl MultiPolygon {
    pub fn bbox(&self) -> Option<BoundingBox> {
        let mut result: Option<BoundingBox> = None;
        for poly in &self.polygons {
            if let Some(bb) = poly.bbox() {
                result = Some(match result {
                    Some(mut r) => {
                        r.expand_to_include_box(&bb);
                        r
                    }
                    None => bb,
                });
            }
        }
        result
    }

    pub fn to_wkt(&self) -> String {
        let polys: Vec<String> = self
            .polygons
            .iter()
            .map(|p| {
                let mut rings = vec![ring_to_wkt(&p.exterior.points)];
                for hole in &p.holes {
                    rings.push(ring_to_wkt(&hole.points));
                }
                format!("({})", rings.join(", "))
            })
            .collect();
        format!("MULTIPOLYGON({})", polys.join(", "))
    }

    pub fn from_wkt(wkt: &str) -> Result<Self, GeoError> {
        // Simplified parser - full implementation would be more complex
        let content = extract_parens(wkt, "MULTIPOLYGON")?;
        let mut polygons = Vec::new();

        // Parse nested polygons - this is a simplified version
        let mut depth = 0;
        let mut current = String::new();

        for c in content.chars() {
            match c {
                '(' => {
                    depth += 1;
                    if depth > 1 {
                        current.push(c);
                    }
                }
                ')' => {
                    depth -= 1;
                    if depth > 0 {
                        current.push(c);
                    } else if !current.trim().is_empty() {
                        let poly_wkt = format!("POLYGON({})", current.trim());
                        polygons.push(Polygon::from_wkt(&poly_wkt)?);
                        current.clear();
                    }
                }
                ',' if depth == 1 => {
                    // Skip comma between polygons
                }
                _ => {
                    current.push(c);
                }
            }
        }

        Ok(Self {
            polygons,
            srid: None,
        })
    }
}

/// GeometryCollection
#[derive(Debug, Clone, PartialEq)]
pub struct GeometryCollection {
    pub geometries: Vec<Geometry>,
    pub srid: Option<Srid>,
}

impl GeometryCollection {
    pub fn bbox(&self) -> Option<BoundingBox> {
        let mut result: Option<BoundingBox> = None;
        for geom in &self.geometries {
            if let Some(bb) = geom.bbox() {
                result = Some(match result {
                    Some(mut r) => {
                        r.expand_to_include_box(&bb);
                        r
                    }
                    None => bb,
                });
            }
        }
        result
    }

    pub fn to_wkt(&self) -> String {
        let geoms: Vec<String> = self.geometries.iter().map(|g| g.to_wkt()).collect();
        format!("GEOMETRYCOLLECTION({})", geoms.join(", "))
    }
}

/// Geospatial errors
#[derive(Debug, Clone)]
pub enum GeoError {
    ParseError(String),
    InvalidGeometry(String),
    UnsupportedOperation(String),
}

impl std::fmt::Display for GeoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GeoError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            GeoError::InvalidGeometry(msg) => write!(f, "Invalid geometry: {}", msg),
            GeoError::UnsupportedOperation(msg) => write!(f, "Unsupported operation: {}", msg),
        }
    }
}

impl std::error::Error for GeoError {}

// Helper functions

fn extract_parens(wkt: &str, prefix: &str) -> Result<String, GeoError> {
    let wkt = wkt.trim();
    if !wkt.to_uppercase().starts_with(prefix) {
        return Err(GeoError::ParseError(format!("Expected {}", prefix)));
    }
    let start = wkt
        .find('(')
        .ok_or_else(|| GeoError::ParseError("Missing opening parenthesis".into()))?;
    let end = wkt
        .rfind(')')
        .ok_or_else(|| GeoError::ParseError("Missing closing parenthesis".into()))?;
    Ok(wkt[start + 1..end].to_string())
}

fn parse_coordinate(s: &str) -> Result<Coordinate, GeoError> {
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() < 2 {
        return Err(GeoError::ParseError(format!("Invalid coordinate: {}", s)));
    }
    let x = parts[0]
        .parse::<f64>()
        .map_err(|_| GeoError::ParseError(format!("Invalid x coordinate: {}", parts[0])))?;
    let y = parts[1]
        .parse::<f64>()
        .map_err(|_| GeoError::ParseError(format!("Invalid y coordinate: {}", parts[1])))?;
    Ok(Coordinate::new(x, y))
}

fn parse_coordinates(s: &str) -> Result<Vec<Coordinate>, GeoError> {
    s.split(',')
        .map(|part| parse_coordinate(part.trim()))
        .collect()
}

fn parse_rings(s: &str) -> Result<Vec<Vec<Coordinate>>, GeoError> {
    let mut rings = Vec::new();
    let mut current = String::new();
    let mut depth = 0;

    for c in s.chars() {
        match c {
            '(' => {
                depth += 1;
                if depth > 1 {
                    current.push(c);
                }
            }
            ')' => {
                depth -= 1;
                if depth == 0 && !current.trim().is_empty() {
                    rings.push(parse_coordinates(&current)?);
                    current.clear();
                } else if depth > 0 {
                    current.push(c);
                }
            }
            ',' if depth == 0 => {
                // Skip comma between rings
            }
            _ => {
                current.push(c);
            }
        }
    }

    Ok(rings)
}

fn ring_to_wkt(points: &[Coordinate]) -> String {
    let coords: Vec<String> = points.iter().map(|c| format!("{} {}", c.x, c.y)).collect();
    format!("({})", coords.join(", "))
}

fn shoelace_area(points: &[Coordinate]) -> f64 {
    if points.len() < 3 {
        return 0.0;
    }
    let mut area = 0.0;
    for i in 0..points.len() - 1 {
        area += points[i].x * points[i + 1].y;
        area -= points[i + 1].x * points[i].y;
    }
    area / 2.0
}

fn point_in_ring(point: &Coordinate, ring: &[Coordinate]) -> bool {
    if ring.len() < 3 {
        return false;
    }

    let mut inside = false;
    let mut j = ring.len() - 1;

    for i in 0..ring.len() {
        let xi = ring[i].x;
        let yi = ring[i].y;
        let xj = ring[j].x;
        let yj = ring[j].y;

        let intersect = ((yi > point.y) != (yj > point.y))
            && (point.x < (xj - xi) * (point.y - yi) / (yj - yi) + xi);

        if intersect {
            inside = !inside;
        }
        j = i;
    }

    inside
}

fn point_to_segment_distance(
    point: &Coordinate,
    seg_start: &Coordinate,
    seg_end: &Coordinate,
) -> f64 {
    let dx = seg_end.x - seg_start.x;
    let dy = seg_end.y - seg_start.y;

    if dx == 0.0 && dy == 0.0 {
        return point.distance_euclidean(seg_start);
    }

    let t = ((point.x - seg_start.x) * dx + (point.y - seg_start.y) * dy) / (dx * dx + dy * dy);
    let t = t.clamp(0.0, 1.0);

    let nearest = Coordinate::new(seg_start.x + t * dx, seg_start.y + t * dy);

    point.distance_euclidean(&nearest)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_wkt() {
        let point = Point::new(1.0, 2.0);
        assert_eq!(point.to_wkt(), "POINT(1 2)");

        let parsed = Point::from_wkt("POINT(3.5 4.5)").unwrap();
        assert_eq!(parsed.x, 3.5);
        assert_eq!(parsed.y, 4.5);
    }

    #[test]
    fn test_polygon_contains() {
        let poly = Polygon::new(vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 0.0),
            Coordinate::new(10.0, 10.0),
            Coordinate::new(0.0, 10.0),
            Coordinate::new(0.0, 0.0),
        ]);

        assert!(poly.contains_point(&Coordinate::new(5.0, 5.0)));
        assert!(!poly.contains_point(&Coordinate::new(15.0, 5.0)));
    }

    #[test]
    fn test_distance_sphere() {
        // New York to Los Angeles (approximately)
        let ny = Coordinate::new(-74.006, 40.7128);
        let la = Coordinate::new(-118.2437, 34.0522);

        let distance = ny.distance_sphere(&la);
        // Should be approximately 3940 km
        assert!(distance > 3_900_000.0 && distance < 4_000_000.0);
    }

    #[test]
    fn test_polygon_area() {
        // 10x10 square
        let poly = Polygon::new(vec![
            Coordinate::new(0.0, 0.0),
            Coordinate::new(10.0, 0.0),
            Coordinate::new(10.0, 10.0),
            Coordinate::new(0.0, 10.0),
            Coordinate::new(0.0, 0.0),
        ]);

        assert!((poly.area() - 100.0).abs() < 0.001);
    }
}
