// XML Support - XML parsing and XPath queries for BoyoDB
//
// Provides PostgreSQL-style XML support:
// - XML data type
// - XML parsing and validation
// - XPath queries
// - XML construction functions
// - XML to text/table conversion

use std::collections::HashMap;
use std::fmt;

// ============================================================================
// XML Document
// ============================================================================

/// An XML document or fragment
#[derive(Debug, Clone, PartialEq)]
pub struct XmlDocument {
    /// Root node
    pub root: Option<XmlNode>,
    /// XML declaration version
    pub version: Option<String>,
    /// Encoding
    pub encoding: Option<String>,
    /// Standalone declaration
    pub standalone: Option<bool>,
}

impl XmlDocument {
    /// Create an empty document
    pub fn new() -> Self {
        Self {
            root: None,
            version: Some("1.0".to_string()),
            encoding: Some("UTF-8".to_string()),
            standalone: None,
        }
    }

    /// Create a document with a root element
    pub fn with_root(root: XmlNode) -> Self {
        Self {
            root: Some(root),
            version: Some("1.0".to_string()),
            encoding: Some("UTF-8".to_string()),
            standalone: None,
        }
    }

    /// Parse XML from string
    pub fn parse(s: &str) -> Result<Self, XmlError> {
        let mut parser = XmlParser::new(s);
        parser.parse_document()
    }

    /// Check if document is well-formed
    pub fn is_well_formed(&self) -> bool {
        self.root.is_some()
    }

    /// Get the root element
    pub fn root_element(&self) -> Option<&XmlNode> {
        self.root.as_ref()
    }

    /// Convert to string with formatting
    pub fn to_string_formatted(&self, indent: usize) -> String {
        let mut output = String::new();

        // XML declaration
        output.push_str("<?xml");
        if let Some(ref v) = self.version {
            output.push_str(&format!(" version=\"{}\"", v));
        }
        if let Some(ref e) = self.encoding {
            output.push_str(&format!(" encoding=\"{}\"", e));
        }
        if let Some(s) = self.standalone {
            output.push_str(&format!(" standalone=\"{}\"", if s { "yes" } else { "no" }));
        }
        output.push_str("?>\n");

        // Root element
        if let Some(ref root) = self.root {
            output.push_str(&root.to_string_formatted(0, indent));
        }

        output
    }

    /// Execute an XPath query
    pub fn xpath(&self, query: &str) -> Result<Vec<XPathResult>, XmlError> {
        let xpath = XPath::parse(query)?;
        Ok(xpath.evaluate(self))
    }

    /// Get text content of all matching elements
    pub fn xpath_text(&self, query: &str) -> Result<Vec<String>, XmlError> {
        let results = self.xpath(query)?;
        Ok(results.into_iter().filter_map(|r| match r {
            XPathResult::Node(n) => Some(n.text_content()),
            XPathResult::Text(t) => Some(t),
            _ => None,
        }).collect())
    }
}

impl Default for XmlDocument {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for XmlDocument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string_formatted(2))
    }
}

// ============================================================================
// XML Node
// ============================================================================

/// Type of XML node
#[derive(Debug, Clone, PartialEq)]
pub enum XmlNodeType {
    Element,
    Text,
    Comment,
    CData,
    ProcessingInstruction,
}

/// An XML node (element, text, etc.)
#[derive(Debug, Clone, PartialEq)]
pub struct XmlNode {
    /// Node type
    pub node_type: XmlNodeType,
    /// Element name (for elements)
    pub name: Option<String>,
    /// Namespace URI
    pub namespace: Option<String>,
    /// Attributes (for elements)
    pub attributes: HashMap<String, String>,
    /// Child nodes
    pub children: Vec<XmlNode>,
    /// Text content (for text/comment/cdata nodes)
    pub text: Option<String>,
}

impl XmlNode {
    /// Create a new element node
    pub fn element(name: &str) -> Self {
        Self {
            node_type: XmlNodeType::Element,
            name: Some(name.to_string()),
            namespace: None,
            attributes: HashMap::new(),
            children: Vec::new(),
            text: None,
        }
    }

    /// Create a text node
    pub fn text(content: &str) -> Self {
        Self {
            node_type: XmlNodeType::Text,
            name: None,
            namespace: None,
            attributes: HashMap::new(),
            children: Vec::new(),
            text: Some(content.to_string()),
        }
    }

    /// Create a comment node
    pub fn comment(content: &str) -> Self {
        Self {
            node_type: XmlNodeType::Comment,
            name: None,
            namespace: None,
            attributes: HashMap::new(),
            children: Vec::new(),
            text: Some(content.to_string()),
        }
    }

    /// Create a CDATA node
    pub fn cdata(content: &str) -> Self {
        Self {
            node_type: XmlNodeType::CData,
            name: None,
            namespace: None,
            attributes: HashMap::new(),
            children: Vec::new(),
            text: Some(content.to_string()),
        }
    }

    /// Add an attribute
    pub fn with_attribute(mut self, name: &str, value: &str) -> Self {
        self.attributes.insert(name.to_string(), value.to_string());
        self
    }

    /// Set namespace
    pub fn with_namespace(mut self, ns: &str) -> Self {
        self.namespace = Some(ns.to_string());
        self
    }

    /// Add a child node
    pub fn with_child(mut self, child: XmlNode) -> Self {
        self.children.push(child);
        self
    }

    /// Add text content
    pub fn with_text(mut self, text: &str) -> Self {
        self.children.push(XmlNode::text(text));
        self
    }

    /// Get an attribute value
    pub fn get_attribute(&self, name: &str) -> Option<&String> {
        self.attributes.get(name)
    }

    /// Check if element has a specific attribute
    pub fn has_attribute(&self, name: &str) -> bool {
        self.attributes.contains_key(name)
    }

    /// Get all text content recursively
    pub fn text_content(&self) -> String {
        let mut content = String::new();

        if let Some(ref text) = self.text {
            content.push_str(text);
        }

        for child in &self.children {
            content.push_str(&child.text_content());
        }

        content
    }

    /// Find child elements by name
    pub fn find_children(&self, name: &str) -> Vec<&XmlNode> {
        self.children.iter()
            .filter(|c| c.name.as_deref() == Some(name))
            .collect()
    }

    /// Find first child element by name
    pub fn find_child(&self, name: &str) -> Option<&XmlNode> {
        self.children.iter()
            .find(|c| c.name.as_deref() == Some(name))
    }

    /// Find descendants by name (recursive)
    pub fn find_descendants(&self, name: &str) -> Vec<&XmlNode> {
        let mut result = Vec::new();
        self.collect_descendants(name, &mut result);
        result
    }

    fn collect_descendants<'a>(&'a self, name: &str, result: &mut Vec<&'a XmlNode>) {
        for child in &self.children {
            if child.name.as_deref() == Some(name) {
                result.push(child);
            }
            child.collect_descendants(name, result);
        }
    }

    /// Get element name
    pub fn element_name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Check if this is an element node
    pub fn is_element(&self) -> bool {
        matches!(self.node_type, XmlNodeType::Element)
    }

    /// Check if this is a text node
    pub fn is_text(&self) -> bool {
        matches!(self.node_type, XmlNodeType::Text)
    }

    /// Convert to formatted string
    pub fn to_string_formatted(&self, level: usize, indent: usize) -> String {
        let ind = " ".repeat(level * indent);

        match self.node_type {
            XmlNodeType::Element => {
                let name = self.name.as_deref().unwrap_or("unknown");
                let mut output = format!("{}<{}", ind, name);

                // Attributes
                for (k, v) in &self.attributes {
                    output.push_str(&format!(" {}=\"{}\"", k, escape_xml(v)));
                }

                if self.children.is_empty() {
                    output.push_str("/>\n");
                } else {
                    output.push_str(">\n");

                    for child in &self.children {
                        output.push_str(&child.to_string_formatted(level + 1, indent));
                    }

                    output.push_str(&format!("{}</{}>\n", ind, name));
                }

                output
            }
            XmlNodeType::Text => {
                let text = self.text.as_deref().unwrap_or("");
                if text.trim().is_empty() {
                    String::new()
                } else {
                    format!("{}{}\n", ind, escape_xml(text.trim()))
                }
            }
            XmlNodeType::Comment => {
                let text = self.text.as_deref().unwrap_or("");
                format!("{}<!--{}-->\n", ind, text)
            }
            XmlNodeType::CData => {
                let text = self.text.as_deref().unwrap_or("");
                format!("{}<![CDATA[{}]]>\n", ind, text)
            }
            XmlNodeType::ProcessingInstruction => {
                let name = self.name.as_deref().unwrap_or("unknown");
                let text = self.text.as_deref().unwrap_or("");
                format!("{}<?{} {}?>\n", ind, name, text)
            }
        }
    }
}

impl fmt::Display for XmlNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string_formatted(0, 2))
    }
}

/// Escape XML special characters
fn escape_xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Unescape XML entities
fn unescape_xml(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

// ============================================================================
// XML Parser
// ============================================================================

/// Simple XML parser
struct XmlParser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> XmlParser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn parse_document(&mut self) -> Result<XmlDocument, XmlError> {
        let mut doc = XmlDocument::new();

        self.skip_whitespace();

        // Parse XML declaration if present
        if self.peek_str("<?xml") {
            self.parse_xml_declaration(&mut doc)?;
        }

        self.skip_whitespace();

        // Parse root element
        if self.peek_char() == Some('<') && self.peek_char_at(1) != Some('!') && self.peek_char_at(1) != Some('?') {
            doc.root = Some(self.parse_element()?);
        }

        Ok(doc)
    }

    fn parse_xml_declaration(&mut self, doc: &mut XmlDocument) -> Result<(), XmlError> {
        self.expect_str("<?xml")?;
        self.skip_whitespace();

        // Parse attributes
        while !self.peek_str("?>") {
            let name = self.parse_name()?;
            self.skip_whitespace();
            self.expect_char('=')?;
            self.skip_whitespace();
            let value = self.parse_quoted_string()?;
            self.skip_whitespace();

            match name.as_str() {
                "version" => doc.version = Some(value),
                "encoding" => doc.encoding = Some(value),
                "standalone" => doc.standalone = Some(value == "yes"),
                _ => {}
            }
        }

        self.expect_str("?>")?;
        Ok(())
    }

    fn parse_element(&mut self) -> Result<XmlNode, XmlError> {
        self.expect_char('<')?;
        let name = self.parse_name()?;

        let mut node = XmlNode::element(&name);

        // Parse attributes
        self.skip_whitespace();
        while self.peek_char() != Some('>') && self.peek_char() != Some('/') {
            let attr_name = self.parse_name()?;
            self.skip_whitespace();
            self.expect_char('=')?;
            self.skip_whitespace();
            let attr_value = self.parse_quoted_string()?;
            node.attributes.insert(attr_name, attr_value);
            self.skip_whitespace();
        }

        // Self-closing tag?
        if self.peek_char() == Some('/') {
            self.advance();
            self.expect_char('>')?;
            return Ok(node);
        }

        self.expect_char('>')?;

        // Parse children
        loop {
            self.skip_whitespace_preserve();

            if self.peek_str("</") {
                break;
            }

            if self.peek_str("<!--") {
                node.children.push(self.parse_comment()?);
            } else if self.peek_str("<![CDATA[") {
                node.children.push(self.parse_cdata()?);
            } else if self.peek_char() == Some('<') {
                node.children.push(self.parse_element()?);
            } else {
                let text = self.parse_text()?;
                if !text.is_empty() {
                    node.children.push(XmlNode::text(&text));
                }
            }
        }

        // Closing tag
        self.expect_str("</")?;
        let closing_name = self.parse_name()?;
        if closing_name != name {
            return Err(XmlError::MismatchedTags(name, closing_name));
        }
        self.skip_whitespace();
        self.expect_char('>')?;

        Ok(node)
    }

    fn parse_comment(&mut self) -> Result<XmlNode, XmlError> {
        self.expect_str("<!--")?;
        let start = self.pos;
        while !self.peek_str("-->") && self.pos < self.input.len() {
            self.advance();
        }
        let content = &self.input[start..self.pos];
        self.expect_str("-->")?;
        Ok(XmlNode::comment(content))
    }

    fn parse_cdata(&mut self) -> Result<XmlNode, XmlError> {
        self.expect_str("<![CDATA[")?;
        let start = self.pos;
        while !self.peek_str("]]>") && self.pos < self.input.len() {
            self.advance();
        }
        let content = &self.input[start..self.pos];
        self.expect_str("]]>")?;
        Ok(XmlNode::cdata(content))
    }

    fn parse_text(&mut self) -> Result<String, XmlError> {
        let start = self.pos;
        while self.peek_char() != Some('<') && self.pos < self.input.len() {
            self.advance();
        }
        let text = &self.input[start..self.pos];
        Ok(unescape_xml(text))
    }

    fn parse_name(&mut self) -> Result<String, XmlError> {
        let start = self.pos;
        while let Some(c) = self.peek_char() {
            if c.is_alphanumeric() || c == '_' || c == '-' || c == ':' || c == '.' {
                self.advance();
            } else {
                break;
            }
        }
        if start == self.pos {
            return Err(XmlError::ParseError("Expected name".to_string()));
        }
        Ok(self.input[start..self.pos].to_string())
    }

    fn parse_quoted_string(&mut self) -> Result<String, XmlError> {
        let quote = self.peek_char()
            .ok_or_else(|| XmlError::ParseError("Expected quote".to_string()))?;
        if quote != '"' && quote != '\'' {
            return Err(XmlError::ParseError("Expected quoted string".to_string()));
        }
        self.advance();

        let start = self.pos;
        while let Some(c) = self.peek_char() {
            if c == quote {
                break;
            }
            self.advance();
        }
        let value = &self.input[start..self.pos];
        self.advance(); // Closing quote

        Ok(unescape_xml(value))
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek_char() {
            if c.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn skip_whitespace_preserve(&mut self) {
        // Skip whitespace but preserve for text nodes
    }

    fn peek_char(&self) -> Option<char> {
        self.input[self.pos..].chars().next()
    }

    fn peek_char_at(&self, offset: usize) -> Option<char> {
        self.input[self.pos..].chars().nth(offset)
    }

    fn peek_str(&self, s: &str) -> bool {
        self.input[self.pos..].starts_with(s)
    }

    fn advance(&mut self) {
        if let Some(c) = self.peek_char() {
            self.pos += c.len_utf8();
        }
    }

    fn expect_char(&mut self, expected: char) -> Result<(), XmlError> {
        if self.peek_char() == Some(expected) {
            self.advance();
            Ok(())
        } else {
            Err(XmlError::ParseError(format!("Expected '{}'", expected)))
        }
    }

    fn expect_str(&mut self, expected: &str) -> Result<(), XmlError> {
        if self.peek_str(expected) {
            self.pos += expected.len();
            Ok(())
        } else {
            Err(XmlError::ParseError(format!("Expected '{}'", expected)))
        }
    }
}

// ============================================================================
// XPath
// ============================================================================

/// XPath query result
#[derive(Debug, Clone)]
pub enum XPathResult {
    Node(XmlNode),
    Text(String),
    Number(f64),
    Boolean(bool),
    NodeSet(Vec<XmlNode>),
}

/// Simple XPath implementation
pub struct XPath {
    steps: Vec<XPathStep>,
}

#[derive(Debug, Clone)]
enum XPathStep {
    Root,
    Element(String),
    AnyElement,
    Descendant(String),
    AnyDescendant,
    Attribute(String),
    Text,
    Predicate(XPathPredicate),
}

#[derive(Debug, Clone)]
enum XPathPredicate {
    Position(usize),
    AttributeEquals(String, String),
    Last,
}

impl XPath {
    /// Parse an XPath expression
    pub fn parse(query: &str) -> Result<Self, XmlError> {
        let mut steps = Vec::new();
        let parts: Vec<&str> = query.split('/').collect();

        for (i, part) in parts.iter().enumerate() {
            if part.is_empty() {
                if i == 0 {
                    steps.push(XPathStep::Root);
                }
                continue;
            }

            // Check for descendant axis (//)
            // For // we get two consecutive empty strings in parts
            // e.g., "//foo" splits to ["", "", "foo"], so at i=2, parts[1]=""
            // But "/foo" splits to ["", "foo"], at i=1, parts[0]="" is the root marker
            // So we need i > 1 to distinguish // from /
            if i > 1 && parts.get(i - 1) == Some(&"") {
                if *part == "*" {
                    steps.push(XPathStep::AnyDescendant);
                } else {
                    steps.push(XPathStep::Descendant(part.to_string()));
                }
                continue;
            }

            // Parse the step
            if *part == "*" {
                steps.push(XPathStep::AnyElement);
            } else if part.starts_with('@') {
                steps.push(XPathStep::Attribute(part[1..].to_string()));
            } else if *part == "text()" {
                steps.push(XPathStep::Text);
            } else if let Some(bracket_pos) = part.find('[') {
                let name = &part[..bracket_pos];
                let predicate_str = &part[bracket_pos + 1..part.len() - 1];

                if !name.is_empty() {
                    steps.push(XPathStep::Element(name.to_string()));
                }

                // Parse predicate
                if let Ok(n) = predicate_str.parse::<usize>() {
                    steps.push(XPathStep::Predicate(XPathPredicate::Position(n)));
                } else if predicate_str == "last()" {
                    steps.push(XPathStep::Predicate(XPathPredicate::Last));
                } else if let Some(eq_pos) = predicate_str.find('=') {
                    let attr = predicate_str[1..eq_pos].to_string();
                    let value = predicate_str[eq_pos + 2..predicate_str.len() - 1].to_string();
                    steps.push(XPathStep::Predicate(XPathPredicate::AttributeEquals(attr, value)));
                }
            } else {
                steps.push(XPathStep::Element(part.to_string()));
            }
        }

        Ok(XPath { steps })
    }

    /// Evaluate the XPath on a document
    pub fn evaluate(&self, doc: &XmlDocument) -> Vec<XPathResult> {
        let mut results: Vec<&XmlNode> = Vec::new();
        let mut at_document_level = false;

        for step in &self.steps {
            match step {
                XPathStep::Root => {
                    // Mark that we're at document level, waiting for first element
                    at_document_level = true;
                    results.clear();
                }
                XPathStep::Element(name) if at_document_level => {
                    // At document level, check if root element matches this name
                    if let Some(ref root) = doc.root {
                        if root.name.as_deref() == Some(name.as_str()) {
                            results.push(root);
                        }
                    }
                    at_document_level = false;
                }
                XPathStep::AnyElement if at_document_level => {
                    // At document level, select root element
                    if let Some(ref root) = doc.root {
                        if root.is_element() {
                            results.push(root);
                        }
                    }
                    at_document_level = false;
                }
                _ => {
                    // Normal step processing - if still at document level with no match, start from root
                    if at_document_level {
                        if let Some(ref root) = doc.root {
                            results.push(root);
                        }
                        at_document_level = false;
                    }
                    results = self.apply_step(step, &results);
                }
            }
        }

        // If we only had Root step and nothing else, return the root
        if at_document_level {
            if let Some(ref root) = doc.root {
                results.push(root);
            }
        }

        results.into_iter()
            .map(|n| XPathResult::Node(n.clone()))
            .collect()
    }

    fn apply_step<'a>(&self, step: &XPathStep, nodes: &[&'a XmlNode]) -> Vec<&'a XmlNode> {
        let mut results = Vec::new();

        for node in nodes {
            match step {
                XPathStep::Root => {
                    results.push(*node);
                }
                XPathStep::Element(name) => {
                    for child in &node.children {
                        if child.name.as_deref() == Some(name.as_str()) {
                            results.push(child);
                        }
                    }
                }
                XPathStep::AnyElement => {
                    for child in &node.children {
                        if child.is_element() {
                            results.push(child);
                        }
                    }
                }
                XPathStep::Descendant(name) => {
                    self.collect_descendants_by_name(*node, name, &mut results);
                }
                XPathStep::AnyDescendant => {
                    self.collect_all_descendants(*node, &mut results);
                }
                XPathStep::Attribute(_name) => {
                    // Handled separately
                }
                XPathStep::Text => {
                    for child in &node.children {
                        if child.is_text() {
                            results.push(child);
                        }
                    }
                }
                XPathStep::Predicate(pred) => {
                    match pred {
                        XPathPredicate::Position(n) => {
                            if *n > 0 && *n <= results.len() {
                                let item = results[*n - 1];
                                results.clear();
                                results.push(item);
                            } else {
                                results.clear();
                            }
                        }
                        XPathPredicate::Last => {
                            if let Some(last) = results.last() {
                                let item = *last;
                                results.clear();
                                results.push(item);
                            }
                        }
                        XPathPredicate::AttributeEquals(attr, value) => {
                            results.retain(|n| n.get_attribute(attr) == Some(value));
                        }
                    }
                }
            }
        }

        results
    }

    fn collect_descendants_by_name<'a>(&self, node: &'a XmlNode, name: &str, results: &mut Vec<&'a XmlNode>) {
        for child in &node.children {
            if child.name.as_deref() == Some(name) {
                results.push(child);
            }
            self.collect_descendants_by_name(child, name, results);
        }
    }

    fn collect_all_descendants<'a>(&self, node: &'a XmlNode, results: &mut Vec<&'a XmlNode>) {
        for child in &node.children {
            if child.is_element() {
                results.push(child);
            }
            self.collect_all_descendants(child, results);
        }
    }
}

// ============================================================================
// XML Functions
// ============================================================================

/// Create an XML element
pub fn xmlelement(name: &str, content: Option<&str>, attributes: &[(&str, &str)]) -> XmlNode {
    let mut node = XmlNode::element(name);
    for (k, v) in attributes {
        node.attributes.insert(k.to_string(), v.to_string());
    }
    if let Some(text) = content {
        node.children.push(XmlNode::text(text));
    }
    node
}

/// Create an XML forest (multiple root elements)
pub fn xmlforest(elements: &[(&str, &str)]) -> Vec<XmlNode> {
    elements.iter()
        .map(|(name, value)| {
            XmlNode::element(name).with_text(value)
        })
        .collect()
}

/// Concatenate XML nodes
pub fn xmlconcat(nodes: Vec<XmlNode>) -> XmlNode {
    let mut root = XmlNode::element("root");
    root.children = nodes;
    root
}

/// Check if XML is well-formed
pub fn xml_is_well_formed(s: &str) -> bool {
    XmlDocument::parse(s).is_ok()
}

/// Convert XML to table rows
pub fn xmltable(doc: &XmlDocument, row_xpath: &str, columns: &[(&str, &str)]) -> Result<Vec<Vec<String>>, XmlError> {
    let xpath = XPath::parse(row_xpath)?;
    let rows = xpath.evaluate(doc);

    let mut result = Vec::new();

    for row in rows {
        if let XPathResult::Node(ref node) = row {
            let mut row_data = Vec::new();
            for (_, col_xpath) in columns {
                let col_query = XPath::parse(col_xpath)?;
                let nodes = col_query.apply_step(
                    &XPathStep::Root,
                    &[node],
                );
                let value = nodes.first()
                    .map(|n| n.text_content())
                    .unwrap_or_default();
                row_data.push(value);
            }
            result.push(row_data);
        }
    }

    Ok(result)
}

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug)]
pub enum XmlError {
    ParseError(String),
    MismatchedTags(String, String),
    InvalidXPath(String),
    NotWellFormed,
}

impl fmt::Display for XmlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ParseError(s) => write!(f, "XML parse error: {}", s),
            Self::MismatchedTags(open, close) => {
                write!(f, "Mismatched tags: <{}> closed with </{}>", open, close)
            }
            Self::InvalidXPath(s) => write!(f, "Invalid XPath: {}", s),
            Self::NotWellFormed => write!(f, "XML is not well-formed"),
        }
    }
}

impl std::error::Error for XmlError {}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xml_parse_simple() {
        let xml = r#"<?xml version="1.0"?><root><child>Hello</child></root>"#;
        let doc = XmlDocument::parse(xml).unwrap();

        assert!(doc.is_well_formed());
        assert_eq!(doc.root.as_ref().unwrap().name, Some("root".to_string()));
    }

    #[test]
    fn test_xml_parse_with_attributes() {
        let xml = r#"<root attr1="value1" attr2="value2"><child/></root>"#;
        let doc = XmlDocument::parse(xml).unwrap();

        let root = doc.root.as_ref().unwrap();
        assert_eq!(root.get_attribute("attr1"), Some(&"value1".to_string()));
        assert_eq!(root.get_attribute("attr2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_xml_parse_nested() {
        let xml = r#"<root><parent><child>Text</child></parent></root>"#;
        let doc = XmlDocument::parse(xml).unwrap();

        let root = doc.root.as_ref().unwrap();
        let parent = root.find_child("parent").unwrap();
        let child = parent.find_child("child").unwrap();
        assert_eq!(child.text_content(), "Text");
    }

    #[test]
    fn test_xml_self_closing() {
        let xml = r#"<root><empty/></root>"#;
        let doc = XmlDocument::parse(xml).unwrap();

        let root = doc.root.as_ref().unwrap();
        let empty = root.find_child("empty").unwrap();
        assert!(empty.children.is_empty());
    }

    #[test]
    fn test_xml_node_creation() {
        let node = XmlNode::element("test")
            .with_attribute("id", "1")
            .with_text("Hello");

        assert_eq!(node.name, Some("test".to_string()));
        assert_eq!(node.get_attribute("id"), Some(&"1".to_string()));
        assert_eq!(node.text_content(), "Hello");
    }

    #[test]
    fn test_xml_text_content() {
        let xml = r#"<root>Hello <b>World</b>!</root>"#;
        let doc = XmlDocument::parse(xml).unwrap();

        let root = doc.root.as_ref().unwrap();
        // Note: This gets all text recursively
        assert!(root.text_content().contains("Hello"));
        assert!(root.text_content().contains("World"));
    }

    #[test]
    fn test_xpath_simple() {
        let xml = r#"<root><child>Text</child></root>"#;
        let doc = XmlDocument::parse(xml).unwrap();

        let results = doc.xpath("/root/child").unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_xpath_descendant() {
        let xml = r#"<root><a><b><target>Found</target></b></a></root>"#;
        let doc = XmlDocument::parse(xml).unwrap();

        let results = doc.xpath("//target").unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_xpath_text() {
        let xml = r#"<root><item>One</item><item>Two</item></root>"#;
        let doc = XmlDocument::parse(xml).unwrap();

        let texts = doc.xpath_text("/root/item").unwrap();
        assert_eq!(texts.len(), 2);
        assert!(texts.contains(&"One".to_string()));
        assert!(texts.contains(&"Two".to_string()));
    }

    #[test]
    fn test_xml_escape() {
        assert_eq!(escape_xml("<test>"), "&lt;test&gt;");
        assert_eq!(escape_xml("a & b"), "a &amp; b");
        assert_eq!(unescape_xml("&lt;test&gt;"), "<test>");
    }

    #[test]
    fn test_xmlelement() {
        let elem = xmlelement("book", Some("Title"), &[("id", "1")]);
        assert_eq!(elem.name, Some("book".to_string()));
        assert_eq!(elem.get_attribute("id"), Some(&"1".to_string()));
        assert_eq!(elem.text_content(), "Title");
    }

    #[test]
    fn test_xmlforest() {
        let forest = xmlforest(&[("name", "John"), ("age", "30")]);
        assert_eq!(forest.len(), 2);
        assert_eq!(forest[0].name, Some("name".to_string()));
        assert_eq!(forest[1].name, Some("age".to_string()));
    }

    #[test]
    fn test_xml_is_well_formed() {
        assert!(xml_is_well_formed("<root><child/></root>"));
        assert!(!xml_is_well_formed("<root><child></root>")); // Mismatched tags
    }

    #[test]
    fn test_xml_comment() {
        let xml = r#"<root><!-- This is a comment --><child/></root>"#;
        let doc = XmlDocument::parse(xml).unwrap();
        assert!(doc.is_well_formed());
    }

    #[test]
    fn test_xml_cdata() {
        let xml = r#"<root><![CDATA[Some <raw> content]]></root>"#;
        let doc = XmlDocument::parse(xml).unwrap();

        let root = doc.root.as_ref().unwrap();
        assert!(!root.children.is_empty());
    }

    #[test]
    fn test_find_descendants() {
        let xml = r#"<root><a><item>1</item></a><b><item>2</item></b></root>"#;
        let doc = XmlDocument::parse(xml).unwrap();

        let root = doc.root.as_ref().unwrap();
        let items = root.find_descendants("item");
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn test_xml_document_display() {
        let doc = XmlDocument::with_root(
            XmlNode::element("root")
                .with_attribute("version", "1.0")
                .with_child(XmlNode::element("child").with_text("Hello"))
        );

        let output = doc.to_string();
        assert!(output.contains("<?xml"));
        assert!(output.contains("<root"));
        assert!(output.contains("version=\"1.0\""));
        assert!(output.contains("<child>"));
        assert!(output.contains("Hello"));
    }

    #[test]
    fn test_error_display() {
        let errors = vec![
            XmlError::ParseError("bad".to_string()),
            XmlError::MismatchedTags("a".to_string(), "b".to_string()),
            XmlError::InvalidXPath("//".to_string()),
            XmlError::NotWellFormed,
        ];

        for error in errors {
            let msg = format!("{}", error);
            assert!(!msg.is_empty());
        }
    }
}
