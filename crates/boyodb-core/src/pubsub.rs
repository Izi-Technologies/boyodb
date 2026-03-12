//! LISTEN/NOTIFY Pub/Sub System
//!
//! PostgreSQL-compatible pub/sub messaging for inter-process communication.
//!
//! # Features
//! - LISTEN channel - subscribe to notifications
//! - UNLISTEN channel - unsubscribe from notifications
//! - NOTIFY channel, 'payload' - send notifications
//! - pg_notify(channel, payload) - function form
//! - Async notification delivery
//! - Channel wildcards (LISTEN *)
//!
//! # Example
//! ```sql
//! -- Session 1: Subscribe
//! LISTEN order_updates;
//!
//! -- Session 2: Publish
//! NOTIFY order_updates, 'order_id=12345';
//!
//! -- Session 1: Receives notification
//! -- Asynchronous Notification: channel=order_updates, payload=order_id=12345
//! ```

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

// ============================================================================
// Types and Errors
// ============================================================================

/// Unique identifier for a notification
pub type NotificationId = u64;

/// Unique identifier for a session/connection
pub type SessionId = u64;

/// Errors from the pub/sub system
#[derive(Debug, Clone)]
pub enum PubSubError {
    /// Channel name is invalid
    InvalidChannel(String),
    /// Session not found
    SessionNotFound(SessionId),
    /// Channel not found
    ChannelNotFound(String),
    /// Payload too large (max 8000 bytes like PostgreSQL)
    PayloadTooLarge { size: usize, max: usize },
    /// Too many pending notifications
    QueueFull { session_id: SessionId, count: usize },
    /// Internal error
    Internal(String),
}

impl std::fmt::Display for PubSubError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidChannel(name) => write!(f, "invalid channel name: {}", name),
            Self::SessionNotFound(id) => write!(f, "session not found: {}", id),
            Self::ChannelNotFound(name) => write!(f, "channel not found: {}", name),
            Self::PayloadTooLarge { size, max } => {
                write!(f, "payload too large: {} bytes (max {})", size, max)
            }
            Self::QueueFull { session_id, count } => {
                write!(
                    f,
                    "notification queue full for session {}: {} pending",
                    session_id, count
                )
            }
            Self::Internal(msg) => write!(f, "internal error: {}", msg),
        }
    }
}

impl std::error::Error for PubSubError {}

// ============================================================================
// Notification
// ============================================================================

/// A notification message
#[derive(Debug, Clone)]
pub struct Notification {
    /// Unique notification ID
    pub id: NotificationId,
    /// Channel name
    pub channel: String,
    /// Optional payload (max 8000 bytes)
    pub payload: Option<String>,
    /// Session that sent the notification
    pub sender_session_id: SessionId,
    /// Timestamp when notification was created
    pub timestamp: SystemTime,
}

impl Notification {
    /// Create a new notification
    pub fn new(
        id: NotificationId,
        channel: String,
        payload: Option<String>,
        sender_session_id: SessionId,
    ) -> Self {
        Self {
            id,
            channel,
            payload,
            sender_session_id,
            timestamp: SystemTime::now(),
        }
    }

    /// Format notification for display
    pub fn format(&self) -> String {
        match &self.payload {
            Some(p) => format!(
                "Asynchronous Notification: channel={}, payload={}",
                self.channel, p
            ),
            None => format!("Asynchronous Notification: channel={}", self.channel),
        }
    }
}

// ============================================================================
// Session State
// ============================================================================

/// Session's pub/sub state
#[derive(Debug)]
pub struct SessionPubSubState {
    /// Session ID
    pub session_id: SessionId,
    /// Channels this session is listening to
    pub listening_channels: HashSet<String>,
    /// Whether listening to all channels (LISTEN *)
    pub listen_all: bool,
    /// Pending notifications queue
    pub pending_notifications: VecDeque<Notification>,
    /// Maximum pending notifications (default 10000)
    pub max_pending: usize,
    /// Last notification ID received
    pub last_notification_id: NotificationId,
}

impl SessionPubSubState {
    /// Create new session state
    pub fn new(session_id: SessionId) -> Self {
        Self {
            session_id,
            listening_channels: HashSet::new(),
            listen_all: false,
            pending_notifications: VecDeque::new(),
            max_pending: 10000,
            last_notification_id: 0,
        }
    }

    /// Check if session is listening to a channel
    pub fn is_listening(&self, channel: &str) -> bool {
        self.listen_all || self.listening_channels.contains(channel)
    }

    /// Add a channel to listen to
    pub fn listen(&mut self, channel: &str) {
        if channel == "*" {
            self.listen_all = true;
        } else {
            self.listening_channels.insert(channel.to_string());
        }
    }

    /// Remove a channel
    pub fn unlisten(&mut self, channel: &str) {
        if channel == "*" {
            self.listen_all = false;
            self.listening_channels.clear();
        } else {
            self.listening_channels.remove(channel);
        }
    }

    /// Queue a notification
    pub fn queue_notification(&mut self, notification: Notification) -> Result<(), PubSubError> {
        if self.pending_notifications.len() >= self.max_pending {
            return Err(PubSubError::QueueFull {
                session_id: self.session_id,
                count: self.pending_notifications.len(),
            });
        }
        self.last_notification_id = notification.id;
        self.pending_notifications.push_back(notification);
        Ok(())
    }

    /// Get next pending notification
    pub fn poll_notification(&mut self) -> Option<Notification> {
        self.pending_notifications.pop_front()
    }

    /// Get all pending notifications
    pub fn drain_notifications(&mut self) -> Vec<Notification> {
        self.pending_notifications.drain(..).collect()
    }

    /// Check if there are pending notifications
    pub fn has_pending(&self) -> bool {
        !self.pending_notifications.is_empty()
    }

    /// Get pending count
    pub fn pending_count(&self) -> usize {
        self.pending_notifications.len()
    }
}

// ============================================================================
// Channel
// ============================================================================

/// A notification channel
#[derive(Debug)]
pub struct Channel {
    /// Channel name
    pub name: String,
    /// Sessions listening to this channel
    pub listeners: HashSet<SessionId>,
    /// Total notifications sent on this channel
    pub notification_count: u64,
    /// Last notification time
    pub last_notification: Option<SystemTime>,
    /// Channel created time
    pub created_at: SystemTime,
}

impl Channel {
    /// Create a new channel
    pub fn new(name: String) -> Self {
        Self {
            name,
            listeners: HashSet::new(),
            notification_count: 0,
            last_notification: None,
            created_at: SystemTime::now(),
        }
    }

    /// Add a listener
    pub fn add_listener(&mut self, session_id: SessionId) {
        self.listeners.insert(session_id);
    }

    /// Remove a listener
    pub fn remove_listener(&mut self, session_id: SessionId) {
        self.listeners.remove(&session_id);
    }

    /// Check if channel has listeners
    pub fn has_listeners(&self) -> bool {
        !self.listeners.is_empty()
    }
}

// ============================================================================
// Pub/Sub Manager
// ============================================================================

/// Statistics for the pub/sub system
#[derive(Debug, Clone, Default)]
pub struct PubSubStats {
    /// Total notifications sent
    pub total_notifications: u64,
    /// Total notifications delivered
    pub total_delivered: u64,
    /// Total notifications dropped (queue full)
    pub total_dropped: u64,
    /// Active channels
    pub active_channels: usize,
    /// Active sessions
    pub active_sessions: usize,
    /// Total listens
    pub total_listens: u64,
    /// Total unlistens
    pub total_unlistens: u64,
}

/// Pub/Sub Manager
pub struct PubSubManager {
    /// All channels (name -> Channel)
    channels: RwLock<HashMap<String, Channel>>,
    /// All sessions (session_id -> SessionPubSubState)
    sessions: RwLock<HashMap<SessionId, SessionPubSubState>>,
    /// Sessions listening to all channels
    listen_all_sessions: RwLock<HashSet<SessionId>>,
    /// Next notification ID
    next_notification_id: AtomicU64,
    /// Maximum payload size (8000 bytes like PostgreSQL)
    max_payload_size: usize,
    /// Statistics
    stats: RwLock<PubSubStats>,
}

impl Default for PubSubManager {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSubManager {
    /// Create a new pub/sub manager
    pub fn new() -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
            listen_all_sessions: RwLock::new(HashSet::new()),
            next_notification_id: AtomicU64::new(1),
            max_payload_size: 8000,
            stats: RwLock::new(PubSubStats::default()),
        }
    }

    /// Register a new session
    pub fn register_session(&self, session_id: SessionId) {
        let mut sessions = self.sessions.write().unwrap();
        sessions
            .entry(session_id)
            .or_insert_with(|| SessionPubSubState::new(session_id));

        let mut stats = self.stats.write().unwrap();
        stats.active_sessions = sessions.len();
    }

    /// Unregister a session (cleanup on disconnect)
    pub fn unregister_session(&self, session_id: SessionId) {
        // Remove from sessions
        let listening_channels = {
            let mut sessions = self.sessions.write().unwrap();
            if let Some(state) = sessions.remove(&session_id) {
                let mut stats = self.stats.write().unwrap();
                stats.active_sessions = sessions.len();
                state.listening_channels
            } else {
                return;
            }
        };

        // Remove from listen_all
        {
            let mut listen_all = self.listen_all_sessions.write().unwrap();
            listen_all.remove(&session_id);
        }

        // Remove from channels
        {
            let mut channels = self.channels.write().unwrap();
            for channel_name in listening_channels {
                if let Some(channel) = channels.get_mut(&channel_name) {
                    channel.remove_listener(session_id);
                }
            }
        }
    }

    /// Validate channel name
    fn validate_channel_name(&self, name: &str) -> Result<(), PubSubError> {
        if name.is_empty() {
            return Err(PubSubError::InvalidChannel("channel name cannot be empty".into()));
        }
        if name.len() > 63 {
            return Err(PubSubError::InvalidChannel(
                "channel name too long (max 63 chars)".into(),
            ));
        }
        // Allow alphanumeric, underscore, and * for wildcard
        if !name.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '*') {
            return Err(PubSubError::InvalidChannel(
                "channel name must be alphanumeric with underscores".into(),
            ));
        }
        Ok(())
    }

    /// LISTEN - Subscribe to a channel
    pub fn listen(&self, session_id: SessionId, channel: &str) -> Result<(), PubSubError> {
        self.validate_channel_name(channel)?;

        // Update session state
        {
            let mut sessions = self.sessions.write().unwrap();
            let state = sessions
                .get_mut(&session_id)
                .ok_or(PubSubError::SessionNotFound(session_id))?;
            state.listen(channel);
        }

        // Handle LISTEN *
        if channel == "*" {
            let mut listen_all = self.listen_all_sessions.write().unwrap();
            listen_all.insert(session_id);
        } else {
            // Add to channel listeners
            let mut channels = self.channels.write().unwrap();
            let channel_obj = channels
                .entry(channel.to_string())
                .or_insert_with(|| Channel::new(channel.to_string()));
            channel_obj.add_listener(session_id);

            let mut stats = self.stats.write().unwrap();
            stats.active_channels = channels.len();
        }

        // Update stats
        {
            let mut stats = self.stats.write().unwrap();
            stats.total_listens += 1;
        }

        Ok(())
    }

    /// UNLISTEN - Unsubscribe from a channel
    pub fn unlisten(&self, session_id: SessionId, channel: &str) -> Result<(), PubSubError> {
        self.validate_channel_name(channel)?;

        // Update session state
        {
            let mut sessions = self.sessions.write().unwrap();
            let state = sessions
                .get_mut(&session_id)
                .ok_or(PubSubError::SessionNotFound(session_id))?;
            state.unlisten(channel);
        }

        // Handle UNLISTEN *
        if channel == "*" {
            let mut listen_all = self.listen_all_sessions.write().unwrap();
            listen_all.remove(&session_id);

            // Also remove from all channel listeners
            let mut channels = self.channels.write().unwrap();
            for channel_obj in channels.values_mut() {
                channel_obj.remove_listener(session_id);
            }
        } else {
            // Remove from channel listeners
            let mut channels = self.channels.write().unwrap();
            if let Some(channel_obj) = channels.get_mut(channel) {
                channel_obj.remove_listener(session_id);
            }
        }

        // Update stats
        {
            let mut stats = self.stats.write().unwrap();
            stats.total_unlistens += 1;
        }

        Ok(())
    }

    /// NOTIFY - Send a notification to a channel
    pub fn notify(
        &self,
        sender_session_id: SessionId,
        channel: &str,
        payload: Option<&str>,
    ) -> Result<NotifyResult, PubSubError> {
        self.validate_channel_name(channel)?;

        // Validate payload size
        if let Some(p) = payload {
            if p.len() > self.max_payload_size {
                return Err(PubSubError::PayloadTooLarge {
                    size: p.len(),
                    max: self.max_payload_size,
                });
            }
        }

        let notification_id = self.next_notification_id.fetch_add(1, Ordering::SeqCst);
        let notification = Notification::new(
            notification_id,
            channel.to_string(),
            payload.map(String::from),
            sender_session_id,
        );

        let mut delivered = 0u64;
        let mut dropped = 0u64;

        // Get all listeners: channel-specific + listen_all sessions
        let listeners: Vec<SessionId> = {
            let channels = self.channels.read().unwrap();
            let listen_all = self.listen_all_sessions.read().unwrap();

            let mut all_listeners: HashSet<SessionId> = listen_all.iter().copied().collect();

            if let Some(channel_obj) = channels.get(channel) {
                all_listeners.extend(channel_obj.listeners.iter());
            }

            all_listeners.into_iter().collect()
        };

        // Deliver to all listeners
        {
            let mut sessions = self.sessions.write().unwrap();
            for listener_id in listeners {
                if let Some(state) = sessions.get_mut(&listener_id) {
                    match state.queue_notification(notification.clone()) {
                        Ok(()) => delivered += 1,
                        Err(_) => dropped += 1,
                    }
                }
            }
        }

        // Update channel stats
        {
            let mut channels = self.channels.write().unwrap();
            if let Some(channel_obj) = channels.get_mut(channel) {
                channel_obj.notification_count += 1;
                channel_obj.last_notification = Some(SystemTime::now());
            }
        }

        // Update global stats
        {
            let mut stats = self.stats.write().unwrap();
            stats.total_notifications += 1;
            stats.total_delivered += delivered;
            stats.total_dropped += dropped;
        }

        Ok(NotifyResult {
            notification_id,
            delivered,
            dropped,
        })
    }

    /// Poll for pending notifications for a session
    pub fn poll(&self, session_id: SessionId) -> Result<Option<Notification>, PubSubError> {
        let mut sessions = self.sessions.write().unwrap();
        let state = sessions
            .get_mut(&session_id)
            .ok_or(PubSubError::SessionNotFound(session_id))?;
        Ok(state.poll_notification())
    }

    /// Drain all pending notifications for a session
    pub fn drain(&self, session_id: SessionId) -> Result<Vec<Notification>, PubSubError> {
        let mut sessions = self.sessions.write().unwrap();
        let state = sessions
            .get_mut(&session_id)
            .ok_or(PubSubError::SessionNotFound(session_id))?;
        Ok(state.drain_notifications())
    }

    /// Check if session has pending notifications
    pub fn has_pending(&self, session_id: SessionId) -> Result<bool, PubSubError> {
        let sessions = self.sessions.read().unwrap();
        let state = sessions
            .get(&session_id)
            .ok_or(PubSubError::SessionNotFound(session_id))?;
        Ok(state.has_pending())
    }

    /// Get pending notification count for a session
    pub fn pending_count(&self, session_id: SessionId) -> Result<usize, PubSubError> {
        let sessions = self.sessions.read().unwrap();
        let state = sessions
            .get(&session_id)
            .ok_or(PubSubError::SessionNotFound(session_id))?;
        Ok(state.pending_count())
    }

    /// Get channels a session is listening to
    pub fn listening_channels(&self, session_id: SessionId) -> Result<Vec<String>, PubSubError> {
        let sessions = self.sessions.read().unwrap();
        let state = sessions
            .get(&session_id)
            .ok_or(PubSubError::SessionNotFound(session_id))?;
        Ok(state.listening_channels.iter().cloned().collect())
    }

    /// Get all active channels
    pub fn list_channels(&self) -> Vec<ChannelInfo> {
        let channels = self.channels.read().unwrap();
        channels
            .values()
            .map(|c| ChannelInfo {
                name: c.name.clone(),
                listener_count: c.listeners.len(),
                notification_count: c.notification_count,
                last_notification: c.last_notification,
                created_at: c.created_at,
            })
            .collect()
    }

    /// Get pub/sub statistics
    pub fn stats(&self) -> PubSubStats {
        self.stats.read().unwrap().clone()
    }

    /// pg_notify function implementation
    pub fn pg_notify(
        &self,
        session_id: SessionId,
        channel: &str,
        payload: &str,
    ) -> Result<NotifyResult, PubSubError> {
        self.notify(session_id, channel, Some(payload))
    }
}

/// Result of a NOTIFY operation
#[derive(Debug, Clone)]
pub struct NotifyResult {
    /// Notification ID assigned
    pub notification_id: NotificationId,
    /// Number of sessions that received the notification
    pub delivered: u64,
    /// Number of sessions that dropped the notification (queue full)
    pub dropped: u64,
}

/// Channel information for listing
#[derive(Debug, Clone)]
pub struct ChannelInfo {
    /// Channel name
    pub name: String,
    /// Number of active listeners
    pub listener_count: usize,
    /// Total notifications sent
    pub notification_count: u64,
    /// Last notification timestamp
    pub last_notification: Option<SystemTime>,
    /// Channel created timestamp
    pub created_at: SystemTime,
}

// ============================================================================
// SQL Parser Helpers
// ============================================================================

/// Parse LISTEN statement
/// LISTEN channel_name
pub fn parse_listen(sql: &str) -> Option<String> {
    let sql = sql.trim();
    if !sql.to_uppercase().starts_with("LISTEN ") {
        return None;
    }

    let channel = sql[7..].trim().trim_matches('"').trim_matches('\'');
    if channel.is_empty() {
        return None;
    }

    Some(channel.to_string())
}

/// Parse UNLISTEN statement
/// UNLISTEN channel_name | UNLISTEN *
pub fn parse_unlisten(sql: &str) -> Option<String> {
    let sql = sql.trim();
    if !sql.to_uppercase().starts_with("UNLISTEN ") {
        return None;
    }

    let channel = sql[9..].trim().trim_matches('"').trim_matches('\'');
    if channel.is_empty() {
        return None;
    }

    Some(channel.to_string())
}

/// Parse NOTIFY statement
/// NOTIFY channel_name [, 'payload']
pub fn parse_notify(sql: &str) -> Option<(String, Option<String>)> {
    let sql = sql.trim();
    if !sql.to_uppercase().starts_with("NOTIFY ") {
        return None;
    }

    let rest = sql[7..].trim();

    // Check for payload
    if let Some(comma_pos) = rest.find(',') {
        let channel = rest[..comma_pos].trim().trim_matches('"').trim_matches('\'');
        let payload = rest[comma_pos + 1..]
            .trim()
            .trim_matches('\'')
            .trim_matches('"');

        if channel.is_empty() {
            return None;
        }

        Some((channel.to_string(), Some(payload.to_string())))
    } else {
        let channel = rest.trim_matches('"').trim_matches('\'');
        if channel.is_empty() {
            return None;
        }

        Some((channel.to_string(), None))
    }
}

/// Parse pg_notify function call
/// SELECT pg_notify('channel', 'payload')
pub fn parse_pg_notify(sql: &str) -> Option<(String, String)> {
    let sql = sql.trim().to_uppercase();

    // Look for pg_notify function
    if !sql.contains("PG_NOTIFY") {
        return None;
    }

    // Extract arguments from pg_notify('channel', 'payload')
    let original = sql.trim();
    if let Some(start) = original.to_uppercase().find("PG_NOTIFY(") {
        let rest = &original[start + 10..];
        if let Some(end) = rest.find(')') {
            let args = &rest[..end];
            let parts: Vec<&str> = args.split(',').collect();
            if parts.len() == 2 {
                let channel = parts[0].trim().trim_matches('\'').trim_matches('"');
                let payload = parts[1].trim().trim_matches('\'').trim_matches('"');
                return Some((channel.to_string(), payload.to_string()));
            }
        }
    }

    None
}

// ============================================================================
// Async Notification Handler (for integration)
// ============================================================================

/// Callback type for notification delivery
pub type NotificationCallback = Box<dyn Fn(&Notification) + Send + Sync>;

/// Async notification dispatcher
pub struct NotificationDispatcher {
    /// Pub/sub manager
    manager: Arc<PubSubManager>,
    /// Per-session callbacks
    callbacks: RwLock<HashMap<SessionId, Vec<NotificationCallback>>>,
    /// Running flag
    running: std::sync::atomic::AtomicBool,
}

impl NotificationDispatcher {
    /// Create a new dispatcher
    pub fn new(manager: Arc<PubSubManager>) -> Self {
        Self {
            manager,
            callbacks: RwLock::new(HashMap::new()),
            running: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Register a callback for a session
    pub fn register_callback(&self, session_id: SessionId, callback: NotificationCallback) {
        let mut callbacks = self.callbacks.write().unwrap();
        callbacks
            .entry(session_id)
            .or_insert_with(Vec::new)
            .push(callback);
    }

    /// Unregister all callbacks for a session
    pub fn unregister_session(&self, session_id: SessionId) {
        let mut callbacks = self.callbacks.write().unwrap();
        callbacks.remove(&session_id);
    }

    /// Poll and dispatch notifications for a session
    pub fn dispatch(&self, session_id: SessionId) -> Result<usize, PubSubError> {
        let notifications = self.manager.drain(session_id)?;
        let count = notifications.len();

        let callbacks = self.callbacks.read().unwrap();
        if let Some(session_callbacks) = callbacks.get(&session_id) {
            for notification in &notifications {
                for callback in session_callbacks {
                    callback(notification);
                }
            }
        }

        Ok(count)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_listen_notify() {
        let manager = PubSubManager::new();

        // Register sessions
        manager.register_session(1);
        manager.register_session(2);

        // Session 1 listens to a channel
        manager.listen(1, "orders").unwrap();

        // Session 2 sends a notification
        let result = manager.notify(2, "orders", Some("order_123")).unwrap();
        assert_eq!(result.delivered, 1);

        // Session 1 should have the notification
        let notification = manager.poll(1).unwrap().unwrap();
        assert_eq!(notification.channel, "orders");
        assert_eq!(notification.payload.as_deref(), Some("order_123"));
        assert_eq!(notification.sender_session_id, 2);
    }

    #[test]
    fn test_multiple_listeners() {
        let manager = PubSubManager::new();

        manager.register_session(1);
        manager.register_session(2);
        manager.register_session(3);

        manager.listen(1, "updates").unwrap();
        manager.listen(2, "updates").unwrap();
        // Session 3 does not listen

        let result = manager.notify(3, "updates", Some("hello")).unwrap();
        assert_eq!(result.delivered, 2);

        assert!(manager.has_pending(1).unwrap());
        assert!(manager.has_pending(2).unwrap());
        assert!(!manager.has_pending(3).unwrap());
    }

    #[test]
    fn test_listen_all() {
        let manager = PubSubManager::new();

        manager.register_session(1);
        manager.register_session(2);

        // Session 1 listens to all channels
        manager.listen(1, "*").unwrap();

        // Send to various channels
        manager.notify(2, "channel1", Some("msg1")).unwrap();
        manager.notify(2, "channel2", Some("msg2")).unwrap();
        manager.notify(2, "channel3", Some("msg3")).unwrap();

        // Session 1 should have all notifications
        assert_eq!(manager.pending_count(1).unwrap(), 3);

        let notifications = manager.drain(1).unwrap();
        assert_eq!(notifications.len(), 3);
        assert_eq!(notifications[0].channel, "channel1");
        assert_eq!(notifications[1].channel, "channel2");
        assert_eq!(notifications[2].channel, "channel3");
    }

    #[test]
    fn test_unlisten() {
        let manager = PubSubManager::new();

        manager.register_session(1);
        manager.register_session(2);

        manager.listen(1, "channel").unwrap();
        manager.notify(2, "channel", Some("msg1")).unwrap();
        assert_eq!(manager.pending_count(1).unwrap(), 1);

        // Drain and unlisten
        manager.drain(1).unwrap();
        manager.unlisten(1, "channel").unwrap();

        // Should not receive new notifications
        manager.notify(2, "channel", Some("msg2")).unwrap();
        assert_eq!(manager.pending_count(1).unwrap(), 0);
    }

    #[test]
    fn test_unlisten_all() {
        let manager = PubSubManager::new();

        manager.register_session(1);
        manager.register_session(2);

        manager.listen(1, "channel1").unwrap();
        manager.listen(1, "channel2").unwrap();
        manager.listen(1, "channel3").unwrap();

        // Unlisten all
        manager.unlisten(1, "*").unwrap();

        // Should not receive any notifications
        manager.notify(2, "channel1", Some("msg")).unwrap();
        manager.notify(2, "channel2", Some("msg")).unwrap();
        assert_eq!(manager.pending_count(1).unwrap(), 0);
    }

    #[test]
    fn test_session_cleanup() {
        let manager = PubSubManager::new();

        manager.register_session(1);
        manager.register_session(2);

        manager.listen(1, "channel").unwrap();
        manager.notify(2, "channel", Some("msg")).unwrap();

        // Unregister session 1
        manager.unregister_session(1);

        // Session 1 should no longer receive notifications
        let result = manager.notify(2, "channel", Some("msg2")).unwrap();
        assert_eq!(result.delivered, 0);

        // Session 1 should not exist
        assert!(manager.poll(1).is_err());
    }

    #[test]
    fn test_payload_too_large() {
        let manager = PubSubManager::new();
        manager.register_session(1);
        manager.register_session(2);
        manager.listen(1, "channel").unwrap();

        // Create payload larger than 8000 bytes
        let large_payload = "x".repeat(9000);
        let result = manager.notify(2, "channel", Some(&large_payload));

        assert!(matches!(result, Err(PubSubError::PayloadTooLarge { .. })));
    }

    #[test]
    fn test_invalid_channel_name() {
        let manager = PubSubManager::new();
        manager.register_session(1);

        // Empty channel name
        assert!(manager.listen(1, "").is_err());

        // Invalid characters
        assert!(manager.listen(1, "channel@name").is_err());

        // Too long
        let long_name = "x".repeat(100);
        assert!(manager.listen(1, &long_name).is_err());
    }

    #[test]
    fn test_parse_listen() {
        assert_eq!(parse_listen("LISTEN orders"), Some("orders".to_string()));
        assert_eq!(parse_listen("listen ORDERS"), Some("ORDERS".to_string()));
        assert_eq!(parse_listen("LISTEN *"), Some("*".to_string()));
        assert_eq!(parse_listen("LISTEN 'quoted'"), Some("quoted".to_string()));
        assert_eq!(parse_listen("SELECT 1"), None);
    }

    #[test]
    fn test_parse_unlisten() {
        assert_eq!(parse_unlisten("UNLISTEN orders"), Some("orders".to_string()));
        assert_eq!(parse_unlisten("UNLISTEN *"), Some("*".to_string()));
        assert_eq!(parse_unlisten("SELECT 1"), None);
    }

    #[test]
    fn test_parse_notify() {
        assert_eq!(
            parse_notify("NOTIFY orders"),
            Some(("orders".to_string(), None))
        );
        assert_eq!(
            parse_notify("NOTIFY orders, 'payload'"),
            Some(("orders".to_string(), Some("payload".to_string())))
        );
        assert_eq!(
            parse_notify("NOTIFY channel, 'hello world'"),
            Some(("channel".to_string(), Some("hello world".to_string())))
        );
        assert_eq!(parse_notify("SELECT 1"), None);
    }

    #[test]
    fn test_pg_notify() {
        let manager = PubSubManager::new();

        manager.register_session(1);
        manager.register_session(2);
        manager.listen(1, "events").unwrap();

        let result = manager.pg_notify(2, "events", "data").unwrap();
        assert_eq!(result.delivered, 1);

        let notification = manager.poll(1).unwrap().unwrap();
        assert_eq!(notification.payload.as_deref(), Some("data"));
    }

    #[test]
    fn test_channel_info() {
        let manager = PubSubManager::new();

        manager.register_session(1);
        manager.register_session(2);
        manager.listen(1, "channel1").unwrap();
        manager.listen(2, "channel1").unwrap();
        manager.listen(1, "channel2").unwrap();

        manager.notify(1, "channel1", Some("msg")).unwrap();

        let channels = manager.list_channels();
        assert_eq!(channels.len(), 2);

        let ch1 = channels.iter().find(|c| c.name == "channel1").unwrap();
        assert_eq!(ch1.listener_count, 2);
        assert_eq!(ch1.notification_count, 1);

        let ch2 = channels.iter().find(|c| c.name == "channel2").unwrap();
        assert_eq!(ch2.listener_count, 1);
        assert_eq!(ch2.notification_count, 0);
    }

    #[test]
    fn test_stats() {
        let manager = PubSubManager::new();

        manager.register_session(1);
        manager.register_session(2);
        manager.listen(1, "channel").unwrap();
        manager.notify(2, "channel", Some("msg")).unwrap();

        let stats = manager.stats();
        assert_eq!(stats.total_notifications, 1);
        assert_eq!(stats.total_delivered, 1);
        assert_eq!(stats.total_listens, 1);
        assert_eq!(stats.active_sessions, 2);
    }

    #[test]
    fn test_notification_format() {
        let notification = Notification::new(1, "orders".to_string(), Some("order_123".to_string()), 1);
        assert_eq!(
            notification.format(),
            "Asynchronous Notification: channel=orders, payload=order_123"
        );

        let notification_no_payload = Notification::new(2, "events".to_string(), None, 1);
        assert_eq!(
            notification_no_payload.format(),
            "Asynchronous Notification: channel=events"
        );
    }
}
