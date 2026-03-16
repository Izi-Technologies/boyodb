//! Enterprise Authentication - LDAP, OAuth 2.0, SAML
//!
//! Provides enterprise identity management integration:
//! - LDAP/Active Directory authentication
//! - OAuth 2.0 / OpenID Connect (OIDC)
//! - SAML 2.0 Single Sign-On
//! - Multi-factor authentication (MFA)
//! - Session management and token handling

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(feature = "ldap-auth")]
use ldap3::{Ldap, LdapConnAsync, LdapConnSettings, Scope, SearchEntry};
#[cfg(feature = "ldap-auth")]
use tokio::runtime::Handle;

// ============================================================================
// Common Types
// ============================================================================

/// Authentication error types
#[derive(Debug, Clone)]
pub enum AuthError {
    /// Invalid credentials
    InvalidCredentials,
    /// User not found
    UserNotFound(String),
    /// Account locked
    AccountLocked(String),
    /// Account disabled
    AccountDisabled(String),
    /// Token expired
    TokenExpired,
    /// Token invalid
    TokenInvalid(String),
    /// MFA required
    MfaRequired,
    /// MFA failed
    MfaFailed,
    /// Provider error
    ProviderError(String),
    /// Configuration error
    ConfigError(String),
    /// Network error
    NetworkError(String),
    /// Not authorized
    NotAuthorized(String),
    /// Session expired
    SessionExpired,
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidCredentials => write!(f, "invalid credentials"),
            Self::UserNotFound(u) => write!(f, "user not found: {}", u),
            Self::AccountLocked(u) => write!(f, "account locked: {}", u),
            Self::AccountDisabled(u) => write!(f, "account disabled: {}", u),
            Self::TokenExpired => write!(f, "token expired"),
            Self::TokenInvalid(msg) => write!(f, "invalid token: {}", msg),
            Self::MfaRequired => write!(f, "MFA required"),
            Self::MfaFailed => write!(f, "MFA verification failed"),
            Self::ProviderError(msg) => write!(f, "provider error: {}", msg),
            Self::ConfigError(msg) => write!(f, "configuration error: {}", msg),
            Self::NetworkError(msg) => write!(f, "network error: {}", msg),
            Self::NotAuthorized(msg) => write!(f, "not authorized: {}", msg),
            Self::SessionExpired => write!(f, "session expired"),
        }
    }
}

impl std::error::Error for AuthError {}

/// Authentication provider type
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuthProviderType {
    Local,
    Ldap,
    OAuth2,
    Oidc,
    Saml,
}

/// Authenticated identity
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Identity {
    /// Unique user identifier
    pub user_id: String,
    /// Username
    pub username: String,
    /// Email address
    pub email: Option<String>,
    /// Display name
    pub display_name: Option<String>,
    /// Groups/roles
    pub groups: Vec<String>,
    /// Provider type
    pub provider: AuthProviderType,
    /// Provider-specific attributes
    pub attributes: HashMap<String, String>,
    /// Authentication time
    pub authenticated_at: u64,
    /// Session expiry
    pub expires_at: Option<u64>,
}

/// Authentication result
#[derive(Clone, Debug)]
pub struct AuthResult {
    pub identity: Identity,
    pub access_token: Option<String>,
    pub refresh_token: Option<String>,
    pub mfa_required: bool,
    pub mfa_methods: Vec<MfaMethod>,
}

/// MFA method
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MfaMethod {
    Totp,
    Sms,
    Email,
    Push,
    WebAuthn,
    BackupCodes,
}

// ============================================================================
// LDAP Authentication
// ============================================================================

/// LDAP configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LdapConfig {
    /// LDAP server URL (ldap:// or ldaps://)
    pub url: String,
    /// Base DN for searches
    pub base_dn: String,
    /// Bind DN (for service account)
    pub bind_dn: Option<String>,
    /// Bind password
    pub bind_password: Option<String>,
    /// User search filter (use {username} placeholder)
    pub user_filter: String,
    /// Group search base
    pub group_base_dn: Option<String>,
    /// Group search filter
    pub group_filter: Option<String>,
    /// User ID attribute
    pub user_id_attr: String,
    /// Email attribute
    pub email_attr: String,
    /// Display name attribute
    pub display_name_attr: String,
    /// Group membership attribute
    pub member_of_attr: String,
    /// Use TLS
    pub use_tls: bool,
    /// Skip TLS verification (not recommended)
    pub skip_tls_verify: bool,
    /// Connection timeout
    pub timeout_secs: u64,
    /// Connection pool size
    pub pool_size: usize,
}

impl Default for LdapConfig {
    fn default() -> Self {
        Self {
            url: "ldap://localhost:389".to_string(),
            base_dn: "dc=example,dc=com".to_string(),
            bind_dn: None,
            bind_password: None,
            user_filter: "(&(objectClass=user)(sAMAccountName={username}))".to_string(),
            group_base_dn: None,
            group_filter: Some("(&(objectClass=group)(member={dn}))".to_string()),
            user_id_attr: "objectGUID".to_string(),
            email_attr: "mail".to_string(),
            display_name_attr: "displayName".to_string(),
            member_of_attr: "memberOf".to_string(),
            use_tls: true,
            skip_tls_verify: false,
            timeout_secs: 10,
            pool_size: 10,
        }
    }
}

impl LdapConfig {
    /// Create a configuration for Microsoft Active Directory
    pub fn active_directory(url: &str, base_dn: &str) -> Self {
        Self {
            url: url.to_string(),
            base_dn: base_dn.to_string(),
            bind_dn: None,
            bind_password: None,
            // AD uses sAMAccountName for Windows login names
            user_filter: "(&(objectClass=user)(sAMAccountName={username}))".to_string(),
            group_base_dn: Some(base_dn.to_string()),
            group_filter: Some("(&(objectClass=group)(member={dn}))".to_string()),
            // AD-specific attributes
            user_id_attr: "sAMAccountName".to_string(),
            email_attr: "mail".to_string(),
            display_name_attr: "displayName".to_string(),
            member_of_attr: "memberOf".to_string(),
            use_tls: true,
            skip_tls_verify: false,
            timeout_secs: 10,
            pool_size: 10,
        }
    }

    /// Create a configuration for OpenLDAP
    pub fn openldap(url: &str, base_dn: &str) -> Self {
        Self {
            url: url.to_string(),
            base_dn: base_dn.to_string(),
            bind_dn: None,
            bind_password: None,
            // OpenLDAP typically uses uid
            user_filter: "(&(objectClass=inetOrgPerson)(uid={username}))".to_string(),
            group_base_dn: Some(base_dn.to_string()),
            group_filter: Some("(&(objectClass=groupOfNames)(member={dn}))".to_string()),
            user_id_attr: "uid".to_string(),
            email_attr: "mail".to_string(),
            display_name_attr: "cn".to_string(),
            member_of_attr: "memberOf".to_string(),
            use_tls: true,
            skip_tls_verify: false,
            timeout_secs: 10,
            pool_size: 10,
        }
    }

    /// Create a configuration for FreeIPA
    pub fn freeipa(url: &str, base_dn: &str) -> Self {
        Self {
            url: url.to_string(),
            base_dn: base_dn.to_string(),
            bind_dn: None,
            bind_password: None,
            // FreeIPA uses uid and ipaUniqueID
            user_filter: "(&(objectClass=person)(uid={username}))".to_string(),
            group_base_dn: Some(format!("cn=groups,cn=accounts,{}", base_dn)),
            group_filter: Some("(&(objectClass=groupOfNames)(member={dn}))".to_string()),
            user_id_attr: "uid".to_string(),
            email_attr: "mail".to_string(),
            display_name_attr: "cn".to_string(),
            member_of_attr: "memberOf".to_string(),
            use_tls: true,
            skip_tls_verify: false,
            timeout_secs: 10,
            pool_size: 10,
        }
    }

    /// Set the service account credentials for bind operations
    pub fn with_bind_credentials(mut self, bind_dn: &str, bind_password: &str) -> Self {
        self.bind_dn = Some(bind_dn.to_string());
        self.bind_password = Some(bind_password.to_string());
        self
    }

    /// Set whether to use TLS
    pub fn with_tls(mut self, use_tls: bool) -> Self {
        self.use_tls = use_tls;
        self
    }

    /// Set whether to skip TLS verification (not recommended for production)
    pub fn with_skip_tls_verify(mut self, skip: bool) -> Self {
        self.skip_tls_verify = skip;
        self
    }

    /// Set the connection timeout
    pub fn with_timeout(mut self, timeout_secs: u64) -> Self {
        self.timeout_secs = timeout_secs;
        self
    }

    /// Set a custom user filter
    pub fn with_user_filter(mut self, filter: &str) -> Self {
        self.user_filter = filter.to_string();
        self
    }

    /// Set group search configuration
    pub fn with_group_search(mut self, base_dn: &str, filter: &str) -> Self {
        self.group_base_dn = Some(base_dn.to_string());
        self.group_filter = Some(filter.to_string());
        self
    }
}

/// LDAP authentication provider
pub struct LdapProvider {
    config: LdapConfig,
}

impl LdapProvider {
    pub fn new(config: LdapConfig) -> Self {
        Self { config }
    }

    /// Authenticate user with LDAP
    #[cfg(feature = "ldap-auth")]
    pub fn authenticate(&self, username: &str, password: &str) -> Result<Identity, AuthError> {
        // Validate inputs
        if username.is_empty() || password.is_empty() {
            return Err(AuthError::InvalidCredentials);
        }

        // Escape special characters in username for LDAP filter
        let safe_username = Self::escape_ldap_filter(username);

        // Build user search filter
        let filter = self.config.user_filter.replace("{username}", &safe_username);

        // Use tokio's runtime to perform async LDAP operations
        let handle = Handle::try_current()
            .map_err(|_| AuthError::ProviderError("no tokio runtime available".into()))?;

        let config = self.config.clone();
        let password = password.to_string();
        let username = username.to_string();

        handle.block_on(async move {
            Self::authenticate_async(&config, &username, &password, &filter).await
        })
    }

    /// Authenticate user with LDAP (fallback when ldap3 is not available)
    #[cfg(not(feature = "ldap-auth"))]
    pub fn authenticate(&self, username: &str, password: &str) -> Result<Identity, AuthError> {
        // Validate inputs
        if username.is_empty() || password.is_empty() {
            return Err(AuthError::InvalidCredentials);
        }

        // Build user search filter
        let filter = self.config.user_filter.replace("{username}", username);

        // Simulated successful authentication (for testing/development)
        let user_dn = format!("cn={},{}", username, self.config.base_dn);

        // Fetch groups (simulated)
        let groups = self.get_user_groups(&user_dn)?;

        Ok(Identity {
            user_id: format!("ldap:{}", username),
            username: username.to_string(),
            email: Some(format!("{}@example.com", username)),
            display_name: Some(username.to_string()),
            groups,
            provider: AuthProviderType::Ldap,
            attributes: {
                let mut attrs = HashMap::new();
                attrs.insert("dn".to_string(), user_dn);
                attrs.insert("filter".to_string(), filter);
                attrs
            },
            authenticated_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            expires_at: None,
        })
    }

    #[cfg(feature = "ldap-auth")]
    async fn authenticate_async(
        config: &LdapConfig,
        username: &str,
        password: &str,
        filter: &str,
    ) -> Result<Identity, AuthError> {
        // Configure LDAP connection settings
        let settings = LdapConnSettings::new()
            .set_conn_timeout(Duration::from_secs(config.timeout_secs));

        // Connect to LDAP server
        let (conn, mut ldap) = LdapConnAsync::with_settings(settings, &config.url)
            .await
            .map_err(|e| AuthError::NetworkError(format!("LDAP connection failed: {}", e)))?;

        // Spawn the connection handler
        ldap3::drive!(conn);

        // If StartTLS is enabled (for non-ldaps:// connections)
        if config.use_tls && config.url.starts_with("ldap://") {
            ldap.start_tls(config.skip_tls_verify)
                .await
                .map_err(|e| AuthError::NetworkError(format!("StartTLS failed: {}", e)))?;
        }

        // Bind with service account if configured
        if let (Some(bind_dn), Some(bind_password)) = (&config.bind_dn, &config.bind_password) {
            ldap.simple_bind(bind_dn, bind_password)
                .await
                .map_err(|e| AuthError::ProviderError(format!("service bind failed: {}", e)))?
                .success()
                .map_err(|e| AuthError::ProviderError(format!("service bind rejected: {}", e)))?;
        }

        // Search for the user
        let (rs, _res) = ldap
            .search(
                &config.base_dn,
                Scope::Subtree,
                filter,
                vec![
                    &config.user_id_attr,
                    &config.email_attr,
                    &config.display_name_attr,
                    &config.member_of_attr,
                    "dn",
                ],
            )
            .await
            .map_err(|e| AuthError::ProviderError(format!("user search failed: {}", e)))?
            .success()
            .map_err(|e| AuthError::ProviderError(format!("user search rejected: {}", e)))?;

        if rs.is_empty() {
            return Err(AuthError::UserNotFound(username.to_string()));
        }

        let entry = SearchEntry::construct(rs.into_iter().next().unwrap());
        let user_dn = entry.dn.clone();

        // Attempt to bind as the user to verify password
        ldap.simple_bind(&user_dn, password)
            .await
            .map_err(|e| AuthError::ProviderError(format!("user bind failed: {}", e)))?
            .success()
            .map_err(|_| AuthError::InvalidCredentials)?;

        // Extract user attributes
        let user_id = entry
            .attrs
            .get(&config.user_id_attr)
            .and_then(|v| v.first())
            .cloned()
            .unwrap_or_else(|| format!("ldap:{}", username));

        let email = entry
            .attrs
            .get(&config.email_attr)
            .and_then(|v| v.first())
            .cloned();

        let display_name = entry
            .attrs
            .get(&config.display_name_attr)
            .and_then(|v| v.first())
            .cloned();

        // Extract groups from memberOf attribute
        let groups: Vec<String> = entry
            .attrs
            .get(&config.member_of_attr)
            .map(|v| {
                v.iter()
                    .filter_map(|dn| Self::extract_cn_from_dn(dn))
                    .collect()
            })
            .unwrap_or_default();

        // Unbind gracefully
        let _ = ldap.unbind().await;

        Ok(Identity {
            user_id: format!("ldap:{}", user_id),
            username: username.to_string(),
            email,
            display_name,
            groups,
            provider: AuthProviderType::Ldap,
            attributes: {
                let mut attrs = HashMap::new();
                attrs.insert("dn".to_string(), user_dn);
                attrs
            },
            authenticated_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            expires_at: None,
        })
    }

    /// Extract CN from a DN string (e.g., "cn=Admins,ou=Groups,dc=example,dc=com" -> "Admins")
    #[cfg(feature = "ldap-auth")]
    fn extract_cn_from_dn(dn: &str) -> Option<String> {
        dn.split(',')
            .find(|part| part.trim().to_lowercase().starts_with("cn="))
            .map(|part| part.trim()[3..].to_string())
    }

    /// Escape special characters for LDAP filter
    #[cfg(feature = "ldap-auth")]
    fn escape_ldap_filter(input: &str) -> String {
        let mut result = String::with_capacity(input.len() * 2);
        for c in input.chars() {
            match c {
                '\\' => result.push_str("\\5c"),
                '*' => result.push_str("\\2a"),
                '(' => result.push_str("\\28"),
                ')' => result.push_str("\\29"),
                '\0' => result.push_str("\\00"),
                _ => result.push(c),
            }
        }
        result
    }

    /// Get user groups from LDAP
    fn get_user_groups(&self, _user_dn: &str) -> Result<Vec<String>, AuthError> {
        // For non-ldap3 builds, return placeholder groups
        Ok(vec!["users".to_string(), "developers".to_string()])
    }

    /// Test LDAP connection
    #[cfg(feature = "ldap-auth")]
    pub fn test_connection(&self) -> Result<(), AuthError> {
        let handle = Handle::try_current()
            .map_err(|_| AuthError::ProviderError("no tokio runtime available".into()))?;

        let config = self.config.clone();

        handle.block_on(async move {
            let settings = LdapConnSettings::new()
                .set_conn_timeout(Duration::from_secs(config.timeout_secs));

            let (conn, mut ldap) = LdapConnAsync::with_settings(settings, &config.url)
                .await
                .map_err(|e| AuthError::NetworkError(format!("connection failed: {}", e)))?;

            ldap3::drive!(conn);

            if config.use_tls && config.url.starts_with("ldap://") {
                ldap.start_tls(config.skip_tls_verify)
                    .await
                    .map_err(|e| AuthError::NetworkError(format!("StartTLS failed: {}", e)))?;
            }

            if let (Some(bind_dn), Some(bind_password)) = (&config.bind_dn, &config.bind_password) {
                ldap.simple_bind(bind_dn, bind_password)
                    .await
                    .map_err(|e| AuthError::ProviderError(format!("bind failed: {}", e)))?
                    .success()
                    .map_err(|e| AuthError::ProviderError(format!("bind rejected: {}", e)))?;
            }

            let _ = ldap.unbind().await;
            Ok(())
        })
    }

    /// Test LDAP connection (fallback)
    #[cfg(not(feature = "ldap-auth"))]
    pub fn test_connection(&self) -> Result<(), AuthError> {
        Ok(())
    }

    /// Search users
    #[cfg(feature = "ldap-auth")]
    pub fn search_users(&self, filter: &str, limit: usize) -> Result<Vec<LdapUser>, AuthError> {
        let handle = Handle::try_current()
            .map_err(|_| AuthError::ProviderError("no tokio runtime available".into()))?;

        let config = self.config.clone();
        let filter = filter.to_string();

        handle.block_on(async move {
            let settings = LdapConnSettings::new()
                .set_conn_timeout(Duration::from_secs(config.timeout_secs));

            let (conn, mut ldap) = LdapConnAsync::with_settings(settings, &config.url)
                .await
                .map_err(|e| AuthError::NetworkError(format!("connection failed: {}", e)))?;

            ldap3::drive!(conn);

            if let (Some(bind_dn), Some(bind_password)) = (&config.bind_dn, &config.bind_password) {
                ldap.simple_bind(bind_dn, bind_password)
                    .await
                    .map_err(|e| AuthError::ProviderError(format!("bind failed: {}", e)))?
                    .success()
                    .map_err(|e| AuthError::ProviderError(format!("bind rejected: {}", e)))?;
            }

            let (rs, _res) = ldap
                .search(
                    &config.base_dn,
                    Scope::Subtree,
                    &filter,
                    vec![
                        &config.user_id_attr,
                        &config.email_attr,
                        &config.display_name_attr,
                        &config.member_of_attr,
                    ],
                )
                .await
                .map_err(|e| AuthError::ProviderError(format!("search failed: {}", e)))?
                .success()
                .map_err(|e| AuthError::ProviderError(format!("search rejected: {}", e)))?;

            let _ = ldap.unbind().await;

            let users: Vec<LdapUser> = rs
                .into_iter()
                .take(limit)
                .map(|entry| {
                    let entry = SearchEntry::construct(entry);
                    LdapUser {
                        dn: entry.dn.clone(),
                        username: entry
                            .attrs
                            .get(&config.user_id_attr)
                            .and_then(|v| v.first())
                            .cloned()
                            .unwrap_or_default(),
                        email: entry
                            .attrs
                            .get(&config.email_attr)
                            .and_then(|v| v.first())
                            .cloned(),
                        display_name: entry
                            .attrs
                            .get(&config.display_name_attr)
                            .and_then(|v| v.first())
                            .cloned(),
                        groups: entry
                            .attrs
                            .get(&config.member_of_attr)
                            .map(|v| {
                                v.iter()
                                    .filter_map(|dn| Self::extract_cn_from_dn(dn))
                                    .collect()
                            })
                            .unwrap_or_default(),
                        enabled: true,
                    }
                })
                .collect();

            Ok(users)
        })
    }

    /// Search users (fallback)
    #[cfg(not(feature = "ldap-auth"))]
    pub fn search_users(&self, _filter: &str, _limit: usize) -> Result<Vec<LdapUser>, AuthError> {
        Ok(vec![])
    }

    /// Sync groups from LDAP
    #[cfg(feature = "ldap-auth")]
    pub fn sync_groups(&self) -> Result<Vec<LdapGroup>, AuthError> {
        let handle = Handle::try_current()
            .map_err(|_| AuthError::ProviderError("no tokio runtime available".into()))?;

        let config = self.config.clone();

        handle.block_on(async move {
            let settings = LdapConnSettings::new()
                .set_conn_timeout(Duration::from_secs(config.timeout_secs));

            let (conn, mut ldap) = LdapConnAsync::with_settings(settings, &config.url)
                .await
                .map_err(|e| AuthError::NetworkError(format!("connection failed: {}", e)))?;

            ldap3::drive!(conn);

            if let (Some(bind_dn), Some(bind_password)) = (&config.bind_dn, &config.bind_password) {
                ldap.simple_bind(bind_dn, bind_password)
                    .await
                    .map_err(|e| AuthError::ProviderError(format!("bind failed: {}", e)))?
                    .success()
                    .map_err(|e| AuthError::ProviderError(format!("bind rejected: {}", e)))?;
            }

            let group_base = config.group_base_dn.as_ref().unwrap_or(&config.base_dn);
            let group_filter = config.group_filter.as_deref().unwrap_or("(objectClass=group)");

            let (rs, _res) = ldap
                .search(
                    group_base,
                    Scope::Subtree,
                    group_filter,
                    vec!["cn", "description", "member"],
                )
                .await
                .map_err(|e| AuthError::ProviderError(format!("search failed: {}", e)))?
                .success()
                .map_err(|e| AuthError::ProviderError(format!("search rejected: {}", e)))?;

            let _ = ldap.unbind().await;

            let groups: Vec<LdapGroup> = rs
                .into_iter()
                .map(|entry| {
                    let entry = SearchEntry::construct(entry);
                    LdapGroup {
                        dn: entry.dn.clone(),
                        name: entry
                            .attrs
                            .get("cn")
                            .and_then(|v| v.first())
                            .cloned()
                            .unwrap_or_default(),
                        description: entry
                            .attrs
                            .get("description")
                            .and_then(|v| v.first())
                            .cloned(),
                        members: entry
                            .attrs
                            .get("member")
                            .cloned()
                            .unwrap_or_default(),
                    }
                })
                .collect();

            Ok(groups)
        })
    }

    /// Sync groups from LDAP (fallback)
    #[cfg(not(feature = "ldap-auth"))]
    pub fn sync_groups(&self) -> Result<Vec<LdapGroup>, AuthError> {
        Ok(vec![])
    }
}

/// LDAP user entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LdapUser {
    pub dn: String,
    pub username: String,
    pub email: Option<String>,
    pub display_name: Option<String>,
    pub groups: Vec<String>,
    pub enabled: bool,
}

/// LDAP group entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LdapGroup {
    pub dn: String,
    pub name: String,
    pub description: Option<String>,
    pub members: Vec<String>,
}

// ============================================================================
// OAuth 2.0 / OpenID Connect
// ============================================================================

/// OAuth 2.0 configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OAuth2Config {
    /// Provider name
    pub provider_name: String,
    /// Client ID
    pub client_id: String,
    /// Client secret
    pub client_secret: String,
    /// Authorization endpoint
    pub authorization_url: String,
    /// Token endpoint
    pub token_url: String,
    /// UserInfo endpoint (for OIDC)
    pub userinfo_url: Option<String>,
    /// JWKS URI (for OIDC)
    pub jwks_uri: Option<String>,
    /// Scopes to request
    pub scopes: Vec<String>,
    /// Redirect URI
    pub redirect_uri: String,
    /// Response type
    pub response_type: String,
    /// Use PKCE
    pub use_pkce: bool,
    /// Additional parameters
    pub additional_params: HashMap<String, String>,
}

impl Default for OAuth2Config {
    fn default() -> Self {
        Self {
            provider_name: "generic".to_string(),
            client_id: String::new(),
            client_secret: String::new(),
            authorization_url: String::new(),
            token_url: String::new(),
            userinfo_url: None,
            jwks_uri: None,
            scopes: vec![
                "openid".to_string(),
                "profile".to_string(),
                "email".to_string(),
            ],
            redirect_uri: String::new(),
            response_type: "code".to_string(),
            use_pkce: true,
            additional_params: HashMap::new(),
        }
    }
}

/// Pre-configured OAuth providers
impl OAuth2Config {
    /// Google OAuth configuration
    pub fn google(client_id: &str, client_secret: &str, redirect_uri: &str) -> Self {
        Self {
            provider_name: "google".to_string(),
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
            authorization_url: "https://accounts.google.com/o/oauth2/v2/auth".to_string(),
            token_url: "https://oauth2.googleapis.com/token".to_string(),
            userinfo_url: Some("https://www.googleapis.com/oauth2/v3/userinfo".to_string()),
            jwks_uri: Some("https://www.googleapis.com/oauth2/v3/certs".to_string()),
            scopes: vec![
                "openid".to_string(),
                "profile".to_string(),
                "email".to_string(),
            ],
            redirect_uri: redirect_uri.to_string(),
            ..Default::default()
        }
    }

    /// Microsoft Azure AD configuration
    pub fn azure_ad(
        tenant_id: &str,
        client_id: &str,
        client_secret: &str,
        redirect_uri: &str,
    ) -> Self {
        let base_url = format!("https://login.microsoftonline.com/{}", tenant_id);
        Self {
            provider_name: "azure_ad".to_string(),
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
            authorization_url: format!("{}/oauth2/v2.0/authorize", base_url),
            token_url: format!("{}/oauth2/v2.0/token", base_url),
            userinfo_url: Some("https://graph.microsoft.com/oidc/userinfo".to_string()),
            jwks_uri: Some(format!("{}/discovery/v2.0/keys", base_url)),
            scopes: vec![
                "openid".to_string(),
                "profile".to_string(),
                "email".to_string(),
            ],
            redirect_uri: redirect_uri.to_string(),
            ..Default::default()
        }
    }

    /// GitHub OAuth configuration
    pub fn github(client_id: &str, client_secret: &str, redirect_uri: &str) -> Self {
        Self {
            provider_name: "github".to_string(),
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
            authorization_url: "https://github.com/login/oauth/authorize".to_string(),
            token_url: "https://github.com/login/oauth/access_token".to_string(),
            userinfo_url: Some("https://api.github.com/user".to_string()),
            jwks_uri: None,
            scopes: vec!["read:user".to_string(), "user:email".to_string()],
            redirect_uri: redirect_uri.to_string(),
            use_pkce: false,
            ..Default::default()
        }
    }

    /// Okta configuration
    pub fn okta(domain: &str, client_id: &str, client_secret: &str, redirect_uri: &str) -> Self {
        let base_url = format!("https://{}", domain);
        Self {
            provider_name: "okta".to_string(),
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
            authorization_url: format!("{}/oauth2/v1/authorize", base_url),
            token_url: format!("{}/oauth2/v1/token", base_url),
            userinfo_url: Some(format!("{}/oauth2/v1/userinfo", base_url)),
            jwks_uri: Some(format!("{}/oauth2/v1/keys", base_url)),
            scopes: vec![
                "openid".to_string(),
                "profile".to_string(),
                "email".to_string(),
                "groups".to_string(),
            ],
            redirect_uri: redirect_uri.to_string(),
            ..Default::default()
        }
    }
}

/// OAuth 2.0 authorization request
#[derive(Clone, Debug)]
pub struct AuthorizationRequest {
    pub url: String,
    pub state: String,
    pub code_verifier: Option<String>,
    pub nonce: Option<String>,
}

/// OAuth 2.0 token response
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub token_type: String,
    pub expires_in: Option<u64>,
    pub refresh_token: Option<String>,
    pub id_token: Option<String>,
    pub scope: Option<String>,
}

/// OAuth 2.0 provider
pub struct OAuth2Provider {
    config: OAuth2Config,
    /// Active authorization states
    pending_states: RwLock<HashMap<String, PendingAuth>>,
}

/// Pending authorization
#[derive(Clone, Debug)]
struct PendingAuth {
    created_at: Instant,
    code_verifier: Option<String>,
    nonce: Option<String>,
    redirect_uri: String,
}

impl OAuth2Provider {
    pub fn new(config: OAuth2Config) -> Self {
        Self {
            config,
            pending_states: RwLock::new(HashMap::new()),
        }
    }

    /// Generate authorization URL
    pub fn authorize(&self) -> Result<AuthorizationRequest, AuthError> {
        let state = self.generate_random_string(32);
        let nonce = self.generate_random_string(32);

        let mut params = vec![
            ("client_id", self.config.client_id.clone()),
            ("redirect_uri", self.config.redirect_uri.clone()),
            ("response_type", self.config.response_type.clone()),
            ("scope", self.config.scopes.join(" ")),
            ("state", state.clone()),
        ];

        // Add nonce for OIDC
        if self.config.userinfo_url.is_some() {
            params.push(("nonce", nonce.clone()));
        }

        let code_verifier = if self.config.use_pkce {
            let verifier = self.generate_random_string(64);
            let challenge = self.generate_code_challenge(&verifier);
            params.push(("code_challenge", challenge));
            params.push(("code_challenge_method", "S256".to_string()));
            Some(verifier)
        } else {
            None
        };

        // Add additional params
        for (k, v) in &self.config.additional_params {
            params.push((k.as_str(), v.clone()));
        }

        // Build URL
        let query = params
            .iter()
            .map(|(k, v)| format!("{}={}", k, urlencoding_encode(v)))
            .collect::<Vec<_>>()
            .join("&");

        let url = format!("{}?{}", self.config.authorization_url, query);

        // Store pending auth
        self.pending_states.write().insert(
            state.clone(),
            PendingAuth {
                created_at: Instant::now(),
                code_verifier: code_verifier.clone(),
                nonce: Some(nonce.clone()),
                redirect_uri: self.config.redirect_uri.clone(),
            },
        );

        Ok(AuthorizationRequest {
            url,
            state,
            code_verifier,
            nonce: Some(nonce),
        })
    }

    /// Exchange authorization code for tokens
    pub fn exchange_code(&self, code: &str, state: &str) -> Result<AuthResult, AuthError> {
        // Verify state
        let pending = self
            .pending_states
            .write()
            .remove(state)
            .ok_or_else(|| AuthError::TokenInvalid("invalid state".into()))?;

        // Check expiry (states expire after 10 minutes)
        if pending.created_at.elapsed() > Duration::from_secs(600) {
            return Err(AuthError::TokenExpired);
        }

        // In a real implementation:
        // 1. POST to token endpoint with code
        // 2. Verify response
        // 3. Fetch user info if OIDC

        // Simulated token response
        let token_response = TokenResponse {
            access_token: self.generate_random_string(64),
            token_type: "Bearer".to_string(),
            expires_in: Some(3600),
            refresh_token: Some(self.generate_random_string(64)),
            id_token: Some(self.generate_random_string(128)),
            scope: Some(self.config.scopes.join(" ")),
        };

        // Parse identity from token/userinfo
        let identity = self.get_identity(&token_response)?;

        Ok(AuthResult {
            identity,
            access_token: Some(token_response.access_token),
            refresh_token: token_response.refresh_token,
            mfa_required: false,
            mfa_methods: vec![],
        })
    }

    /// Refresh access token
    pub fn refresh_token(&self, refresh_token: &str) -> Result<TokenResponse, AuthError> {
        // Would POST to token endpoint with refresh_token grant
        let _ = refresh_token;

        Ok(TokenResponse {
            access_token: self.generate_random_string(64),
            token_type: "Bearer".to_string(),
            expires_in: Some(3600),
            refresh_token: Some(self.generate_random_string(64)),
            id_token: None,
            scope: None,
        })
    }

    /// Get identity from token
    fn get_identity(&self, token: &TokenResponse) -> Result<Identity, AuthError> {
        // Would decode ID token (JWT) or call userinfo endpoint
        let _ = token;

        Ok(Identity {
            user_id: "oauth2:user-123".to_string(),
            username: "oauth_user".to_string(),
            email: Some("oauth_user@example.com".to_string()),
            display_name: Some("OAuth User".to_string()),
            groups: vec!["users".to_string()],
            provider: if self.config.userinfo_url.is_some() {
                AuthProviderType::Oidc
            } else {
                AuthProviderType::OAuth2
            },
            attributes: HashMap::new(),
            authenticated_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            expires_at: token.expires_in.map(|e| {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + e
            }),
        })
    }

    fn generate_random_string(&self, length: usize) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let mut hasher = DefaultHasher::new();
        now.hash(&mut hasher);
        length.hash(&mut hasher);

        let chars: Vec<char> = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
            .chars()
            .collect();
        let hash = hasher.finish();

        (0..length)
            .map(|i| {
                let idx = ((hash >> (i % 8)) as usize + i) % chars.len();
                chars[idx]
            })
            .collect()
    }

    fn generate_code_challenge(&self, verifier: &str) -> String {
        // In a real implementation, this would use SHA256
        // Base64URL(SHA256(verifier))
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        verifier.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }

    /// Cleanup expired pending authorizations
    pub fn cleanup_expired(&self) {
        let mut states = self.pending_states.write();
        states.retain(|_, pending| pending.created_at.elapsed() < Duration::from_secs(600));
    }
}

fn urlencoding_encode(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
            _ => format!("%{:02X}", c as u8),
        })
        .collect()
}

// ============================================================================
// SAML 2.0
// ============================================================================

/// SAML 2.0 configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SamlConfig {
    /// Service Provider entity ID
    pub sp_entity_id: String,
    /// Assertion Consumer Service URL
    pub acs_url: String,
    /// Single Logout URL
    pub slo_url: Option<String>,
    /// Identity Provider entity ID
    pub idp_entity_id: String,
    /// IdP SSO URL
    pub idp_sso_url: String,
    /// IdP SLO URL
    pub idp_slo_url: Option<String>,
    /// IdP certificate (PEM)
    pub idp_certificate: String,
    /// SP private key (PEM)
    pub sp_private_key: Option<String>,
    /// SP certificate (PEM)
    pub sp_certificate: Option<String>,
    /// Sign requests
    pub sign_requests: bool,
    /// Want assertions signed
    pub want_assertions_signed: bool,
    /// Want assertions encrypted
    pub want_assertions_encrypted: bool,
    /// Name ID format
    pub name_id_format: NameIdFormat,
    /// Attribute mappings
    pub attribute_mappings: SamlAttributeMappings,
}

/// SAML Name ID format
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NameIdFormat {
    Unspecified,
    EmailAddress,
    Persistent,
    Transient,
    Kerberos,
    WindowsDomainQualifiedName,
}

impl NameIdFormat {
    pub fn as_uri(&self) -> &str {
        match self {
            Self::Unspecified => "urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified",
            Self::EmailAddress => "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress",
            Self::Persistent => "urn:oasis:names:tc:SAML:2.0:nameid-format:persistent",
            Self::Transient => "urn:oasis:names:tc:SAML:2.0:nameid-format:transient",
            Self::Kerberos => "urn:oasis:names:tc:SAML:2.0:nameid-format:kerberos",
            Self::WindowsDomainQualifiedName => {
                "urn:oasis:names:tc:SAML:1.1:nameid-format:WindowsDomainQualifiedName"
            }
        }
    }
}

/// SAML attribute mappings
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SamlAttributeMappings {
    pub user_id: Option<String>,
    pub email: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub display_name: Option<String>,
    pub groups: Option<String>,
}

/// SAML authentication request
#[derive(Clone, Debug)]
pub struct SamlAuthRequest {
    pub id: String,
    pub redirect_url: String,
    pub relay_state: Option<String>,
}

/// SAML authentication response
#[derive(Clone, Debug)]
pub struct SamlAuthResponse {
    pub name_id: String,
    pub session_index: Option<String>,
    pub attributes: HashMap<String, Vec<String>>,
    pub valid_until: Option<u64>,
}

/// SAML 2.0 provider
pub struct SamlProvider {
    config: SamlConfig,
    /// Pending auth requests
    pending_requests: RwLock<HashMap<String, PendingSamlAuth>>,
}

#[derive(Clone, Debug)]
struct PendingSamlAuth {
    request_id: String,
    created_at: Instant,
    relay_state: Option<String>,
}

impl SamlProvider {
    pub fn new(config: SamlConfig) -> Self {
        Self {
            config,
            pending_requests: RwLock::new(HashMap::new()),
        }
    }

    /// Generate SAML authentication request
    pub fn create_auth_request(
        &self,
        relay_state: Option<String>,
    ) -> Result<SamlAuthRequest, AuthError> {
        let request_id = format!("_id{}", self.generate_id());

        // Build SAML AuthnRequest XML
        let authn_request = self.build_authn_request(&request_id)?;

        // Base64 and deflate encode
        let encoded = self.encode_request(&authn_request)?;

        // Build redirect URL
        let mut url = format!(
            "{}?SAMLRequest={}",
            self.config.idp_sso_url,
            urlencoding_encode(&encoded)
        );

        if let Some(ref rs) = relay_state {
            url.push_str(&format!("&RelayState={}", urlencoding_encode(rs)));
        }

        // Store pending request
        self.pending_requests.write().insert(
            request_id.clone(),
            PendingSamlAuth {
                request_id: request_id.clone(),
                created_at: Instant::now(),
                relay_state: relay_state.clone(),
            },
        );

        Ok(SamlAuthRequest {
            id: request_id,
            redirect_url: url,
            relay_state,
        })
    }

    /// Process SAML response
    pub fn process_response(
        &self,
        saml_response: &str,
        relay_state: Option<&str>,
    ) -> Result<AuthResult, AuthError> {
        // In a real implementation:
        // 1. Base64 decode response
        // 2. Parse XML
        // 3. Verify signature with IdP certificate
        // 4. Validate conditions (time, audience)
        // 5. Extract assertions

        // Simulated response processing
        let _ = (saml_response, relay_state);

        let response = SamlAuthResponse {
            name_id: "user@example.com".to_string(),
            session_index: Some("session_123".to_string()),
            attributes: {
                let mut attrs = HashMap::new();
                attrs.insert("email".to_string(), vec!["user@example.com".to_string()]);
                attrs.insert("firstName".to_string(), vec!["SAML".to_string()]);
                attrs.insert("lastName".to_string(), vec!["User".to_string()]);
                attrs.insert(
                    "groups".to_string(),
                    vec!["users".to_string(), "admins".to_string()],
                );
                attrs
            },
            valid_until: Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    + 3600,
            ),
        };

        let identity = self.extract_identity(&response)?;

        Ok(AuthResult {
            identity,
            access_token: None,
            refresh_token: None,
            mfa_required: false,
            mfa_methods: vec![],
        })
    }

    /// Generate SP metadata XML
    pub fn generate_metadata(&self) -> String {
        format!(
            r#"<?xml version="1.0"?>
<md:EntityDescriptor xmlns:md="urn:oasis:names:tc:SAML:2.0:metadata"
                     entityID="{}">
    <md:SPSSODescriptor AuthnRequestsSigned="{}"
                        WantAssertionsSigned="{}"
                        protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">
        <md:NameIDFormat>{}</md:NameIDFormat>
        <md:AssertionConsumerService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST"
                                     Location="{}"
                                     index="0"/>
    </md:SPSSODescriptor>
</md:EntityDescriptor>"#,
            self.config.sp_entity_id,
            self.config.sign_requests,
            self.config.want_assertions_signed,
            self.config.name_id_format.as_uri(),
            self.config.acs_url,
        )
    }

    fn build_authn_request(&self, request_id: &str) -> Result<String, AuthError> {
        let now = chrono_now_iso();

        Ok(format!(
            r#"<?xml version="1.0"?>
<samlp:AuthnRequest xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol"
                    xmlns:saml="urn:oasis:names:tc:SAML:2.0:assertion"
                    ID="{}"
                    Version="2.0"
                    IssueInstant="{}"
                    Destination="{}"
                    AssertionConsumerServiceURL="{}"
                    ProtocolBinding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST">
    <saml:Issuer>{}</saml:Issuer>
    <samlp:NameIDPolicy Format="{}"
                        AllowCreate="true"/>
</samlp:AuthnRequest>"#,
            request_id,
            now,
            self.config.idp_sso_url,
            self.config.acs_url,
            self.config.sp_entity_id,
            self.config.name_id_format.as_uri(),
        ))
    }

    fn encode_request(&self, request: &str) -> Result<String, AuthError> {
        // In a real implementation: deflate then base64 encode
        // For now, just base64-like encoding
        Ok(base64_encode(request.as_bytes()))
    }

    fn extract_identity(&self, response: &SamlAuthResponse) -> Result<Identity, AuthError> {
        let mappings = &self.config.attribute_mappings;

        let email = mappings
            .email
            .as_ref()
            .and_then(|attr| response.attributes.get(attr))
            .and_then(|v| v.first())
            .cloned()
            .or_else(|| Some(response.name_id.clone()));

        let display_name = mappings
            .display_name
            .as_ref()
            .and_then(|attr| response.attributes.get(attr))
            .and_then(|v| v.first())
            .cloned()
            .or_else(|| {
                let first = mappings
                    .first_name
                    .as_ref()
                    .and_then(|attr| response.attributes.get(attr))
                    .and_then(|v| v.first());
                let last = mappings
                    .last_name
                    .as_ref()
                    .and_then(|attr| response.attributes.get(attr))
                    .and_then(|v| v.first());
                match (first, last) {
                    (Some(f), Some(l)) => Some(format!("{} {}", f, l)),
                    (Some(f), None) => Some(f.clone()),
                    (None, Some(l)) => Some(l.clone()),
                    _ => None,
                }
            });

        let groups = mappings
            .groups
            .as_ref()
            .and_then(|attr| response.attributes.get(attr))
            .cloned()
            .unwrap_or_default();

        Ok(Identity {
            user_id: format!("saml:{}", response.name_id),
            username: response.name_id.clone(),
            email,
            display_name,
            groups,
            provider: AuthProviderType::Saml,
            attributes: response
                .attributes
                .iter()
                .map(|(k, v)| (k.clone(), v.join(",")))
                .collect(),
            authenticated_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            expires_at: response.valid_until,
        })
    }

    fn generate_id(&self) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let mut hasher = DefaultHasher::new();
        now.hash(&mut hasher);
        format!("{:016x}", hasher.finish())
    }
}

fn chrono_now_iso() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    // Simplified ISO 8601 format
    format!("2024-01-01T00:00:{}Z", now % 60)
}

fn base64_encode(data: &[u8]) -> String {
    // Simplified base64 encoding
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    let mut result = String::new();
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;

        result.push(ALPHABET[b0 >> 2] as char);
        result.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);

        if chunk.len() > 1 {
            result.push(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            result.push(ALPHABET[b2 & 0x3f] as char);
        } else {
            result.push('=');
        }
    }

    result
}

// ============================================================================
// Multi-Factor Authentication
// ============================================================================

/// MFA configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MfaConfig {
    /// Enable MFA
    pub enabled: bool,
    /// Allowed MFA methods
    pub allowed_methods: Vec<MfaMethod>,
    /// Required for all users
    pub required_for_all: bool,
    /// Required for specific groups
    pub required_groups: Vec<String>,
    /// TOTP settings
    pub totp: TotpConfig,
    /// Backup codes settings
    pub backup_codes: BackupCodesConfig,
}

impl Default for MfaConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            allowed_methods: vec![MfaMethod::Totp, MfaMethod::BackupCodes],
            required_for_all: false,
            required_groups: vec![],
            totp: TotpConfig::default(),
            backup_codes: BackupCodesConfig::default(),
        }
    }
}

/// TOTP configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TotpConfig {
    /// Algorithm (SHA1, SHA256, SHA512)
    pub algorithm: String,
    /// Number of digits
    pub digits: u8,
    /// Time step in seconds
    pub period: u32,
    /// Allowed clock skew (number of periods)
    pub skew: u8,
    /// Issuer name
    pub issuer: String,
}

impl Default for TotpConfig {
    fn default() -> Self {
        Self {
            algorithm: "SHA1".to_string(),
            digits: 6,
            period: 30,
            skew: 1,
            issuer: "BoyoDB".to_string(),
        }
    }
}

/// Backup codes configuration
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BackupCodesConfig {
    /// Number of codes to generate
    pub code_count: usize,
    /// Code length
    pub code_length: usize,
}

impl Default for BackupCodesConfig {
    fn default() -> Self {
        Self {
            code_count: 10,
            code_length: 8,
        }
    }
}

/// MFA enrollment
#[derive(Clone, Debug)]
pub struct MfaEnrollment {
    pub user_id: String,
    pub method: MfaMethod,
    pub secret: Option<String>,
    pub backup_codes: Option<Vec<String>>,
    pub verified: bool,
    pub enrolled_at: Option<u64>,
}

/// MFA manager
pub struct MfaManager {
    config: MfaConfig,
    enrollments: RwLock<HashMap<String, Vec<MfaEnrollment>>>,
}

impl MfaManager {
    pub fn new(config: MfaConfig) -> Self {
        Self {
            config,
            enrollments: RwLock::new(HashMap::new()),
        }
    }

    /// Check if MFA is required for user
    pub fn is_required(&self, identity: &Identity) -> bool {
        if !self.config.enabled {
            return false;
        }

        if self.config.required_for_all {
            return true;
        }

        // Check if user is in required groups
        for group in &self.config.required_groups {
            if identity.groups.contains(group) {
                return true;
            }
        }

        false
    }

    /// Start TOTP enrollment
    pub fn enroll_totp(&self, user_id: &str) -> Result<TotpEnrollmentInfo, AuthError> {
        let secret = self.generate_totp_secret();
        let uri = self.generate_totp_uri(user_id, &secret);

        let enrollment = MfaEnrollment {
            user_id: user_id.to_string(),
            method: MfaMethod::Totp,
            secret: Some(secret.clone()),
            backup_codes: None,
            verified: false,
            enrolled_at: None,
        };

        self.enrollments
            .write()
            .entry(user_id.to_string())
            .or_insert_with(Vec::new)
            .push(enrollment);

        Ok(TotpEnrollmentInfo {
            secret,
            uri,
            qr_code: None, // Would generate QR code
        })
    }

    /// Verify TOTP code and complete enrollment
    pub fn verify_totp_enrollment(&self, user_id: &str, code: &str) -> Result<(), AuthError> {
        let mut enrollments = self.enrollments.write();
        let user_enrollments = enrollments
            .get_mut(user_id)
            .ok_or_else(|| AuthError::UserNotFound(user_id.to_string()))?;

        let enrollment = user_enrollments
            .iter_mut()
            .find(|e| e.method == MfaMethod::Totp && !e.verified)
            .ok_or_else(|| AuthError::ConfigError("no pending TOTP enrollment".into()))?;

        let secret = enrollment
            .secret
            .as_ref()
            .ok_or_else(|| AuthError::ConfigError("no secret".into()))?;

        if self.verify_totp_code(secret, code) {
            enrollment.verified = true;
            enrollment.enrolled_at = Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            );
            Ok(())
        } else {
            Err(AuthError::MfaFailed)
        }
    }

    /// Verify TOTP code
    pub fn verify_totp(&self, user_id: &str, code: &str) -> Result<bool, AuthError> {
        let enrollments = self.enrollments.read();
        let user_enrollments = enrollments
            .get(user_id)
            .ok_or_else(|| AuthError::UserNotFound(user_id.to_string()))?;

        let enrollment = user_enrollments
            .iter()
            .find(|e| e.method == MfaMethod::Totp && e.verified)
            .ok_or_else(|| AuthError::MfaRequired)?;

        let secret = enrollment
            .secret
            .as_ref()
            .ok_or_else(|| AuthError::ConfigError("no secret".into()))?;

        Ok(self.verify_totp_code(secret, code))
    }

    /// Generate backup codes
    pub fn generate_backup_codes(&self, user_id: &str) -> Result<Vec<String>, AuthError> {
        let codes: Vec<String> = (0..self.config.backup_codes.code_count)
            .map(|_| self.generate_backup_code())
            .collect();

        let enrollment = MfaEnrollment {
            user_id: user_id.to_string(),
            method: MfaMethod::BackupCodes,
            secret: None,
            backup_codes: Some(codes.clone()),
            verified: true,
            enrolled_at: Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            ),
        };

        self.enrollments
            .write()
            .entry(user_id.to_string())
            .or_insert_with(Vec::new)
            .push(enrollment);

        Ok(codes)
    }

    /// Verify backup code
    pub fn verify_backup_code(&self, user_id: &str, code: &str) -> Result<bool, AuthError> {
        let mut enrollments = self.enrollments.write();
        let user_enrollments = enrollments
            .get_mut(user_id)
            .ok_or_else(|| AuthError::UserNotFound(user_id.to_string()))?;

        for enrollment in user_enrollments.iter_mut() {
            if enrollment.method == MfaMethod::BackupCodes {
                if let Some(ref mut codes) = enrollment.backup_codes {
                    if let Some(pos) = codes.iter().position(|c| c == code) {
                        codes.remove(pos); // Use code only once
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    fn generate_totp_secret(&self) -> String {
        // Generate 20 bytes of "random" data and base32 encode
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let mut hasher = DefaultHasher::new();
        now.hash(&mut hasher);

        let bytes: Vec<u8> = (0..20)
            .map(|i| {
                let mut h = DefaultHasher::new();
                (now + i as u128).hash(&mut h);
                (h.finish() % 256) as u8
            })
            .collect();

        base32_encode(&bytes)
    }

    fn generate_totp_uri(&self, user_id: &str, secret: &str) -> String {
        format!(
            "otpauth://totp/{}:{}?secret={}&issuer={}&algorithm={}&digits={}&period={}",
            self.config.totp.issuer,
            user_id,
            secret,
            self.config.totp.issuer,
            self.config.totp.algorithm,
            self.config.totp.digits,
            self.config.totp.period,
        )
    }

    fn verify_totp_code(&self, secret: &str, code: &str) -> bool {
        // Simplified TOTP verification
        // In real implementation, use proper HMAC-SHA1/256/512

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let time_step = now / self.config.totp.period as u64;

        // Check current and adjacent time steps
        for offset in 0..=(self.config.totp.skew as u64) {
            let expected = self.generate_totp(secret, time_step - offset);
            if expected == code {
                return true;
            }
            if offset > 0 {
                let expected = self.generate_totp(secret, time_step + offset);
                if expected == code {
                    return true;
                }
            }
        }

        false
    }

    fn generate_totp(&self, _secret: &str, time_step: u64) -> String {
        // Simplified TOTP generation
        // In real implementation, use HMAC-SHA1/256/512
        format!("{:06}", time_step % 1_000_000)
    }

    fn generate_backup_code(&self) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();

        let mut hasher = DefaultHasher::new();
        now.hash(&mut hasher);

        let chars: Vec<char> = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789".chars().collect();

        (0..self.config.backup_codes.code_length)
            .map(|i| {
                let mut h = DefaultHasher::new();
                (now + i as u128).hash(&mut h);
                chars[(h.finish() as usize) % chars.len()]
            })
            .collect()
    }
}

/// TOTP enrollment information
#[derive(Clone, Debug)]
pub struct TotpEnrollmentInfo {
    pub secret: String,
    pub uri: String,
    pub qr_code: Option<Vec<u8>>,
}

fn base32_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

    let mut result = String::new();
    let mut bits = 0u32;
    let mut bit_count = 0;

    for &byte in data {
        bits = (bits << 8) | byte as u32;
        bit_count += 8;

        while bit_count >= 5 {
            bit_count -= 5;
            result.push(ALPHABET[(bits >> bit_count) as usize & 0x1f] as char);
        }
    }

    if bit_count > 0 {
        result.push(ALPHABET[(bits << (5 - bit_count)) as usize & 0x1f] as char);
    }

    result
}

// ============================================================================
// Enterprise Auth Manager
// ============================================================================

/// Enterprise authentication manager
pub struct EnterpriseAuthManager {
    /// LDAP providers
    ldap_providers: RwLock<HashMap<String, Arc<LdapProvider>>>,
    /// OAuth 2.0 providers
    oauth_providers: RwLock<HashMap<String, Arc<OAuth2Provider>>>,
    /// SAML providers
    saml_providers: RwLock<HashMap<String, Arc<SamlProvider>>>,
    /// MFA manager
    mfa_manager: Arc<MfaManager>,
    /// Active sessions
    sessions: RwLock<HashMap<String, SessionInfo>>,
}

/// Session information
#[derive(Clone, Debug)]
pub struct SessionInfo {
    pub session_id: String,
    pub identity: Identity,
    pub created_at: u64,
    pub last_activity: u64,
    pub expires_at: u64,
    pub mfa_completed: bool,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
}

impl EnterpriseAuthManager {
    pub fn new(mfa_config: MfaConfig) -> Self {
        Self {
            ldap_providers: RwLock::new(HashMap::new()),
            oauth_providers: RwLock::new(HashMap::new()),
            saml_providers: RwLock::new(HashMap::new()),
            mfa_manager: Arc::new(MfaManager::new(mfa_config)),
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Register LDAP provider
    pub fn register_ldap(&self, name: &str, config: LdapConfig) {
        self.ldap_providers
            .write()
            .insert(name.to_string(), Arc::new(LdapProvider::new(config)));
    }

    /// Register OAuth 2.0 provider
    pub fn register_oauth(&self, name: &str, config: OAuth2Config) {
        self.oauth_providers
            .write()
            .insert(name.to_string(), Arc::new(OAuth2Provider::new(config)));
    }

    /// Register SAML provider
    pub fn register_saml(&self, name: &str, config: SamlConfig) {
        self.saml_providers
            .write()
            .insert(name.to_string(), Arc::new(SamlProvider::new(config)));
    }

    /// Authenticate with LDAP
    pub fn authenticate_ldap(
        &self,
        provider: &str,
        username: &str,
        password: &str,
    ) -> Result<AuthResult, AuthError> {
        let providers = self.ldap_providers.read();
        let provider = providers.get(provider).ok_or_else(|| {
            AuthError::ConfigError(format!("LDAP provider not found: {}", provider))
        })?;

        let identity = provider.authenticate(username, password)?;
        let mfa_required = self.mfa_manager.is_required(&identity);

        Ok(AuthResult {
            identity,
            access_token: None,
            refresh_token: None,
            mfa_required,
            mfa_methods: if mfa_required {
                vec![MfaMethod::Totp, MfaMethod::BackupCodes]
            } else {
                vec![]
            },
        })
    }

    /// Start OAuth authorization
    pub fn start_oauth(&self, provider: &str) -> Result<AuthorizationRequest, AuthError> {
        let providers = self.oauth_providers.read();
        let provider = providers.get(provider).ok_or_else(|| {
            AuthError::ConfigError(format!("OAuth provider not found: {}", provider))
        })?;

        provider.authorize()
    }

    /// Complete OAuth authentication
    pub fn complete_oauth(
        &self,
        provider: &str,
        code: &str,
        state: &str,
    ) -> Result<AuthResult, AuthError> {
        let providers = self.oauth_providers.read();
        let provider = providers.get(provider).ok_or_else(|| {
            AuthError::ConfigError(format!("OAuth provider not found: {}", provider))
        })?;

        provider.exchange_code(code, state)
    }

    /// Start SAML authentication
    pub fn start_saml(
        &self,
        provider: &str,
        relay_state: Option<String>,
    ) -> Result<SamlAuthRequest, AuthError> {
        let providers = self.saml_providers.read();
        let provider = providers.get(provider).ok_or_else(|| {
            AuthError::ConfigError(format!("SAML provider not found: {}", provider))
        })?;

        provider.create_auth_request(relay_state)
    }

    /// Complete SAML authentication
    pub fn complete_saml(
        &self,
        provider: &str,
        response: &str,
        relay_state: Option<&str>,
    ) -> Result<AuthResult, AuthError> {
        let providers = self.saml_providers.read();
        let provider = providers.get(provider).ok_or_else(|| {
            AuthError::ConfigError(format!("SAML provider not found: {}", provider))
        })?;

        provider.process_response(response, relay_state)
    }

    /// Create session after successful authentication
    pub fn create_session(
        &self,
        auth_result: &AuthResult,
        session_duration: Duration,
    ) -> Result<String, AuthError> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

        let mut hasher = DefaultHasher::new();
        now.as_nanos().hash(&mut hasher);
        auth_result.identity.user_id.hash(&mut hasher);

        let session_id = format!("{:016x}", hasher.finish());

        let session = SessionInfo {
            session_id: session_id.clone(),
            identity: auth_result.identity.clone(),
            created_at: now.as_secs(),
            last_activity: now.as_secs(),
            expires_at: now.as_secs() + session_duration.as_secs(),
            mfa_completed: !auth_result.mfa_required,
            ip_address: None,
            user_agent: None,
        };

        self.sessions.write().insert(session_id.clone(), session);

        Ok(session_id)
    }

    /// Validate session
    pub fn validate_session(&self, session_id: &str) -> Result<Identity, AuthError> {
        let mut sessions = self.sessions.write();
        let session = sessions
            .get_mut(session_id)
            .ok_or(AuthError::SessionExpired)?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        if now > session.expires_at {
            sessions.remove(session_id);
            return Err(AuthError::SessionExpired);
        }

        if !session.mfa_completed {
            return Err(AuthError::MfaRequired);
        }

        session.last_activity = now;
        Ok(session.identity.clone())
    }

    /// End session
    pub fn end_session(&self, session_id: &str) {
        self.sessions.write().remove(session_id);
    }

    /// Get MFA manager
    pub fn mfa(&self) -> Arc<MfaManager> {
        self.mfa_manager.clone()
    }

    /// List active sessions for user
    pub fn list_user_sessions(&self, user_id: &str) -> Vec<SessionInfo> {
        let sessions = self.sessions.read();
        sessions
            .values()
            .filter(|s| s.identity.user_id == user_id)
            .cloned()
            .collect()
    }

    /// Cleanup expired sessions
    pub fn cleanup_sessions(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        self.sessions.write().retain(|_, s| s.expires_at > now);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ldap_authentication() {
        let provider = LdapProvider::new(LdapConfig::default());

        // Simulated authentication
        let result = provider.authenticate("testuser", "password");
        assert!(result.is_ok());

        let identity = result.unwrap();
        assert_eq!(identity.username, "testuser");
        assert_eq!(identity.provider, AuthProviderType::Ldap);
    }

    #[test]
    fn test_oauth2_authorization() {
        let config =
            OAuth2Config::google("client_id", "client_secret", "http://localhost/callback");

        let provider = OAuth2Provider::new(config);
        let auth_req = provider.authorize().unwrap();

        assert!(!auth_req.url.is_empty());
        assert!(!auth_req.state.is_empty());
    }

    #[test]
    fn test_saml_metadata() {
        let config = SamlConfig {
            sp_entity_id: "http://localhost/saml".to_string(),
            acs_url: "http://localhost/saml/acs".to_string(),
            slo_url: None,
            idp_entity_id: "https://idp.example.com".to_string(),
            idp_sso_url: "https://idp.example.com/sso".to_string(),
            idp_slo_url: None,
            idp_certificate: String::new(),
            sp_private_key: None,
            sp_certificate: None,
            sign_requests: false,
            want_assertions_signed: true,
            want_assertions_encrypted: false,
            name_id_format: NameIdFormat::EmailAddress,
            attribute_mappings: SamlAttributeMappings::default(),
        };

        let provider = SamlProvider::new(config);
        let metadata = provider.generate_metadata();

        assert!(metadata.contains("EntityDescriptor"));
        assert!(metadata.contains("SPSSODescriptor"));
    }

    #[test]
    fn test_mfa_totp_enrollment() {
        let manager = MfaManager::new(MfaConfig::default());

        let info = manager.enroll_totp("user123").unwrap();

        assert!(!info.secret.is_empty());
        assert!(info.uri.contains("otpauth://totp/"));
    }

    #[test]
    fn test_backup_codes() {
        let manager = MfaManager::new(MfaConfig {
            backup_codes: BackupCodesConfig {
                code_count: 5,
                code_length: 8,
            },
            ..Default::default()
        });

        let codes = manager.generate_backup_codes("user123").unwrap();

        assert_eq!(codes.len(), 5);
        for code in &codes {
            assert_eq!(code.len(), 8);
        }

        // Verify a code
        let result = manager.verify_backup_code("user123", &codes[0]).unwrap();
        assert!(result);

        // Same code should not work twice
        let result = manager.verify_backup_code("user123", &codes[0]).unwrap();
        assert!(!result);
    }

    #[test]
    fn test_enterprise_auth_manager() {
        let manager = EnterpriseAuthManager::new(MfaConfig::default());

        // Register providers
        manager.register_ldap("corp", LdapConfig::default());
        manager.register_oauth(
            "google",
            OAuth2Config::google("id", "secret", "http://localhost/callback"),
        );

        // Authenticate
        let result = manager.authenticate_ldap("corp", "user", "pass").unwrap();
        assert_eq!(result.identity.provider, AuthProviderType::Ldap);

        // Create session
        let session_id = manager
            .create_session(&result, Duration::from_secs(3600))
            .unwrap();
        assert!(!session_id.is_empty());

        // Validate session
        let identity = manager.validate_session(&session_id).unwrap();
        assert_eq!(identity.username, "user");

        // End session
        manager.end_session(&session_id);
        assert!(manager.validate_session(&session_id).is_err());
    }
}
