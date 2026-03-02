# Security Guide

Comprehensive security documentation for BoyoDB.

## Overview

BoyoDB implements multiple layers of security:

1. **Authentication**: Verify user identity
2. **Authorization**: Control access to resources
3. **Encryption**: Protect data in transit
4. **Rate Limiting**: Prevent abuse
5. **Input Validation**: Protect against attacks

---

## Authentication

### Token-Based Authentication

For simple deployments, use a shared token:

```bash
# Start server with token
boyodb-server /data 0.0.0.0:8765 --token my-secret-token

# Client must include token in requests
{"auth": "my-secret-token", "op": "query", "sql": "SELECT 1"}
```

**Best Practices:**
- Use a strong, random token (minimum 32 characters)
- Rotate tokens periodically
- Never commit tokens to version control
- Use environment variables for token storage

### User Authentication

For production deployments, use user-based authentication:

```sql
-- Create users
CREATE USER analyst WITH PASSWORD 'SecurePass123!';
CREATE USER readonly_user WITH PASSWORD 'AnotherPass456!';

-- Login
login analyst
Password: ********
```

#### Password Requirements

Default password policy:
- Minimum 8 characters
- At least one uppercase letter
- At least one lowercase letter
- At least one digit
- At least one special character

#### Password Hashing

Passwords are hashed using **Argon2id** with secure parameters:
- Memory: 19 MiB
- Iterations: 2
- Parallelism: 1
- Salt: 16 random bytes
- Hash length: 32 bytes

```rust
// Internal hashing (for reference)
Argon2::new(
    Algorithm::Argon2id,
    Version::V0x13,
    Params::new(19456, 2, 1, Some(32))
)
```

### Session Management

After successful login, a session is created:

```json
{
  "status": "ok",
  "session_id": "550e8400-e29b-41d4-a716-446655440000",
  "message": "logged in as analyst"
}
```

**Session Properties:**
- UUID-based session IDs
- Automatic expiration (configurable)
- Server-side session storage
- Session cleanup on logout

---

## Authorization

### Role-Based Access Control (RBAC)

BoyoDB uses a role-based permission model.

#### Built-in Roles

| Role | Description | Privileges |
|------|-------------|------------|
| `admin` | Full administrative access | SUPERUSER |
| `readonly` | Read-only access | SELECT on all |
| `readwrite` | Read and write access | SELECT, INSERT, UPDATE, DELETE |

#### Custom Roles

```sql
-- Create custom role
CREATE ROLE analysts;

-- Grant privileges to role
GRANT SELECT ON DATABASE analytics TO analysts;
GRANT INSERT ON TABLE analytics.events TO analysts;

-- Assign role to user
GRANT analysts TO analyst_user;
```

### Privilege Hierarchy

```
SUPERUSER (root access)
│
├── ALL (all standard privileges)
│   ├── SELECT    - Read data
│   ├── INSERT    - Add data
│   ├── UPDATE    - Modify data
│   ├── DELETE    - Remove data
│   ├── CREATE    - Create objects
│   ├── DROP      - Remove objects
│   ├── ALTER     - Modify objects
│   ├── TRUNCATE  - Empty tables
│   └── GRANT     - Delegate privileges
│
└── Administrative
    ├── CREATEDB   - Create databases
    ├── CREATEUSER - Manage users
    ├── CONNECT    - Connect to database
    └── USAGE      - Use schema/objects
```

### Privilege Targets

Privileges can be granted at different levels:

```sql
-- Database level
GRANT SELECT ON DATABASE analytics TO analyst;

-- Table level
GRANT INSERT ON TABLE analytics.events TO writer;

-- All databases
GRANT SELECT ON ALL DATABASES TO reader;

-- All tables in database
GRANT SELECT ON ALL TABLES IN DATABASE analytics TO analyst;
```

### Viewing Grants

```sql
-- Show user's privileges
SHOW GRANTS FOR analyst;

-- Show all users
SHOW USERS;

-- Show all roles
SHOW ROLES;
```

### Privilege Checks

Every operation is checked against the user's privileges:

| Operation | Required Privilege |
|-----------|-------------------|
| `SELECT` | SELECT on table |
| `INSERT` | INSERT on table |
| `UPDATE` | UPDATE on table |
| `DELETE` | DELETE on table |
| `CREATE TABLE` | CREATE on database |
| `DROP TABLE` | DROP on database |
| `CREATE DATABASE` | CREATEDB |
| `CREATE USER` | CREATEUSER or SUPERUSER |

---

## Encryption

### TLS Configuration

Enable TLS for encrypted connections:

```bash
# Generate certificates (for testing)
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes

# Start server with TLS
boyodb-server /data 0.0.0.0:8765 \
  --tls \
  --cert /path/to/cert.pem \
  --key /path/to/key.pem

# Client connection
boyodb-cli shell --host server:8765 --tls --ca-cert /path/to/ca.pem
```

### TLS Settings

| Option | Description | Default |
|--------|-------------|---------|
| `--tls` | Enable TLS | false |
| `--cert` | Certificate file | Required if TLS |
| `--key` | Private key file | Required if TLS |
| `--ca-cert` | CA certificate (client) | System CA |
| `--insecure` | Skip verification | false |

### Certificate Requirements

**Production certificates should:**
- Be signed by a trusted CA
- Have valid dates (not expired)
- Match the server hostname
- Use RSA 2048+ or ECDSA P-256+
- Use SHA-256 or stronger

### Mutual TLS (mTLS)

For maximum security, enable client certificate verification:

```bash
boyodb-server /data 0.0.0.0:8765 \
  --tls \
  --cert server.pem \
  --key server-key.pem \
  --client-ca client-ca.pem \
  --require-client-cert
```

---

## Rate Limiting

### Authentication Rate Limiting

Prevents brute-force attacks on login:

| Parameter | Default | Description |
|-----------|---------|-------------|
| Max attempts | 10 | Attempts per window |
| Window | 60 seconds | Time window |
| Lockout | Automatic | After threshold |

After exceeding the limit:
```json
{
  "status": "error",
  "message": "rate limit exceeded, try again later"
}
```

### Account Lockout

Failed login attempts are tracked per user:

```sql
-- Lock a user manually
LOCK USER suspicious_user;

-- Unlock a user
UNLOCK USER restored_user;

-- Check user status
SHOW USERS;
-- Returns: username | status | roles
--          analyst  | active | readonly
--          hacker   | locked |
```

**Automatic lockout triggers:**
- 5 consecutive failed login attempts
- Suspicious activity patterns
- Administrative action

---

## Input Validation

### SQL Injection Prevention

BoyoDB uses parameterized queries internally:

```sql
-- Use prepared statements for dynamic values
PREPARE get_user AS SELECT * FROM users WHERE id = $1;
EXECUTE get_user (42);

-- Never concatenate user input into SQL strings
```

### Size Limits

Configurable limits prevent resource exhaustion:

| Limit | Default | Flag |
|-------|---------|------|
| Max IPC payload | 32 MB | `--max-ipc-bytes` |
| Max frame size | 64 MB | `--max-frame-bytes` |
| Max query length | 32 KB | `--max-query-len` |
| Max connections | 64 | `--max-conns` |

### Decompression Bomb Protection

When accepting compressed data, BoyoDB validates:
- Compression ratio limits
- Maximum decompressed size
- Valid compression format

```rust
// Internal check
if decompressed_size > compressed_size * MAX_COMPRESSION_RATIO {
    return Err("decompression bomb detected");
}
```

---

## Security Best Practices

### Deployment Checklist

- [ ] **Enable TLS** for all production deployments
- [ ] **Use strong passwords** meeting policy requirements
- [ ] **Create dedicated users** instead of sharing credentials
- [ ] **Apply least privilege** - grant only necessary permissions
- [ ] **Rotate credentials** periodically
- [ ] **Monitor logs** for suspicious activity
- [ ] **Keep updated** with security patches
- [ ] **Backup regularly** including auth_store.json

### Network Security

```bash
# Bind to specific interface (not 0.0.0.0 in production)
boyodb-server /data 10.0.1.5:8765

# Use firewall rules
iptables -A INPUT -p tcp --dport 8765 -s 10.0.0.0/8 -j ACCEPT
iptables -A INPUT -p tcp --dport 8765 -j DROP
```

### File Permissions

```bash
# Secure the data directory
chmod 700 /var/lib/boyodb
chown boyodb:boyodb /var/lib/boyodb

# Secure auth store
chmod 600 /var/lib/boyodb/auth_store.json

# Secure TLS keys
chmod 600 /etc/boyodb/key.pem
```

### Configuration File Security

```bash
# Secure client config
chmod 600 ~/.boyodbrc
```

Example secure `.boyodbrc`:
```ini
host = "production-db.example.com:8765"
tls = true
ca_cert = "/etc/ssl/certs/ca-bundle.crt"
# Never store passwords in config files
# Use environment variables instead
```

### Environment Variables

```bash
# Use environment variables for secrets
export BoyoDB_TOKEN="your-secret-token"
export BoyoDB_PASSWORD="your-password"

# In scripts
boyodb-cli shell --host $BoyoDB_HOST --token $BoyoDB_TOKEN
```

---

## Audit Logging

### What's Logged

BoyoDB logs security-relevant events:

- Authentication attempts (success/failure)
- Authorization denials
- User management operations
- Role and privilege changes
- Connection events

### Log Format

```
2024-01-15T10:30:00Z INFO  [auth] login success: user=analyst ip=10.0.1.50
2024-01-15T10:30:05Z WARN  [auth] login failed: user=admin ip=192.168.1.100 reason=invalid_password
2024-01-15T10:30:10Z INFO  [auth] privilege granted: grantor=root grantee=analyst privilege=SELECT target=analytics
2024-01-15T10:31:00Z WARN  [auth] access denied: user=analyst operation=DROP target=production.users
```

### Enabling Verbose Logging

```bash
# Enable debug logging
RUST_LOG=boyodb_server=debug boyodb-server /data 0.0.0.0:8765

# Log to file
boyodb-server /data 0.0.0.0:8765 2>&1 | tee /var/log/boyodb/server.log
```

---

## Security Architecture

### Defense in Depth

```
┌─────────────────────────────────────────────────────────────┐
│                      Network Layer                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │  Firewall   │  │     TLS     │  │ Rate Limit  │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
├─────────────────────────────────────────────────────────────┤
│                   Authentication Layer                       │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │   Token     │  │  Password   │  │   Session   │         │
│  │    Auth     │  │    Auth     │  │  Manager    │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
├─────────────────────────────────────────────────────────────┤
│                   Authorization Layer                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │    RBAC     │  │  Privilege  │  │   Access    │         │
│  │   Engine    │  │   Checks    │  │   Control   │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
├─────────────────────────────────────────────────────────────┤
│                    Validation Layer                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │    Input    │  │    Size     │  │    SQL      │         │
│  │ Validation  │  │   Limits    │  │  Parsing    │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
├─────────────────────────────────────────────────────────────┤
│                      Data Layer                              │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐         │
│  │  Encrypted  │  │  Integrity  │  │   Access    │         │
│  │   Storage   │  │   Checks    │  │   Logging   │         │
│  └─────────────┘  └─────────────┘  └─────────────┘         │
└─────────────────────────────────────────────────────────────┘
```

### Threat Model

| Threat | Mitigation |
|--------|------------|
| Eavesdropping | TLS encryption |
| Credential theft | Argon2id hashing, no plaintext storage |
| Brute force | Rate limiting, account lockout |
| SQL injection | Parameterized queries, input validation |
| DoS attacks | Size limits, rate limiting, connection limits |
| Privilege escalation | RBAC, principle of least privilege |
| Data tampering | CRC32 checksums on segments |

---

## Incident Response

### Suspected Breach

1. **Isolate** the affected system
2. **Preserve** logs and evidence
3. **Rotate** all credentials
4. **Review** access logs
5. **Notify** affected parties

### Credential Compromise

```sql
-- Immediately lock compromised account
LOCK USER compromised_user;

-- Change password for affected user
ALTER USER compromised_user PASSWORD 'NewSecurePassword123!';

-- Review and revoke unnecessary privileges
REVOKE ALL ON ALL DATABASES FROM compromised_user;

-- Unlock after securing
UNLOCK USER compromised_user;
```

### Rotating Server Token

```bash
# Generate new token
NEW_TOKEN=$(openssl rand -base64 32)

# Update server (requires restart)
boyodb-server /data 0.0.0.0:8765 --token "$NEW_TOKEN"

# Update all clients with new token
```

---

## Compliance Considerations

### Data Protection

- **Encryption in transit**: TLS for all connections
- **Access control**: RBAC with audit logging
- **Data integrity**: Checksums on all segments

### Audit Trail

All security-relevant operations are logged:
- Who accessed what data
- When access occurred
- What operations were performed
- Success/failure status

### Data Retention

Configure WAL and segment retention:
```bash
boyodb-server /data 0.0.0.0:8765 \
  --wal-retention-count 10 \
  --wal-max-bytes 104857600
```

