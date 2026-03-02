#!/bin/bash
#
# CDRDB Installation Script
# Installs cdrdb-server and cdrdb-cli on Linux AMD64 systems
#

set -e

# Configuration
CDRDB_VERSION="${CDRDB_VERSION:-latest}"
CDRDB_REPO="${CDRDB_REPO:-loreste/drdb}"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"
DATA_DIR="${DATA_DIR:-/var/lib/cdrdb}"
CONFIG_DIR="${CONFIG_DIR:-/etc/cdrdb}"
LOG_DIR="${LOG_DIR:-/var/log/cdrdb}"
CDRDB_USER="${CDRDB_USER:-cdrdb}"
CDRDB_GROUP="${CDRDB_GROUP:-cdrdb}"
CDRDB_PORT="${CDRDB_PORT:-9876}"
CDRDB_HOST="${CDRDB_HOST:-0.0.0.0}"
DOWNLOAD_URL="${DOWNLOAD_URL:-}"
TMP_DIR="${TMP_DIR:-/tmp/cdrdb-install}"
CHECKSUM_URL="${CHECKSUM_URL:-}"
SKIP_CHECKSUM="${SKIP_CHECKSUM:-false}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running as root
check_root() {
    if [ "$EUID" -ne 0 ]; then
        log_error "This script must be run as root (use sudo)"
        exit 1
    fi
}

# Check system requirements
check_requirements() {
    log_info "Checking system requirements..."

    # Check architecture
    ARCH=$(uname -m)
    if [ "$ARCH" != "x86_64" ]; then
        log_error "Unsupported architecture: $ARCH. Only x86_64 (AMD64) is supported."
        exit 1
    fi

    # Check OS
    if [ ! -f /etc/os-release ]; then
        log_error "Cannot detect OS. /etc/os-release not found."
        exit 1
    fi

    . /etc/os-release
    log_info "Detected OS: $PRETTY_NAME"
    log_info "Architecture: $ARCH"
}

# Create cdrdb user and group
create_user() {
    log_info "Creating cdrdb user and group..."

    if ! getent group "$CDRDB_GROUP" > /dev/null 2>&1; then
        groupadd --system "$CDRDB_GROUP"
        log_success "Created group: $CDRDB_GROUP"
    else
        log_info "Group $CDRDB_GROUP already exists"
    fi

    if ! getent passwd "$CDRDB_USER" > /dev/null 2>&1; then
        useradd --system --gid "$CDRDB_GROUP" --home-dir "$DATA_DIR" --shell /sbin/nologin "$CDRDB_USER"
        log_success "Created user: $CDRDB_USER"
    else
        log_info "User $CDRDB_USER already exists"
    fi
}

# Create directories
create_directories() {
    log_info "Creating directories..."

    # Data directory
    mkdir -p "$DATA_DIR"
    chown "$CDRDB_USER:$CDRDB_GROUP" "$DATA_DIR"
    chmod 750 "$DATA_DIR"
    log_success "Created data directory: $DATA_DIR"

    # Config directory
    mkdir -p "$CONFIG_DIR"
    chmod 755 "$CONFIG_DIR"
    log_success "Created config directory: $CONFIG_DIR"

    # Log directory
    mkdir -p "$LOG_DIR"
    chown "$CDRDB_USER:$CDRDB_GROUP" "$LOG_DIR"
    chmod 750 "$LOG_DIR"
    log_success "Created log directory: $LOG_DIR"
}

# Validate URL format (basic check for safe characters)
validate_url() {
    local url="$1"
    local name="$2"

    # Check for shell metacharacters that could enable injection
    if echo "$url" | grep -qE '[;&|`$(){}]'; then
        log_error "Invalid $name: contains unsafe characters"
        log_error "URL must not contain: ; & | \` $ ( ) { }"
        exit 1
    fi

    # Validate URL starts with http:// or https://
    if ! echo "$url" | grep -qE '^https?://'; then
        log_error "Invalid $name: must start with http:// or https://"
        exit 1
    fi
}

# Validate version string (alphanumeric, dots, dashes, underscores, and 'v' prefix)
validate_version() {
    local version="$1"
    if ! echo "$version" | grep -qE '^[a-zA-Z0-9._-]+$'; then
        log_error "Invalid CDRDB_VERSION: contains unsafe characters"
        log_error "Version must only contain: letters, numbers, dots, dashes, underscores"
        exit 1
    fi
}

# Validate repository name (owner/repo format)
validate_repo() {
    local repo="$1"
    if ! echo "$repo" | grep -qE '^[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+$'; then
        log_error "Invalid CDRDB_REPO: must be in 'owner/repo' format"
        log_error "Repository name must only contain: letters, numbers, dashes, underscores"
        exit 1
    fi
}

# Check for required download tools
check_download_tools() {
    if command -v curl &> /dev/null; then
        DOWNLOAD_CMD="curl -fsSL"
        DOWNLOAD_OUTPUT="-o"
    elif command -v wget &> /dev/null; then
        DOWNLOAD_CMD="wget -q"
        DOWNLOAD_OUTPUT="-O"
    else
        log_error "Neither curl nor wget found. Please install one of them."
        exit 1
    fi
}

# Download and extract release
download_release() {
    log_info "Downloading CDRDB release..."

    # Validate inputs before using them in URLs/commands
    validate_repo "$CDRDB_REPO"
    if [ "$CDRDB_VERSION" != "latest" ]; then
        validate_version "$CDRDB_VERSION"
    fi
    if [ -n "$DOWNLOAD_URL" ]; then
        validate_url "$DOWNLOAD_URL" "DOWNLOAD_URL"
    fi
    if [ -n "$CHECKSUM_URL" ]; then
        validate_url "$CHECKSUM_URL" "CHECKSUM_URL"
    fi

    # Create temp directory
    rm -rf "$TMP_DIR"
    mkdir -p "$TMP_DIR"

    # Determine download URL
    if [ -n "$DOWNLOAD_URL" ]; then
        RELEASE_URL="$DOWNLOAD_URL"
        log_info "Using provided download URL: $RELEASE_URL"
    elif [ "$CDRDB_VERSION" = "latest" ]; then
        # Get latest release from GitHub API
        log_info "Fetching latest release information..."
        if command -v curl &> /dev/null; then
            RELEASE_INFO=$(curl -fsSL "https://api.github.com/repos/$CDRDB_REPO/releases/latest" 2>/dev/null || echo "")
        else
            RELEASE_INFO=$(wget -qO- "https://api.github.com/repos/$CDRDB_REPO/releases/latest" 2>/dev/null || echo "")
        fi

        if [ -z "$RELEASE_INFO" ]; then
            log_warn "Could not fetch release info from GitHub API"
            log_info "Falling back to direct release URL..."
            RELEASE_URL="https://github.com/$CDRDB_REPO/releases/latest/download/cdrdb-linux-amd64.tar.gz"
        else
            RELEASE_URL=$(echo "$RELEASE_INFO" | grep -o '"browser_download_url": *"[^"]*linux-amd64\.tar\.gz"' | head -1 | cut -d'"' -f4)
            if [ -z "$RELEASE_URL" ]; then
                RELEASE_URL="https://github.com/$CDRDB_REPO/releases/latest/download/cdrdb-linux-amd64.tar.gz"
            fi
        fi
    else
        RELEASE_URL="https://github.com/$CDRDB_REPO/releases/download/$CDRDB_VERSION/cdrdb-linux-amd64.tar.gz"
    fi

    log_info "Downloading from: $RELEASE_URL"

    # Download the tarball
    TARBALL="$TMP_DIR/cdrdb-linux-amd64.tar.gz"
    if ! $DOWNLOAD_CMD "$DOWNLOAD_OUTPUT" "$TARBALL" "$RELEASE_URL"; then
        log_error "Failed to download release from $RELEASE_URL"
        log_info "You can manually download the release and set BINARY_DIR to its location"
        rm -rf "$TMP_DIR"
        exit 1
    fi

    log_success "Downloaded release tarball"

    # Verify it's a valid gzip file
    if ! file "$TARBALL" | grep -q "gzip"; then
        log_error "Downloaded file is not a valid gzip archive"
        log_info "The download URL may be incorrect or the release may not exist"
        rm -rf "$TMP_DIR"
        exit 1
    fi

    # Verify SHA256 checksum if available
    if [ "$SKIP_CHECKSUM" != "true" ]; then
        # Try to download checksum file
        if [ -n "$CHECKSUM_URL" ]; then
            CHECKSUM_FILE_URL="$CHECKSUM_URL"
        else
            CHECKSUM_FILE_URL="${RELEASE_URL}.sha256"
        fi

        log_info "Attempting to verify SHA256 checksum..."
        CHECKSUM_FILE="$TMP_DIR/checksum.sha256"

        if $DOWNLOAD_CMD "$DOWNLOAD_OUTPUT" "$CHECKSUM_FILE" "$CHECKSUM_FILE_URL" 2>/dev/null; then
            EXPECTED_CHECKSUM=$(cat "$CHECKSUM_FILE" | awk '{print $1}')
            ACTUAL_CHECKSUM=$(sha256sum "$TARBALL" | awk '{print $1}')

            if [ "$EXPECTED_CHECKSUM" = "$ACTUAL_CHECKSUM" ]; then
                log_success "SHA256 checksum verified"
            else
                log_error "SHA256 checksum verification FAILED!"
                log_error "Expected: $EXPECTED_CHECKSUM"
                log_error "Actual:   $ACTUAL_CHECKSUM"
                log_error "The downloaded file may be corrupted or tampered with."
                rm -rf "$TMP_DIR"
                exit 1
            fi
        else
            log_warn "Checksum file not available at $CHECKSUM_FILE_URL"
            log_warn "Skipping checksum verification (use SKIP_CHECKSUM=true to suppress this warning)"

            # Show security warning for custom URLs
            if [ -n "$DOWNLOAD_URL" ]; then
                log_warn "SECURITY WARNING: You are installing from a custom URL without checksum verification"
                log_warn "Ensure you trust the source: $DOWNLOAD_URL"
            fi
        fi
    else
        log_warn "Checksum verification skipped (SKIP_CHECKSUM=true)"
    fi

    # Extract the tarball
    log_info "Extracting release..."
    cd "$TMP_DIR"
    if ! tar -xzf "$TARBALL"; then
        log_error "Failed to extract tarball"
        rm -rf "$TMP_DIR"
        exit 1
    fi

    # Find the extracted directory (could be cdrdb-linux-amd64 or just the binaries)
    if [ -d "$TMP_DIR/cdrdb-linux-amd64" ]; then
        BINARY_DIR="$TMP_DIR/cdrdb-linux-amd64"
    elif [ -f "$TMP_DIR/cdrdb-server" ]; then
        BINARY_DIR="$TMP_DIR"
    else
        # Look for binaries in any subdirectory
        BINARY_DIR=$(find "$TMP_DIR" -name "cdrdb-server" -type f -exec dirname {} \; | head -1)
        if [ -z "$BINARY_DIR" ]; then
            log_error "Could not find cdrdb-server binary in extracted archive"
            rm -rf "$TMP_DIR"
            exit 1
        fi
    fi

    log_success "Extracted release to $BINARY_DIR"
    export BINARY_DIR
}

# Cleanup temporary files
cleanup_temp() {
    if [ -d "$TMP_DIR" ]; then
        rm -rf "$TMP_DIR"
        log_info "Cleaned up temporary files"
    fi
}

# Install binaries
install_binaries() {
    log_info "Installing binaries to $INSTALL_DIR..."

    # Check if binaries exist in current directory or download location
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

    # If BINARY_DIR is not set, check script directory first
    if [ -z "${BINARY_DIR:-}" ]; then
        if [ -f "$SCRIPT_DIR/cdrdb-server" ] && [ -f "$SCRIPT_DIR/cdrdb-cli" ]; then
            BINARY_DIR="$SCRIPT_DIR"
            log_info "Using binaries from $BINARY_DIR"
        else
            # Try to download
            log_info "Binaries not found locally, attempting to download..."
            check_download_tools
            download_release
        fi
    fi

    if [ ! -f "$BINARY_DIR/cdrdb-server" ] || [ ! -f "$BINARY_DIR/cdrdb-cli" ]; then
        log_error "Binaries not found in $BINARY_DIR"
        log_info "Please ensure cdrdb-server and cdrdb-cli are available"
        log_info "Options:"
        log_info "  1. Place binaries in the same directory as this script"
        log_info "  2. Set BINARY_DIR environment variable"
        log_info "  3. Set DOWNLOAD_URL to a direct download link"
        exit 1
    fi

    # Install server
    cp "$BINARY_DIR/cdrdb-server" "$INSTALL_DIR/"
    chmod 755 "$INSTALL_DIR/cdrdb-server"
    log_success "Installed cdrdb-server to $INSTALL_DIR"

    # Install CLI
    cp "$BINARY_DIR/cdrdb-cli" "$INSTALL_DIR/"
    chmod 755 "$INSTALL_DIR/cdrdb-cli"
    log_success "Installed cdrdb-cli to $INSTALL_DIR"
}

# Create configuration file
create_config() {
    log_info "Creating configuration file..."

    CONFIG_FILE="$CONFIG_DIR/cdrdb.conf"

    if [ -f "$CONFIG_FILE" ]; then
        log_warn "Configuration file already exists at $CONFIG_FILE"
        log_info "Backing up existing config to ${CONFIG_FILE}.bak"
        cp "$CONFIG_FILE" "${CONFIG_FILE}.bak"
    fi

    cat > "$CONFIG_FILE" << EOF
# CDRDB Configuration File
# Generated by install.sh

# Server bind address and port
CDRDB_HOST=$CDRDB_HOST
CDRDB_PORT=$CDRDB_PORT

# Data directory
CDRDB_DATA_DIR=$DATA_DIR

# Performance tuning
CDRDB_SEGMENT_CACHE=16
CDRDB_MAX_CONNECTIONS=128

# Security settings
CDRDB_AUTH_MAX_FAILURES=5
CDRDB_AUTH_LOCKOUT_DURATION=300

# Logging
CDRDB_LOG_LEVEL=info
EOF

    chmod 644 "$CONFIG_FILE"
    log_success "Created configuration file: $CONFIG_FILE"

    # Create CLI wrapper script for easy access
    CLI_WRAPPER="$INSTALL_DIR/cdrdb"
    cat > "$CLI_WRAPPER" << 'WRAPPER_EOF'
#!/bin/bash
set -euo pipefail

# CDRDB CLI wrapper - connects to local server by default
# Usage: cdrdb shell | cdrdb query "SELECT ..." | cdrdb --help

CDRDB_CLI="INSTALL_DIR_PLACEHOLDER/cdrdb-cli"
DEFAULT_HOST="HOST_PLACEHOLDER"

# Check if --host is already specified in arguments
has_host=false
for arg in "$@"; do
    if [[ "$arg" == "--host" ]] || [[ "$arg" == --host=* ]]; then
        has_host=true
        break
    fi
done

if [ "$has_host" = true ]; then
    exec "$CDRDB_CLI" "$@"
else
    exec "$CDRDB_CLI" --host "$DEFAULT_HOST" "$@"
fi
WRAPPER_EOF
    # Replace placeholders with actual values
    sed -i "s|INSTALL_DIR_PLACEHOLDER|$INSTALL_DIR|g" "$CLI_WRAPPER"
    sed -i "s|HOST_PLACEHOLDER|127.0.0.1:$CDRDB_PORT|g" "$CLI_WRAPPER"
    chmod 755 "$CLI_WRAPPER"
    log_success "Created CLI wrapper: $CLI_WRAPPER"

    # Create symlinks in /usr/bin if different from INSTALL_DIR (for broader PATH coverage)
    if [ "$INSTALL_DIR" != "/usr/bin" ] && [ -d "/usr/bin" ]; then
        ln -sf "$INSTALL_DIR/cdrdb" /usr/bin/cdrdb 2>/dev/null || true
        ln -sf "$INSTALL_DIR/cdrdb-cli" /usr/bin/cdrdb-cli 2>/dev/null || true
        log_info "Created symlinks in /usr/bin for broader PATH coverage"
    fi

    # Create shell profile snippet for tab completion
    COMPLETION_FILE="$CONFIG_DIR/cdrdb-completion.bash"
    cat > "$COMPLETION_FILE" << 'EOF'
# CDRDB CLI bash completion
_cdrdb_completions() {
    local cur="${COMP_WORDS[COMP_CWORD]}"
    local commands="shell query ingest health"

    if [ "${COMP_CWORD}" -eq 1 ]; then
        COMPREPLY=($(compgen -W "${commands}" -- "${cur}"))
    fi
}
complete -F _cdrdb_completions cdrdb
complete -F _cdrdb_completions cdrdb-cli
EOF
    chmod 644 "$COMPLETION_FILE"
    log_success "Created bash completion: $COMPLETION_FILE"
    log_info "To enable completion, add to ~/.bashrc: source $COMPLETION_FILE"
}

# Create systemd service
create_systemd_service() {
    log_info "Creating systemd service..."

    SERVICE_FILE="/etc/systemd/system/cdrdb.service"

    cat > "$SERVICE_FILE" << EOF
[Unit]
Description=CDRDB - High-Performance CDR Database
Documentation=https://github.com/loreste/drdb
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=$CDRDB_USER
Group=$CDRDB_GROUP
EnvironmentFile=-$CONFIG_DIR/cdrdb.conf
ExecStart=$INSTALL_DIR/cdrdb-server \$CDRDB_DATA_DIR \$CDRDB_HOST:\$CDRDB_PORT --segment-cache \$CDRDB_SEGMENT_CACHE --max-connections \$CDRDB_MAX_CONNECTIONS
ExecReload=/bin/kill -HUP \$MAINPID
Restart=on-failure
RestartSec=5
LimitNOFILE=65536
LimitNPROC=4096

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$DATA_DIR $LOG_DIR
PrivateTmp=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true

[Install]
WantedBy=multi-user.target
EOF

    chmod 644 "$SERVICE_FILE"

    # Reload systemd
    systemctl daemon-reload

    log_success "Created systemd service: cdrdb.service"
}

# Enable and start service
start_service() {
    log_info "Enabling and starting CDRDB service..."

    systemctl enable cdrdb.service
    systemctl start cdrdb.service

    # Wait a moment for startup
    sleep 2

    if systemctl is-active --quiet cdrdb.service; then
        log_success "CDRDB service is running"
    else
        log_error "CDRDB service failed to start"
        log_info "Check logs with: journalctl -u cdrdb.service"
        exit 1
    fi
}

# Print post-installation info
print_info() {
    echo ""
    echo "========================================"
    echo "  CDRDB Installation Complete!"
    echo "========================================"
    echo ""
    echo "Installation Summary:"
    echo "  - Server:       $INSTALL_DIR/cdrdb-server"
    echo "  - CLI:          $INSTALL_DIR/cdrdb-cli"
    echo "  - CLI Wrapper:  $INSTALL_DIR/cdrdb (auto-connects to local server)"
    echo "  - Data:         $DATA_DIR"
    echo "  - Config:       $CONFIG_DIR/cdrdb.conf"
    echo "  - Logs:         $LOG_DIR"
    echo "  - Service:      cdrdb.service"
    echo ""
    echo "Server is listening on: $CDRDB_HOST:$CDRDB_PORT"
    echo ""
    echo "Useful commands:"
    echo "  - Connect:       cdrdb shell                    (uses wrapper)"
    echo "  - Connect:       cdrdb-cli shell --host HOST    (manual host)"
    echo "  - Status:        systemctl status cdrdb"
    echo "  - Logs:          journalctl -u cdrdb -f"
    echo "  - Stop:          systemctl stop cdrdb"
    echo "  - Restart:       systemctl restart cdrdb"
    echo ""
    echo "Enable bash completion:"
    echo "  echo 'source $CONFIG_DIR/cdrdb-completion.bash' >> ~/.bashrc"
    echo ""
    echo "To uninstall, run: sudo cdrdb-uninstall.sh"
    echo ""
}

# Create uninstall script
create_uninstall_script() {
    log_info "Creating uninstall script..."

    UNINSTALL_SCRIPT="$INSTALL_DIR/cdrdb-uninstall.sh"

    cat > "$UNINSTALL_SCRIPT" << 'EOF'
#!/bin/bash
#
# CDRDB Uninstallation Script
#

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }

if [ "$EUID" -ne 0 ]; then
    echo -e "${RED}[ERROR]${NC} This script must be run as root (use sudo)"
    exit 1
fi

echo "This will uninstall CDRDB from your system."
echo ""
read -p "Do you want to remove data directory (/var/lib/cdrdb)? [y/N] " REMOVE_DATA
read -p "Continue with uninstallation? [y/N] " CONFIRM

if [ "$CONFIRM" != "y" ] && [ "$CONFIRM" != "Y" ]; then
    echo "Uninstallation cancelled."
    exit 0
fi

log_info "Stopping CDRDB service..."
systemctl stop cdrdb.service 2>/dev/null || true
systemctl disable cdrdb.service 2>/dev/null || true

log_info "Removing systemd service..."
rm -f /etc/systemd/system/cdrdb.service
systemctl daemon-reload

log_info "Removing binaries and symlinks..."
rm -f /usr/local/bin/cdrdb-server
rm -f /usr/local/bin/cdrdb-cli
rm -f /usr/local/bin/cdrdb
rm -f /usr/bin/cdrdb 2>/dev/null || true
rm -f /usr/bin/cdrdb-cli 2>/dev/null || true

log_info "Removing configuration..."
rm -rf /etc/cdrdb

log_info "Removing log directory..."
rm -rf /var/log/cdrdb

if [ "$REMOVE_DATA" = "y" ] || [ "$REMOVE_DATA" = "Y" ]; then
    log_warn "Removing data directory..."
    rm -rf /var/lib/cdrdb
fi

log_info "Removing cdrdb user and group..."
userdel cdrdb 2>/dev/null || true
groupdel cdrdb 2>/dev/null || true

# Remove this script
rm -f /usr/local/bin/cdrdb-uninstall.sh

log_success "CDRDB has been uninstalled successfully!"
EOF

    chmod 755 "$UNINSTALL_SCRIPT"
    log_success "Created uninstall script: $UNINSTALL_SCRIPT"
}

# Main installation flow
main() {
    echo ""
    echo "========================================"
    echo "  CDRDB Installation Script"
    echo "========================================"
    echo ""

    # Set up cleanup trap
    trap cleanup_temp EXIT

    check_root
    check_requirements
    create_user
    create_directories
    install_binaries
    create_config
    create_systemd_service
    create_uninstall_script
    cleanup_temp
    start_service
    print_info
}

# Handle command line arguments
case "${1:-}" in
    --help|-h)
        echo "Usage: $0 [OPTIONS]"
        echo ""
        echo "Options:"
        echo "  --help, -h     Show this help message"
        echo "  --no-start     Install but don't start the service"
        echo "  --download     Force download even if binaries exist locally"
        echo ""
        echo "Environment Variables:"
        echo "  CDRDB_VERSION  Version to install (default: latest)"
        echo "  DOWNLOAD_URL   Direct URL to download tarball from"
        echo "  BINARY_DIR     Directory containing cdrdb binaries (skips download)"
        echo ""
        echo "  INSTALL_DIR    Binary installation directory (default: /usr/local/bin)"
        echo "  DATA_DIR       Data directory (default: /var/lib/cdrdb)"
        echo "  CONFIG_DIR     Configuration directory (default: /etc/cdrdb)"
        echo "  LOG_DIR        Log directory (default: /var/log/cdrdb)"
        echo "  CDRDB_USER     Service user (default: cdrdb)"
        echo "  CDRDB_PORT     Server port (default: 9876)"
        echo "  CDRDB_HOST     Server bind address (default: 0.0.0.0)"
        echo ""
        echo "Examples:"
        echo "  # Install latest from GitHub release"
        echo "  sudo ./install.sh"
        echo ""
        echo "  # Install specific version"
        echo "  sudo CDRDB_VERSION=v1.0.0 ./install.sh"
        echo ""
        echo "  # Install from local binaries"
        echo "  sudo BINARY_DIR=/path/to/binaries ./install.sh"
        echo ""
        echo "  # Install from custom URL"
        echo "  sudo DOWNLOAD_URL=https://example.com/cdrdb.tar.gz ./install.sh"
        echo ""
        exit 0
        ;;
    --no-start)
        trap cleanup_temp EXIT
        check_root
        check_requirements
        create_user
        create_directories
        install_binaries
        create_config
        create_systemd_service
        create_uninstall_script
        cleanup_temp
        echo ""
        log_success "Installation complete (service not started)"
        log_info "Start the service with: systemctl start cdrdb"
        ;;
    --download)
        # Force download even if local binaries exist
        trap cleanup_temp EXIT
        check_root
        check_requirements
        check_download_tools
        download_release
        create_user
        create_directories
        install_binaries
        create_config
        create_systemd_service
        create_uninstall_script
        cleanup_temp
        start_service
        print_info
        ;;
    *)
        main
        ;;
esac
