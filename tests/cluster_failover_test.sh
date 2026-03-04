#!/bin/bash
#
# BoyoDB Cluster Failover Test Script
#
# This script tests the two-node cluster setup and basic failover scenario.
# It requires boyodb-server and boyodb-cli binaries to be built.
#
# Usage: ./tests/cluster_failover_test.sh
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Configuration
NODE1_DATA_DIR="/tmp/boyodb_test_node1"
NODE2_DATA_DIR="/tmp/boyodb_test_node2"
NODE1_PORT=18765
NODE2_PORT=18766
NODE1_GOSSIP_PORT=18770
NODE2_GOSSIP_PORT=18771
CLUSTER_ID="test_cluster"
TIMEOUT=30

# Cleanup function
cleanup() {
    log_info "Cleaning up..."
    # Kill any running server processes
    pkill -f "boyodb-server.*$NODE1_PORT" 2>/dev/null || true
    pkill -f "boyodb-server.*$NODE2_PORT" 2>/dev/null || true
    sleep 1
    # Remove test directories
    rm -rf "$NODE1_DATA_DIR" "$NODE2_DATA_DIR" 2>/dev/null || true
}

# Set trap to cleanup on exit
trap cleanup EXIT

# Check if binaries exist
check_binaries() {
    if ! command -v cargo &> /dev/null; then
        log_error "cargo not found. Please install Rust."
        exit 1
    fi

    log_info "Building binaries..."
    cargo build --release -p boyodb-server -p boyodb-cli 2>/dev/null || {
        log_error "Failed to build binaries"
        exit 1
    }
}

# Start a node
start_node() {
    local node_num=$1
    local data_dir=$2
    local port=$3
    local gossip_port=$4
    local seed_node=$5

    mkdir -p "$data_dir"

    log_info "Starting node $node_num on port $port..."

    local cmd="./target/release/boyodb-server $data_dir 0.0.0.0:$port \
        --cluster \
        --cluster-id $CLUSTER_ID \
        --two-node-mode \
        --gossip-addr 0.0.0.0:$gossip_port"

    if [ -n "$seed_node" ]; then
        cmd="$cmd --seed-nodes $seed_node"
    fi

    $cmd > "/tmp/boyodb_node${node_num}.log" 2>&1 &
    echo $! > "/tmp/boyodb_node${node_num}.pid"

    sleep 2
}

# Check if node is healthy (uses Python for proper frame-based protocol)
check_node_health() {
    local port=$1
    local node_num=${2:-1}
    local max_attempts=10
    local attempt=0

    while [ $attempt -lt $max_attempts ]; do
        # Check if server process is still running
        if [ -f "/tmp/boyodb_node${node_num}.pid" ]; then
            if ! kill -0 $(cat "/tmp/boyodb_node${node_num}.pid") 2>/dev/null; then
                log_warn "Server process died, waiting..."
                sleep 1
                attempt=$((attempt + 1))
                continue
            fi
        fi

        result=$(python3 -c "
import socket
import struct
import json
import sys

try:
    msg = json.dumps({'op': 'health'}).encode()
    frame = struct.pack('>I', len(msg)) + msg

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(2)
    s.connect(('127.0.0.1', $port))
    s.sendall(frame)

    length_data = s.recv(4)
    if len(length_data) < 4:
        print('FAIL:incomplete_length')
        sys.exit(1)
    length = struct.unpack('>I', length_data)[0]
    response = json.loads(s.recv(length).decode())
    s.close()

    if response.get('status') == 'ok':
        print('OK')
        sys.exit(0)
    print('FAIL:status=' + str(response.get('status')))
    sys.exit(1)
except Exception as e:
    print('FAIL:' + str(e))
    sys.exit(1)
" 2>&1)

        if [ "$result" = "OK" ]; then
            return 0
        fi
        log_warn "Health check attempt $((attempt + 1)): $result"
        attempt=$((attempt + 1))
        sleep 1
    done
    return 1
}

# Execute a query on a node (uses Python for proper frame-based protocol)
execute_query() {
    local port=$1
    local sql=$2

    python3 -c "
import socket
import struct
import json

msg = json.dumps({'op': 'query', 'sql': '''$sql'''}).encode()
frame = struct.pack('>I', len(msg)) + msg

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(5)
s.connect(('127.0.0.1', $port))
s.sendall(frame)

length_data = s.recv(4)
length = struct.unpack('>I', length_data)[0]
response = s.recv(length).decode()
s.close()
print(response)
" 2>/dev/null
}

# Get cluster status from a node
get_cluster_status() {
    local port=$1

    python3 -c "
import socket
import struct
import json

msg = json.dumps({'op': 'clusterstatus'}).encode()
frame = struct.pack('>I', len(msg)) + msg

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(5)
s.connect(('127.0.0.1', $port))
s.sendall(frame)

length_data = s.recv(4)
length = struct.unpack('>I', length_data)[0]
response = s.recv(length).decode()
s.close()
print(response)
" 2>/dev/null
}

# Main test sequence
main() {
    log_info "=== BoyoDB Cluster Failover Test ==="

    # Cleanup any previous test
    cleanup

    # Check binaries
    check_binaries

    # Start Node 1 (initial primary)
    start_node 1 "$NODE1_DATA_DIR" $NODE1_PORT $NODE1_GOSSIP_PORT ""

    # Wait for Node 1 to be ready
    log_info "Waiting for Node 1 to be ready..."
    if ! check_node_health $NODE1_PORT 1; then
        log_error "Node 1 failed to start"
        cat /tmp/boyodb_node1.log
        exit 1
    fi
    log_info "Node 1 is healthy"

    # Start Node 2 (replica) with Node 1 as seed
    start_node 2 "$NODE2_DATA_DIR" $NODE2_PORT $NODE2_GOSSIP_PORT "127.0.0.1:$NODE1_GOSSIP_PORT"

    # Wait for Node 2 to be ready
    log_info "Waiting for Node 2 to be ready..."
    if ! check_node_health $NODE2_PORT 2; then
        log_error "Node 2 failed to start"
        cat /tmp/boyodb_node2.log
        exit 1
    fi
    log_info "Node 2 is healthy"

    # Wait for cluster formation
    log_info "Waiting for cluster formation..."
    sleep 5

    # Test 1: Create database and table on primary
    log_info "Test 1: Creating database and table on Node 1..."
    result=$(execute_query $NODE1_PORT "CREATE DATABASE testdb")
    if echo "$result" | grep -q "error"; then
        log_error "Failed to create database: $result"
        exit 1
    fi
    log_info "Database created successfully"

    # Test 2: Insert data
    log_info "Test 2: Inserting test data..."
    result=$(execute_query $NODE1_PORT "CREATE TABLE testdb.events (id INT, name STRING)")
    if echo "$result" | grep -q "error"; then
        log_warn "Table creation message: $result"
    fi

    result=$(execute_query $NODE1_PORT "INSERT INTO testdb.events VALUES (1, 'test_event')")
    if echo "$result" | grep -q "error"; then
        log_warn "Insert result: $result"
    fi

    # Test 3: Query data
    log_info "Test 3: Querying data from Node 1..."
    result=$(execute_query $NODE1_PORT "SELECT * FROM testdb.events")
    log_info "Query result: $result"

    # Test 4: Check cluster status
    log_info "Test 4: Checking cluster status..."
    result=$(get_cluster_status $NODE1_PORT)
    log_info "Cluster status: $result"

    # Test 5: Simulate failover by killing Node 1
    log_info "Test 5: Simulating failover - killing Node 1..."
    if [ -f /tmp/boyodb_node1.pid ]; then
        kill $(cat /tmp/boyodb_node1.pid) 2>/dev/null || true
        rm /tmp/boyodb_node1.pid
    fi

    # Wait for failover
    log_info "Waiting for failover (up to ${TIMEOUT}s)..."
    sleep 10

    # Test 6: Check if Node 2 is still healthy after failover
    log_info "Test 6: Checking Node 2 health after failover..."
    if check_node_health $NODE2_PORT 2; then
        log_info "Node 2 is still healthy after failover"
    else
        log_error "Node 2 is not healthy after failover"
        cat /tmp/boyodb_node2.log
        exit 1
    fi

    # Test 7: Query data from Node 2 (should work if data was replicated)
    log_info "Test 7: Querying data from Node 2..."
    result=$(execute_query $NODE2_PORT "SELECT * FROM testdb.events")
    log_info "Query result from Node 2: $result"

    # Test 8: Check cluster status on Node 2
    log_info "Test 8: Checking cluster status on Node 2..."
    result=$(get_cluster_status $NODE2_PORT)
    log_info "Cluster status on Node 2: $result"

    log_info "=== All cluster tests completed ==="
    log_info "Check logs at /tmp/boyodb_node1.log and /tmp/boyodb_node2.log for details"
}

# Run main function
main "$@"
