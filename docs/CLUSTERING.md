# BoyoDB Clustering Guide

This guide covers setting up and managing BoyoDB clusters for high availability and scalability.

## Table of Contents

1. [Cluster Architecture](#cluster-architecture)
2. [Two-Node Mode](#two-node-mode)
3. [Three-Node Cluster](#three-node-cluster)
4. [Read Replicas](#read-replicas)
5. [Failover Testing](#failover-testing)
6. [Production Deployment](#production-deployment)
7. [Monitoring](#monitoring)
8. [Troubleshooting](#troubleshooting)

---

## Cluster Architecture

BoyoDB uses a distributed architecture with the following components:

### Gossip Protocol (SWIM-based)
- UDP-based membership protocol
- Failure detection with configurable thresholds
- Automatic node discovery and membership updates

### Leader Election
- Raft-lite consensus algorithm
- Lease-based leadership to prevent split-brain
- Automatic failover on leader failure

### Write Replication
- Quorum-based writes for durability
- Configurable replication factor
- Async replication with consistency guarantees

---

## Two-Node Mode

Two-node mode is ideal for:
- Development and testing
- Small deployments needing HA
- Cost-sensitive environments

### Configuration

**Node 1 (Primary):**
```bash
boyodb-server /data/node1 0.0.0.0:8765 \
    --cluster \
    --cluster-id myapp \
    --node-id primary \
    --two-node-mode \
    --gossip-addr 0.0.0.0:8766
```

**Node 2 (Replica):**
```bash
boyodb-server /data/node2 0.0.0.0:8765 \
    --cluster \
    --cluster-id myapp \
    --node-id replica \
    --two-node-mode \
    --gossip-addr 0.0.0.0:8766 \
    --seed-nodes "primary-host:8766"
```

### Two-Node Mode Characteristics

| Feature | Behavior |
|---------|----------|
| Quorum | 1 (can operate with one node down) |
| Failover | Automatic (replica promotes to leader) |
| Write availability | Yes, even with one node down |
| Data durability | Best-effort replication before failover |

### Verifying Two-Node Cluster

```bash
# Check cluster status on Node 1
boyodb-cli cluster-status --host node1:8765

# Expected output:
# {
#   "cluster_id": "myapp",
#   "node_id": "primary",
#   "role": "Leader",
#   "alive_nodes": 2,
#   "total_nodes": 2,
#   "has_quorum": true,
#   "leader_id": "primary"
# }
```

---

## Three-Node Cluster

Three-node clusters provide:
- True quorum-based consensus
- Tolerance for one node failure
- Better data durability guarantees

### Configuration

**Node 1 (Seed Node):**
```bash
boyodb-server /data/node1 0.0.0.0:8765 \
    --cluster \
    --cluster-id production \
    --node-id node1 \
    --gossip-addr 0.0.0.0:8766
```

**Node 2:**
```bash
boyodb-server /data/node2 0.0.0.0:8765 \
    --cluster \
    --cluster-id production \
    --node-id node2 \
    --gossip-addr 0.0.0.0:8766 \
    --seed-nodes "node1-host:8766"
```

**Node 3:**
```bash
boyodb-server /data/node3 0.0.0.0:8765 \
    --cluster \
    --cluster-id production \
    --node-id node3 \
    --gossip-addr 0.0.0.0:8766 \
    --seed-nodes "node1-host:8766"
```

### Three-Node Quorum

| Nodes Up | Quorum | Writes | Reads |
|----------|--------|--------|-------|
| 3 | Yes | Yes | Yes |
| 2 | Yes | Yes | Yes |
| 1 | No | No | Limited |

---

## Read Replicas

For scaling read-heavy workloads without full clustering:

### Primary Server

```bash
boyodb-server /data/primary 0.0.0.0:8765
```

### Read Replica

```bash
boyodb-server /data/replica 0.0.0.0:8765 \
    --replicate-from primary-host:8765 \
    --replicate-interval-ms 100 \
    --replicate-max-bytes 10485760
```

### With TLS

```bash
boyodb-server /data/replica 0.0.0.0:8765 \
    --replicate-from primary-host:8765 \
    --replicate-tls \
    --replicate-ca /path/to/ca.crt \
    --replicate-sni primary-host
```

### Read Replica Options

| Option | Description | Default |
|--------|-------------|---------|
| `--replicate-from <host:port>` | Primary server address | - |
| `--replicate-token <token>` | Auth token for replication | - |
| `--replicate-interval-ms <ms>` | Poll interval | 1000 |
| `--replicate-max-bytes <bytes>` | Max bytes per batch | 10MB |
| `--replicate-tls` | Use TLS | false |
| `--replicate-ca <path>` | CA certificate | - |
| `--replicate-sni <host>` | SNI hostname | - |

---

## Failover Testing

### Automated Test Script

BoyoDB includes a cluster failover test script:

```bash
./tests/cluster_failover_test.sh
```

This script:
1. Starts a two-node cluster
2. Creates a database and table
3. Inserts test data
4. Verifies cluster formation
5. Simulates primary failure
6. Verifies replica health
7. Tests data availability

### Manual Failover Test

**Step 1: Start two-node cluster**

```bash
# Terminal 1 - Node 1
rm -rf /tmp/boyodb_node1 && mkdir -p /tmp/boyodb_node1
boyodb-server /tmp/boyodb_node1 0.0.0.0:18765 \
    --cluster \
    --cluster-id test \
    --two-node-mode \
    --gossip-addr 0.0.0.0:18770

# Terminal 2 - Node 2
rm -rf /tmp/boyodb_node2 && mkdir -p /tmp/boyodb_node2
boyodb-server /tmp/boyodb_node2 0.0.0.0:18766 \
    --cluster \
    --cluster-id test \
    --two-node-mode \
    --gossip-addr 0.0.0.0:18771 \
    --seed-nodes "127.0.0.1:18770"
```

**Step 2: Verify cluster formation**

```bash
boyodb-cli cluster-status --host localhost:18765
```

Expected output:
```json
{
  "cluster_id": "test",
  "node_id": "...",
  "role": "Leader",
  "alive_nodes": 2,
  "total_nodes": 2,
  "has_quorum": true
}
```

**Step 3: Create test data**

```bash
boyodb-cli shell --host localhost:18765 -c "CREATE DATABASE testdb"
boyodb-cli shell --host localhost:18765 -c "CREATE TABLE testdb.events (id INT, name STRING)"
boyodb-cli shell --host localhost:18765 -c "INSERT INTO testdb.events VALUES (1, 'test')"
```

**Step 4: Kill the primary**

```bash
# Find and kill Node 1
pkill -f "boyodb-server.*18765"
```

**Step 5: Verify failover**

```bash
# Wait a few seconds for failover
sleep 10

# Check Node 2 status
boyodb-cli cluster-status --host localhost:18766
```

Expected output:
```json
{
  "cluster_id": "test",
  "node_id": "...",
  "role": "Candidate",  # or "Leader" if elected
  "alive_nodes": 1,
  "total_nodes": 2,
  "has_quorum": false   # Two-node mode with one node
}
```

**Step 6: Verify Node 2 is operational**

```bash
boyodb-cli shell --host localhost:18766 -c "SELECT 1"
```

---

## Production Deployment

### Docker Compose Example

```yaml
version: '3.8'

services:
  boyodb-node1:
    image: boyodb:latest
    command: >
      boyodb-server /data 0.0.0.0:8765
      --cluster
      --cluster-id production
      --node-id node1
      --gossip-addr 0.0.0.0:8766
      --max-connections 256
      --workers 8
    volumes:
      - node1-data:/data
    ports:
      - "8765:8765"
      - "8766:8766/udp"
    networks:
      - boyodb-cluster

  boyodb-node2:
    image: boyodb:latest
    command: >
      boyodb-server /data 0.0.0.0:8765
      --cluster
      --cluster-id production
      --node-id node2
      --gossip-addr 0.0.0.0:8766
      --seed-nodes boyodb-node1:8766
      --max-connections 256
      --workers 8
    volumes:
      - node2-data:/data
    ports:
      - "8767:8765"
      - "8768:8766/udp"
    networks:
      - boyodb-cluster
    depends_on:
      - boyodb-node1

  boyodb-node3:
    image: boyodb:latest
    command: >
      boyodb-server /data 0.0.0.0:8765
      --cluster
      --cluster-id production
      --node-id node3
      --gossip-addr 0.0.0.0:8766
      --seed-nodes boyodb-node1:8766
      --max-connections 256
      --workers 8
    volumes:
      - node3-data:/data
    ports:
      - "8769:8765"
      - "8770:8766/udp"
    networks:
      - boyodb-cluster
    depends_on:
      - boyodb-node1

volumes:
  node1-data:
  node2-data:
  node3-data:

networks:
  boyodb-cluster:
    driver: bridge
```

### Kubernetes Example

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: boyodb
spec:
  serviceName: boyodb
  replicas: 3
  selector:
    matchLabels:
      app: boyodb
  template:
    metadata:
      labels:
        app: boyodb
    spec:
      containers:
      - name: boyodb
        image: boyodb:latest
        command:
        - boyodb-server
        - /data
        - 0.0.0.0:8765
        - --cluster
        - --cluster-id=production
        - --node-id=$(POD_NAME)
        - --gossip-addr=0.0.0.0:8766
        - --seed-nodes=boyodb-0.boyodb.default.svc.cluster.local:8766
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        ports:
        - containerPort: 8765
          name: client
        - containerPort: 8766
          name: gossip
          protocol: UDP
        volumeMounts:
        - name: data
          mountPath: /data
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 100Gi
---
apiVersion: v1
kind: Service
metadata:
  name: boyodb
spec:
  clusterIP: None
  selector:
    app: boyodb
  ports:
  - port: 8765
    name: client
  - port: 8766
    name: gossip
    protocol: UDP
---
apiVersion: v1
kind: Service
metadata:
  name: boyodb-client
spec:
  type: LoadBalancer
  selector:
    app: boyodb
  ports:
  - port: 8765
    targetPort: 8765
```

### Production Checklist

- [ ] Use dedicated storage volumes for each node
- [ ] Configure appropriate `--max-connections` for expected load
- [ ] Enable TLS for client and cluster communication
- [ ] Set up monitoring and alerting
- [ ] Configure backup schedule
- [ ] Test failover scenarios before production
- [ ] Document recovery procedures
- [ ] Set up log aggregation

---

## Monitoring

### Cluster Health Metrics

```bash
# Get metrics from any node
boyodb-cli metrics --host node1:8765 --format json
```

Key metrics to monitor:

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `cluster_alive_nodes` | Number of healthy nodes | < expected |
| `cluster_has_quorum` | Cluster has quorum | false |
| `cluster_role` | Current node role | - |
| `cluster_term` | Election term | rapid increase |
| `replication_lag_ms` | Replication delay | > 1000 |

### Health Checks

```bash
# Basic health check
boyodb-cli shell -H node1:8765 -c "SELECT 1"

# Cluster status check
boyodb-cli cluster-status --host node1:8765
```

### Prometheus Integration

Configure Prometheus to scrape metrics:

```yaml
scrape_configs:
  - job_name: 'boyodb'
    static_configs:
      - targets:
        - 'node1:8765'
        - 'node2:8765'
        - 'node3:8765'
    metrics_path: '/metrics'
```

---

## Troubleshooting

### Node Won't Join Cluster

1. **Check cluster ID matches:**
   ```bash
   # Both nodes must use same --cluster-id
   --cluster-id production
   ```

2. **Verify gossip port is accessible:**
   ```bash
   # Test UDP connectivity
   nc -u node1 8766
   ```

3. **Check seed nodes are correct:**
   ```bash
   --seed-nodes "node1:8766"  # Use gossip port, not client port
   ```

4. **Check firewall rules:**
   - TCP port for client connections (8765)
   - UDP port for gossip (8766)

### Split-Brain Scenario

If nodes have conflicting leader state:

1. Stop all nodes
2. Delete cluster state: `rm -rf /data/cluster_state`
3. Restart nodes in order (seed node first)
4. Verify cluster formation

### Replication Lag

If replica is falling behind:

1. Check network latency between nodes
2. Increase `--replicate-max-bytes`
3. Decrease `--replicate-interval-ms`
4. Check for disk I/O bottlenecks

### Failover Not Occurring

1. Check failure detection thresholds
2. Verify gossip communication
3. Check logs for election messages
4. Ensure quorum can be achieved

### Logs to Check

```bash
# Server logs
grep -i "cluster\|election\|gossip\|replication" /var/log/boyodb.log

# Look for:
# - "leader elected"
# - "node joined"
# - "node suspected"
# - "node failed"
# - "replication started"
```

---

## Network Requirements

### Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 8765 | TCP | Client connections |
| 8766 | UDP | Gossip protocol |
| 8767 | TCP | Cluster RPC (optional) |
| 8865 | TCP | Replication (auto-assigned) |

### Latency Requirements

| Scenario | Max Latency |
|----------|-------------|
| Same datacenter | < 1ms recommended |
| Cross-AZ | < 5ms recommended |
| Cross-region | Not recommended for HA |

### Bandwidth

Estimate bandwidth based on:
- Write throughput × replication factor
- Gossip overhead (minimal, ~1KB/s per node)
- Replication catch-up during recovery

---

## Best Practices

1. **Use odd number of nodes** for clear quorum (3, 5, 7)
2. **Spread nodes across failure domains** (AZs, racks)
3. **Use persistent storage** for data durability
4. **Configure appropriate timeouts** for your network
5. **Monitor replication lag** in production
6. **Test failover regularly** in staging
7. **Document recovery procedures** for your team
8. **Use TLS** for all cluster communication in production

---

*For more information, see the [User Guide](USER_GUIDE.md) or [GitHub Issues](https://github.com/your-org/boyodb/issues).*
