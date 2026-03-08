# BoyoDB Documentation

Welcome to the BoyoDB documentation. This directory contains comprehensive guides for installing, configuring, and using BoyoDB.

## Documentation Index

### Getting Started
- **[GETTING_STARTED.md](GETTING_STARTED.md)** - Quick introduction and first steps
- **[QUICK_REFERENCE.md](QUICK_REFERENCE.md)** - Cheat sheet for common operations

### Core Documentation
- **[USER_GUIDE.md](USER_GUIDE.md)** - Comprehensive user guide covering all features
- **[SQL.md](SQL.md)** - Complete SQL syntax reference
- **[CLI.md](CLI.md)** - Command-line interface reference
- **[SHELL.md](SHELL.md)** - Interactive shell commands

### Operations
- **[CLUSTERING.md](CLUSTERING.md)** - High availability and clustering guide
- **[SECURITY.md](SECURITY.md)** - Authentication, authorization, and TLS
- **[STORAGE_PHASES.md](STORAGE_PHASES.md)** - Storage tiers and data lifecycle

### Development
- **[API.md](API.md)** - Protocol and API documentation
- **[ROADMAP.md](ROADMAP.md)** - Feature roadmap and planned improvements

## Quick Links

### Start a Server
```bash
boyodb-server /var/lib/boyodb 0.0.0.0:8765
```

### Connect with CLI
```bash
boyodb-cli shell --host localhost:8765
```

### Create Your First Table
```sql
CREATE DATABASE mydb;
CREATE TABLE mydb.events (id INT64, name STRING, ts TIMESTAMP);
INSERT INTO mydb.events VALUES (1, 'test', NOW());
SELECT * FROM mydb.events;
```

### Set Up a Cluster
```bash
# Node 1
boyodb-server /data/node1 0.0.0.0:8765 \
    --cluster --cluster-id prod --gossip-addr 0.0.0.0:8766

# Node 2
boyodb-server /data/node2 0.0.0.0:8767 \
    --cluster --cluster-id prod --gossip-addr 0.0.0.0:8768 \
    --seed-nodes "node1:8766"
```

## Support

- [GitHub Issues](https://github.com/Izi-Technologies/boyodb/issues)
- [Discussions](https://github.com/Izi-Technologies/boyodb/discussions)
- [Changelog](../CHANGELOG.md) - Release history and version changes
