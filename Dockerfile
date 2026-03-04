# Build stage
FROM rust:latest AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY bindings ./bindings

# Build release binaries
RUN cargo build --release -p boyodb-server -p boyodb-cli

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -r -s /bin/false -m -d /var/lib/boyodb boyodb

# Create data directory
RUN mkdir -p /var/lib/boyodb/data /var/lib/boyodb/wal && \
    chown -R boyodb:boyodb /var/lib/boyodb

# Copy binaries from builder
COPY --from=builder /app/target/release/boyodb-server /usr/local/bin/
COPY --from=builder /app/target/release/boyodb-cli /usr/local/bin/

# Switch to non-root user
USER boyodb

# Set working directory
WORKDIR /var/lib/boyodb

# Default environment
ENV RUST_LOG=info
ENV BOYODB_LOG_FORMAT=json

# Expose default port
EXPOSE 5555

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD boyodb-cli info --host 127.0.0.1:5555 || exit 1

# Default command: boyodb-server <data_dir> <bind_addr> [options]
ENTRYPOINT ["boyodb-server"]
CMD ["/var/lib/boyodb/data", "0.0.0.0:5555"]
