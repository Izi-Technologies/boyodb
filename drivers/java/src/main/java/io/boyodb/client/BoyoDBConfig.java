package io.boyodb.client;

import java.time.Duration;

/**
 * Configuration for BoyoDB client.
 */
public class BoyoDBConfig {
    private final String host;
    private final int port;
    private final boolean tls;
    private final String caFile;
    private final boolean insecureSkipVerify;
    private final Duration connectTimeout;
    private final Duration readTimeout;
    private final Duration writeTimeout;
    private final String token;
    private final int maxRetries;
    private final Duration retryDelay;
    private final String database;
    private final long queryTimeout;
    private final int poolSize;

    private BoyoDBConfig(Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.tls = builder.tls;
        this.caFile = builder.caFile;
        this.insecureSkipVerify = builder.insecureSkipVerify;
        this.connectTimeout = builder.connectTimeout;
        this.readTimeout = builder.readTimeout;
        this.writeTimeout = builder.writeTimeout;
        this.token = builder.token;
        this.maxRetries = builder.maxRetries;
        this.retryDelay = builder.retryDelay;
        this.database = builder.database;
        this.queryTimeout = builder.queryTimeout;
        this.poolSize = builder.poolSize;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getHost() { return host; }
    public int getPort() { return port; }
    public boolean isTls() { return tls; }
    public String getCaFile() { return caFile; }
    public boolean isInsecureSkipVerify() { return insecureSkipVerify; }
    public Duration getConnectTimeout() { return connectTimeout; }
    public Duration getReadTimeout() { return readTimeout; }
    public Duration getWriteTimeout() { return writeTimeout; }
    public String getToken() { return token; }
    public int getMaxRetries() { return maxRetries; }
    public Duration getRetryDelay() { return retryDelay; }
    public String getDatabase() { return database; }
    public long getQueryTimeout() { return queryTimeout; }
    public int getPoolSize() { return poolSize; }

    public static class Builder {
        private String host = "localhost";
        private int port = 8765;
        private boolean tls = false;
        private String caFile = null;
        private boolean insecureSkipVerify = false;
        private Duration connectTimeout = Duration.ofSeconds(10);
        private Duration readTimeout = Duration.ofSeconds(30);
        private Duration writeTimeout = Duration.ofSeconds(10);
        private String token = null;
        private int maxRetries = 3;
        private Duration retryDelay = Duration.ofSeconds(1);
        private String database = null;
        private long queryTimeout = 30000;
        private int poolSize = 10;

        public Builder host(String host) {
            this.host = host;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder tls(boolean tls) {
            this.tls = tls;
            return this;
        }

        public Builder caFile(String caFile) {
            this.caFile = caFile;
            return this;
        }

        /**
         * Skip TLS certificate verification.
         * WARNING: SECURITY RISK - Only use for testing.
         */
        public Builder insecureSkipVerify(boolean insecureSkipVerify) {
            this.insecureSkipVerify = insecureSkipVerify;
            return this;
        }

        public Builder connectTimeout(Duration connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder readTimeout(Duration readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        public Builder writeTimeout(Duration writeTimeout) {
            this.writeTimeout = writeTimeout;
            return this;
        }

        public Builder token(String token) {
            this.token = token;
            return this;
        }

        public Builder maxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder retryDelay(Duration retryDelay) {
            this.retryDelay = retryDelay;
            return this;
        }

        public Builder database(String database) {
            this.database = database;
            return this;
        }

        public Builder queryTimeout(long queryTimeout) {
            this.queryTimeout = queryTimeout;
            return this;
        }

        public Builder poolSize(int poolSize) {
            this.poolSize = poolSize;
            return this;
        }

        public BoyoDBConfig build() {
            return new BoyoDBConfig(this);
        }
    }
}
