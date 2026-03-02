<?php

declare(strict_types=1);

namespace Boyodb;

/**
 * Configuration options for the boyodb client.
 */
class Config
{
    /**
     * Enable TLS encryption.
     */
    public bool $tls = false;

    /**
     * Path to CA certificate file.
     */
    public ?string $caFile = null;

    /**
     * Skip TLS certificate verification.
     * WARNING: SECURITY RISK - Disables certificate validation, making connections
     * vulnerable to man-in-the-middle (MITM) attacks. NEVER use in production.
     */
    public bool $insecureSkipVerify = false;

    /**
     * Connection timeout in seconds.
     */
    public float $connectTimeout = 10.0;

    /**
     * Read timeout in seconds.
     */
    public float $readTimeout = 30.0;

    /**
     * Write timeout in seconds.
     */
    public float $writeTimeout = 10.0;

    /**
     * Authentication token.
     */
    public ?string $token = null;

    /**
     * Maximum connection retry attempts.
     */
    public int $maxRetries = 3;

    /**
     * Delay between retries in seconds.
     */
    public float $retryDelay = 1.0;

    /**
     * Default database for queries.
     */
    public ?string $database = null;

    /**
     * Default query timeout in milliseconds.
     */
    public int $queryTimeout = 30000;

    /**
     * Create a new Config instance.
     */
    public function __construct(array $options = [])
    {
        foreach ($options as $key => $value) {
            if (property_exists($this, $key)) {
                $this->$key = $value;
            }
        }
    }
}
