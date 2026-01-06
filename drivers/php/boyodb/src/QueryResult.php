<?php

declare(strict_types=1);

namespace Boyodb;

use ArrayIterator;
use Countable;
use IteratorAggregate;
use Traversable;

/**
 * Result of a query execution.
 */
class QueryResult implements Countable, IteratorAggregate
{
    /**
     * @var array<int, array<string, mixed>> List of rows
     */
    public array $rows = [];

    /**
     * @var array<int, string> Column names
     */
    public array $columns = [];

    /**
     * Number of segments scanned.
     */
    public int $segmentsScanned = 0;

    /**
     * Bytes skipped due to pruning.
     */
    public int $dataSkippedBytes = 0;

    /**
     * Get number of rows.
     */
    public function count(): int
    {
        return count($this->rows);
    }

    /**
     * Get row count (alias).
     */
    public function rowCount(): int
    {
        return $this->count();
    }

    /**
     * Check if result is empty.
     */
    public function isEmpty(): bool
    {
        return empty($this->rows);
    }

    /**
     * Get iterator for rows.
     */
    public function getIterator(): Traversable
    {
        return new ArrayIterator($this->rows);
    }

    /**
     * Get a row by index.
     */
    public function getRow(int $index): ?array
    {
        return $this->rows[$index] ?? null;
    }

    /**
     * Get all rows.
     */
    public function getRows(): array
    {
        return $this->rows;
    }

    /**
     * Get column names.
     */
    public function getColumns(): array
    {
        return $this->columns;
    }
}

/**
 * Table metadata.
 */
class TableInfo
{
    public function __construct(
        public string $database = '',
        public string $name = '',
        public ?string $schemaJson = null
    ) {
    }

    /**
     * Create from array.
     */
    public static function fromArray(array $data): self
    {
        return new self(
            $data['database'] ?? '',
            $data['name'] ?? '',
            $data['schema_json'] ?? null
        );
    }
}
