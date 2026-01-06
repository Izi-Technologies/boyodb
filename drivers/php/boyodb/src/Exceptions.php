<?php

declare(strict_types=1);

namespace Boyodb;

use Exception;

/**
 * Base exception for boyodb errors.
 */
class BoyodbException extends Exception
{
}

/**
 * Thrown when connection to the server fails.
 */
class ConnectionException extends BoyodbException
{
}

/**
 * Thrown when a query fails.
 */
class QueryException extends BoyodbException
{
}

/**
 * Thrown when authentication fails.
 */
class AuthException extends BoyodbException
{
}

/**
 * Thrown when an operation times out.
 */
class TimeoutException extends BoyodbException
{
}
