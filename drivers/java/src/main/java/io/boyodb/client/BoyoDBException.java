package io.boyodb.client;

/**
 * Exception thrown by BoyoDB client operations.
 */
public class BoyoDBException extends Exception {

    public BoyoDBException(String message) {
        super(message);
    }

    public BoyoDBException(String message, Throwable cause) {
        super(message, cause);
    }

    public BoyoDBException(Throwable cause) {
        super(cause);
    }
}
