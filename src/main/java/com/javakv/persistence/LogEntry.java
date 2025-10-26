package com.javakv.persistence;

import java.time.Instant;

/**
 * Immutable record representing a single operation in the Write-Ahead Log (WAL).
 *
 * <p>This record uses Java 21's record feature to create a compact, immutable
 * data carrier for log entries. Each entry captures an operation performed on
 * the key-value store along with metadata.
 *
 * @param operation the type of operation (PUT, DELETE, CLEAR)
 * @param key the key affected by the operation (null for CLEAR operations)
 * @param value the value for PUT operations (null for DELETE and CLEAR)
 * @param timestamp when the operation occurred
 */
public record LogEntry(
    OperationType operation,
    String key,
    String value,
    Instant timestamp
) {
    /**
     * Compact constructor for validation.
     * Ensures that the operation is never null and validates key/value requirements
     * based on operation type.
     *
     * @throws IllegalArgumentException if validation fails
     */
    public LogEntry {
        if (operation == null) {
            throw new IllegalArgumentException("Operation cannot be null");
        }

        // Validate key and value based on operation type
        switch (operation) {
            case PUT -> {
                if (key == null || key.isEmpty()) {
                    throw new IllegalArgumentException("Key cannot be null or empty for PUT operation");
                }
                if (value == null) {
                    throw new IllegalArgumentException("Value cannot be null for PUT operation");
                }
            }
            case DELETE -> {
                if (key == null || key.isEmpty()) {
                    throw new IllegalArgumentException("Key cannot be null or empty for DELETE operation");
                }
            }
            case CLEAR -> {
                // Key and value should be null for CLEAR operations
            }
        }

        if (timestamp == null) {
            throw new IllegalArgumentException("Timestamp cannot be null");
        }
    }

    /**
     * Creates a PUT log entry.
     *
     * @param key the key to store
     * @param value the value to associate with the key
     * @return a new LogEntry for a PUT operation
     */
    public static LogEntry put(String key, String value) {
        return new LogEntry(OperationType.PUT, key, value, Instant.now());
    }

    /**
     * Creates a DELETE log entry.
     *
     * @param key the key to delete
     * @return a new LogEntry for a DELETE operation
     */
    public static LogEntry delete(String key) {
        return new LogEntry(OperationType.DELETE, key, null, Instant.now());
    }

    /**
     * Creates a CLEAR log entry.
     *
     * @return a new LogEntry for a CLEAR operation
     */
    public static LogEntry clear() {
        return new LogEntry(OperationType.CLEAR, null, null, Instant.now());
    }
}
