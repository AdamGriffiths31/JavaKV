package com.javakv.persistence;

/**
 * Enum representing the type of operation performed on the key-value store.
 * Used for Write-Ahead Log (WAL) entries to track state changes.
 */
public enum OperationType {
    /**
     * A put operation - stores or updates a key-value pair.
     */
    PUT,

    /**
     * A delete operation - removes a key-value pair.
     */
    DELETE,

    /**
     * A clear operation - removes all key-value pairs.
     */
    CLEAR
}
