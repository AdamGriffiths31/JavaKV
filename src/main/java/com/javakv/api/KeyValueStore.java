package com.javakv.api;

import java.util.Optional;

/**
 * Core interface for key-value store operations.
 *
 * <p>This interface defines the contract for a simple key-value storage system
 * supporting basic CRUD operations. All implementations must be thread-safe.
 *
 * <p>Keys and values are represented as Strings. Keys cannot be null or empty.
 */
public interface KeyValueStore {

    /**
     * Stores a key-value pair in the store.
     * If the key already exists, its value will be updated.
     *
     * @param key the key to store (must not be null or empty)
     * @param value the value to associate with the key (must not be null)
     * @throws IllegalArgumentException if key or value is null or if key is empty
     */
    void put(String key, String value);

    /**
     * Retrieves the value associated with the given key.
     *
     * @param key the key to look up (must not be null or empty)
     * @return an Optional containing the value if found, or empty if not found
     * @throws IllegalArgumentException if key is null or empty
     */
    Optional<String> get(String key);

    /**
     * Deletes a key-value pair from the store.
     *
     * @param key the key to delete (must not be null or empty)
     * @return true if the key existed and was deleted, false if the key was not found
     * @throws IllegalArgumentException if key is null or empty
     */
    boolean delete(String key);

    /**
     * Checks whether a key exists in the store.
     *
     * @param key the key to check (must not be null or empty)
     * @return true if the key exists, false otherwise
     * @throws IllegalArgumentException if key is null or empty
     */
    boolean exists(String key);

    /**
     * Returns the number of key-value pairs currently stored.
     *
     * @return the number of entries in the store
     */
    int size();

    /**
     * Removes all key-value pairs from the store.
     * This operation cannot be undone.
     */
    void clear();
}
