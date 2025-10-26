package com.javakv.store;

import com.javakv.api.KeyValueStore;
import com.javakv.api.StorageEngine;
import com.javakv.persistence.LogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe in-memory implementation of the KeyValueStore interface.
 *
 * <p>This implementation uses a {@link ConcurrentHashMap} for thread-safe storage
 * and integrates with a {@link StorageEngine} for durability via Write-Ahead Logging.
 *
 * <p>All write operations (put, delete, clear) are persisted to the storage engine
 * before being applied to the in-memory map, ensuring crash recovery capability.
 */
public class InMemoryKeyValueStore implements KeyValueStore {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryKeyValueStore.class);

    private final ConcurrentHashMap<String, String> store;
    private final StorageEngine storageEngine;

    /**
     * Constructs a new InMemoryKeyValueStore with the given storage engine.
     *
     * @param storageEngine the storage engine for persistence (can be null for in-memory only mode)
     */
    public InMemoryKeyValueStore(StorageEngine storageEngine) {
        this.store = new ConcurrentHashMap<>();
        this.storageEngine = storageEngine;
        logger.info("InMemoryKeyValueStore initialized with storage engine: {}",
                    storageEngine != null ? storageEngine.getClass().getSimpleName() : "none");
    }

    /**
     * Constructs a new InMemoryKeyValueStore without persistence.
     * Useful for testing or when persistence is not required.
     */
    public InMemoryKeyValueStore() {
        this(null);
    }

    @Override
    public void put(String key, String value) {
        validateKey(key);
        validateValue(value);

        logger.debug("PUT: {} = {}", key, value);

        // Write to WAL before updating memory
        if (storageEngine != null) {
            try {
                storageEngine.append(LogEntry.put(key, value));
            } catch (Exception e) {
                logger.error("Failed to append PUT operation to storage engine for key: {}", key, e);
                throw new StorageException("Failed to persist PUT operation", e);
            }
        }

        // Update in-memory store
        store.put(key, value);
        logger.trace("PUT completed: {} = {}", key, value);
    }

    @Override
    public Optional<String> get(String key) {
        validateKey(key);

        String value = store.get(key);
        logger.debug("GET: {} = {}", key, value != null ? value : "<not found>");

        return Optional.ofNullable(value);
    }

    @Override
    public boolean delete(String key) {
        validateKey(key);

        logger.debug("DELETE: {}", key);

        // Remove from memory first (atomic operation)
        // ConcurrentHashMap.remove() ensures only one thread gets the actual value
        // This eliminates the check-then-act race condition
        String removed = store.remove(key);

        if (removed == null) {
            logger.debug("DELETE failed: key not found: {}", key);
            return false;
        }

        // Write to WAL after successful removal
        // If this fails, we attempt rollback to maintain consistency
        if (storageEngine != null) {
            try {
                storageEngine.append(LogEntry.delete(key));
            } catch (Exception e) {
                // Rollback: restore the removed value using putIfAbsent
                // putIfAbsent prevents overwriting any value that was written concurrently
                String existing = store.putIfAbsent(key, removed);

                if (existing != null) {
                    // Edge case: Another thread wrote a new value during our failure
                    // We cannot fully rollback, but the new value takes precedence
                    logger.warn("Cannot fully rollback DELETE for key: {} - key was updated during WAL failure", key);
                }

                logger.error("Failed to append DELETE operation to storage engine for key: {}, attempted rollback", key, e);
                throw new StorageException("Failed to persist DELETE operation", e);
            }
        }

        logger.trace("DELETE completed: {} (was: {})", key, removed);
        return true;
    }

    @Override
    public boolean exists(String key) {
        validateKey(key);

        boolean exists = store.containsKey(key);
        logger.debug("EXISTS: {} = {}", key, exists);

        return exists;
    }

    @Override
    public int size() {
        int size = store.size();
        logger.debug("SIZE: {}", size);

        return size;
    }

    @Override
    public void clear() {
        logger.debug("CLEAR: removing {} entries", store.size());

        // Write to WAL before clearing memory
        if (storageEngine != null) {
            try {
                storageEngine.append(LogEntry.clear());
            } catch (Exception e) {
                logger.error("Failed to append CLEAR operation to storage engine", e);
                throw new StorageException("Failed to persist CLEAR operation", e);
            }
        }

        // Clear in-memory store
        store.clear();
        logger.info("CLEAR completed: all entries removed");
    }

    /**
     * Validates that a key is not null or empty.
     *
     * @param key the key to validate
     * @throws IllegalArgumentException if the key is null or empty
     */
    private void validateKey(String key) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be empty");
        }
    }

    /**
     * Validates that a value is not null.
     *
     * @param value the value to validate
     * @throws IllegalArgumentException if the value is null
     */
    private void validateValue(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
    }
}
