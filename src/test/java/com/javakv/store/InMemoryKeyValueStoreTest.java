package com.javakv.store;

import com.javakv.api.KeyValueStore;
import com.javakv.api.StorageEngine;
import com.javakv.persistence.LogEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

@DisplayName("InMemoryKeyValueStore Tests")
class InMemoryKeyValueStoreTest {

    private KeyValueStore store;
    private TestStorageEngine testStorage;

    /**
     * Test implementation of StorageEngine for testing purposes.
     */
    static class TestStorageEngine implements StorageEngine {
        private final List<LogEntry> entries = new ArrayList<>();
        private boolean shouldFail = false;
        private RuntimeException failureException;

        @Override
        public void append(LogEntry entry) {
            if (shouldFail) {
                throw failureException != null ? failureException : new RuntimeException("Test failure");
            }
            entries.add(entry);
        }

        @Override
        public List<LogEntry> recover() {
            return new ArrayList<>(entries);
        }

        @Override
        public void flush() {
            // No-op for testing
        }

        @Override
        public void close() {
            // No-op for testing
        }

        public void setShouldFail(boolean shouldFail) {
            this.shouldFail = shouldFail;
        }

        public void setFailureException(RuntimeException exception) {
            this.failureException = exception;
        }

        public List<LogEntry> getEntries() {
            return new ArrayList<>(entries);
        }

        public LogEntry getLastEntry() {
            return entries.isEmpty() ? null : entries.get(entries.size() - 1);
        }

        public void reset() {
            entries.clear();
            shouldFail = false;
            failureException = null;
        }
    }

    @BeforeEach
    void setUp() {
        // Create a fresh store for each test
        store = new InMemoryKeyValueStore();
    }

    @Test
    @DisplayName("Should store and retrieve a value")
    void testPutAndGet() {
        store.put("key1", "value1");

        Optional<String> result = store.get("key1");

        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo("value1");
    }

    @Test
    @DisplayName("Should return empty Optional for non-existent key")
    void testGetNonExistentKey() {
        Optional<String> result = store.get("nonexistent");

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("Should update existing value")
    void testPutOverwrite() {
        store.put("key1", "value1");
        store.put("key1", "value2");

        Optional<String> result = store.get("key1");

        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo("value2");
    }

    @Test
    @DisplayName("Should delete existing key")
    void testDelete() {
        store.put("key1", "value1");

        boolean deleted = store.delete("key1");

        assertThat(deleted).isTrue();
        assertThat(store.get("key1")).isEmpty();
    }

    @Test
    @DisplayName("Should return false when deleting non-existent key")
    void testDeleteNonExistent() {
        boolean deleted = store.delete("nonexistent");

        assertThat(deleted).isFalse();
    }

    @Test
    @DisplayName("Should check if key exists")
    void testExists() {
        store.put("key1", "value1");

        assertThat(store.exists("key1")).isTrue();
        assertThat(store.exists("nonexistent")).isFalse();
    }

    @Test
    @DisplayName("Should return correct size")
    void testSize() {
        assertThat(store.size()).isEqualTo(0);

        store.put("key1", "value1");
        assertThat(store.size()).isEqualTo(1);

        store.put("key2", "value2");
        assertThat(store.size()).isEqualTo(2);

        store.put("key1", "updated");  // Overwrite doesn't increase size
        assertThat(store.size()).isEqualTo(2);

        store.delete("key1");
        assertThat(store.size()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should clear all entries")
    void testClear() {
        store.put("key1", "value1");
        store.put("key2", "value2");
        store.put("key3", "value3");

        assertThat(store.size()).isEqualTo(3);

        store.clear();

        assertThat(store.size()).isEqualTo(0);
        assertThat(store.exists("key1")).isFalse();
        assertThat(store.exists("key2")).isFalse();
        assertThat(store.exists("key3")).isFalse();
    }

    // Validation Tests

    @Test
    @DisplayName("Should throw exception for null key in put")
    void testPutNullKey() {
        assertThatThrownBy(() -> store.put(null, "value"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Key cannot be null");
    }

    @Test
    @DisplayName("Should throw exception for empty key in put")
    void testPutEmptyKey() {
        assertThatThrownBy(() -> store.put("", "value"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Key cannot be empty");
    }

    @Test
    @DisplayName("Should throw exception for null value in put")
    void testPutNullValue() {
        assertThatThrownBy(() -> store.put("key", null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Value cannot be null");
    }

    @Test
    @DisplayName("Should throw exception for null key in get")
    void testGetNullKey() {
        assertThatThrownBy(() -> store.get(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Key cannot be null");
    }

    @Test
    @DisplayName("Should throw exception for empty key in get")
    void testGetEmptyKey() {
        assertThatThrownBy(() -> store.get(""))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Key cannot be empty");
    }

    @Test
    @DisplayName("Should throw exception for null key in delete")
    void testDeleteNullKey() {
        assertThatThrownBy(() -> store.delete(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Key cannot be null");
    }

    @Test
    @DisplayName("Should throw exception for null key in exists")
    void testExistsNullKey() {
        assertThatThrownBy(() -> store.exists(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Key cannot be null");
    }

    // Storage Engine Integration Tests

    @Test
    @DisplayName("Should append to storage engine on put")
    void testPutWithStorageEngine() {
        testStorage = new TestStorageEngine();
        store = new InMemoryKeyValueStore(testStorage);

        store.put("key1", "value1");

        LogEntry entry = testStorage.getLastEntry();
        assertThat(entry).isNotNull();
        assertThat(entry.key()).isEqualTo("key1");
        assertThat(entry.value()).isEqualTo("value1");
        assertThat(entry.operation()).isEqualTo(com.javakv.persistence.OperationType.PUT);
    }

    @Test
    @DisplayName("Should append to storage engine on delete")
    void testDeleteWithStorageEngine() {
        testStorage = new TestStorageEngine();
        store = new InMemoryKeyValueStore(testStorage);

        store.put("key1", "value1");
        testStorage.reset(); // Clear previous put interaction

        store.delete("key1");

        LogEntry entry = testStorage.getLastEntry();
        assertThat(entry).isNotNull();
        assertThat(entry.key()).isEqualTo("key1");
        assertThat(entry.value()).isNull();
        assertThat(entry.operation()).isEqualTo(com.javakv.persistence.OperationType.DELETE);
    }

    @Test
    @DisplayName("Should append to storage engine on clear")
    void testClearWithStorageEngine() {
        testStorage = new TestStorageEngine();
        store = new InMemoryKeyValueStore(testStorage);

        store.put("key1", "value1");
        testStorage.reset();

        store.clear();

        LogEntry entry = testStorage.getLastEntry();
        assertThat(entry).isNotNull();
        assertThat(entry.key()).isNull();
        assertThat(entry.value()).isNull();
        assertThat(entry.operation()).isEqualTo(com.javakv.persistence.OperationType.CLEAR);
    }

    @Test
    @DisplayName("Should throw StorageException when storage engine fails on put")
    void testPutStorageFailure() {
        testStorage = new TestStorageEngine();
        testStorage.setShouldFail(true);
        testStorage.setFailureException(new RuntimeException("Disk full"));
        store = new InMemoryKeyValueStore(testStorage);

        assertThatThrownBy(() -> store.put("key1", "value1"))
            .isInstanceOf(StorageException.class)
            .hasMessageContaining("Failed to persist PUT operation")
            .hasCauseInstanceOf(RuntimeException.class);

        // Verify the in-memory store was NOT updated
        assertThat(store.get("key1")).isEmpty();
    }

    @Test
    @DisplayName("Should throw StorageException when storage engine fails on delete")
    void testDeleteStorageFailure() {
        testStorage = new TestStorageEngine();
        store = new InMemoryKeyValueStore(testStorage);

        // Put a value successfully
        store.put("key1", "value1");

        // Make storage fail on next append
        testStorage.setShouldFail(true);
        testStorage.setFailureException(new RuntimeException("Disk error"));

        assertThatThrownBy(() -> store.delete("key1"))
            .isInstanceOf(StorageException.class)
            .hasMessageContaining("Failed to persist DELETE operation");

        // Verify the in-memory store was NOT updated (key still exists)
        assertThat(store.exists("key1")).isTrue();
    }

    @Test
    @DisplayName("Should work without storage engine (in-memory only mode)")
    void testWithoutStorageEngine() {
        store = new InMemoryKeyValueStore(null);

        store.put("key1", "value1");
        assertThat(store.get("key1")).isPresent();

        store.delete("key1");
        assertThat(store.get("key1")).isEmpty();

        // Should not throw any exceptions
    }

    // Edge Cases

    @Test
    @DisplayName("Should handle special characters in keys and values")
    void testSpecialCharacters() {
        store.put("key:with:colons", "value with spaces");
        store.put("key/with/slashes", "value\nwith\nnewlines");
        store.put("key@#$%", "value!@#$%^&*()");

        assertThat(store.get("key:with:colons")).contains("value with spaces");
        assertThat(store.get("key/with/slashes")).contains("value\nwith\nnewlines");
        assertThat(store.get("key@#$%")).contains("value!@#$%^&*()");
    }

    @Test
    @DisplayName("Should handle very long keys and values")
    void testLongKeysAndValues() {
        String longKey = "k".repeat(1000);
        String longValue = "v".repeat(10000);

        store.put(longKey, longValue);

        assertThat(store.get(longKey)).contains(longValue);
    }

    @Test
    @DisplayName("Should allow empty string as value")
    void testEmptyValue() {
        store.put("key1", "");

        assertThat(store.get("key1")).isPresent();
        assertThat(store.get("key1").get()).isEmpty();
    }

    @Test
    @DisplayName("Should handle concurrent delete operations without race condition")
    void testConcurrentDeleteNoRaceCondition() throws InterruptedException {
        // This test validates that the delete race condition is fixed
        // Previously: check-then-act pattern allowed two threads to both write to WAL
        // Now: Only one thread gets the value from remove(), preventing double-WAL writes

        testStorage = new TestStorageEngine();
        store = new InMemoryKeyValueStore(testStorage);

        store.put("key1", "value1");
        testStorage.reset(); // Clear the PUT entry

        // Create two threads that will try to delete the same key simultaneously
        Thread thread1 = new Thread(() -> {
            try {
                boolean deleted = store.delete("key1");
                // Only one thread should succeed
            } catch (Exception e) {
                // Ignore exceptions for this test
            }
        });

        Thread thread2 = new Thread(() -> {
            try {
                boolean deleted = store.delete("key1");
                // Only one thread should succeed
            } catch (Exception e) {
                // Ignore exceptions for this test
            }
        });

        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();

        // Validate: There should be exactly ONE delete entry in the WAL
        // If the race condition existed, both threads could write to WAL
        List<LogEntry> entries = testStorage.recover();
        long deleteCount = entries.stream()
            .filter(e -> e.operation() == com.javakv.persistence.OperationType.DELETE)
            .count();

        assertThat(deleteCount)
            .as("Only one DELETE should be logged to WAL, even with concurrent deletes")
            .isEqualTo(1);

        // The key should be deleted
        assertThat(store.exists("key1")).isFalse();
    }

    @Test
    @DisplayName("Should handle concurrent operations on different keys safely")
    void testConcurrentOperationsDifferentKeys() throws InterruptedException {
        // Test that operations on different keys don't interfere
        final int threadCount = 10;
        final int operationsPerThread = 100;
        Thread[] threads = new Thread[threadCount];

        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < operationsPerThread; j++) {
                    String key = "thread" + threadId + "_key" + j;
                    store.put(key, "value" + j);
                    assertThat(store.get(key)).contains("value" + j);
                    if (j % 2 == 0) {
                        store.delete(key);
                        assertThat(store.exists(key)).isFalse();
                    }
                }
            });
        }

        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Wait for completion
        for (Thread thread : threads) {
            thread.join();
        }

        // Verify final state: only odd-numbered keys should remain
        int expectedSize = threadCount * (operationsPerThread / 2);
        assertThat(store.size()).isEqualTo(expectedSize);
    }
}
