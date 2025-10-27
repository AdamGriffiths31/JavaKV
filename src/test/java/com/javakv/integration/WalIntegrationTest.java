package com.javakv.integration;

import com.javakv.api.KeyValueStore;
import com.javakv.persistence.WriteAheadLog;
import com.javakv.store.InMemoryKeyValueStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for the complete WAL + InMemoryKeyValueStore system.
 *
 * <p>These tests verify end-to-end functionality with real components,
 * including crash recovery, concurrent access, and large datasets.
 *
 * <p>Tagged as "integration" - these tests are slower than unit tests
 * and can be excluded from fast test runs using:
 * {@code mvn test -Dgroups="!integration"}
 */
@Tag("integration")
@DisplayName("WAL Integration Tests")
class WalIntegrationTest {

  @TempDir
  Path tempDir;

  @Test
  @DisplayName("Should persist and recover data across process restarts")
  void testEndToEndPersistence() throws Exception {
    Path walPath = tempDir.resolve("test.wal");

    // Session 1: Write data
    WriteAheadLog wal1 = new WriteAheadLog(walPath);
    KeyValueStore store1 = new InMemoryKeyValueStore(wal1);

    store1.put("user:1", "Alice");
    store1.put("user:2", "Bob");
    store1.put("user:3", "Charlie");
    store1.delete("user:2");
    store1.put("user:1", "Alice Updated");

    assertThat(store1.size()).isEqualTo(2);
    wal1.close();

    // Session 2: Recover and verify
    WriteAheadLog wal2 = new WriteAheadLog(walPath);
    KeyValueStore store2 = new InMemoryKeyValueStore(wal2);

    assertThat(store2.size()).isEqualTo(2);
    assertThat(store2.get("user:1")).hasValue("Alice Updated");
    assertThat(store2.get("user:2")).isEmpty();
    assertThat(store2.get("user:3")).hasValue("Charlie");
    wal2.close();
  }

  @Test
  @DisplayName("Should handle multiple restart cycles")
  void testMultipleRestartCycles() throws Exception {
    Path walPath = tempDir.resolve("test.wal");

    for (int i = 0; i < 5; i++) {
      WriteAheadLog wal = new WriteAheadLog(walPath);
      KeyValueStore store = new InMemoryKeyValueStore(wal);

      // Add new data in each session
      store.put("session:" + i, "data" + i);

      // Verify all previous sessions' data is still there
      for (int j = 0; j <= i; j++) {
        assertThat(store.get("session:" + j)).hasValue("data" + j);
      }

      assertThat(store.size()).isEqualTo(i + 1);
      wal.close();
    }
  }

  @Test
  @DisplayName("Should recover after clear operation")
  void testClearAndRecover() throws Exception {
    Path walPath = tempDir.resolve("test.wal");

    // Session 1: Write data then clear
    WriteAheadLog wal1 = new WriteAheadLog(walPath);
    KeyValueStore store1 = new InMemoryKeyValueStore(wal1);

    store1.put("key1", "value1");
    store1.put("key2", "value2");
    store1.clear();
    store1.put("key3", "value3");

    assertThat(store1.size()).isEqualTo(1);
    wal1.close();

    // Session 2: Verify recovery
    WriteAheadLog wal2 = new WriteAheadLog(walPath);
    KeyValueStore store2 = new InMemoryKeyValueStore(wal2);

    assertThat(store2.size()).isEqualTo(1);
    assertThat(store2.get("key1")).isEmpty();
    assertThat(store2.get("key2")).isEmpty();
    assertThat(store2.get("key3")).hasValue("value3");
    wal2.close();
  }

  @Test
  @DisplayName("Should handle large datasets (10k entries)")
  void testLargeDataset() throws Exception {
    Path walPath = tempDir.resolve("test.wal");
    int numEntries = 10_000;

    // Session 1: Write large dataset
    long startWrite = System.currentTimeMillis();
    WriteAheadLog wal1 = new WriteAheadLog(walPath);
    KeyValueStore store1 = new InMemoryKeyValueStore(wal1);

    for (int i = 0; i < numEntries; i++) {
      store1.put("key:" + i, "value:" + i);
    }

    assertThat(store1.size()).isEqualTo(numEntries);
    wal1.close();
    long writeTime = System.currentTimeMillis() - startWrite;

    // Session 2: Recover and verify
    long startRecover = System.currentTimeMillis();
    WriteAheadLog wal2 = new WriteAheadLog(walPath);
    KeyValueStore store2 = new InMemoryKeyValueStore(wal2);
    long recoverTime = System.currentTimeMillis() - startRecover;

    assertThat(store2.size()).isEqualTo(numEntries);

    // Spot check some entries
    assertThat(store2.get("key:0")).hasValue("value:0");
    assertThat(store2.get("key:5000")).hasValue("value:5000");
    assertThat(store2.get("key:9999")).hasValue("value:9999");

    wal2.close();

    // Performance assertions (from plan.md requirements)
    // Recovery should be < 1 second for 10K entries
    assertThat(recoverTime).isLessThan(1000);

    System.out.printf("Large dataset test: Write=%dms, Recover=%dms%n",
             writeTime, recoverTime);
  }

  @Test
  @DisplayName("Should handle concurrent writes with WAL")
  void testConcurrentWritesWithWal() throws Exception {
    Path walPath = tempDir.resolve("test.wal");
    WriteAheadLog wal = new WriteAheadLog(walPath);
    KeyValueStore store = new InMemoryKeyValueStore(wal);

    int numThreads = 10;
    int opsPerThread = 100;
    CountDownLatch startLatch = new CountDownLatch(1);
    CountDownLatch doneLatch = new CountDownLatch(numThreads);
    List<Exception> exceptions = new ArrayList<>();

    // Create threads
    List<Thread> threads = new ArrayList<>();
    for (int t = 0; t < numThreads; t++) {
      final int threadId = t;
      Thread thread = new Thread(() -> {
        try {
          startLatch.await(); // Wait for all threads to be ready
          for (int i = 0; i < opsPerThread; i++) {
            String key = "thread" + threadId + "_key" + i;
            store.put(key, "value" + i);
          }
        } catch (Exception e) {
          synchronized (exceptions) {
            exceptions.add(e);
          }
        } finally {
          doneLatch.countDown();
        }
      });
      thread.start();
      threads.add(thread);
    }

    // Release all threads at once
    startLatch.countDown();

    // Wait for completion
    boolean completed = doneLatch.await(10, TimeUnit.SECONDS);

    assertThat(completed).isTrue();
    assertThat(exceptions).isEmpty();
    assertThat(store.size()).isEqualTo(numThreads * opsPerThread);

    wal.close();
  }

  @Test
  @DisplayName("Should handle updates to same key correctly")
  void testKeyUpdatesAndRecovery() throws Exception {
    Path walPath = tempDir.resolve("test.wal");

    WriteAheadLog wal1 = new WriteAheadLog(walPath);
    KeyValueStore store1 = new InMemoryKeyValueStore(wal1);

    // Update same key multiple times
    for (int i = 0; i < 100; i++) {
      store1.put("counter", String.valueOf(i));
    }

    assertThat(store1.get("counter")).hasValue("99");
    assertThat(store1.size()).isEqualTo(1);
    wal1.close();

    // Verify only final value is retained
    WriteAheadLog wal2 = new WriteAheadLog(walPath);
    KeyValueStore store2 = new InMemoryKeyValueStore(wal2);

    assertThat(store2.get("counter")).hasValue("99");
    assertThat(store2.size()).isEqualTo(1);
    wal2.close();
  }

  @Test
  @DisplayName("Should recover correctly with mixed operations")
  void testMixedOperationsRecovery() throws Exception {
    Path walPath = tempDir.resolve("test.wal");

    WriteAheadLog wal1 = new WriteAheadLog(walPath);
    KeyValueStore store1 = new InMemoryKeyValueStore(wal1);

    // Realistic workload: puts, updates, deletes
    store1.put("active:1", "user1");
    store1.put("active:2", "user2");
    store1.put("active:3", "user3");
    store1.put("deleted:1", "temp1");
    store1.delete("deleted:1");
    store1.put("active:1", "user1_updated");
    store1.put("active:4", "user4");
    store1.delete("active:2");

    assertThat(store1.size()).isEqualTo(3); // active:1, active:3, active:4
    wal1.close();

    // Verify final state after recovery
    WriteAheadLog wal2 = new WriteAheadLog(walPath);
    KeyValueStore store2 = new InMemoryKeyValueStore(wal2);

    assertThat(store2.size()).isEqualTo(3);
    assertThat(store2.get("active:1")).hasValue("user1_updated");
    assertThat(store2.get("active:2")).isEmpty();
    assertThat(store2.get("active:3")).hasValue("user3");
    assertThat(store2.get("active:4")).hasValue("user4");
    assertThat(store2.get("deleted:1")).isEmpty();
    wal2.close();
  }

  @Test
  @DisplayName("Should handle special characters in keys and values")
  void testSpecialCharactersWithPersistence() throws Exception {
    Path walPath = tempDir.resolve("test.wal");

    WriteAheadLog wal1 = new WriteAheadLog(walPath);
    KeyValueStore store1 = new InMemoryKeyValueStore(wal1);

    // Test various special characters
    store1.put("key\twith\ttabs", "value\twith\ttabs");
    store1.put("key\nwith\nnewlines", "value\nwith\nnewlines");
    store1.put("key with spaces", "value with spaces");
    store1.put("key🎉emoji", "value🎉emoji");
    store1.put("key\"quotes\"", "value\"quotes\"");

    wal1.close();

    // Verify all special characters survived serialization
    WriteAheadLog wal2 = new WriteAheadLog(walPath);
    KeyValueStore store2 = new InMemoryKeyValueStore(wal2);

    assertThat(store2.get("key\twith\ttabs")).hasValue("value\twith\ttabs");
    assertThat(store2.get("key\nwith\nnewlines")).hasValue("value\nwith\nnewlines");
    assertThat(store2.get("key with spaces")).hasValue("value with spaces");
    assertThat(store2.get("key🎉emoji")).hasValue("value🎉emoji");
    assertThat(store2.get("key\"quotes\"")).hasValue("value\"quotes\"");
    wal2.close();
  }

  @Test
  @DisplayName("Should maintain consistency with rapid restarts")
  void testRapidRestartConsistency() throws Exception {
    Path walPath = tempDir.resolve("test.wal");

    // Simulate rapid application restarts with ongoing writes
    for (int restart = 0; restart < 20; restart++) {
      WriteAheadLog wal = new WriteAheadLog(walPath);
      KeyValueStore store = new InMemoryKeyValueStore(wal);

      // Do a few operations
      store.put("restart_count", String.valueOf(restart));
      store.put("temp:" + restart, "data");

      // Every 5 restarts, clean up temp data
      if (restart % 5 == 0 && restart > 0) {
        for (int i = restart - 5; i < restart; i++) {
          store.delete("temp:" + i);
        }
      }

      wal.close();
    }

    // Final verification
    WriteAheadLog finalWal = new WriteAheadLog(walPath);
    KeyValueStore finalStore = new InMemoryKeyValueStore(finalWal);

    assertThat(finalStore.get("restart_count")).hasValue("19");
    // Temp data from last 4 restarts should exist
    assertThat(finalStore.exists("temp:19")).isTrue();
    assertThat(finalStore.exists("temp:18")).isTrue();
    // Older temp data should be deleted
    assertThat(finalStore.exists("temp:14")).isFalse();

    finalWal.close();
  }

  @Test
  @DisplayName("Should handle empty values correctly")
  void testEmptyValuesWithPersistence() throws Exception {
    Path walPath = tempDir.resolve("test.wal");

    WriteAheadLog wal1 = new WriteAheadLog(walPath);
    KeyValueStore store1 = new InMemoryKeyValueStore(wal1);

    store1.put("empty", "");
    store1.put("normal", "value");

    wal1.close();

    WriteAheadLog wal2 = new WriteAheadLog(walPath);
    KeyValueStore store2 = new InMemoryKeyValueStore(wal2);

    assertThat(store2.get("empty")).hasValue("");
    assertThat(store2.get("normal")).hasValue("value");
    wal2.close();
  }
}
