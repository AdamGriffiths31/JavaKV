package com.javakv.cli;

import com.javakv.api.KeyValueStore;
import com.javakv.persistence.WriteAheadLog;
import com.javakv.store.InMemoryKeyValueStore;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Demo of JavaKV with Write-Ahead Log persistence.
 * Demonstrates crash-safe storage and recovery capabilities.
 */
public final class Main {
  private static final Path WAL_PATH = Paths.get("data", "javakv.wal");

  private Main() {
    // Utility class - prevent instantiation
  }

  /**
   * Main entry point for the JavaKV demo application.
   *
   * @param args command line arguments (not used)
   * @throws Exception if WAL operations fail
   */
  public static void main(String[] args) throws Exception {
    System.out.println("=== JavaKV Demo - With Persistence ===\n");

    // Part 1: Create store with WAL and perform operations
    System.out.println("PART 1: Writing data with WAL persistence");
    System.out.println("==========================================\n");

    WriteAheadLog wal = new WriteAheadLog(WAL_PATH);
    KeyValueStore store = new InMemoryKeyValueStore(wal);

    System.out.println("1. Storing some values...");
    store.put("user:1", "Alice");
    store.put("user:2", "Bob");
    store.put("user:3", "Charlie");
    System.out.println("   Stored 3 users (written to WAL)\n");

    System.out.println("2. Retrieving values...");
    System.out.println("   user:1 = " + store.get("user:1").orElse("NOT FOUND"));
    System.out.println("   user:2 = " + store.get("user:2").orElse("NOT FOUND"));
    System.out.println("   user:3 = " + store.get("user:3").orElse("NOT FOUND"));
    System.out.println();

    System.out.println("3. Current size: " + store.size() + " entries\n");

    System.out.println("4. Updating a value...");
    store.put("user:1", "Alice Smith");
    System.out.println("   user:1 = " + store.get("user:1").orElse("NOT FOUND"));
    System.out.println();

    System.out.println("5. Deleting a key...");
    boolean deleted = store.delete("user:2");
    System.out.println("   Deleted user:2? " + deleted);
    System.out.println("   Final size: " + store.size() + " entries\n");

    // Close the store (simulates graceful shutdown)
    System.out.println("6. Closing store (flushing WAL to disk)...\n");
    wal.close();

    // Part 2: Recovery - simulate restart
    System.out.println("\nPART 2: Simulating crash recovery");
    System.out.println("==========================================\n");

    System.out.println("Reopening store (simulating process restart)...");
    WriteAheadLog wal2 = new WriteAheadLog(WAL_PATH);
    KeyValueStore recoveredStore = new InMemoryKeyValueStore(wal2);

    System.out.println("✓ Store recovered from WAL!\n");

    System.out.println("Verifying recovered data:");
    System.out.println("   user:1 = " + recoveredStore.get("user:1").orElse("NOT FOUND"));
    System.out.println(
        "   user:2 = " + recoveredStore.get("user:2").orElse("NOT FOUND") + " (was deleted)");
    System.out.println("   user:3 = " + recoveredStore.get("user:3").orElse("NOT FOUND"));
    System.out.println("   Size: " + recoveredStore.size() + " entries\n");

    // Part 3: More operations on recovered store
    System.out.println("\nPART 3: Continuing operations after recovery");
    System.out.println("==========================================\n");

    System.out.println("Adding more data...");
    recoveredStore.put("user:4", "Diana");
    recoveredStore.put("user:5", "Eve");
    System.out.println("   Added users 4 and 5");
    System.out.println("   Final size: " + recoveredStore.size() + " entries\n");

    // Cleanup
    wal2.close();

    System.out.println("=== Demo Complete ===");
    System.out.println("\nAll data has been persisted to: " + WAL_PATH);
    System.out.println("The data will survive process restarts!");
    System.out.println("\nRun this demo again to see recovery in action.");
  }
}
