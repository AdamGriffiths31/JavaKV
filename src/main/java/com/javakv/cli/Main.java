package com.javakv.cli;

import com.javakv.api.KeyValueStore;
import com.javakv.store.InMemoryKeyValueStore;

/**
 * Simple demo of JavaKV in-memory store.
 * Full CLI will be implemented in Task 1.5.
 */
public class Main {
    public static void main(String[] args) {
        System.out.println("=== JavaKV Demo ===\n");

        // Create in-memory store (no persistence for now)
        KeyValueStore store = new InMemoryKeyValueStore();

        // Demo basic operations
        System.out.println("1. Storing some values...");
        store.put("user:1", "Alice");
        store.put("user:2", "Bob");
        store.put("user:3", "Charlie");
        System.out.println("   Stored 3 users\n");

        System.out.println("2. Retrieving values...");
        System.out.println("   user:1 = " + store.get("user:1").orElse("NOT FOUND"));
        System.out.println("   user:2 = " + store.get("user:2").orElse("NOT FOUND"));
        System.out.println("   user:3 = " + store.get("user:3").orElse("NOT FOUND"));
        System.out.println();

        System.out.println("3. Current size: " + store.size() + " entries\n");

        System.out.println("4. Checking existence...");
        System.out.println("   user:1 exists? " + store.exists("user:1"));
        System.out.println("   user:999 exists? " + store.exists("user:999"));
        System.out.println();

        System.out.println("5. Updating a value...");
        store.put("user:1", "Alice Smith");
        System.out.println("   user:1 = " + store.get("user:1").orElse("NOT FOUND"));
        System.out.println();

        System.out.println("6. Deleting a key...");
        boolean deleted = store.delete("user:2");
        System.out.println("   Deleted user:2? " + deleted);
        System.out.println("   user:2 = " + store.get("user:2").orElse("NOT FOUND"));
        System.out.println();

        System.out.println("7. Current size: " + store.size() + " entries\n");

        System.out.println("8. Clearing all entries...");
        store.clear();
        System.out.println("   Size after clear: " + store.size());
        System.out.println();

        System.out.println("=== Demo Complete ===");
        System.out.println("\nNote: This is in-memory only. Persistence (WAL) coming in Task 1.4!");
    }
}
