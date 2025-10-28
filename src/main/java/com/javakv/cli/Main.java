package com.javakv.cli;

import com.javakv.api.KeyValueStore;
import com.javakv.persistence.WriteAheadLog;
import com.javakv.store.InMemoryKeyValueStore;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Main entry point for the JavaKV interactive shell.
 * Initializes the key-value store with Write-Ahead Log persistence
 * and starts the interactive REPL interface.
 */
public final class Main {
  private static final String VERSION = "1.0-SNAPSHOT";

  // Allow WAL path to be configured via system property for testing
  private static final Path WAL_PATH = System.getProperty("wal.path") != null
      ? Paths.get(System.getProperty("wal.path"))
      : Paths.get("data", "javakv.wal");

  private static volatile WriteAheadLog wal;
  private static final AtomicBoolean shutdownCalled = new AtomicBoolean(false);
  private static Thread shutdownHook;

  private Main() {
    // Utility class - prevent instantiation
  }

  /**
   * Main entry point for the JavaKV interactive shell.
   *
   * @param args command line arguments (currently not used)
   */
  public static void main(String[] args) {
    try {
      // Register shutdown hook for graceful cleanup
      registerShutdownHook();

      // Initialize WAL and store with recovery
      long recoveryStartTime = System.nanoTime();
      wal = new WriteAheadLog(WAL_PATH);
      KeyValueStore store = new InMemoryKeyValueStore(wal);
      long recoveryTimeMs = (System.nanoTime() - recoveryStartTime) / 1_000_000;
      int recoveredEntries = store.size();

      // Display welcome banner with recovery statistics
      displayWelcomeBanner(recoveryTimeMs, recoveredEntries);

      // Start interactive REPL
      CommandLineInterface cli = new CommandLineInterface(store);
      cli.start();

      // Normal exit
      shutdown();

    } catch (Exception e) {
      System.err.println(AnsiColors.error("Fatal error during startup: " + e.getMessage()));
      e.printStackTrace();
      System.exit(1);
    }
  }

  /**
   * Displays the welcome banner with recovery statistics.
   *
   * @param recoveryTimeMs the recovery time in milliseconds
   * @param recoveredEntries the number of entries recovered
   */
  private static void displayWelcomeBanner(long recoveryTimeMs, int recoveredEntries) {
    System.out.println("\n" + AnsiColors.bold("=== JavaKV Interactive Shell ==="));
    System.out.println("Version: " + VERSION);
    System.out.println("WAL Path: " + WAL_PATH);

    if (recoveredEntries > 0) {
      String entriesWord = recoveredEntries == 1 ? "entry" : "entries";
      String checkmark = "\u2713";  // ✓
      System.out.println(
          AnsiColors.success(checkmark + " Recovered " + recoveredEntries + " " + entriesWord
                            + " in " + recoveryTimeMs + "ms"));
    } else {
      System.out.println(AnsiColors.info("Starting with empty store"));
    }

    System.out.println();
  }

  /**
   * Registers a JVM shutdown hook for graceful cleanup.
   * This ensures the WAL is properly flushed even on Ctrl+C or kill signals.
   */
  private static void registerShutdownHook() {
    shutdownHook = new Thread(() -> {
      shutdown();
    });
    Runtime.getRuntime().addShutdownHook(shutdownHook);
  }

  /**
   * Performs graceful shutdown: flushes WAL and closes resources.
   * Removes the shutdown hook to prevent double execution during normal exit.
   * Thread-safe using atomic compare-and-swap to prevent double execution.
   */
  private static void shutdown() {
    // Atomically check and set in one operation to prevent race conditions
    if (!shutdownCalled.compareAndSet(false, true)) {
      return;  // Already called by another thread
    }

    // Remove shutdown hook to prevent double execution on normal exit
    if (shutdownHook != null) {
      try {
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
      } catch (IllegalStateException e) {
        // Already shutting down - hook is running, this is expected
      }
    }

    if (wal != null) {
      try {
        System.out.println("\n" + AnsiColors.info("Shutting down... Flushing WAL..."));
        wal.close();
        System.out.println(AnsiColors.success("Goodbye!"));
      } catch (Exception e) {
        System.err.println(AnsiColors.error("Error during shutdown: " + e.getMessage()));
      }
    }
  }
}
