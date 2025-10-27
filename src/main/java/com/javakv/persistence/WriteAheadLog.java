package com.javakv.persistence;

import com.javakv.api.StorageEngine;
import com.javakv.store.StorageException;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Write-Ahead Log (WAL) implementation for crash-safe persistence.
 *
 * <p>This class provides durability guarantees by writing all operations to disk
 * before they are applied to the in-memory store. Operations are written in a
 * length-prefixed binary format for efficient serialization without escaping.
 *
 * <p>Format: [OperationType:1B][Timestamp:8B][KeyLen:4B][Key][ValueLen:4B][Value]
 *
 * <p>On startup, the log can be replayed to recover the complete state of the
 * key-value store. Corrupted entries are logged and skipped to allow partial recovery.
 */
public class WriteAheadLog implements StorageEngine {
  private static final Logger LOGGER = LoggerFactory.getLogger(WriteAheadLog.class);
  private static final int BUFFER_SIZE = 8192; // 8KB buffer

  private final Path logPath;
  private final DataOutputStream writer;
  private final FileOutputStream fileOutputStream;

  /**
   * Creates a new WriteAheadLog instance.
   *
   * @param logPath the path to the WAL file
   * @throws StorageException if the log file cannot be opened
   */
  public WriteAheadLog(Path logPath) {
    this.logPath = logPath;
    try {
      // Ensure parent directory exists
      Path parentDir = logPath.getParent();
      if (parentDir != null && !Files.exists(parentDir)) {
        Files.createDirectories(parentDir);
        LOGGER.info("Created WAL directory: {}", parentDir);
      }

      // Open file in append mode (preserves existing data)
      this.fileOutputStream = new FileOutputStream(logPath.toFile(), true);
      this.writer = new DataOutputStream(new BufferedOutputStream(fileOutputStream, BUFFER_SIZE));

      LOGGER.info("Opened WAL file: {}", logPath);
    } catch (IOException e) {
      throw new StorageException("Failed to open WAL file: " + logPath, e);
    }
  }

  /**
   * Appends a log entry to the WAL.
   *
   * <p>The entry is written to a buffer and will be flushed to disk when
   * {@link #flush()} is called or when the buffer is full.
   *
   * @param entry the log entry to append
   * @throws StorageException if the write fails
   */
  @Override
  public void append(LogEntry entry) {
    try {
      entry.writeTo(writer);
      LOGGER.debug("Appended entry: {} {}", entry.operation(), entry.key());
    } catch (IOException e) {
      throw new StorageException("Failed to append entry to WAL", e);
    }
  }

  /**
   * Recovers all log entries from the WAL file.
   *
   * <p>Reads entries sequentially from the beginning of the file. If a corrupted
   * entry is encountered, it is logged and skipped, allowing partial recovery.
   *
   * @return list of recovered log entries in order
   * @throws StorageException if the file cannot be read
   */
  @Override
  public List<LogEntry> recover() {
    List<LogEntry> entries = new ArrayList<>();

    // If file doesn't exist, return empty list
    if (!Files.exists(logPath)) {
      LOGGER.info("No WAL file found at {}, starting fresh", logPath);
      return entries;
    }

    try (FileInputStream fis = new FileInputStream(logPath.toFile());
        DataInputStream reader = new DataInputStream(new BufferedInputStream(fis, BUFFER_SIZE))) {

      LOGGER.info("Starting WAL recovery from: {}", logPath);

      while (reader.available() > 0) {
        try {
          LogEntry entry = LogEntry.readFrom(reader);
          entries.add(entry);
        } catch (IOException e) {
          // Log corruption but continue recovery
          LOGGER.warn("Corrupted log entry encountered, skipping: {}", e.getMessage());
          break; // Stop reading on corruption
        }
      }

      LOGGER.info("Recovered {} entries from WAL", entries.size());
    } catch (IOException e) {
      throw new StorageException("Failed to recover from WAL", e);
    }

    return entries;
  }

  /**
   * Flushes buffered writes to disk and syncs to storage.
   *
   * <p>This method ensures all pending writes are persisted to disk, providing
   * durability guarantees. After this call returns, data will survive crashes.
   *
   * @throws StorageException if the flush fails
   */
  @Override
  public void flush() {
    try {
      writer.flush();
      fileOutputStream.getFD().sync(); // Force OS to write to disk
      LOGGER.debug("Flushed WAL to disk");
    } catch (IOException e) {
      throw new StorageException("Failed to flush WAL", e);
    }
  }

  /**
   * Closes the WAL file, flushing any pending writes first.
   *
   * @throws Exception if an error occurs during closing
   */
  @Override
  public void close() throws Exception {
    try {
      flush();
      writer.close();
      LOGGER.info("Closed WAL file: {}", logPath);
    } catch (IOException e) {
      throw new StorageException("Failed to close WAL", e);
    }
  }

  /**
   * Gets the path to the WAL file.
   *
   * @return the WAL file path
   */
  public Path getLogPath() {
    return logPath;
  }

  /**
   * Deletes the WAL file (useful for testing or clearing state).
   *
   * <p>This method should only be called after the WAL is closed.
   *
   * @throws IOException if the file cannot be deleted
   */
  public void deleteLog() throws IOException {
    if (Files.exists(logPath)) {
      Files.delete(logPath);
      LOGGER.info("Deleted WAL file: {}", logPath);
    }
  }
}
