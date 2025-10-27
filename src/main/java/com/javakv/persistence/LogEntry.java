package com.javakv.persistence;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Immutable record representing a single operation in the Write-Ahead Log (WAL).
 *
 * <p>This record uses Java 21's record feature to create a compact, immutable
 * data carrier for log entries. Each entry captures an operation performed on
 * the key-value store along with metadata.
 *
 * <p>The binary format uses length-prefixed encoding:
 * <pre>
 * [OperationType:1B][Timestamp:8B][KeyLen:4B][Key][ValueLen:4B][Value]
 * </pre>
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
        // Allow empty string values, but not null
        if (value == null) {
          throw new IllegalArgumentException(
              "Value cannot be null for PUT operation (empty string is allowed)");
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
      default -> throw new IllegalStateException("Unknown operation type: " + operation);
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

  /**
   * Serializes this log entry to a binary format using length-prefixed encoding.
   *
   * <p>Format: [OperationType:1B][Timestamp:8B][KeyLen:4B][Key][ValueLen:4B][Value]
   *
   * @param out the output stream to write to
   * @throws IOException if an I/O error occurs
   */
  public void writeTo(DataOutputStream out) throws IOException {
    // Write operation type (1 byte)
    out.writeByte(operation.ordinal());

    // Write timestamp (8 bytes)
    out.writeLong(timestamp.toEpochMilli());

    // Write key with length prefix
    if (key == null) {
      out.writeInt(0);
    } else {
      byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
      out.writeInt(keyBytes.length);
      out.write(keyBytes);
    }

    // Write value with length prefix
    if (value == null) {
      out.writeInt(0);
    } else {
      byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
      out.writeInt(valueBytes.length);
      out.write(valueBytes);
    }
  }

  /**
   * Deserializes a log entry from a binary format.
   *
   * @param in the input stream to read from
   * @return the deserialized LogEntry
   * @throws IOException if an I/O error occurs or the data is corrupted
   */
  public static LogEntry readFrom(DataInputStream in) throws IOException {
    // Read operation type (1 byte)
    int opOrdinal = in.readByte();
    if (opOrdinal < 0 || opOrdinal >= OperationType.values().length) {
      throw new IOException("Invalid operation type: " + opOrdinal);
    }
    OperationType operation = OperationType.values()[opOrdinal];

    // Read timestamp (8 bytes)
    long epochMilli = in.readLong();
    Instant timestamp = Instant.ofEpochMilli(epochMilli);

    // Read key with length prefix
    int keyLength = in.readInt();
    String key;
    if (keyLength > 0) {
      byte[] keyBytes = new byte[keyLength];
      in.readFully(keyBytes);
      key = new String(keyBytes, StandardCharsets.UTF_8);
    } else if (keyLength == 0 && operation == OperationType.PUT) {
      // For PUT operations, length 0 means empty string, not null
      key = "";
    } else {
      // For DELETE/CLEAR, length 0 means null
      key = null;
    }

    // Read value with length prefix
    int valueLength = in.readInt();
    String value;
    if (valueLength > 0) {
      byte[] valueBytes = new byte[valueLength];
      in.readFully(valueBytes);
      value = new String(valueBytes, StandardCharsets.UTF_8);
    } else if (valueLength == 0 && operation == OperationType.PUT) {
      // For PUT operations, length 0 means empty string, not null
      value = "";
    } else {
      // For DELETE/CLEAR, length 0 means null
      value = null;
    }

    return new LogEntry(operation, key, value, timestamp);
  }
}
