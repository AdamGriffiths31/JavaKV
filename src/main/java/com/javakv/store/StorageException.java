package com.javakv.store;

/**
 * Exception thrown when a storage or persistence operation fails.
 *
 * <p>This exception indicates failures in the persistence layer such as:
 * <ul>
 *   <li>I/O errors when writing to the Write-Ahead Log</li>
 *   <li>Corruption detected during recovery</li>
 *   <li>Filesystem errors</li>
 *   <li>Serialization/deserialization failures</li>
 * </ul>
 */
public class StorageException extends RuntimeException {

  /**
   * Constructs a new StorageException with the specified detail message.
   *
   * @param message the detail message
   */
  public StorageException(String message) {
    super(message);
  }

  /**
   * Constructs a new StorageException with the specified detail message and cause.
   *
   * @param message the detail message
   * @param cause the cause of the exception
   */
  public StorageException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a new StorageException with the specified cause.
   *
   * @param cause the cause of the exception
   */
  public StorageException(Throwable cause) {
    super(cause);
  }
}
