package com.javakv.store;

/**
 * Exception thrown when a requested key is not found in the store.
 *
 * <p>This is a runtime exception that indicates an attempt to access or delete
 * a key that does not exist in the key-value store.
 */
public class KeyNotFoundException extends RuntimeException {

  /**
   * Constructs a new KeyNotFoundException with the specified key.
   *
   * @param key the key that was not found
   */
  public KeyNotFoundException(String key) {
    super("Key not found: " + key);
  }

  /**
   * Constructs a new KeyNotFoundException with a custom message.
   *
   * @param message the detail message
   */
  public KeyNotFoundException(String message, Throwable cause) {
    super(message, cause);
  }
}
