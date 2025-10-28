package com.javakv.cli;

import com.javakv.api.KeyValueStore;
import com.javakv.store.InMemoryKeyValueStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for CommandLineInterface.
 * Tests command parsing and execution logic without the full REPL.
 */
@DisplayName("CommandLineInterface Unit Tests")
class CommandLineInterfaceTest {

  private KeyValueStore store;
  private CommandLineInterface cli;
  private ByteArrayOutputStream outputStream;
  private PrintStream originalOut;

  @BeforeEach
  void setUp() {
    // Create in-memory store without persistence
    store = new InMemoryKeyValueStore(null);
    cli = new CommandLineInterface(store);

    // Capture System.out
    outputStream = new ByteArrayOutputStream();
    originalOut = System.out;
    System.setOut(new PrintStream(outputStream));
  }

  @Test
  @DisplayName("Should parse and execute PUT command")
  void testPutCommand() {
    cli.executeCommand("PUT test value");

    assertThat(store.get("test")).hasValue("value");
    assertThat(getOutput()).contains("Stored 'test'");
  }

  @Test
  @DisplayName("Should parse and execute PUT with multi-word value")
  void testPutCommandWithSpaces() {
    cli.executeCommand("PUT user:1 Alice Johnson");

    assertThat(store.get("user:1")).hasValue("Alice Johnson");
    assertThat(getOutput()).contains("Stored 'user:1'");
  }

  @Test
  @DisplayName("Should parse and execute GET command")
  void testGetCommand() {
    store.put("existing", "value");

    cli.executeCommand("GET existing");

    assertThat(getOutput()).contains("existing = value");
  }

  @Test
  @DisplayName("Should handle GET for non-existent key")
  void testGetNonExistentKey() {
    cli.executeCommand("GET nonexistent");

    assertThat(getOutput()).contains("Key not found: nonexistent");
  }

  @Test
  @DisplayName("Should parse and execute DELETE command")
  void testDeleteCommand() {
    store.put("temp", "value");

    cli.executeCommand("DELETE temp");

    assertThat(store.get("temp")).isEmpty();
    assertThat(getOutput()).contains("Deleted 'temp'");
  }

  @Test
  @DisplayName("Should handle DELETE for non-existent key")
  void testDeleteNonExistentKey() {
    cli.executeCommand("DELETE nonexistent");

    assertThat(getOutput()).contains("Key not found: nonexistent");
  }

  @Test
  @DisplayName("Should parse and execute EXISTS command")
  void testExistsCommand() {
    store.put("existing", "value");

    cli.executeCommand("EXISTS existing");

    assertThat(getOutput()).contains("Key 'existing' exists");
  }

  @Test
  @DisplayName("Should handle EXISTS for non-existent key")
  void testExistsNonExistentKey() {
    cli.executeCommand("EXISTS nonexistent");

    assertThat(getOutput()).contains("Key 'nonexistent' does not exist");
  }

  @Test
  @DisplayName("Should parse and execute SIZE command")
  void testSizeCommand() {
    store.put("key1", "value1");
    store.put("key2", "value2");

    cli.executeCommand("SIZE");

    assertThat(getOutput()).contains("Store contains 2 entries");
  }

  @Test
  @DisplayName("Should handle empty store SIZE")
  void testSizeEmptyStore() {
    cli.executeCommand("SIZE");

    assertThat(getOutput()).contains("Store contains 0 entries");
  }

  @Test
  @DisplayName("Should be case-insensitive for commands")
  void testCaseInsensitiveCommands() {
    cli.executeCommand("put lower value");
    cli.executeCommand("PUT upper VALUE");
    cli.executeCommand("Put mixed MiXeD");

    assertThat(store.size()).isEqualTo(3);
    assertThat(store.get("lower")).hasValue("value");
    assertThat(store.get("upper")).hasValue("VALUE");
    assertThat(store.get("mixed")).hasValue("MiXeD");
  }

  @Test
  @DisplayName("Should handle unknown commands")
  void testUnknownCommand() {
    cli.executeCommand("INVALID");

    assertThat(getOutput()).contains("Unknown command: INVALID");
  }

  @Test
  @DisplayName("Should handle missing arguments for PUT")
  void testPutMissingArguments() {
    cli.executeCommand("PUT");
    assertThat(getOutput()).contains("Usage: PUT <key> <value>");

    outputStream.reset();
    cli.executeCommand("PUT onlykey");
    assertThat(getOutput()).contains("Usage: PUT <key> <value>");
  }

  @Test
  @DisplayName("Should handle missing arguments for GET")
  void testGetMissingArguments() {
    cli.executeCommand("GET");

    assertThat(getOutput()).contains("Usage: GET <key>");
  }

  @Test
  @DisplayName("Should handle missing arguments for DELETE")
  void testDeleteMissingArguments() {
    cli.executeCommand("DELETE");

    assertThat(getOutput()).contains("Usage: DELETE <key>");
  }

  @Test
  @DisplayName("Should handle missing arguments for EXISTS")
  void testExistsMissingArguments() {
    cli.executeCommand("EXISTS");

    assertThat(getOutput()).contains("Usage: EXISTS <key>");
  }

  @Test
  @DisplayName("Should show timing information")
  void testTimingDisplay() {
    cli.executeCommand("PUT test value");

    // Should contain timing in format "(Xms)"
    assertThat(getOutput()).containsPattern("\\(\\d+ms\\)");
  }

  @Test
  @DisplayName("Should reject keys that are too long")
  void testPutKeyTooLong() {
    String longKey = "a".repeat(2000);  // Exceeds MAX_KEY_LENGTH of 1024
    cli.executeCommand("PUT " + longKey + " value");

    assertThat(getOutput()).contains("Key too long");
    assertThat(store.size()).isEqualTo(0);
  }

  @Test
  @DisplayName("Should reject values that are too long")
  void testPutValueTooLong() {
    String longValue = "a".repeat(2_000_000);  // Exceeds MAX_VALUE_LENGTH of 1MB
    cli.executeCommand("PUT key " + longValue);

    assertThat(getOutput()).contains("Value too long");
    assertThat(store.size()).isEqualTo(0);
  }

  @Test
  @DisplayName("Should handle CLEAR on empty store")
  void testClearEmptyStore() {
    cli.executeCommand("CLEAR");

    assertThat(getOutput()).contains("Store is already empty");
  }

  @Test
  @DisplayName("Should reject CLEAR in non-interactive mode")
  void testClearNonInteractiveMode() {
    store.put("key1", "value1");
    store.put("key2", "value2");

    cli.executeCommand("CLEAR");

    // Should refuse to clear and show error message
    assertThat(getOutput()).contains("Cannot prompt for confirmation in non-interactive mode");
    assertThat(getOutput()).contains("Clear operation cancelled");
    // Store should still have entries
    assertThat(store.size()).isEqualTo(2);
  }

  @Test
  @DisplayName("Should handle blank lines")
  void testBlankLines() {
    cli.executeCommand("");
    cli.executeCommand("   ");

    // Should not crash, and store should be empty
    assertThat(store.size()).isEqualTo(0);
  }

  private String getOutput() {
    System.out.flush();
    return outputStream.toString();
  }

  @org.junit.jupiter.api.AfterEach
  void tearDown() {
    System.setOut(originalOut);
  }
}
