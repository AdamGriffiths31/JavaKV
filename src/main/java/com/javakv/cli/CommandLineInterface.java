package com.javakv.cli;

import com.javakv.api.KeyValueStore;
import java.util.Optional;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

/**
 * Interactive command-line interface (REPL) for JavaKV.
 * Provides a user-friendly shell for interacting with the key-value store.
 *
 * <p>Supported commands:
 * <ul>
 *   <li>PUT key value - Store a key-value pair</li>
 *   <li>GET key - Retrieve a value</li>
 *   <li>DELETE key - Remove a key</li>
 *   <li>EXISTS key - Check if a key exists</li>
 *   <li>SIZE - Show the number of entries</li>
 *   <li>CLEAR - Remove all entries</li>
 *   <li>HELP - Display command reference</li>
 *   <li>EXIT/QUIT - Shutdown gracefully</li>
 * </ul>
 *
 * <p>Features:
 * <ul>
 *   <li>Colorized output for better readability</li>
 *   <li>Operation timing for performance monitoring</li>
 *   <li>Command history (up/down arrows)</li>
 *   <li>Line editing support</li>
 *   <li>Case-insensitive commands</li>
 * </ul>
 */
public final class CommandLineInterface {
  private static final String PROMPT = "> ";
  private static final String CHECKMARK = "\u2713";  // ✓
  private static final String CROSSMARK = "\u2717";  // ✗

  // Input length limits to prevent memory exhaustion
  private static final int MAX_KEY_LENGTH = 1024;  // 1KB
  private static final int MAX_VALUE_LENGTH = 1_048_576;  // 1MB

  private final KeyValueStore store;
  private LineReader reader;

  /**
   * Creates a new command-line interface for the specified key-value store.
   *
   * @param store the key-value store to interact with
   */
  public CommandLineInterface(KeyValueStore store) {
    this.store = store;
  }

  /**
   * Starts the interactive REPL (Read-Eval-Print Loop).
   * This method blocks until the user exits the shell.
   */
  public void start() {
    try (Terminal terminal = TerminalBuilder.builder().system(true).build()) {
      this.reader = LineReaderBuilder.builder()
          .terminal(terminal)
          .build();

      System.out.println(AnsiColors.info("Type 'HELP' for commands, 'EXIT' to quit\n"));

      // Main REPL loop
      while (true) {
        try {
          String line = reader.readLine(PROMPT);
          if (line == null) {
            continue;
          }

          String trimmedLine = line.trim();
          if (shouldExit(trimmedLine)) {
            break;
          }

          parseAndExecute(trimmedLine);

        } catch (UserInterruptException e) {
          // User pressed Ctrl+C
          System.out.println("\n" + AnsiColors.info("Use 'EXIT' to quit"));
        }
      }
    } catch (Exception e) {
      System.err.println(AnsiColors.error("Fatal error in CLI: " + e.getMessage()));
      e.printStackTrace();
    }
  }

  /**
   * Executes a command string (public for testing).
   *
   * @param line the command line to parse and execute
   */
  public void executeCommand(String line) {
    parseAndExecute(line);
  }

  /**
   * Parses and executes a command line input.
   *
   * @param line the command line to parse and execute
   */
  private void parseAndExecute(String line) {
    // Skip blank lines
    if (line == null || line.isBlank()) {
      return;
    }

    long startTime = System.nanoTime();

    try {
      // Split command and arguments
      String trimmedLine = line.trim();
      String[] parts = trimmedLine.split("\\s+", 2);
      String command = parts[0].toUpperCase();
      String args = parts.length > 1 ? parts[1] : "";

      // Route to appropriate command handler using switch expression
      switch (command) {
        case "PUT" -> executePut(args, startTime);
        case "GET" -> executeGet(args, startTime);
        case "DELETE" -> executeDelete(args, startTime);
        case "EXISTS" -> executeExists(args, startTime);
        case "SIZE" -> executeSize(startTime);
        case "CLEAR" -> executeClear(startTime);
        case "HELP" -> displayHelp();
        default -> System.out.println(
            AnsiColors.error("Unknown command: " + command) + "\n"
            + "Type " + AnsiColors.bold("HELP") + " for available commands");
      }
    } catch (Exception e) {
      System.out.println(AnsiColors.error(CROSSMARK + " Error: " + e.getMessage()));
    }
  }

  /**
   * Executes the PUT command to store a key-value pair.
   *
   * @param args the arguments (key and value)
   * @param startTime the start time in nanoseconds
   */
  private void executePut(String args, long startTime) {
    String[] parts = args.split("\\s+", 2);
    if (parts.length < 2) {
      System.out.println(AnsiColors.error("Usage: PUT <key> <value>"));
      return;
    }

    String key = parts[0];
    String value = parts[1];

    if (key.isBlank()) {
      System.out.println(AnsiColors.error("Key cannot be empty or whitespace"));
      return;
    }

    if (key.length() > MAX_KEY_LENGTH) {
      System.out.println(AnsiColors.error(
          "Key too long (max " + MAX_KEY_LENGTH + " characters, got " + key.length() + ")"));
      return;
    }

    if (value.length() > MAX_VALUE_LENGTH) {
      System.out.println(AnsiColors.error(
          "Value too long (max " + MAX_VALUE_LENGTH + " characters, got " + value.length() + ")"));
      return;
    }

    store.put(key, value);
    long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;
    System.out.println(
        AnsiColors.success(CHECKMARK + " Stored '" + key + "'")
        + AnsiColors.info(" (" + elapsedMs + "ms)"));
  }

  /**
   * Executes the GET command to retrieve a value.
   *
   * @param args the arguments (key)
   * @param startTime the start time in nanoseconds
   */
  private void executeGet(String args, long startTime) {
    if (args.isBlank()) {
      System.out.println(AnsiColors.error("Usage: GET <key>"));
      return;
    }

    String key = args.trim();

    if (key.isBlank()) {
      System.out.println(AnsiColors.error("Key cannot be empty or whitespace"));
      return;
    }

    if (key.length() > MAX_KEY_LENGTH) {
      System.out.println(AnsiColors.error(
          "Key too long (max " + MAX_KEY_LENGTH + " characters, got " + key.length() + ")"));
      return;
    }

    Optional<String> result = store.get(key);
    long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;

    if (result.isPresent()) {
      System.out.println(
          key + " = " + AnsiColors.value(result.get())
          + AnsiColors.info(" (" + elapsedMs + "ms)"));
    } else {
      System.out.println(
          AnsiColors.error(CROSSMARK + " Key not found: " + key)
          + AnsiColors.info(" (" + elapsedMs + "ms)"));
    }
  }

  /**
   * Executes the DELETE command to remove a key.
   *
   * @param args the arguments (key)
   * @param startTime the start time in nanoseconds
   */
  private void executeDelete(String args, long startTime) {
    if (args.isBlank()) {
      System.out.println(AnsiColors.error("Usage: DELETE <key>"));
      return;
    }

    String key = args.trim();

    if (key.isBlank()) {
      System.out.println(AnsiColors.error("Key cannot be empty or whitespace"));
      return;
    }

    if (key.length() > MAX_KEY_LENGTH) {
      System.out.println(AnsiColors.error(
          "Key too long (max " + MAX_KEY_LENGTH + " characters, got " + key.length() + ")"));
      return;
    }

    boolean deleted = store.delete(key);
    long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;

    if (deleted) {
      System.out.println(
          AnsiColors.success(CHECKMARK + " Deleted '" + key + "'")
          + AnsiColors.info(" (" + elapsedMs + "ms)"));
    } else {
      System.out.println(
          AnsiColors.error(CROSSMARK + " Key not found: " + key)
          + AnsiColors.info(" (" + elapsedMs + "ms)"));
    }
  }

  /**
   * Executes the EXISTS command to check if a key exists.
   *
   * @param args the arguments (key)
   * @param startTime the start time in nanoseconds
   */
  private void executeExists(String args, long startTime) {
    if (args.isBlank()) {
      System.out.println(AnsiColors.error("Usage: EXISTS <key>"));
      return;
    }

    String key = args.trim();

    if (key.isBlank()) {
      System.out.println(AnsiColors.error("Key cannot be empty or whitespace"));
      return;
    }

    if (key.length() > MAX_KEY_LENGTH) {
      System.out.println(AnsiColors.error(
          "Key too long (max " + MAX_KEY_LENGTH + " characters, got " + key.length() + ")"));
      return;
    }

    boolean exists = store.exists(key);
    long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;

    if (exists) {
      System.out.println(
          AnsiColors.success(CHECKMARK + " Key '" + key + "' exists")
          + AnsiColors.info(" (" + elapsedMs + "ms)"));
    } else {
      System.out.println(
          AnsiColors.warning("Key '" + key + "' does not exist")
          + AnsiColors.info(" (" + elapsedMs + "ms)"));
    }
  }

  /**
   * Executes the SIZE command to show the number of entries.
   *
   * @param startTime the start time in nanoseconds
   */
  private void executeSize(long startTime) {
    int size = store.size();
    long elapsedMs = (System.nanoTime() - startTime) / 1_000_000;

    String entriesWord = size == 1 ? "entry" : "entries";
    System.out.println(
        "Store contains " + AnsiColors.value(String.valueOf(size)) + " " + entriesWord
        + AnsiColors.info(" (" + elapsedMs + "ms)"));
  }

  /**
   * Executes the CLEAR command to remove all entries.
   * Prompts for confirmation before clearing.
   *
   * @param startTime the start time in nanoseconds
   */
  private void executeClear(long startTime) {
    int currentSize = store.size();
    if (currentSize == 0) {
      System.out.println(AnsiColors.info("Store is already empty"));
      return;
    }

    // Show warning and confirmation
    System.out.println(
        AnsiColors.warning("This will delete all " + currentSize + " entries!"));
    System.out.print("Are you sure? (yes/no): ");

    try {
      String confirmation;
      if (reader != null) {
        // Use JLine reader for consistent UX with history and line editing
        confirmation = reader.readLine();
      } else {
        // Non-interactive mode - cannot prompt for confirmation
        System.out.println(
            AnsiColors.error("Cannot prompt for confirmation in non-interactive mode"));
        System.out.println(AnsiColors.info("Clear operation cancelled"));
        return;
      }

      if (confirmation != null && confirmation.trim().equalsIgnoreCase("yes")) {
        long clearStartTime = System.nanoTime();
        store.clear();
        long elapsedMs = (System.nanoTime() - clearStartTime) / 1_000_000;
        System.out.println(
            AnsiColors.success(CHECKMARK + " Cleared all entries")
            + AnsiColors.info(" (" + elapsedMs + "ms)"));
      } else {
        System.out.println(AnsiColors.info("Clear operation cancelled"));
      }
    } catch (Exception e) {
      System.out.println(AnsiColors.error("Error reading confirmation: " + e.getMessage()));
    }
  }

  /**
   * Displays the help text with all available commands.
   */
  private void displayHelp() {
    String helpText = """

        %s

        Available Commands:

          %s - Store a key-value pair
            Example: PUT user:1 Alice Johnson

          %s - Retrieve a value for a key
            Example: GET user:1

          %s - Remove a key-value pair
            Example: DELETE user:1

          %s - Check if a key exists
            Example: EXISTS user:1

          %s - Display the number of entries in the store
            Example: SIZE

          %s - Remove all entries from the store (prompts for confirmation)
            Example: CLEAR

          %s - Display this help message
            Example: HELP

          %s - Exit the interactive shell
            Example: EXIT

        Notes:
          - Commands are case-insensitive
          - Values can contain spaces (everything after the key is the value)
          - Use Ctrl+C to interrupt, or EXIT to quit gracefully

        """.formatted(
            AnsiColors.bold("JavaKV Command Reference"),
            AnsiColors.success("PUT <key> <value>"),
            AnsiColors.success("GET <key>"),
            AnsiColors.success("DELETE <key>"),
            AnsiColors.success("EXISTS <key>"),
            AnsiColors.success("SIZE"),
            AnsiColors.warning("CLEAR"),
            AnsiColors.info("HELP"),
            AnsiColors.info("EXIT | QUIT")
        );

    System.out.println(helpText);
  }

  /**
   * Checks if the command is an exit command.
   *
   * @param command the command to check
   * @return true if the command is EXIT or QUIT, false otherwise
   */
  private boolean shouldExit(String command) {
    String upperCommand = command.toUpperCase();
    return upperCommand.equals("EXIT") || upperCommand.equals("QUIT");
  }
}
