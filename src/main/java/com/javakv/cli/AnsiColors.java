package com.javakv.cli;

/**
 * Utility class for ANSI terminal color codes.
 * Provides methods to colorize output strings for better readability in terminals.
 * Automatically detects if the terminal supports colors and disables them if not.
 *
 * <p>Example usage:
 * <pre>{@code
 * System.out.println(AnsiColors.success("Operation completed!"));
 * System.out.println(AnsiColors.error("Something went wrong"));
 * System.out.println(AnsiColors.info("Processing..."));
 * }</pre>
 */
public final class AnsiColors {
  // ANSI color codes
  private static final String RESET = "\u001B[0m";
  private static final String GREEN = "\u001B[32m";
  private static final String RED = "\u001B[31m";
  private static final String YELLOW = "\u001B[33m";
  private static final String BLUE = "\u001B[34m";
  private static final String CYAN = "\u001B[36m";
  private static final String BOLD = "\u001B[1m";

  // Check if colors are supported (console is available)
  private static final boolean COLORS_ENABLED = System.console() != null;

  private AnsiColors() {
    // Utility class - prevent instantiation
  }

  /**
   * Formats a success message in green.
   *
   * @param message the message to format
   * @return the formatted message with green color, or plain message if colors are disabled
   */
  public static String success(String message) {
    return colorize(message, GREEN);
  }

  /**
   * Formats an error message in red.
   *
   * @param message the message to format
   * @return the formatted message with red color, or plain message if colors are disabled
   */
  public static String error(String message) {
    return colorize(message, RED);
  }

  /**
   * Formats a warning message in yellow.
   *
   * @param message the message to format
   * @return the formatted message with yellow color, or plain message if colors are disabled
   */
  public static String warning(String message) {
    return colorize(message, YELLOW);
  }

  /**
   * Formats an informational message in blue.
   *
   * @param message the message to format
   * @return the formatted message with blue color, or plain message if colors are disabled
   */
  public static String info(String message) {
    return colorize(message, BLUE);
  }

  /**
   * Formats a value or data string in cyan.
   *
   * @param message the message to format
   * @return the formatted message with cyan color, or plain message if colors are disabled
   */
  public static String value(String message) {
    return colorize(message, CYAN);
  }

  /**
   * Formats a header or title in bold.
   *
   * @param message the message to format
   * @return the formatted message in bold, or plain message if colors are disabled
   */
  public static String bold(String message) {
    return colorize(message, BOLD);
  }

  /**
   * Applies the specified ANSI color code to a message.
   *
   * @param message the message to colorize
   * @param colorCode the ANSI color code to apply
   * @return the colorized message, or plain message if colors are disabled
   */
  private static String colorize(String message, String colorCode) {
    if (!COLORS_ENABLED) {
      return message;
    }
    return colorCode + message + RESET;
  }

  /**
   * Checks if terminal colors are enabled.
   *
   * @return true if colors are enabled, false otherwise
   */
  public static boolean isColorsEnabled() {
    return COLORS_ENABLED;
  }
}
