package com.javakv.persistence;

import com.javakv.store.StorageException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

/**
 * Test suite for WriteAheadLog implementation.
 */
class WriteAheadLogTest {

  @TempDir
  Path tempDir;

  private Path walPath;
  private WriteAheadLog wal;

  @BeforeEach
  void setUp() {
    walPath = tempDir.resolve("test.wal");
  }

  @AfterEach
  void tearDown() throws Exception {
    if (wal != null) {
      try {
        wal.close();
      } catch (Exception e) {
        // Ignore close errors in cleanup
      }
    }
  }

  @Test
  void testAppendAndRecoverSinglePutEntry() throws Exception {
    // Write an entry
    wal = new WriteAheadLog(walPath);
    LogEntry putEntry = LogEntry.put("key1", "value1");
    wal.append(putEntry);
    wal.flush();
    wal.close();

    // Recover and verify
    wal = new WriteAheadLog(walPath);
    List<LogEntry> recovered = wal.recover();

    assertThat(recovered).hasSize(1);
    LogEntry recoveredEntry = recovered.get(0);
    assertThat(recoveredEntry.operation()).isEqualTo(OperationType.PUT);
    assertThat(recoveredEntry.key()).isEqualTo("key1");
    assertThat(recoveredEntry.value()).isEqualTo("value1");
  }

  @Test
  void testAppendAndRecoverMultipleEntries() throws Exception {
    // Write multiple entries
    wal = new WriteAheadLog(walPath);
    wal.append(LogEntry.put("key1", "value1"));
    wal.append(LogEntry.put("key2", "value2"));
    wal.append(LogEntry.delete("key1"));
    wal.append(LogEntry.clear());
    wal.flush();
    wal.close();

    // Recover and verify
    wal = new WriteAheadLog(walPath);
    List<LogEntry> recovered = wal.recover();

    assertThat(recovered).hasSize(4);
    assertThat(recovered.get(0).operation()).isEqualTo(OperationType.PUT);
    assertThat(recovered.get(0).key()).isEqualTo("key1");
    assertThat(recovered.get(1).operation()).isEqualTo(OperationType.PUT);
    assertThat(recovered.get(1).key()).isEqualTo("key2");
    assertThat(recovered.get(2).operation()).isEqualTo(OperationType.DELETE);
    assertThat(recovered.get(2).key()).isEqualTo("key1");
    assertThat(recovered.get(3).operation()).isEqualTo(OperationType.CLEAR);
  }

  @Test
  void testDeleteOperation() throws Exception {
    wal = new WriteAheadLog(walPath);
    LogEntry deleteEntry = LogEntry.delete("keyToDelete");
    wal.append(deleteEntry);
    wal.flush();
    wal.close();

    wal = new WriteAheadLog(walPath);
    List<LogEntry> recovered = wal.recover();

    assertThat(recovered).hasSize(1);
    LogEntry recoveredEntry = recovered.get(0);
    assertThat(recoveredEntry.operation()).isEqualTo(OperationType.DELETE);
    assertThat(recoveredEntry.key()).isEqualTo("keyToDelete");
    assertThat(recoveredEntry.value()).isNull();
  }

  @Test
  void testClearOperation() throws Exception {
    wal = new WriteAheadLog(walPath);
    LogEntry clearEntry = LogEntry.clear();
    wal.append(clearEntry);
    wal.flush();
    wal.close();

    wal = new WriteAheadLog(walPath);
    List<LogEntry> recovered = wal.recover();

    assertThat(recovered).hasSize(1);
    LogEntry recoveredEntry = recovered.get(0);
    assertThat(recoveredEntry.operation()).isEqualTo(OperationType.CLEAR);
    assertThat(recoveredEntry.key()).isNull();
    assertThat(recoveredEntry.value()).isNull();
  }

  @Test
  void testSpecialCharactersInKeysAndValues() throws Exception {
    wal = new WriteAheadLog(walPath);

    // Test various special characters that would break text-based formats
    String specialKey = "key\twith\ntabs\rand newlines";
    String specialValue = "value with 🎉 emoji and \"quotes\" and \\backslashes";

    wal.append(LogEntry.put(specialKey, specialValue));
    wal.flush();
    wal.close();

    wal = new WriteAheadLog(walPath);
    List<LogEntry> recovered = wal.recover();

    assertThat(recovered).hasSize(1);
    assertThat(recovered.get(0).key()).isEqualTo(specialKey);
    assertThat(recovered.get(0).value()).isEqualTo(specialValue);
  }

  @Test
  void testEmptyStringValues() throws Exception {
    wal = new WriteAheadLog(walPath);
    wal.append(LogEntry.put("key", ""));
    wal.flush();
    wal.close();

    wal = new WriteAheadLog(walPath);
    List<LogEntry> recovered = wal.recover();

    assertThat(recovered).hasSize(1);
    assertThat(recovered.get(0).key()).isEqualTo("key");
    assertThat(recovered.get(0).value()).isEqualTo("");
  }

  @Test
  void testLargeValues() throws Exception {
    wal = new WriteAheadLog(walPath);

    // Create a large value (1MB)
    String largeValue = "x".repeat(1024 * 1024);
    wal.append(LogEntry.put("largeKey", largeValue));
    wal.flush();
    wal.close();

    wal = new WriteAheadLog(walPath);
    List<LogEntry> recovered = wal.recover();

    assertThat(recovered).hasSize(1);
    assertThat(recovered.get(0).value()).hasSize(1024 * 1024);
    assertThat(recovered.get(0).value()).isEqualTo(largeValue);
  }

  @Test
  void testRecoverEmptyLog() throws Exception {
    wal = new WriteAheadLog(walPath);
    List<LogEntry> recovered = wal.recover();
    assertThat(recovered).isEmpty();
  }

  @Test
  void testRecoverNonExistentFile() throws Exception {
    Path nonExistent = tempDir.resolve("nonexistent.wal");
    wal = new WriteAheadLog(nonExistent);
    List<LogEntry> recovered = wal.recover();
    assertThat(recovered).isEmpty();
  }

  @Test
  void testAppendAfterRecovery() throws Exception {
    // Write initial entry
    wal = new WriteAheadLog(walPath);
    wal.append(LogEntry.put("key1", "value1"));
    wal.flush();
    wal.close();

    // Open again and append more
    wal = new WriteAheadLog(walPath);
    wal.append(LogEntry.put("key2", "value2"));
    wal.flush();
    wal.close();

    // Verify all entries are present
    wal = new WriteAheadLog(walPath);
    List<LogEntry> recovered = wal.recover();
    assertThat(recovered).hasSize(2);
    assertThat(recovered.get(0).key()).isEqualTo("key1");
    assertThat(recovered.get(1).key()).isEqualTo("key2");
  }

  @Test
  void testTimestampPreservation() throws Exception {
    wal = new WriteAheadLog(walPath);

    Instant beforeWrite = Instant.now();
    wal.append(LogEntry.put("key", "value"));
    Instant afterWrite = Instant.now();

    wal.flush();
    wal.close();

    wal = new WriteAheadLog(walPath);
    List<LogEntry> recovered = wal.recover();

    assertThat(recovered).hasSize(1);
    Instant timestamp = recovered.get(0).timestamp();
    assertThat(timestamp).isBetween(beforeWrite.minusSeconds(1), afterWrite.plusSeconds(1));
  }

  @Test
  void testFlushEnsuresDurability() throws Exception {
    wal = new WriteAheadLog(walPath);
    wal.append(LogEntry.put("key", "value"));

    // Don't close, just flush
    wal.flush();

    // Open a new WAL instance (simulating crash recovery)
    WriteAheadLog wal2 = new WriteAheadLog(walPath);
    List<LogEntry> recovered = wal2.recover();

    assertThat(recovered).hasSize(1);
    assertThat(recovered.get(0).key()).isEqualTo("key");

    wal2.close();
  }

  @Test
  void testDeleteLogFile() throws Exception {
    wal = new WriteAheadLog(walPath);
    wal.append(LogEntry.put("key", "value"));
    wal.flush();
    wal.close();

    assertThat(Files.exists(walPath)).isTrue();

    wal.deleteLog();
    assertThat(Files.exists(walPath)).isFalse();
  }

  @Test
  void testMultipleOperationsSequence() throws Exception {
    wal = new WriteAheadLog(walPath);

    // Simulate a realistic sequence of operations
    wal.append(LogEntry.put("user:1", "Alice"));
    wal.append(LogEntry.put("user:2", "Bob"));
    wal.append(LogEntry.put("user:3", "Charlie"));
    wal.append(LogEntry.delete("user:2"));
    wal.append(LogEntry.put("user:1", "Alice Updated"));
    wal.flush();
    wal.close();

    wal = new WriteAheadLog(walPath);
    List<LogEntry> recovered = wal.recover();

    assertThat(recovered).hasSize(5);
    assertThat(recovered.get(0).key()).isEqualTo("user:1");
    assertThat(recovered.get(0).value()).isEqualTo("Alice");
    assertThat(recovered.get(3).operation()).isEqualTo(OperationType.DELETE);
    assertThat(recovered.get(4).value()).isEqualTo("Alice Updated");
  }

  @Test
  void testWalCreatesParentDirectory() {
    Path nestedPath = tempDir.resolve("nested/dir/test.wal");
    assertThat(Files.exists(nestedPath.getParent())).isFalse();

    wal = new WriteAheadLog(nestedPath);

    assertThat(Files.exists(nestedPath.getParent())).isTrue();
    assertThat(Files.exists(nestedPath)).isTrue();
  }
}
