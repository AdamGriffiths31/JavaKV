package com.javakv.persistence;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.time.Instant;

import static org.assertj.core.api.Assertions.*;

@DisplayName("LogEntry Tests")
class LogEntryTest {

  @Test
  @DisplayName("Should create PUT entry using factory method")
  void testPutFactoryMethod() {
    LogEntry entry = LogEntry.put("mykey", "myvalue");

    assertThat(entry.operation()).isEqualTo(OperationType.PUT);
    assertThat(entry.key()).isEqualTo("mykey");
    assertThat(entry.value()).isEqualTo("myvalue");
    assertThat(entry.timestamp()).isNotNull();
    assertThat(entry.timestamp()).isBeforeOrEqualTo(Instant.now());
  }

  @Test
  @DisplayName("Should create DELETE entry using factory method")
  void testDeleteFactoryMethod() {
    LogEntry entry = LogEntry.delete("mykey");

    assertThat(entry.operation()).isEqualTo(OperationType.DELETE);
    assertThat(entry.key()).isEqualTo("mykey");
    assertThat(entry.value()).isNull();
    assertThat(entry.timestamp()).isNotNull();
  }

  @Test
  @DisplayName("Should create CLEAR entry using factory method")
  void testClearFactoryMethod() {
    LogEntry entry = LogEntry.clear();

    assertThat(entry.operation()).isEqualTo(OperationType.CLEAR);
    assertThat(entry.key()).isNull();
    assertThat(entry.value()).isNull();
    assertThat(entry.timestamp()).isNotNull();
  }

  @Test
  @DisplayName("Should create entry with explicit parameters")
  void testExplicitConstructor() {
    Instant now = Instant.now();
    LogEntry entry = new LogEntry(OperationType.PUT, "key1", "value1", now);

    assertThat(entry.operation()).isEqualTo(OperationType.PUT);
    assertThat(entry.key()).isEqualTo("key1");
    assertThat(entry.value()).isEqualTo("value1");
    assertThat(entry.timestamp()).isEqualTo(now);
  }

  // Validation Tests

  @Test
  @DisplayName("Should throw exception for null operation")
  void testNullOperation() {
    assertThatThrownBy(() -> new LogEntry(null, "key", "value", Instant.now()))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("Operation cannot be null");
  }

  @Test
  @DisplayName("Should throw exception for null timestamp")
  void testNullTimestamp() {
    assertThatThrownBy(() -> new LogEntry(OperationType.PUT, "key", "value", null))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("Timestamp cannot be null");
  }

  @Test
  @DisplayName("Should throw exception for PUT with null key")
  void testPutWithNullKey() {
    assertThatThrownBy(() -> new LogEntry(OperationType.PUT, null, "value", Instant.now()))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("Key cannot be null or empty for PUT operation");
  }

  @Test
  @DisplayName("Should throw exception for PUT with empty key")
  void testPutWithEmptyKey() {
    assertThatThrownBy(() -> new LogEntry(OperationType.PUT, "", "value", Instant.now()))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("Key cannot be null or empty for PUT operation");
  }

  @Test
  @DisplayName("Should throw exception for PUT with null value")
  void testPutWithNullValue() {
    assertThatThrownBy(() -> new LogEntry(OperationType.PUT, "key", null, Instant.now()))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("Value cannot be null for PUT operation");
  }

  @Test
  @DisplayName("Should throw exception for DELETE with null key")
  void testDeleteWithNullKey() {
    assertThatThrownBy(() -> new LogEntry(OperationType.DELETE, null, null, Instant.now()))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("Key cannot be null or empty for DELETE operation");
  }

  @Test
  @DisplayName("Should throw exception for DELETE with empty key")
  void testDeleteWithEmptyKey() {
    assertThatThrownBy(() -> new LogEntry(OperationType.DELETE, "", null, Instant.now()))
      .isInstanceOf(IllegalArgumentException.class)
      .hasMessageContaining("Key cannot be null or empty for DELETE operation");
  }

  @Test
  @DisplayName("Should allow CLEAR with null key and value")
  void testClearWithNullKeyAndValue() {
    // This should NOT throw - CLEAR operations have null key and value
    assertThatCode(() -> new LogEntry(OperationType.CLEAR, null, null, Instant.now()))
      .doesNotThrowAnyException();
  }

  // Record Behavior Tests

  @Test
  @DisplayName("Should implement equals correctly")
  void testEquals() {
    Instant now = Instant.now();
    LogEntry entry1 = new LogEntry(OperationType.PUT, "key1", "value1", now);
    LogEntry entry2 = new LogEntry(OperationType.PUT, "key1", "value1", now);
    LogEntry entry3 = new LogEntry(OperationType.PUT, "key2", "value2", now);

    assertThat(entry1).isEqualTo(entry2);
    assertThat(entry1).isNotEqualTo(entry3);
  }

  @Test
  @DisplayName("Should implement hashCode correctly")
  void testHashCode() {
    Instant now = Instant.now();
    LogEntry entry1 = new LogEntry(OperationType.PUT, "key1", "value1", now);
    LogEntry entry2 = new LogEntry(OperationType.PUT, "key1", "value1", now);

    assertThat(entry1.hashCode()).isEqualTo(entry2.hashCode());
  }

  @Test
  @DisplayName("Should implement toString correctly")
  void testToString() {
    Instant now = Instant.now();
    LogEntry entry = new LogEntry(OperationType.PUT, "key1", "value1", now);

    String toString = entry.toString();

    assertThat(toString).contains("PUT");
    assertThat(toString).contains("key1");
    assertThat(toString).contains("value1");
  }

  // Edge Cases

  @Test
  @DisplayName("Should handle empty string value for PUT")
  void testPutWithEmptyValue() {
    assertThatCode(() -> LogEntry.put("key", ""))
      .doesNotThrowAnyException();

    LogEntry entry = LogEntry.put("key", "");
    assertThat(entry.value()).isEmpty();
  }

  @Test
  @DisplayName("Should handle special characters in key and value")
  void testSpecialCharacters() {
    LogEntry entry = LogEntry.put("key:with:colons", "value\nwith\nnewlines");

    assertThat(entry.key()).isEqualTo("key:with:colons");
    assertThat(entry.value()).isEqualTo("value\nwith\nnewlines");
  }

  @Test
  @DisplayName("Should handle very long keys and values")
  void testLongKeyAndValue() {
    String longKey = "k".repeat(1000);
    String longValue = "v".repeat(10000);

    LogEntry entry = LogEntry.put(longKey, longValue);

    assertThat(entry.key()).isEqualTo(longKey);
    assertThat(entry.value()).isEqualTo(longValue);
  }

  // Serialization Tests

  @Test
  @DisplayName("Should serialize and deserialize PUT entry")
  void testSerializeDeserializePut() throws Exception {
    LogEntry original = LogEntry.put("testKey", "testValue");

    // Serialize
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    original.writeTo(dos);

    // Deserialize
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    LogEntry deserialized = LogEntry.readFrom(dis);

    // Verify
    assertThat(deserialized.operation()).isEqualTo(original.operation());
    assertThat(deserialized.key()).isEqualTo(original.key());
    assertThat(deserialized.value()).isEqualTo(original.value());
    // Timestamp loses nanosecond precision (stored as millis)
    assertThat(deserialized.timestamp().toEpochMilli()).isEqualTo(original.timestamp().toEpochMilli());
  }

  @Test
  @DisplayName("Should serialize and deserialize DELETE entry")
  void testSerializeDeserializeDelete() throws Exception {
    LogEntry original = LogEntry.delete("keyToDelete");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    original.writeTo(dos);

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    LogEntry deserialized = LogEntry.readFrom(dis);

    assertThat(deserialized.operation()).isEqualTo(OperationType.DELETE);
    assertThat(deserialized.key()).isEqualTo("keyToDelete");
    assertThat(deserialized.value()).isNull();
  }

  @Test
  @DisplayName("Should serialize and deserialize CLEAR entry")
  void testSerializeDeserializeClear() throws Exception {
    LogEntry original = LogEntry.clear();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    original.writeTo(dos);

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    LogEntry deserialized = LogEntry.readFrom(dis);

    assertThat(deserialized.operation()).isEqualTo(OperationType.CLEAR);
    assertThat(deserialized.key()).isNull();
    assertThat(deserialized.value()).isNull();
  }

  @Test
  @DisplayName("Should handle special characters in serialization")
  void testSerializeSpecialCharacters() throws Exception {
    String specialKey = "key\twith\ntabs\rand newlines";
    String specialValue = "value with 🎉 emoji and \"quotes\"";
    LogEntry original = LogEntry.put(specialKey, specialValue);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    original.writeTo(dos);

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    LogEntry deserialized = LogEntry.readFrom(dis);

    assertThat(deserialized.key()).isEqualTo(specialKey);
    assertThat(deserialized.value()).isEqualTo(specialValue);
  }

  @Test
  @DisplayName("Should handle empty value in serialization")
  void testSerializeEmptyValue() throws Exception {
    LogEntry original = LogEntry.put("key", "");

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    original.writeTo(dos);

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    LogEntry deserialized = LogEntry.readFrom(dis);

    assertThat(deserialized.value()).isEmpty();
  }

  @Test
  @DisplayName("Should handle large values in serialization")
  void testSerializeLargeValue() throws Exception {
    String largeValue = "x".repeat(100000);
    LogEntry original = LogEntry.put("key", largeValue);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    original.writeTo(dos);

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    LogEntry deserialized = LogEntry.readFrom(dis);

    assertThat(deserialized.value()).hasSize(100000);
    assertThat(deserialized.value()).isEqualTo(largeValue);
  }

  @Test
  @DisplayName("Should serialize multiple entries sequentially")
  void testSerializeMultipleEntries() throws Exception {
    LogEntry entry1 = LogEntry.put("key1", "value1");
    LogEntry entry2 = LogEntry.delete("key2");
    LogEntry entry3 = LogEntry.clear();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    entry1.writeTo(dos);
    entry2.writeTo(dos);
    entry3.writeTo(dos);

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);

    LogEntry deserialized1 = LogEntry.readFrom(dis);
    LogEntry deserialized2 = LogEntry.readFrom(dis);
    LogEntry deserialized3 = LogEntry.readFrom(dis);

    assertThat(deserialized1.key()).isEqualTo("key1");
    assertThat(deserialized2.operation()).isEqualTo(OperationType.DELETE);
    assertThat(deserialized3.operation()).isEqualTo(OperationType.CLEAR);
  }

  @Test
  @DisplayName("Should throw exception for invalid operation type during deserialization")
  void testDeserializeInvalidOperationType() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    // Write invalid operation type (99)
    dos.writeByte(99);
    dos.writeLong(Instant.now().toEpochMilli());
    dos.writeInt(0); // key length
    dos.writeInt(0); // value length

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);

    assertThatThrownBy(() -> LogEntry.readFrom(dis))
      .isInstanceOf(Exception.class)
      .hasMessageContaining("Invalid operation type");
  }

  @Test
  @DisplayName("Should preserve timestamp precision in serialization")
  void testTimestampPrecision() throws Exception {
    Instant now = Instant.now();
    LogEntry original = new LogEntry(OperationType.PUT, "key", "value", now);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    original.writeTo(dos);

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    LogEntry deserialized = LogEntry.readFrom(dis);

    // Note: We lose nanosecond precision since we store as millis
    assertThat(deserialized.timestamp().toEpochMilli()).isEqualTo(now.toEpochMilli());
  }
}
