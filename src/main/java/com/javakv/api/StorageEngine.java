package com.javakv.api;

import com.javakv.persistence.LogEntry;
import java.util.List;

/**
 * Interface for persistence layer abstraction.
 *
 * <p>This interface defines the contract for storing and recovering operations
 * from persistent storage. Implementations are responsible for ensuring durability
 * and crash recovery capabilities.
 *
 * <p>The storage engine operates on a Write-Ahead Log (WAL) model where operations
 * are appended sequentially and can be replayed during recovery.
 */
public interface StorageEngine extends AutoCloseable {

    /**
     * Appends a log entry to persistent storage.
     *
     * <p>This operation should ensure that the entry is durably written to storage
     * before returning. Implementations may buffer writes for performance but must
     * guarantee that a subsequent {@link #flush()} makes all entries durable.
     *
     * @param entry the log entry to append
     * @throws com.javakv.store.StorageException if the write fails
     */
    void append(LogEntry entry);

    /**
     * Recovers all log entries from persistent storage.
     *
     * <p>This method is typically called during startup to replay operations
     * and rebuild the in-memory state. Entries are returned in the order they
     * were written.
     *
     * @return a list of all log entries, in order
     * @throws com.javakv.store.StorageException if recovery fails
     */
    List<LogEntry> recover();

    /**
     * Flushes any buffered writes to persistent storage.
     *
     * <p>After this method returns, all previously appended entries are guaranteed
     * to be durable (i.e., they will survive a crash or power failure).
     *
     * @throws com.javakv.store.StorageException if the flush fails
     */
    void flush();

    /**
     * Closes the storage engine and releases any resources.
     *
     * <p>This method ensures that all buffered writes are flushed before closing.
     * After calling close, no further operations should be performed on this instance.
     *
     * @throws Exception if an error occurs during closing
     */
    @Override
    void close() throws Exception;
}
