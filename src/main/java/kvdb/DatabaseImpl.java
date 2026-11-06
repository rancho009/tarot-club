package kvdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32C;

public final class DatabaseImpl implements Database {

    private static final int MAX_KEY_SIZE = 1024;
    private static final int MAX_VALUE_SIZE = 1024 * 1024;
    private static final int MAX_BATCH_SIZE = 10_000;
    private static final int MAX_CONSECUTIVE_ZERO_READS = 100;

    // Replaced only after recovery fully succeeds (always accessed under 'lock').
    private Index index = new Index();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private WAL wal;
    private FileChannel reader;
    private Path databasePath;

    private FileChannel lockChannel;
    private FileLock processLock;

    private boolean opened;
    private boolean failed;
    private long nextTransactionId = 1;
    private boolean transactionIdsExhausted;

    @Override
    public void open(String path) throws IOException {
        open(path, new DatabaseConfig());
    }

    @Override
    public void open(String path, DatabaseConfig config) throws IOException {

        lock.writeLock().lock();

        try {
            // Checked before acquiring anything, so a failed second open()
            // can never release the lock held by the live instance.
            if (opened) {
                throw new IOException("Database is already open");
            }

            if (path == null || path.isBlank() || config == null) {
                throw new IllegalArgumentException("Invalid open arguments");
            }

            try {
                openInternal(path, config);
            } catch (IOException | RuntimeException e) {
                cleanupFailedOpen(e);
                throw e;
            }

        } finally {
            lock.writeLock().unlock();
        }
    }

    private void openInternal(String path, DatabaseConfig config) throws IOException {

        databasePath = Paths.get(path).toAbsolutePath().normalize();

        Path parent = databasePath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }

        // 1. Process-level lock, held until close().
        Path lockPath = databasePath.resolveSibling(
                databasePath.getFileName().toString() + ".lock");

        lockChannel = FileChannel.open(
                lockPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);

        try {
            processLock = lockChannel.tryLock();
        } catch (OverlappingFileLockException e) {
            processLock = null;
        }

        if (processLock == null) {
            throw new IOException("Database is already open in another process");
        }

        failed = false;

        // 2. Open the database file.
        reader = FileChannel.open(
                databasePath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);

        // 3. Recover into a staging index; publish it only on success.
        Index staged = new Index();
        Recovery.Report report = Recovery.recover(databasePath, staged);

        // 4. Initialise the next transaction id without overflow.
        long maxTxId = report.maxTxId(); // -1 if no valid record was seen
        if (maxTxId < 1) {
            nextTransactionId = 1;
            transactionIdsExhausted = false;
        } else if (maxTxId == Long.MAX_VALUE) {
            nextTransactionId = Long.MAX_VALUE;
            transactionIdsExhausted = true;
        } else {
            nextTransactionId = maxTxId + 1;
            transactionIdsExhausted = false;
        }

        index = staged;

        // 5. WAL writer.
        wal = new WAL();
        wal.open(databasePath, config.getMaxFileBytes());

        // 6. Success.
        opened = true;
    }

    /** Closes everything acquired by a failed open() and discards staged state. */
    private void cleanupFailedOpen(Exception error) {
        index = new Index();
        nextTransactionId = 1;
        transactionIdsExhausted = false;

        if (wal != null) {
            try {
                wal.close();
            } catch (IOException closeError) {
                error.addSuppressed(closeError);
            }
            wal = null;
        }

        if (reader != null) {
            try {
                reader.close();
            } catch (IOException closeError) {
                error.addSuppressed(closeError);
            }
            reader = null;
        }

        releaseProcessLock(error);
    }

    private void releaseProcessLock(Exception originalError) {

        try {
            if (processLock != null && processLock.isValid()) {
                processLock.release();
            }
        } catch (IOException e) {
            originalError.addSuppressed(e);
        } finally {
            processLock = null;
        }

        try {
            if (lockChannel != null && lockChannel.isOpen()) {
                lockChannel.close();
            }
        } catch (IOException e) {
            originalError.addSuppressed(e);
        } finally {
            lockChannel = null;
        }
    }

    /**
     * Returns the next transaction id, never wrapping or reusing one.
     * Throws before any WAL modification when ids are exhausted, so the
     * instance is NOT marked failed in that case.
     */
    private long allocateTransactionId() throws IOException {
        if (transactionIdsExhausted) {
            throw new IOException("Transaction ids exhausted; no further writes possible");
        }

        long id = nextTransactionId;

        if (id == Long.MAX_VALUE) {
            transactionIdsExhausted = true; // this is the last usable id
        } else {
            nextTransactionId = id + 1;
        }

        return id;
    }

    @Override
    public void put(byte[] key, byte[] value) throws IOException {
        ByteArrayKey wrappedKey = new ByteArrayKey(key);

        if (value == null || value.length > MAX_VALUE_SIZE) {
            throw new IllegalArgumentException("Invalid value");
        }

        byte[] keyBytes = wrappedKey.bytes();
        byte[] valueBytes = value.clone();
        byte[] payload = encodePut(keyBytes, valueBytes);

        lock.writeLock().lock();

        boolean walTouched = false;

        try {
            ensureOpen();

            long txId = allocateTransactionId();

            walTouched = true; // from here the WAL state may be uncertain
            long offset = wal.append(RecordType.PUT, txId, payload);

            wal.force();

            long valueOffset = offset + RecordCodec.HEADER_SIZE
                    + 8 + keyBytes.length;

            index.put(wrappedKey,
                    new ValueLocation(valueOffset, valueBytes.length));

        } catch (IOException | RuntimeException e) {
            if (walTouched || (wal != null && wal.isFailed())) {
                failed = true;
            }
            throw e;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public byte[] get(byte[] key) throws IOException {
        ByteArrayKey wrappedKey = new ByteArrayKey(key);

        lock.readLock().lock();

        try {
            ensureOpen();

            ValueLocation location = index.get(wrappedKey);

            if (location == null) {
                return null;
            }

            long fileSize = reader.size();

            if (location.offset() > fileSize
                    || location.length() > fileSize - location.offset()) {
                throw new IOException("Indexed value outside database file");
            }

            ByteBuffer value = ByteBuffer.allocate(location.length());

            int count = readAtMost(reader, value, location.offset());

            if (count != location.length()) {
                throw new IOException("Incomplete stored value");
            }

            return value.array();

        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void delete(byte[] key) throws IOException {
        ByteArrayKey wrappedKey = new ByteArrayKey(key);
        byte[] keyBytes = wrappedKey.bytes();

        byte[] payload = encodeDelete(keyBytes);

        lock.writeLock().lock();

        boolean walTouched = false;

        try {
            ensureOpen();

            long txId = allocateTransactionId();

            walTouched = true;
            wal.append(RecordType.DELETE, txId, payload);

            wal.force();

            index.remove(wrappedKey);

        } catch (IOException | RuntimeException e) {
            if (walTouched) {
                failed = true;
            }
            throw e;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void batch(List<BatchOperation> operations) throws IOException {

        if (operations == null || operations.isEmpty()
                || operations.size() > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    "Batch must contain 1 to 10000 operations");
        }

        // Validate and copy all operations before writing anything.
        List<BatchOperation> safeOperations = new ArrayList<>();

        for (BatchOperation operation : operations) {
            if (operation == null) {
                throw new IllegalArgumentException(
                        "Batch cannot contain null operations");
            }

            byte[] key = operation.getKey();
            byte[] value = operation.getValue();

            new ByteArrayKey(key); // same key limits as ordinary operations

            if (operation.getType() == BatchOperation.Type.PUT) {
                if (value == null || value.length > MAX_VALUE_SIZE) {
                    throw new IllegalArgumentException("Invalid batch value");
                }

                safeOperations.add(BatchOperation.put(key, value));
            } else {
                safeOperations.add(BatchOperation.delete(key));
            }
        }

        lock.writeLock().lock();

        boolean walTouched = false;

        try {
            ensureOpen();

            long txId = allocateTransactionId();

            ByteBuffer beginPayload = ByteBuffer.allocate(4)
                    .order(ByteOrder.BIG_ENDIAN);
            beginPayload.putInt(safeOperations.size());

            // Set BEFORE the append: a partially written BEGIN that throws
            // must still leave this instance failed.
            walTouched = true;
            wal.append(RecordType.BEGIN, txId, beginPayload.array());

            CRC32C transactionChecksum = new CRC32C();
            List<PendingOperation> pending = new ArrayList<>();

            for (BatchOperation operation : safeOperations) {

                byte[] key = operation.getKey();
                byte[] payload;
                RecordType recordType;

                if (operation.getType() == BatchOperation.Type.PUT) {
                    payload = encodePut(key, operation.getValue());
                    recordType = RecordType.PUT;
                } else {
                    payload = encodeDelete(key);
                    recordType = RecordType.DELETE;
                }

                long recordOffset = wal.append(recordType, txId, payload);

                byte[] encodedRecord = RecordCodec.encode(recordType, txId, payload);
                transactionChecksum.update(encodedRecord, 0, encodedRecord.length);

                long valueOffset = -1;
                int valueLength = 0;
                if (recordType == RecordType.PUT) {
                    valueLength = operation.getValue().length;
                    valueOffset = recordOffset + RecordCodec.HEADER_SIZE
                            + 8 + key.length;
                }

                pending.add(new PendingOperation(
                        recordType, new ByteArrayKey(key), valueOffset, valueLength));
            }

            ByteBuffer commitPayload = ByteBuffer.allocate(8)
                    .order(ByteOrder.BIG_ENDIAN);
            commitPayload.putInt(safeOperations.size());
            commitPayload.putInt((int) transactionChecksum.getValue());

            wal.append(RecordType.COMMIT, txId, commitPayload.array());

            // No index changes before this succeeds.
            wal.force();

            // Apply while still holding the write lock: atomic for readers.
            for (PendingOperation op : pending) {
                if (op.type == RecordType.PUT) {
                    index.put(op.key, new ValueLocation(op.valueOffset, op.valueLength));
                } else {
                    index.remove(op.key);
                }
            }

        } catch (IOException | RuntimeException e) {
            // Rejected before any WAL modification (closed, ids exhausted):
            // not failed. Anything after the first append: uncertain -> failed.
            if (walTouched) {
                failed = true;
            }
            throw e;

        } finally {
            lock.writeLock().unlock();
        }
    }

    private byte[] encodePut(byte[] key, byte[] value) {
        ByteBuffer payload = ByteBuffer.allocate(8 + key.length + value.length)
                .order(ByteOrder.BIG_ENDIAN);

        payload.putInt(key.length);
        payload.putInt(value.length);
        payload.put(key);
        payload.put(value);

        return payload.array();
    }

    private byte[] encodeDelete(byte[] key) {
        ByteBuffer payload = ByteBuffer.allocate(4 + key.length)
                .order(ByteOrder.BIG_ENDIAN);

        payload.putInt(key.length);
        payload.put(key);

        return payload.array();
    }

    /**
     * Reads until the buffer is full or EOF. Zero-byte reads are bounded, not EOF.
     */
    private int readAtMost(FileChannel channel, ByteBuffer buffer, long position)
            throws IOException {

        int totalRead = 0;
        int zeroReads = 0;

        while (buffer.hasRemaining()) {
            int count = channel.read(buffer, position + totalRead);

            if (count < 0) {
                break; // EOF
            }

            if (count == 0) {
                if (++zeroReads > MAX_CONSECUTIVE_ZERO_READS) {
                    throw new IOException("Channel repeatedly returned zero bytes");
                }
                Thread.onSpinWait();
                continue;
            }

            zeroReads = 0;
            totalRead += count;
        }

        return totalRead;
    }

    private void ensureOpen() throws IOException {
        if (!opened || wal == null || reader == null) {
            throw new IOException("Database is not open");
        }

        if (failed) {
            throw new IOException(
                    "Database is in a failed state; close and reopen it");
        }
    }

    @Override
    public void close() throws IOException {

        lock.writeLock().lock();

        try {
            if (!opened) {
                return;
            }

            IOException failure = null;

            try {
                wal.close();
            } catch (IOException e) {
                failure = e;
            }

            try {
                reader.close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }

            wal = null;
            reader = null;
            opened = false;
            failed = false;
            index = new Index();

            try {
                if (processLock != null && processLock.isValid()) {
                    processLock.release();
                }
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            } finally {
                processLock = null;
            }

            try {
                if (lockChannel != null && lockChannel.isOpen()) {
                    lockChannel.close();
                }
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            } finally {
                lockChannel = null;
            }

            if (failure != null) {
                throw failure;
            }

        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Key and value location only; no payload is retained. */
    private static final class PendingOperation {
        final RecordType type;
        final ByteArrayKey key;
        final long valueOffset;
        final int valueLength;

        PendingOperation(RecordType type, ByteArrayKey key,
                long valueOffset, int valueLength) {
            this.type = type;
            this.key = key;
            this.valueOffset = valueOffset;
            this.valueLength = valueLength;
        }
    }
}