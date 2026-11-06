package kvdb;

import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
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

    private final Index index = new Index();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private WAL wal;
    private FileChannel reader;
    private Path databasePath;

    private FileChannel lockChannel;
    private FileLock processLock;

    private boolean opened;
    private boolean failed;
    private long nextTransactionId = 1;

    @Override
    public void open(String path) throws IOException {
        open(path, new DatabaseConfig());
    }

    @Override
    public void open(String path, DatabaseConfig config)
            throws IOException {

        lock.writeLock().lock();

        try {
            if (opened) {
                throw new IOException("Database is already open");
            }

            if (path == null || path.isBlank() || config == null) {
                throw new IllegalArgumentException("Invalid open arguments");
            }

            databasePath = Paths.get(path).toAbsolutePath().normalize();

            Path parent = databasePath.getParent();

            if (parent != null) {
                Files.createDirectories(parent);
            }

            // Acquire process-level lock before touching the database.
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
                throw new IOException(
                        "Database is already open in another process");
            }

            index.clear();
            nextTransactionId = 1;
            failed = false;

            reader = FileChannel.open(
                    databasePath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE);

            try {
                recover();

                wal = new WAL();
                wal.open(databasePath, config.getMaxFileBytes());

                opened = true;

            } catch (IOException | RuntimeException e) {

                if (wal != null) {
                    try {
                        wal.close();
                    } catch (IOException closeError) {
                        e.addSuppressed(closeError);
                    }
                    wal = null;
                }

                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException closeError) {
                        e.addSuppressed(closeError);
                    }
                    reader = null;
                }

                throw e;
            }

        } catch (IOException | RuntimeException e) {

            releaseProcessLock(e);
            throw e;

        } finally {
            lock.writeLock().unlock();
        }
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

        try {
            ensureOpen();

            long txId = nextTransactionId++;
            long offset = wal.append(RecordType.PUT, txId, payload);

            wal.force();

            long valueOffset = offset + RecordCodec.HEADER_SIZE
                    + 8 + keyBytes.length;

            index.put(wrappedKey,
                    new ValueLocation(valueOffset, valueBytes.length));

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

        ByteBuffer payload = ByteBuffer.allocate(4 + keyBytes.length)
                .order(ByteOrder.BIG_ENDIAN);

        payload.putInt(keyBytes.length);
        payload.put(keyBytes);

        lock.writeLock().lock();

        try {
            ensureOpen();

            wal.append(
                    RecordType.DELETE,
                    nextTransactionId++,
                    payload.array());

            wal.force();

            index.remove(wrappedKey);

        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void batch(List<BatchOperation> operations)
            throws IOException {

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

            // Revalidate using the same limits as ordinary operations.
            new ByteArrayKey(key);

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

        boolean transactionStarted = false;

        try {
            ensureOpen();

            long txId = nextTransactionId++;

            ByteBuffer beginPayload = ByteBuffer.allocate(4)
                    .order(ByteOrder.BIG_ENDIAN);

            beginPayload.putInt(safeOperations.size());

            wal.append(RecordType.BEGIN, txId, beginPayload.array());
            transactionStarted = true;

            CRC32C transactionChecksum = new CRC32C();

            List<PendingOperation> pending = new ArrayList<>();

            for (BatchOperation operation : safeOperations) {

                byte[] key = operation.getKey();
                byte[] payload;
                RecordType recordType;

                if (operation.getType() == BatchOperation.Type.PUT) {
                    byte[] value = operation.getValue();

                    payload = encodePut(key, value);
                    recordType = RecordType.PUT;
                } else {
                    ByteBuffer deletePayload = ByteBuffer.allocate(4 + key.length)
                            .order(ByteOrder.BIG_ENDIAN);

                    deletePayload.putInt(key.length);
                    deletePayload.put(key);

                    payload = deletePayload.array();
                    recordType = RecordType.DELETE;
                }

                long recordOffset = wal.append(recordType, txId, payload);

                byte[] encodedRecord = RecordCodec.encode(recordType, txId, payload);

                transactionChecksum.update(encodedRecord, 0,
                        encodedRecord.length);

                pending.add(new PendingOperation(
                        recordType, payload, recordOffset));
            }

            ByteBuffer commitPayload = ByteBuffer.allocate(8)
                    .order(ByteOrder.BIG_ENDIAN);

            commitPayload.putInt(safeOperations.size());
            commitPayload.putInt((int) transactionChecksum.getValue());

            wal.append(RecordType.COMMIT, txId, commitPayload.array());

            // No index changes before this succeeds.
            wal.force();

            // Apply all changes while holding the write lock.
            for (PendingOperation operation : pending) {
                applyRecoveredRecord(
                        operation.type,
                        operation.payload,
                        operation.offset);
            }

        } catch (IOException | RuntimeException e) {
            // A partially written transaction must not be followed
            // by more operations in this database instance.
            if (transactionStarted) {
                failed = true;
            }
            throw e;

        } finally {
            lock.writeLock().unlock();
        }
    }

    private byte[] encodePut(byte[] key, byte[] value) {
        ByteBuffer payload = ByteBuffer.allocate(
                8 + key.length + value.length)
                .order(ByteOrder.BIG_ENDIAN);

        payload.putInt(key.length);
        payload.putInt(value.length);
        payload.put(key);
        payload.put(value);

        return payload.array();
    }

    private void recover() throws IOException {

        long offset = 0;
        long fileSize = reader.size();

        long pendingStart = -1;
        long pendingTxId = -1;
        int expectedCount = 0;

        List<PendingOperation> pending = new ArrayList<>();
        CRC32C transactionChecksum = new CRC32C();

        while (offset < fileSize) {

            ByteBuffer header = ByteBuffer.allocate(
                    RecordCodec.HEADER_SIZE);

            int headerBytes = readAtMost(reader, header, offset);

            if (headerBytes < RecordCodec.HEADER_SIZE) {
                truncateIncompleteTail(
                        pendingStart >= 0 ? pendingStart : offset);
                return;
            }

            header.flip().order(ByteOrder.BIG_ENDIAN);

            int payloadLength = header.getInt(16);

            if (payloadLength < 0
                    || payloadLength > RecordCodec.MAX_PAYLOAD_SIZE) {
                throw new IOException("Invalid payload length at " + offset);
            }

            ByteBuffer record = ByteBuffer.allocate(
                    RecordCodec.HEADER_SIZE + payloadLength);

            header.rewind();
            record.put(header);

            ByteBuffer payload = ByteBuffer.allocate(payloadLength);

            int payloadBytes = readAtMost(
                    reader, payload, offset + RecordCodec.HEADER_SIZE);

            if (payloadBytes < payloadLength) {
                truncateIncompleteTail(
                        pendingStart >= 0 ? pendingStart : offset);
                return;
            }

            payload.flip();
            record.put(payload);
            record.flip();

            RecordCodec.DecodedRecord decoded = RecordCodec.decode(record);

            RecordType type = decoded.getType();
            long txId = decoded.getTransactionId();
            byte[] recordPayload = decoded.getPayload();

            if (type == RecordType.BEGIN) {

                if (pendingStart >= 0 || recordPayload.length != 4) {
                    throw new IOException("Invalid BEGIN record");
                }

                ByteBuffer begin = ByteBuffer.wrap(recordPayload)
                        .order(ByteOrder.BIG_ENDIAN);

                expectedCount = begin.getInt();

                if (expectedCount < 1 || expectedCount > MAX_BATCH_SIZE) {
                    throw new IOException("Invalid batch operation count");
                }

                pendingStart = offset;
                pendingTxId = txId;
                pending.clear();
                transactionChecksum = new CRC32C();

            } else if (type == RecordType.COMMIT) {

                if (pendingStart < 0 || txId != pendingTxId
                        || recordPayload.length != 8) {
                    throw new IOException("Invalid COMMIT record");
                }

                ByteBuffer commit = ByteBuffer.wrap(recordPayload)
                        .order(ByteOrder.BIG_ENDIAN);

                int actualCount = commit.getInt();
                int expectedChecksum = commit.getInt();

                if (actualCount != expectedCount
                        || pending.size() != expectedCount
                        || expectedChecksum != (int) transactionChecksum.getValue()) {
                    throw new IOException("Batch transaction validation failed");
                }

                // Commit is valid. Apply the whole transaction.
                for (PendingOperation operation : pending) {
                    applyRecoveredRecord(
                            operation.type,
                            operation.payload,
                            operation.offset);
                }

                pending.clear();
                pendingStart = -1;
                pendingTxId = -1;
                expectedCount = 0;

            } else {

                if (pendingStart >= 0) {

                    if (txId != pendingTxId
                            || pending.size() >= expectedCount) {
                        throw new IOException(
                                "Invalid operation inside batch");
                    }

                    byte[] encoded = RecordCodec.encode(type, txId, recordPayload);

                    transactionChecksum.update(encoded, 0, encoded.length);

                    pending.add(new PendingOperation(
                            type, recordPayload, offset));

                } else {

                    if (type != RecordType.PUT
                            && type != RecordType.DELETE) {
                        throw new IOException("Unexpected record type");
                    }

                    applyRecoveredRecord(type, recordPayload, offset);
                }
            }

            nextTransactionId = Math.max(nextTransactionId, txId + 1);

            offset += RecordCodec.HEADER_SIZE + payloadLength;
        }

        // A complete sequence without COMMIT is not a committed batch.
        if (pendingStart >= 0) {
            truncateIncompleteTail(pendingStart);
        }
    }

    private void applyRecoveredRecord(
            RecordType type,
            byte[] recordPayload,
            long recordOffset) throws IOException {

        ByteBuffer payload = ByteBuffer.wrap(recordPayload)
                .order(ByteOrder.BIG_ENDIAN);

        if (type == RecordType.PUT) {

            if (payload.remaining() < 8) {
                throw new IOException("Invalid PUT payload");
            }

            int keyLength = payload.getInt();
            int valueLength = payload.getInt();

            if (keyLength < 1 || keyLength > MAX_KEY_SIZE
                    || valueLength < 0 || valueLength > MAX_VALUE_SIZE
                    || payload.remaining() != keyLength + valueLength) {
                throw new IOException("Invalid PUT payload lengths");
            }

            byte[] key = new byte[keyLength];
            payload.get(key);

            long valueOffset = recordOffset
                    + RecordCodec.HEADER_SIZE + 8 + keyLength;

            index.put(new ByteArrayKey(key),
                    new ValueLocation(valueOffset, valueLength));

        } else if (type == RecordType.DELETE) {

            if (payload.remaining() < 4) {
                throw new IOException("Invalid DELETE payload");
            }

            int keyLength = payload.getInt();

            if (keyLength < 1 || keyLength > MAX_KEY_SIZE
                    || payload.remaining() != keyLength) {
                throw new IOException("Invalid DELETE payload length");
            }

            byte[] key = new byte[keyLength];
            payload.get(key);

            index.remove(new ByteArrayKey(key));

        } else {
            throw new IOException("Unsupported operation record");
        }
    }

    private void truncateIncompleteTail(long validLength)
            throws IOException {

        reader.truncate(validLength);
        reader.force(true);
    }

    private int readAtMost(
            FileChannel channel,
            ByteBuffer buffer,
            long position) throws IOException {

        int totalRead = 0;

        while (buffer.hasRemaining()) {
            int count = channel.read(buffer, position + totalRead);

            if (count < 0) {
                break;
            }

            if (count == 0) {
                Thread.onSpinWait();
                continue;
            }

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

    private static final class PendingOperation {
        final RecordType type;
        final byte[] payload;
        final long offset;

        PendingOperation(RecordType type, byte[] payload, long offset) {
            this.type = type;
            this.payload = payload.clone();
            this.offset = offset;
        }
    }
}
