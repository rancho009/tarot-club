package kvdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32C;

@SuppressWarnings("try")
public final class DatabaseImpl implements Database {

    private static final int MAX_KEY_SIZE = 1024;
    private static final int MAX_VALUE_SIZE = 1024 * 1024;
    private static final int MAX_BATCH_SIZE = 10_000;
    private static final int MAX_CONSECUTIVE_ZERO_READS = 100;
    private static final int GENERATION_SLOT_SIZE = 16;
    private static final int GENERATION_FILE_SIZE = GENERATION_SLOT_SIZE * 2;
    private static final ConcurrentHashMap<Path, ReentrantLock> JVM_PATH_LOCKS =
            new ConcurrentHashMap<>();
    private final DatabaseFaultInjector faultInjector;

    // Replaced only after recovery fully succeeds (always accessed under 'lock').
    private Index index = new Index();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private WAL wal;
    private Path databasePath;
    private Path lockPath;
    private ReentrantLock jvmPathLock;
    private long maxFileBytes = DatabaseConfig.DEFAULT_MAX_FILE_BYTES;
    private long fileGeneration;

    private boolean opened;
    private boolean failed;
    private long nextTransactionId = 1;
    private boolean transactionIdsExhausted;
    private long recoveredEnd;
    private long recoveredMaxTxId = -1;

    public DatabaseImpl() {
        this(DatabaseFaultInjector.NONE);
    }

    DatabaseImpl(DatabaseFaultInjector faultInjector) {
        this.faultInjector = java.util.Objects.requireNonNull(faultInjector,
                "faultInjector");
    }

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
                try (Coordination ignored = acquireCoordination()) {
                    try (FileChannel ignoredReader = FileChannel.open(
                            databasePath,
                            StandardOpenOption.CREATE,
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE)) {
                        // The handle is closed before open() releases the path lock.
                    }
                    maxFileBytes = config.getMaxFileBytes();
                    initializeFromWal();
                    fileGeneration = ignored.generation;
                    cleanupOrphanCompactionFiles();
                    opened = true;
                }
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

        // The lock file is stable for this database path; locks are acquired per operation.
        lockPath = databasePath.resolveSibling(
                databasePath.getFileName().toString() + ".lock");
        jvmPathLock = JVM_PATH_LOCKS.computeIfAbsent(databasePath, ignored -> new ReentrantLock());
        maxFileBytes = config.getMaxFileBytes();

        failed = false;

    }

    private void initializeFromWal() throws IOException {
        Index staged = new Index();
        Recovery.Report report = Recovery.recover(databasePath, staged);
        publishRecovery(staged, report);
    }

    private void publishRecovery(Index recoveredIndex, Recovery.Report report) {
        index = recoveredIndex;
        recoveredEnd = report.committedEnd();
        recoveredMaxTxId = report.maxTxId();
        updateNextTransactionId(recoveredMaxTxId);
    }

    private void updateNextTransactionId(long maxTxId) {
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

    }

    private void openWalForOperation() throws IOException {
        if (wal != null) {
            throw new IOException("WAL operation is already active");
        }
        wal = new WAL(faultInjector);
        wal.open(databasePath, maxFileBytes);
    }

    private void closeWalForOperation() throws IOException {
        WAL activeWal = wal;
        wal = null;
        if (activeWal != null) {
            activeWal.close();
        }
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

    }

    /** Acquires the shared JVM lock, then an exclusive OS lock on the stable lock file. */
    private Coordination acquireCoordination() throws IOException {
        boolean jvmLocked = false;
        FileChannel channel = null;
        FileLock fileLock = null;
        try {
            try {
                jvmPathLock.lockInterruptibly();
                jvmLocked = true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for database JVM lock", e);
            }
            channel = FileChannel.open(lockPath, StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            try {
                fileLock = channel.lock();
            } catch (java.nio.channels.FileLockInterruptionException
                    | java.nio.channels.ClosedByInterruptException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted while waiting for database process lock", e);
            }
            GenerationState generation = readGeneration(channel);
            return new Coordination(jvmPathLock, channel, fileLock,
                    generation.value, generation.slot);
        } catch (IOException | RuntimeException e) {
            if (fileLock != null) {
                try { fileLock.release(); } catch (IOException closeError) { e.addSuppressed(closeError); }
            }
            if (channel != null) {
                try { channel.close(); } catch (IOException closeError) { e.addSuppressed(closeError); }
            }
            if (jvmLocked) {
                jvmPathLock.unlock();
            }
            throw e;
        }
    }

    private static final class Coordination implements AutoCloseable {
        private final ReentrantLock jvmLock;
        private final FileChannel channel;
        private final FileLock fileLock;
        private long generation;
        private int generationSlot;

        Coordination(ReentrantLock jvmLock, FileChannel channel, FileLock fileLock,
                long generation, int generationSlot) {
            this.jvmLock = jvmLock;
            this.channel = channel;
            this.fileLock = fileLock;
            this.generation = generation;
            this.generationSlot = generationSlot;
        }

        long bumpGeneration() throws IOException {
            if (generation == Long.MAX_VALUE) {
                throw new IOException("Database file generation exhausted");
            }
            long next = generation + 1;
            int nextSlot = 1 - generationSlot;
            ByteBuffer slot = ByteBuffer.allocate(GENERATION_SLOT_SIZE)
                    .order(ByteOrder.BIG_ENDIAN)
                    .putLong(next).putLong(~next);
            slot.flip();
            writeFully(channel, slot, (long) nextSlot * GENERATION_SLOT_SIZE);
            channel.force(true);
            generation = next;
            generationSlot = nextSlot;
            return next;
        }

        @Override
        public void close() throws IOException {
            IOException failure = null;
            try {
                fileLock.release();
            } catch (IOException e) {
                failure = e;
            }
            try {
                channel.close();
            } catch (IOException e) {
                if (failure == null) failure = e;
                else failure.addSuppressed(e);
            } finally {
                jvmLock.unlock();
            }
            if (failure != null) throw failure;
        }
    }

    private record GenerationState(long value, int slot) { }

    private static GenerationState readGeneration(FileChannel channel) throws IOException {
        long size = channel.size();
        if (size == 0) {
            writeGenerationSlot(channel, 0, 0);
            writeGenerationSlot(channel, 1, 0);
            channel.force(true);
            return new GenerationState(0, 1);
        }
        if (size > GENERATION_FILE_SIZE) {
            throw new IOException("Invalid database lock metadata size: " + size);
        }
        GenerationState best = null;
        for (int slot = 0; slot < 2; slot++) {
            long offset = (long) slot * GENERATION_SLOT_SIZE;
            if (size - offset < GENERATION_SLOT_SIZE) {
                continue;
            }
            ByteBuffer value = ByteBuffer.allocate(GENERATION_SLOT_SIZE)
                    .order(ByteOrder.BIG_ENDIAN);
            readFully(channel, value, offset);
            value.flip();
            long generation = value.getLong();
            long complement = value.getLong();
            if (generation >= 0 && complement == ~generation
                    && (best == null || generation >= best.value())) {
                best = new GenerationState(generation, slot);
            }
        }
        if (best != null) {
            return best;
        }
        // The only safe no-valid-slot case is an interrupted first-time
        // initialization before any successful database operation.
        if (size < GENERATION_SLOT_SIZE) {
            writeGenerationSlot(channel, 0, 0);
            writeGenerationSlot(channel, 1, 0);
            channel.force(true);
            return new GenerationState(0, 1);
        }
        throw new IOException("Both database lock generation slots are invalid");
    }

    private static void writeGenerationSlot(FileChannel channel, int slot, long generation)
            throws IOException {
        ByteBuffer value = ByteBuffer.allocate(GENERATION_SLOT_SIZE)
                .order(ByteOrder.BIG_ENDIAN).putLong(generation).putLong(~generation);
        value.flip();
        writeFully(channel, value, (long) slot * GENERATION_SLOT_SIZE);
    }

    private static void writeFully(FileChannel channel, ByteBuffer buffer, long position)
            throws IOException {
        int zeroWrites = 0;
        while (buffer.hasRemaining()) {
            int written = channel.write(buffer, position);
            if (written < 0) {
                throw new IOException("Unexpected end while writing lock metadata");
            }
            if (written == 0) {
                if (++zeroWrites > MAX_CONSECUTIVE_ZERO_READS) {
                    throw new IOException("Channel repeatedly returned zero bytes");
                }
                Thread.onSpinWait();
                continue;
            }
            zeroWrites = 0;
            position += written;
        }
    }

    private static void readFully(FileChannel channel, ByteBuffer buffer, long position)
            throws IOException {
        int zeroReads = 0;
        while (buffer.hasRemaining()) {
            int count = channel.read(buffer, position);
            if (count < 0) {
                throw new IOException("Unexpected end while reading lock metadata");
            }
            if (count == 0) {
                if (++zeroReads > MAX_CONSECUTIVE_ZERO_READS) {
                    throw new IOException("Channel repeatedly returned zero bytes");
                }
                Thread.onSpinWait();
                continue;
            }
            zeroReads = 0;
            position += count;
        }
    }

    private void cleanupOrphanCompactionFiles() throws IOException {
        Path parent = databasePath.getParent();
        if (parent == null) {
            return;
        }
        String pattern = databasePath.getFileName() + ".compact.*.tmp";
        try (var candidates = Files.newDirectoryStream(parent, pattern)) {
            for (Path candidate : candidates) {
                Files.deleteIfExists(candidate);
            }
        }
    }

    /** Replays only the WAL suffix since the last verified boundary. */
    private void refreshFromWal() throws IOException {
        try {
            Recovery.Report report = Recovery.recoverFrom(
                    databasePath, index, recoveredEnd, recoveredMaxTxId);
            publishRecovery(index, report);
        } catch (IOException | RuntimeException e) {
            // Recovery may have applied part of a valid suffix before finding
            // corruption; require reopen before this index can be used again.
            failed = true;
            throw e;
        }
    }

    private void refreshFromWal(Coordination coordination) throws IOException {
        if (coordination.generation != fileGeneration) {
            try {
                initializeFromWal();
                fileGeneration = coordination.generation;
            } catch (IOException | RuntimeException e) {
                failed = true;
                throw e;
            }
        } else {
            refreshFromWal();
        }
    }

    /** Records the new durable WAL boundary after this instance commits a write. */
    private void checkpointWrite(long transactionId) throws IOException {
        recoveredEnd = wal.size();
        recoveredMaxTxId = Math.max(recoveredMaxTxId, transactionId);
        updateNextTransactionId(recoveredMaxTxId);
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

            try (Coordination coordination = acquireCoordination()) {
            ensureOpen();
            refreshFromWal(coordination);

            long txId = allocateTransactionId();

            openWalForOperation();
            try {
                walTouched = true; // from here the WAL state may be uncertain
                long offset = wal.append(RecordType.PUT, txId, payload);

                wal.force();

                long valueOffset = offset + RecordCodec.HEADER_SIZE
                        + 8 + keyBytes.length;

                index.put(wrappedKey,
                        new ValueLocation(valueOffset, valueBytes.length));
                checkpointWrite(txId);
            } finally {
                closeWalForOperation();
            }
            }

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

            try (Coordination coordination = acquireCoordination()) {
            ensureOpen();
            refreshFromWal(coordination);

            ValueLocation location = index.get(wrappedKey);

            if (location == null) {
                return null;
            }

            try (FileChannel reader = FileChannel.open(databasePath, StandardOpenOption.READ)) {
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
            }
            }

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

            try (Coordination coordination = acquireCoordination()) {
            ensureOpen();
            refreshFromWal(coordination);

            long txId = allocateTransactionId();

            openWalForOperation();
            try {
                walTouched = true;
                wal.append(RecordType.DELETE, txId, payload);

                wal.force();

                index.remove(wrappedKey);
                checkpointWrite(txId);
            } finally {
                closeWalForOperation();
            }
            }

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

            try (Coordination coordination = acquireCoordination()) {
            ensureOpen();
            refreshFromWal(coordination);

            long txId = allocateTransactionId();

            ByteBuffer beginPayload = ByteBuffer.allocate(4)
                    .order(ByteOrder.BIG_ENDIAN);
            beginPayload.putInt(safeOperations.size());

            openWalForOperation();
            try {
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
            checkpointWrite(txId);
            } finally {
                closeWalForOperation();
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

    /** Rewrites only live entries to a validated same-directory candidate WAL. */
    @Override
    public void compact() throws IOException {
        lock.writeLock().lock();
        try {
            ensureOpen();
            try (Coordination coordination = acquireCoordination()) {
                ensureOpen();
                refreshFromWal(coordination);
                Map<ByteArrayKey, ValueLocation> sourceEntries = index.entriesSnapshot();
                Path temporary = databasePath.resolveSibling(
                        databasePath.getFileName() + ".compact." + UUID.randomUUID() + ".tmp");
                boolean temporaryCreated = false;
                boolean replacementCompleted = false;
                try {
                    faultInjector.beforeCandidateCreate(temporary);
                    Files.createFile(temporary);
                    temporaryCreated = true;
                    Index compactedIndex = new Index();
                    long transactionId = 1;
                    try (FileChannel source = FileChannel.open(databasePath,
                            StandardOpenOption.READ);
                            WAL candidate = new WAL()) {
                        candidate.open(temporary, maxFileBytes);
                        for (Map.Entry<ByteArrayKey, ValueLocation> entry
                                : sourceEntries.entrySet()) {
                            faultInjector.beforeCandidateRecord(temporary, transactionId - 1);
                            byte[] key = entry.getKey().bytes();
                            ValueLocation sourceLocation = entry.getValue();
                            ByteBuffer valueBuffer = ByteBuffer.allocate(sourceLocation.length());
                            if (readAtMost(source, valueBuffer, sourceLocation.offset())
                                    != sourceLocation.length()) {
                                throw new IOException("Cannot read live value during compaction");
                            }
                            byte[] value = valueBuffer.array();
                            long recordOffset = candidate.append(RecordType.PUT,
                                    transactionId++, encodePut(key, value));
                            compactedIndex.put(entry.getKey(), new ValueLocation(
                                    recordOffset + RecordCodec.HEADER_SIZE + 8 + key.length,
                                    value.length));
                        }
                        candidate.force();
                    }

                    faultInjector.beforeCandidateValidation(temporary);
                    Index validatedIndex = new Index();
                    Recovery.Report validation = Recovery.recover(temporary, validatedIndex);
                    if (validation.bytesTruncated() != 0
                            || validation.committedEnd() != Files.size(temporary)
                            || validatedIndex.size() != sourceEntries.size()) {
                        throw new IOException("Compaction candidate failed recovery validation");
                    }
                    verifyCompactionCandidate(sourceEntries, compactedIndex,
                            validatedIndex, temporary);

                    // Persist a generation change first. If the process dies before the
                    // rename, all instances simply perform a harmless full recovery of
                    // the still-authoritative old WAL. If it dies after, they must reload.
                    long nextGeneration = coordination.bumpGeneration();
                    faultInjector.beforeCandidateReplacement(databasePath, temporary);
                    try {
                        Files.move(temporary, databasePath,
                                StandardCopyOption.ATOMIC_MOVE,
                                StandardCopyOption.REPLACE_EXISTING);
                        replacementCompleted = true;
                    } catch (IOException moveFailure) {
                        try {
                            initializeFromWal();
                            fileGeneration = coordination.generation;
                        } catch (IOException | RuntimeException recoveryFailure) {
                            failed = true;
                            moveFailure.addSuppressed(recoveryFailure);
                        }
                        throw moveFailure;
                    }

                    faultInjector.afterCandidateReplacement(databasePath);
                    index = validatedIndex;
                    recoveredEnd = validation.committedEnd();
                    recoveredMaxTxId = validation.maxTxId();
                    updateNextTransactionId(recoveredMaxTxId);
                    fileGeneration = nextGeneration;
                } finally {
                    if (temporaryCreated && !replacementCompleted) {
                        Files.deleteIfExists(temporary);
                    }
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void verifyCompactionCandidate(
            Map<ByteArrayKey, ValueLocation> sourceEntries,
            Index generatedIndex,
            Index recoveredIndex,
            Path candidatePath) throws IOException {
        try (FileChannel source = FileChannel.open(databasePath, StandardOpenOption.READ);
                FileChannel candidate = FileChannel.open(candidatePath, StandardOpenOption.READ)) {
            for (Map.Entry<ByteArrayKey, ValueLocation> entry : sourceEntries.entrySet()) {
                ValueLocation generated = generatedIndex.get(entry.getKey());
                ValueLocation recovered = recoveredIndex.get(entry.getKey());
                if (generated == null || recovered == null
                        || generated.length() != recovered.length()
                        || generated.offset() != recovered.offset()) {
                    throw new IOException("Compaction candidate key/index mismatch");
                }
                byte[] sourceValue = readValue(source, entry.getValue());
                byte[] candidateValue = readValue(candidate, recovered);
                if (!java.util.Arrays.equals(sourceValue, candidateValue)) {
                    throw new IOException("Compaction candidate value mismatch");
                }
            }
        }
    }

    private byte[] readValue(FileChannel channel, ValueLocation location) throws IOException {
        ByteBuffer value = ByteBuffer.allocate(location.length());
        if (readAtMost(channel, value, location.offset()) != location.length()) {
            throw new IOException("Incomplete value while validating compaction candidate");
        }
        return value.array();
    }

    /** Full integrity/recovery pass used by the command-line check operation. */
    int checkIntegrity() throws IOException {
        lock.writeLock().lock();
        try {
            ensureOpen();
            try (Coordination coordination = acquireCoordination()) {
                ensureOpen();
                try {
                    initializeFromWal();
                    fileGeneration = coordination.generation;
                } catch (IOException | RuntimeException e) {
                    failed = true;
                    throw e;
                }
                return index.size();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /** Stable key/value snapshot for CLI dump; not part of the public Database API. */
    List<DumpEntry> dumpSnapshot() throws IOException {
        lock.writeLock().lock();
        try {
            ensureOpen();
            try (Coordination coordination = acquireCoordination()) {
                ensureOpen();
                refreshFromWal(coordination);
                Map<ByteArrayKey, ValueLocation> locations = index.entriesSnapshot();
                List<DumpEntry> entries = new ArrayList<>(locations.size());
                try (FileChannel reader = FileChannel.open(databasePath,
                        StandardOpenOption.READ)) {
                for (Map.Entry<ByteArrayKey, ValueLocation> entry : locations.entrySet()) {
                    ValueLocation location = entry.getValue();
                    ByteBuffer value = ByteBuffer.allocate(location.length());
                    if (readAtMost(reader, value, location.offset()) != location.length()) {
                        throw new IOException("Incomplete stored value during dump");
                    }
                    entries.add(new DumpEntry(entry.getKey().bytes(), value.array()));
                }
                }
                return List.copyOf(entries);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    record DumpEntry(byte[] key, byte[] value) {
        DumpEntry {
            key = key.clone();
            value = value.clone();
        }

        @Override
        public byte[] key() { return key.clone(); }

        @Override
        public byte[] value() { return value.clone(); }
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
        if (!opened) {
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

            try (Coordination ignored = acquireCoordination()) {
                IOException failure = null;
                if (wal != null) {
                    try {
                        closeWalForOperation();
                    } catch (IOException e) {
                        failure = e;
                    }
                }
                opened = false;
                failed = false;
                fileGeneration = -1;
                index = new Index();
                if (failure != null) {
                    throw failure;
                }
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
