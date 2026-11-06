package kvdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * Rebuilds the {@link Index} from the write-ahead log and repairs the log tail.
 *
 * <p>
 * Call {@link #recover(Path, Index)} once at startup, <b>before</b>
 * {@code WAL.open}. Recovery opens its own channel (READ and WRITE, never
 * CREATE), so the WAL class is not touched.
 *
 * <p>
 * Decisions that matter for crash safety:
 * <ul>
 * <li>* Standalone PUT and DELETE records are applied after their individual
 * record checksums and payloads have been validated. Batch operations
 * are applied only after their COMMIT has been fully verified.
 * Nothing from an open transaction is ever applied.</li>
 * <li>Only the tail after the last verified COMMIT is ever truncated, and
 * only for an incomplete tail or an uncommitted transaction. Committed
 * offsets never move, so every stored {@link ValueLocation} stays valid.</li>
 * <li>A CORRUPT record or a transaction-rule violation never modifies the
 * file: the Index is cleared and {@link CorruptWalException} is thrown,
 * even when the damage lies after the last COMMIT.</li>
 * <li>The validator uses a bounded 8 MiB sequential byte cache that can contain
 * raw values. Recovery retains operation keys and locations (not duplicate
 * value arrays) for at most one open transaction.</li>
 * </ul>
 */
final class Recovery {

    enum StopReason {
        /** The log ended exactly after a COMMIT (or is empty / absent). */
        CLEAN_END,
        /** The log ended in a partial record; the tail was truncated. */
        INCOMPLETE_TAIL,
        /** The log ended on a record boundary inside an open transaction; truncated. */
        UNCOMMITTED_TRANSACTION
    }

    /**
     * @param committedEnd        offset just after the last verified COMMIT (0 if
     *                            none)
     * @param originalSize        file size before any truncation
     * @param bytesTruncated      {@code originalSize - committedEnd} (0 if nothing
     *                            removed)
     * @param transactionsApplied committed transactions applied to the Index
     * @param operationsApplied   PUT and DELETE operations applied to the Index
     * @param maxTxId             largest txId in any VALID record header, including
     *                            discarded transactions; -1 only if no valid record
     *                            was seen
     * @param stopReason          why the scan stopped
     */
    record Report(
            long committedEnd,
            long originalSize,
            long bytesTruncated,
            long transactionsApplied,
            long operationsApplied,
            long maxTxId,
            StopReason stopReason) {
    }

    /**
     * Thrown for a CORRUPT record or a transaction-rule violation. The WAL is left
     * untouched.
     */
    static final class CorruptWalException extends IOException {
        private static final long serialVersionUID = 1L;

        private final long offset;

        CorruptWalException(long offset, String reason) {
            super("Corrupt WAL at offset " + offset + ": " + reason);
            this.offset = offset;
        }

        /** Start offset of the offending record. */
        long offset() {
            return offset;
        }
    }

    private static final int HEADER_SIZE = RecordCodec.HEADER_SIZE;
    /**
     * Offset of the transaction checksum inside a COMMIT record, from the record
     * start.
     */
    private static final int COMMIT_CHECKSUM_RECORD_OFFSET = HEADER_SIZE + 4;
    private static final int MAX_BATCH_OPERATIONS = 10_000;
    private static final int CHUNK_SIZE = 8192;
    private static final int MAX_CONSECUTIVE_ZERO_READS = 100;

    private Recovery() {
        // Utility class.
    }

    /**
     * Scans the WAL at {@code walPath}, rebuilds {@code index}, and truncates an
     * incomplete or uncommitted tail.
     *
     * @throws CorruptWalException if a CORRUPT record or rule violation is found;
     *                             the file is not modified and the Index is left
     *                             empty
     * @throws IOException         on I/O failure; the Index is left empty
     */
    static Report recover(Path walPath, Index index) throws IOException {
        return recoverFrom(walPath, index, 0, -1);
    }

    /**
     * Replays records appended at or after a previously verified record boundary.
     * The caller must pass an index representing all records before startOffset
     * and hold the database's exclusive process lock for the entire call.
     */
    static Report recoverFrom(
            Path walPath, Index index, long startOffset, long priorMaxTxId)
            throws IOException {
        if (walPath == null) {
            throw new IllegalArgumentException("WAL path cannot be null");
        }
        if (index == null) {
            throw new IllegalArgumentException("Index cannot be null");
        }
        if (startOffset < 0 || priorMaxTxId < -1) {
            throw new IllegalArgumentException("Invalid recovery checkpoint");
        }

        if (startOffset == 0) {
            index.clear();
        }

        boolean success = false;
        try {
            Report report = openAndScan(walPath, index, startOffset, priorMaxTxId);
            success = true;
            return report;
        } finally {
            // Any failure (corruption, I/O error, runtime error, even a failure
            // halfway through applying a committed batch) leaves no partial state.
            if (!success) {
                index.clear();
            }
        }
    }

    private static Report openAndScan(
            Path walPath, Index index, long startOffset, long priorMaxTxId)
            throws IOException {
        FileChannel opened;
        try {
            // No CREATE: a missing file is an empty log, and WAL.open creates it later.
            opened = FileChannel.open(
                    walPath, StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (NoSuchFileException e) {
            index.clear();
            return new Report(0, 0, 0, 0, 0, -1, StopReason.CLEAN_END);
        }
        try (FileChannel ch = opened) {
            return scan(ch, index, startOffset, priorMaxTxId);
        } finally {
            RecordValidator.clearReadCache();
        }
    }

    // ------------------------------------------------------------------
    // Scan
    // ------------------------------------------------------------------

    private static Report scan(
            FileChannel ch, Index index, long startOffset, long priorMaxTxId)
            throws IOException {
        final long fileSize = ch.size(); // read once; the validator trusts it
        if (fileSize < startOffset) {
            // A shorter file means the checkpoint cannot describe this WAL.
            // Rebuild from scratch (for example, after external replacement).
            index.clear();
            startOffset = 0;
            priorMaxTxId = -1;
        }
        final byte[] chunk = new byte[CHUNK_SIZE];
        final State s = new State(startOffset, priorMaxTxId);

        StopReason stop;
        long offset = startOffset;

        while (true) {
            if (offset == fileSize) {
                // The caller (we) handles EOF; the validator rejects offset >= fileSize.
                stop = (s.open != null)
                        ? StopReason.UNCOMMITTED_TRANSACTION
                        : StopReason.CLEAN_END;
                break;
            }

            RecordValidator.Result r = RecordValidator.validateAt(ch, offset, fileSize);

            if (r.status() == RecordValidator.Status.INCOMPLETE_TAIL) {
                stop = StopReason.INCOMPLETE_TAIL;
                break;
            }
            if (r.status() == RecordValidator.Status.CORRUPT) {
                throw new CorruptWalException(offset, r.reason());
            }

            // VALID. nextOffset is only meaningful for VALID results.
            if (r.nextOffset() <= offset || r.nextOffset() > fileSize) {
                throw new IllegalStateException(
                        "RecordValidator returned an invalid nextOffset "
                                + r.nextOffset() + " for record at " + offset);
            }

            // maxTxId covers every VALID header, even in transactions discarded later.
            if (r.txId() > s.maxTxId) {
                s.maxTxId = r.txId();
            }
            s.seenTxId = true;

            switch (r.type()) {
                case BEGIN -> onBegin(ch, s, r, offset);
                case PUT, DELETE -> onOperation(ch, s, r, offset, chunk, index);
                case COMMIT -> onCommit(ch, s, r, offset, index);
            }

            offset = r.nextOffset();
        }

        // Remove everything after the last verified COMMIT: a partial record,
        // or a complete-but-uncommitted transaction. Done only after a clean
        // scan, so a corrupt log is never modified.
        long bytesTruncated = 0;
        if (s.committedEnd < fileSize) {
            ch.truncate(s.committedEnd);
            ch.force(true); // make the truncation durable before WAL.open appends
            bytesTruncated = fileSize - s.committedEnd;
        }

        return new Report(
                s.committedEnd,
                fileSize,
                bytesTruncated,
                s.transactionsApplied,
                s.operationsApplied,
                s.seenTxId ? s.maxTxId : -1L,
                stop);
    }

    // ------------------------------------------------------------------
    // Record handlers
    // ------------------------------------------------------------------

    private static void onBegin(
            FileChannel ch, State s, RecordValidator.Result r, long offset)
            throws IOException {

        if (s.open != null) {
            throw new CorruptWalException(offset,
                    "BEGIN while transaction " + s.open.txId + " is still open");
        }

        // Payload location for BEGIN: recordStart + 24 (VALID results only).
        int expected = readInt(ch, offset + HEADER_SIZE);
        if (expected < 1 || expected > MAX_BATCH_OPERATIONS) {
            throw new CorruptWalException(offset,
                    "BEGIN expected operation count out of range: " + expected);
        }
        s.open = new OpenTransaction(r.txId(), expected);
    }

    private static void onOperation(
            FileChannel ch,
            State s,
            RecordValidator.Result r,
            long offset,
            byte[] chunk,
            Index index) throws IOException {

        OpenTransaction tx = s.open;

        // Standalone PUT/DELETE operation.
        if (tx == null) {

            if (r.type() == RecordType.PUT) {
                index.put(
                        new ByteArrayKey(r.key()),
                        new ValueLocation(r.valueOffset(), r.valueLength()));
            } else {
                index.remove(new ByteArrayKey(r.key()));
            }

            // This standalone record is now part of the valid prefix.
            s.committedEnd = r.nextOffset();
            s.operationsApplied++;

            return;
        }

        // Operation belonging to an atomic batch.
        if (r.txId() != tx.txId) {
            throw new CorruptWalException(offset,
                    r.type() + " has txId " + r.txId()
                            + " but the open transaction is " + tx.txId);
        }

        if (tx.ops.size() >= tx.expectedCount) {
            throw new CorruptWalException(offset,
                    "More operations than BEGIN's expected count "
                            + tx.expectedCount);
        }

        // Include the complete encoded record in the batch checksum.
        updateCrc(ch, offset, r.nextOffset(), tx.crc, chunk);

        if (r.type() == RecordType.PUT) {
            tx.ops.add(new PendingOp(
                    RecordType.PUT,
                    r.key(),
                    r.valueOffset(),
                    r.valueLength()));
        } else {
            tx.ops.add(new PendingOp(
                    RecordType.DELETE,
                    r.key(),
                    -1L,
                    0));
        }
    }

    private static void onCommit(
            FileChannel ch, State s, RecordValidator.Result r, long offset, Index index)
            throws IOException {

        OpenTransaction tx = s.open;
        if (tx == null) {
            throw new CorruptWalException(offset, "COMMIT without BEGIN");
        }
        if (r.txId() != tx.txId) {
            throw new CorruptWalException(offset,
                    "COMMIT has txId " + r.txId()
                            + " but the open transaction is " + tx.txId);
        }

        // COMMIT payload: operationCount at recordStart + 24, checksum at + 28.
        int commitCount = readInt(ch, offset + HEADER_SIZE);
        int storedChecksum = readInt(ch, offset + COMMIT_CHECKSUM_RECORD_OFFSET);
        int actualCount = tx.ops.size();

        if (commitCount != tx.expectedCount || commitCount != actualCount) {
            throw new CorruptWalException(offset,
                    "operation count mismatch: COMMIT=" + commitCount
                            + ", BEGIN expected=" + tx.expectedCount
                            + ", actual=" + actualCount);
        }

        // A zero-operation transaction has CRC32C of no data == 0.
        int computed = (int) tx.crc.getValue();
        if (computed != storedChecksum) {
            throw new CorruptWalException(offset,
                    "transaction checksum mismatch: stored=" + storedChecksum
                            + ", computed=" + computed);
        }

        // Verified. Apply in log order (last operation on a key wins). If this
        // throws halfway, recover() clears the Index before propagating.
        for (PendingOp op : tx.ops) {
            if (op.type() == RecordType.PUT) {
                index.put(new ByteArrayKey(op.key()),
                        new ValueLocation(op.valueOffset(), op.valueLength()));
            } else {
                index.remove(new ByteArrayKey(op.key()));
            }
        }

        // Advance the committed boundary only after the batch was applied.
        s.committedEnd = r.nextOffset();
        s.transactionsApplied++;
        s.operationsApplied += actualCount;
        s.open = null;
    }

    // ------------------------------------------------------------------
    // State
    // ------------------------------------------------------------------

    private static final class State {
        long committedEnd;
        long transactionsApplied = 0;
        long operationsApplied = 0;
        long maxTxId;
        boolean seenTxId;
        OpenTransaction open = null;

        State(long committedEnd, long priorMaxTxId) {
            this.committedEnd = committedEnd;
            this.maxTxId = priorMaxTxId;
            this.seenTxId = priorMaxTxId >= 1;
        }
    }

    private static final class OpenTransaction {
        final long txId;
        final int expectedCount;
        final CRC32C crc = new CRC32C();
        final List<PendingOp> ops;

        OpenTransaction(long txId, int expectedCount) {
            this.txId = txId;
            this.expectedCount = expectedCount;
            this.ops = new ArrayList<>(Math.min(expectedCount, 1024));
        }
    }

    /** Key and value location only; the value bytes are never held in memory. */
    private record PendingOp(RecordType type, byte[] key, long valueOffset, int valueLength) {
    }

    // ------------------------------------------------------------------
    // I/O helpers (private duplicates, as decided; RecordValidator is not touched)
    // ------------------------------------------------------------------

    /** Feeds bytes [start, end) into the CRC through a bounded chunk buffer. */
    private static void updateCrc(
            FileChannel ch, long start, long end, CRC32C crc, byte[] chunk)
            throws IOException {

        long pos = start;
        while (pos < end) {
            int want = (int) Math.min(chunk.length, end - pos); // end - pos > 0, no overflow
            int n = readFully(ch, ByteBuffer.wrap(chunk, 0, want), pos);
            if (n < want) {
                throw new IOException(
                        "Unexpected end of WAL while re-reading record bytes at offset " + pos);
            }
            crc.update(chunk, 0, want);
            pos += want; // pos + want <= end
        }
    }

    private static int readInt(FileChannel ch, long position) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(4); // big-endian by default
        if (readFully(ch, buf, position) < 4) {
            throw new IOException(
                    "Unexpected end of WAL while reading a field at offset " + position);
        }
        return buf.getInt(0);
    }

    /** Positional read-fully: returns fewer bytes than requested only at EOF. */
    private static int readFully(FileChannel ch, ByteBuffer buf, long position)
            throws IOException {
        return RecordValidator.readCached(ch, buf, position);
    }
}
