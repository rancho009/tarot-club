package kvdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Objects;
import java.util.zip.CRC32C;

/**
 * Validates one WAL record at an absolute file offset, using positional reads.
 *
 * <p>
 * Contract summary (see also {@link #validateAt}):
 * <ul>
 * <li>Lengths are validated before any payload is read or allocated.</li>
 * <li>The CRC is streamed in fixed chunks. A bounded 8 MiB sequential-read
 * cache may contain raw WAL bytes, including value bytes, but records are not
 * materialized as whole values by this validator.</li>
 * <li>Only the key (at most 1024 bytes) is copied.</li>
 * <li>For BEGIN and COMMIT, the caller derives the payload location from a
 * VALID result only: payload offset = recordStart + 24, payload length =
 * nextOffset - recordStart - 24. The COMMIT transaction checksum is NOT
 * interpreted here; Recovery verifies it.</li>
 * <li>{@code txId == -1} and {@code nextOffset == offset} on non-VALID results
 * are placeholders. Callers must check {@code status} first.</li>
 * </ul>
 *
 * <p>
 * Integration notes: uses {@link RecordCodec#HEADER_SIZE} and
 * {@link RecordCodec#MAX_PAYLOAD_SIZE}, and {@link RecordType}. RecordCodec's
 * MAGIC and VERSION are private, so they are DUPLICATED below; if Member 3
 * changes them, update these copies. The record CRC range rule (header bytes
 * 0-19 followed by the payload) is also duplicated here.
 */
final class RecordValidator {

    enum Status {
        VALID,
        INCOMPLETE_TAIL,
        CORRUPT
    }

    /**
     * @param key defensively copied on construction and on access; null unless
     *            the record is a VALID PUT or DELETE
     */
    record Result(
            Status status,
            RecordType type,
            long txId,
            byte[] key,
            long valueOffset,
            int valueLength,
            long nextOffset,
            String reason) {

        Result {
            Objects.requireNonNull(status, "status");
            key = (key == null) ? null : key.clone();
        }

        @Override
        public byte[] key() {
            return key == null ? null : key.clone();
        }
    }

    // ---- Format constants (DUPLICATES of RecordCodec's private values) ----
    private static final int MAGIC = 0x4B564442; // "KVDB"
    private static final byte VERSION = 1;

    // ---- Constants shared with RecordCodec ----
    private static final int HEADER_SIZE = RecordCodec.HEADER_SIZE;

    // ---- Layout ----
    private static final int CRC_FIELD_OFFSET = 20; // CRC covers bytes 0-19 + payload
    private static final int TX_ID_OFFSET = 8;
    private static final int PAYLOAD_LENGTH_OFFSET = 16;

    private static final int PUT_PREFIX_SIZE = 8; // keyLength + valueLength
    private static final int DELETE_PREFIX_SIZE = 4; // keyLength
    private static final int COUNT_SIZE = 4; // BEGIN/COMMIT first field

    // ---- Limits ----
    private static final int MAX_KEY_LENGTH = 1024;
    private static final int MAX_VALUE_LENGTH = 1_048_576;
    private static final int MAX_BATCH_OPERATIONS = 10_000;

    private static final int MIN_PUT_PAYLOAD = PUT_PREFIX_SIZE + 1; // key >= 1
    private static final int MAX_PUT_PAYLOAD = PUT_PREFIX_SIZE + MAX_KEY_LENGTH + MAX_VALUE_LENGTH;
    private static final int MIN_DELETE_PAYLOAD = DELETE_PREFIX_SIZE + 1;
    private static final int MAX_DELETE_PAYLOAD = DELETE_PREFIX_SIZE + MAX_KEY_LENGTH;
    private static final int BEGIN_PAYLOAD = 4;
    private static final int COMMIT_PAYLOAD = 8;

    private static final int CHUNK_SIZE = 8192;
    private static final int READ_CACHE_SIZE = 8 * 1024 * 1024;
    private static final int MAX_CONSECUTIVE_ZERO_READS = 100;
    private static final ThreadLocal<ReadCache> READ_CACHE =
            ThreadLocal.withInitial(ReadCache::new);

    private RecordValidator() {
        // Utility class.
    }

    /**
     * Validates the record starting at {@code offset}.
     *
     * @param ch       channel opened by the caller; only positional reads are
     *                 used, the channel position is never touched
     * @param offset   absolute start of the record, must satisfy
     *                 {@code 0 <= offset < fileSize}
     * @param fileSize trusted file size; {@code ch.size()} is never called.
     *                 The caller must handle {@code offset == fileSize} itself.
     * @throws IllegalArgumentException for a null channel, negative offset or
     *                                  file size, or {@code offset >= fileSize}
     * @throws IOException              only for genuine channel failures; a file
     *                                  that ends
     *                                  early is reported as INCOMPLETE_TAIL
     */
    static Result validateAt(FileChannel ch, long offset, long fileSize)
            throws IOException {

        if (ch == null) {
            throw new IllegalArgumentException("Channel cannot be null");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Offset cannot be negative");
        }
        if (fileSize < 0) {
            throw new IllegalArgumentException("File size cannot be negative");
        }
        if (offset >= fileSize) {
            throw new IllegalArgumentException(
                    "Offset must be less than file size; "
                            + "caller must handle end of file");
        }

        // Both operands are non-negative and offset < fileSize: no overflow.
        final long remaining = fileSize - offset;

        // ---- Step 1: read the header (or whatever is available of it) ----
        final int headerWanted = (int) Math.min(HEADER_SIZE, remaining);
        final byte[] h = new byte[HEADER_SIZE];
        final int got = readFully(
                ch, ByteBuffer.wrap(h, 0, headerWanted), offset);

        // ---- Step 2/3: inspect all available fields ----
        String problem = structuralProblem(h, got);
        if (problem != null) {
            return corrupt(offset, problem);
        }
        if (got < HEADER_SIZE) {
            return incomplete(offset,
                    "incomplete header (" + got + " of " + HEADER_SIZE
                            + " bytes)");
        }

        final RecordType type = typeFromCode(h[5]); // known: checked above
        final long txId = readLong(h, TX_ID_OFFSET);
        if (txId < 1) {
            return corrupt(offset, "invalid transaction ID " + txId);
        }
        final int payloadLength = readInt(h, PAYLOAD_LENGTH_OFFSET);
        final int storedCrc = readInt(h, CRC_FIELD_OFFSET);

        // ---- Step 4: payload past EOF (overflow-safe: no addition) ----
        final long availablePayload = remaining - HEADER_SIZE; // remaining >= 24
        if (payloadLength > availablePayload) {
            return payloadPastEof(ch, offset, type, payloadLength,
                    availablePayload);
        }

        // From here the whole record lies within fileSize, so
        // offset + HEADER_SIZE + payloadLength <= fileSize cannot overflow.
        final long payloadStart = offset + HEADER_SIZE;
        final long nextOffset = payloadStart + payloadLength;

        // ---- Step 5: inner structure, before streaming the payload ----
        final int prefixLength = prefixLength(type);
        final ByteBuffer prefix = ByteBuffer.allocate(prefixLength);
        if (readFully(ch, prefix, payloadStart) < prefixLength) {
            return incomplete(offset,
                    "file ended before the size recorded by the caller");
        }

        problem = innerLengthProblem(type, payloadLength, prefix);
        if (problem != null) {
            return corrupt(offset, problem);
        }

        // ---- Step 6: streaming CRC32C ----
        final CRC32C crc = new CRC32C();
        crc.update(h, 0, CRC_FIELD_OFFSET);
        crc.update(prefix.array(), 0, prefixLength);

        long pos = payloadStart + prefixLength;
        int keyLength = 0;
        byte[] key = null;

        if (type == RecordType.PUT || type == RecordType.DELETE) {
            keyLength = prefix.getInt(0); // validated: 1..1024
            key = new byte[keyLength]; // allocated after validation
            if (readFully(ch, ByteBuffer.wrap(key), pos) < keyLength) {
                return incomplete(offset,
                        "file ended before the size recorded by the caller");
            }
            crc.update(key, 0, keyLength);
            pos += keyLength;
        }

        // Remaining payload bytes (PUT value, or COMMIT's checksum field).
        final long rest = (long) payloadLength - prefixLength - keyLength;
        if (!streamIntoCrc(ch, pos, rest, crc)) {
            return incomplete(offset,
                    "file ended before the size recorded by the caller");
        }

        if ((int) crc.getValue() != storedCrc) {
            return corrupt(offset, "record checksum mismatch");
        }

        // ---- Build the VALID result ----
        return switch (type) {
            case PUT -> new Result(
                    Status.VALID, type, txId, key,
                    payloadStart + PUT_PREFIX_SIZE + keyLength,
                    prefix.getInt(4),
                    nextOffset, null);
            case DELETE -> new Result(
                    Status.VALID, type, txId, key,
                    -1L, 0, nextOffset, null);
            case BEGIN, COMMIT -> new Result(
                    Status.VALID, type, txId, null,
                    -1L, 0, nextOffset, null);
        };
    }

    // ------------------------------------------------------------------
    // Header inspection
    // ------------------------------------------------------------------

    /**
     * Checks every header field that is fully present in the first
     * {@code available} bytes. Returns a reason if one is conclusively
     * invalid, otherwise null. Also applies the per-type payload length bounds
     * once bytes 0-19 are present.
     */
    private static String structuralProblem(byte[] h, int available) {
        for (int i = 0; i < Math.min(4, available); i++) {
            byte expected = (byte) (MAGIC >>> (24 - 8 * i));
            if (h[i] != expected) {
                return "invalid magic";
            }
        }
        if (available > 4 && h[4] != VERSION) {
            return "unsupported version " + Byte.toUnsignedInt(h[4]);
        }

        RecordType type = null;
        if (available > 5) {
            type = typeFromCode(h[5]);
            if (type == null) {
                return "unknown record type " + Byte.toUnsignedInt(h[5]);
            }
        }
        if (available > 6 && h[6] != 0) {
            return "unsupported flags";
        }
        if (available > 7 && h[7] != 0) {
            return "unsupported flags";
        }
        if (available >= CRC_FIELD_OFFSET) {
            // type is non-null here because available > 5.
            return lengthProblem(type, readInt(h, PAYLOAD_LENGTH_OFFSET));
        }
        return null;
    }

    /** Per-type payload length bounds. A negative length is always invalid. */
    private static String lengthProblem(RecordType type, int payloadLength) {
        boolean ok = switch (type) {
            case PUT -> payloadLength >= MIN_PUT_PAYLOAD
                    && payloadLength <= MAX_PUT_PAYLOAD;
            case DELETE -> payloadLength >= MIN_DELETE_PAYLOAD
                    && payloadLength <= MAX_DELETE_PAYLOAD;
            case BEGIN -> payloadLength == BEGIN_PAYLOAD;
            case COMMIT -> payloadLength == COMMIT_PAYLOAD;
        };
        return ok ? null
                : "invalid payload length " + payloadLength + " for " + type;
    }

    private static int prefixLength(RecordType type) {
        return switch (type) {
            case PUT -> PUT_PREFIX_SIZE;
            case DELETE -> DELETE_PREFIX_SIZE;
            case BEGIN, COMMIT -> COUNT_SIZE; // COMMIT: only the count
        };
    }

    // ------------------------------------------------------------------
    // Payload structure
    // ------------------------------------------------------------------

    /**
     * Checks the leading payload fields against the limits and, for PUT and
     * DELETE, against the header's payloadLength (exact match). BEGIN and
     * COMMIT counts must be 0..10,000; zero is not judged here.
     */
    private static String innerLengthProblem(
            RecordType type, int payloadLength, ByteBuffer prefix) {

        if (type == RecordType.PUT) {
            int keyLength = prefix.getInt(0);
            int valueLength = prefix.getInt(4);
            if (keyLength < 1 || keyLength > MAX_KEY_LENGTH) {
                return "invalid key length " + keyLength;
            }
            if (valueLength < 0 || valueLength > MAX_VALUE_LENGTH) {
                return "invalid value length " + valueLength;
            }
            if ((long) PUT_PREFIX_SIZE + keyLength + valueLength != payloadLength) {
                return "PUT payload length " + payloadLength
                        + " does not match key/value lengths";
            }
        } else if (type == RecordType.DELETE) {
            int keyLength = prefix.getInt(0);
            if (keyLength < 1 || keyLength > MAX_KEY_LENGTH) {
                return "invalid key length " + keyLength;
            }
            if ((long) DELETE_PREFIX_SIZE + keyLength != payloadLength) {
                return "DELETE payload length " + payloadLength
                        + " does not match key length";
            }
        } else {
            int count = prefix.getInt(0);
            if (count < 1 || count > MAX_BATCH_OPERATIONS) {
                return "invalid operation count " + count;
            }
        }
        return null;
    }

    /**
     * Header is complete and structurally valid, but the payload extends past
     * fileSize. If the leading inner-length fields are themselves within
     * fileSize, cross-check them against payloadLength: a mismatch cannot be a
     * torn write and is reported as CORRUPT. Otherwise INCOMPLETE_TAIL.
     */
    private static Result payloadPastEof(
            FileChannel ch,
            long offset,
            RecordType type,
            int payloadLength,
            long availablePayload) throws IOException {

        if (type == RecordType.PUT || type == RecordType.DELETE) {
            int prefixLength = prefixLength(type);
            if (availablePayload >= prefixLength) {
                // offset + HEADER_SIZE + prefixLength <= fileSize: no overflow.
                ByteBuffer prefix = ByteBuffer.allocate(prefixLength);
                int n = readFully(ch, prefix, offset + HEADER_SIZE);
                if (n == prefixLength) {
                    String problem = innerLengthProblem(type, payloadLength, prefix);
                    if (problem != null) {
                        return corrupt(offset, problem);
                    }
                }
            }
        }
        return incomplete(offset,
                "payload extends past end of file (payload length "
                        + payloadLength + ")");
    }

    // ------------------------------------------------------------------
    // I/O helpers
    // ------------------------------------------------------------------

    /**
     * Reads until {@code buf} is full or EOF. Returns the number of bytes
     * read, which is less than {@code buf.remaining()} only at EOF.
     */
    private static int readFully(
            FileChannel ch, ByteBuffer buf, long position)
            throws IOException {
        return readCached(ch, buf, position);
    }

    /** Shared sequential cache for the validator and Recovery's batch CRC pass. */
    static int readCached(FileChannel channel, ByteBuffer target, long position)
            throws IOException {
        if (channel == null || target == null || position < 0) {
            throw new IllegalArgumentException("Invalid cached WAL read arguments");
        }
        ReadCache cache = READ_CACHE.get();
        int total = 0;
        while (target.hasRemaining()) {
            if (cache.channel != channel || position < cache.start
                    || position >= cache.start + cache.length) {
                cache.refill(channel, position);
            }
            int available = cache.length - (int) (position - cache.start);
            if (available <= 0) {
                break;
            }
            int count = Math.min(target.remaining(), available);
            int sourceOffset = (int) (position - cache.start);
            target.put(cache.bytes, sourceOffset, count);
            position += count;
            total += count;
        }
        return total;
    }

    static void clearReadCache() {
        READ_CACHE.remove();
    }

    private static final class ReadCache {
        final byte[] bytes = new byte[READ_CACHE_SIZE];
        FileChannel channel;
        long start;
        int length;

        void refill(FileChannel source, long position) throws IOException {
            channel = source;
            start = position;
            length = 0;
            ByteBuffer destination = ByteBuffer.wrap(bytes);
            int zeroReads = 0;
            while (destination.hasRemaining()) {
                int count = source.read(destination, position + length);
                if (count < 0) {
                    break;
                }
                if (count == 0) {
                    if (++zeroReads > MAX_CONSECUTIVE_ZERO_READS) {
                        throw new IOException("Channel repeatedly returned zero bytes");
                    }
                    Thread.onSpinWait();
                    continue;
                }
                zeroReads = 0;
                length += count;
            }
        }
    }

    /**
     * Feeds {@code count} bytes starting at {@code position} into the CRC using
     * a bounded chunk buffer. Returns false if EOF is reached first.
     */
    private static boolean streamIntoCrc(
            FileChannel ch, long position, long count, CRC32C crc)
            throws IOException {

        if (count <= 0) {
            return true;
        }
        byte[] chunk = new byte[(int) Math.min(CHUNK_SIZE, count)];
        long left = count;
        long pos = position;
        while (left > 0) {
            int want = (int) Math.min(chunk.length, left);
            int n = readFully(ch, ByteBuffer.wrap(chunk, 0, want), pos);
            if (n < want) {
                return false;
            }
            crc.update(chunk, 0, want);
            pos += want;
            left -= want;
        }
        return true;
    }

    // ------------------------------------------------------------------
    // Misc helpers
    // ------------------------------------------------------------------

    /** Non-throwing lookup built on RecordType.getCode(); null if unknown. */
    private static RecordType typeFromCode(byte code) {
        for (RecordType t : RecordType.values()) {
            if (t.getCode() == code) {
                return t;
            }
        }
        return null;
    }

    private static int readInt(byte[] b, int off) {
        return ((b[off] & 0xFF) << 24)
                | ((b[off + 1] & 0xFF) << 16)
                | ((b[off + 2] & 0xFF) << 8)
                | (b[off + 3] & 0xFF);
    }

    private static long readLong(byte[] b, int off) {
        return ((long) readInt(b, off) << 32)
                | (readInt(b, off + 4) & 0xFFFFFFFFL);
    }

    private static Result corrupt(long offset, String reason) {
        return new Result(Status.CORRUPT, null, -1L, null, -1L, 0, offset,
                reason + " at offset " + offset);
    }

    private static Result incomplete(long offset, String reason) {
        return new Result(Status.INCOMPLETE_TAIL, null, -1L, null, -1L, 0,
                offset, reason + " at offset " + offset);
    }
}
