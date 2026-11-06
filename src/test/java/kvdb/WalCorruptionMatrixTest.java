package kvdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

/** Structural and location-based WAL corruption tests using disposable copies. */
public final class WalCorruptionMatrixTest {
    private WalCorruptionMatrixTest() { }

    public static void main(String[] args) throws Exception {
        Path directory = Files.createTempDirectory("tarot-wal-corruption-");
        Path source = createSource(directory.resolve("intact.db"));
        byte[] sourceBytes = Files.readAllBytes(source);

        rejectCorruption(source, sourceBytes, "invalid magic at beginning", bytes -> bytes[0] ^= 1);
        rejectCorruption(source, sourceBytes, "oversized payload length", bytes ->
                ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)
                        .putInt(16, RecordCodec.MAX_PAYLOAD_SIZE + 1));
        rejectCorruption(source, sourceBytes, "CRC mismatch", bytes -> flipPayload(bytes, 0));
        rejectCorruption(source, sourceBytes, "invalid operation type", bytes ->
                bytes[offsetOf(bytes, 1) + 5] = (byte) 99);
        rejectCorruption(source, sourceBytes, "corruption in middle record", bytes ->
                bytes[offsetOf(bytes, 2)] ^= 0x20);
        rejectCorruption(source, sourceBytes, "corruption in final record", bytes ->
                bytes[offsetOf(bytes, 4) + 20] ^= 1);
        testInvalidBatchMetadata(source, sourceBytes, directory);
        testTruncatedHeaderIsRecoverable(source, sourceBytes, directory);
        testTruncatedPayloadIsRecoverable(source, sourceBytes, directory);

        check(Arrays.equals(sourceBytes, Files.readAllBytes(source)),
                "destructive copy tests changed the original fixture");
        System.out.println("ALL WAL CORRUPTION MATRIX TESTS PASSED");
    }

    private static void rejectCorruption(Path source, byte[] original, String label,
            Mutation mutation) throws Exception {
        Path copy = Files.createTempFile(source.getParent(), "corrupt-", ".db");
        Files.copy(source, copy, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
        byte[] corrupted = original.clone();
        mutation.apply(corrupted);
        Files.write(copy, corrupted, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
        byte[] expectedUnmodifiedCorruptCopy = Files.readAllBytes(copy);

        Database database = new DatabaseImpl();
        IOException failure = null;
        try {
            database.open(copy.toString());
        } catch (IOException expected) {
            failure = expected;
        } finally {
            database.close();
        }
        check(failure != null, label + " was silently accepted");
        check(Arrays.equals(expectedUnmodifiedCorruptCopy, Files.readAllBytes(copy)),
                label + " caused recovery to modify corrupt WAL bytes");
        check(Arrays.equals(original, Files.readAllBytes(source)),
                label + " mutated the preserved original fixture");
        System.out.println("PASS rejected " + label + ": " + failure.getMessage());
    }

    private static void testInvalidBatchMetadata(Path source, byte[] original, Path directory)
            throws Exception {
        Path copy = copyFixture(source, directory, "bad-begin-count");
        byte[] begin = RecordCodec.encode(RecordType.BEGIN, 10, intPayload(0));
        append(copy, begin);
        byte[] invalidBytes = Files.readAllBytes(copy);
        Database database = new DatabaseImpl();
        IOException failure = null;
        try {
            database.open(copy.toString());
        } catch (IOException expected) {
            failure = expected;
        } finally {
            database.close();
        }
        check(failure != null && failure.getMessage().contains("operation count"),
                "invalid BEGIN count was not rejected");
        check(Arrays.equals(invalidBytes, Files.readAllBytes(copy)),
                "invalid batch metadata was silently truncated");
        check(Arrays.equals(original, Files.readAllBytes(source)),
                "invalid batch test changed source fixture");
        System.out.println("PASS invalid batch metadata: zero-operation BEGIN rejected without modifying WAL");
    }

    private static void testTruncatedHeaderIsRecoverable(Path source, byte[] original,
            Path directory) throws Exception {
        Path copy = copyFixture(source, directory, "truncated-header");
        byte[] partial = RecordCodec.encode(RecordType.PUT, 6,
                putPayload(bytes("tail-header"), bytes("value")));
        append(copy, Arrays.copyOf(partial, 11));
        verifyRecoverableTail(copy, original.length, "truncated header");
        check(Arrays.equals(original, Files.readAllBytes(source)),
                "truncated-header recovery modified original fixture");
    }

    private static void testTruncatedPayloadIsRecoverable(Path source, byte[] original,
            Path directory) throws Exception {
        Path copy = copyFixture(source, directory, "truncated-payload");
        byte[] record = RecordCodec.encode(RecordType.PUT, 6,
                putPayload(bytes("tail-payload"), bytes("a longer payload")));
        append(copy, Arrays.copyOf(record, RecordCodec.HEADER_SIZE + 5));
        verifyRecoverableTail(copy, original.length, "truncated payload");
        check(Arrays.equals(original, Files.readAllBytes(source)),
                "truncated-payload recovery modified original fixture");
    }

    private static void verifyRecoverableTail(Path copy, long expectedSize, String label)
            throws Exception {
        DatabaseImpl recovered = new DatabaseImpl();
        recovered.open(copy.toString());
        try {
            check(Arrays.equals(bytes("value-alpha"), recovered.get(bytes("alpha"))),
                    label + " recovery lost preceding committed record");
            check(Arrays.equals(bytes("value-middle"), recovered.get(bytes("middle"))),
                    label + " recovery lost middle committed record");
            check(Arrays.equals(bytes("value-end"), recovered.get(bytes("end"))),
                    label + " recovery lost last committed record");
            check(recovered.get(bytes("removed")) == null,
                    label + " recovery resurrected a deleted key");
            check(recovered.checkIntegrity() == 3,
                    label + " recovery returned incorrect key count");
        } finally {
            recovered.close();
        }
        check(Files.size(copy) == expectedSize,
                label + " recovery did not truncate at the prior valid boundary");
        System.out.println("PASS " + label + " tail truncated; prior records retained and deleted key absent");
    }

    private static Path createSource(Path path) throws Exception {
        Database database = new DatabaseImpl();
        database.open(path.toString());
        database.put(bytes("alpha"), bytes("value-alpha"));
        database.put(bytes("middle"), bytes("value-middle"));
        database.put(bytes("end"), bytes("value-end"));
        database.put(bytes("removed"), bytes("must-stay-deleted"));
        database.delete(bytes("removed"));
        database.close();
        return path;
    }

    private static Path copyFixture(Path source, Path directory, String name) throws IOException {
        Path copy = directory.resolve(name + ".db");
        Files.copy(source, copy);
        return copy;
    }

    private static void flipPayload(byte[] bytes, int recordNumber) {
        int offset = offsetOf(bytes, recordNumber);
        int keyLength = ByteBuffer.wrap(bytes, offset + RecordCodec.HEADER_SIZE, 4)
                .order(ByteOrder.BIG_ENDIAN).getInt();
        bytes[offset + RecordCodec.HEADER_SIZE + 8 + keyLength] ^= 0x40;
    }

    private static int offsetOf(byte[] bytes, int recordNumber) {
        int offset = 0;
        for (int i = 0; i < recordNumber; i++) {
            int payloadLength = ByteBuffer.wrap(bytes, offset + 16, 4)
                    .order(ByteOrder.BIG_ENDIAN).getInt();
            offset += RecordCodec.HEADER_SIZE + payloadLength;
        }
        return offset;
    }

    private static byte[] putPayload(byte[] key, byte[] value) {
        return ByteBuffer.allocate(8 + key.length + value.length).order(ByteOrder.BIG_ENDIAN)
                .putInt(key.length).putInt(value.length).put(key).put(value).array();
    }

    private static byte[] intPayload(int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(value).array();
    }

    private static void append(Path path, byte[] bytes) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            long position = channel.size();
            while (buffer.hasRemaining()) position += channel.write(buffer, position);
            channel.force(true);
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    @FunctionalInterface
    private interface Mutation { void apply(byte[] bytes); }
}
