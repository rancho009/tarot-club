package kvdb;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Exercises the public API's maximum-size atomic batch. */
public final class TenThousandBatchStressTest {
    private static final int BATCH_OPERATIONS = 10_000;
    private TenThousandBatchStressTest() { }

    public static void main(String[] args) throws Exception {
        Path directory = Files.createTempDirectory("tarot-batch-10000-");
        Path path = directory.resolve("batch.db");
        DatabaseImpl database = new DatabaseImpl();
        database.open(path.toString());
        Map<String, byte[]> expected = new HashMap<>();
        byte[] updatedKey = bytes("existing-update");
        byte[] deletedKey = bytes("existing-delete");
        database.put(updatedKey, bytes("old"));
        database.put(deletedKey, bytes("remove-me"));
        expected.put(text(updatedKey), bytes("old"));
        expected.put(text(deletedKey), bytes("remove-me"));

        long walBefore = Files.size(path);
        byte[] boundaryKey = new byte[1024];
        Arrays.fill(boundaryKey, (byte) 'K');
        byte[] boundaryValue = new byte[1024 * 1024];
        Arrays.fill(boundaryValue, (byte) 0x5a);
        byte[] emptyKey = bytes("final-empty-value");
        List<BatchOperation> operations = new ArrayList<>(BATCH_OPERATIONS);

        for (int phase = 0; phase < 10; phase++) {
            for (int keyIndex = 0; keyIndex < 1000; keyIndex++) {
                byte[] key;
                BatchOperation operation;
                if (phase == 0 && keyIndex == 0) {
                    key = updatedKey;
                    operation = BatchOperation.put(key, bytes("new"));
                } else if (phase == 0 && keyIndex == 1) {
                    key = deletedKey;
                    operation = BatchOperation.delete(key);
                } else if (phase == 9 && keyIndex == 997) {
                    key = deletedKey;
                    operation = BatchOperation.delete(key);
                } else if (phase == 9 && keyIndex == 998) {
                    key = emptyKey;
                    operation = BatchOperation.put(key, new byte[0]);
                } else if (phase == 9 && keyIndex == 999) {
                    key = boundaryKey;
                    operation = BatchOperation.put(key, boundaryValue);
                } else {
                    key = bytes("duplicate-key-" + keyIndex);
                    operation = createOperation(phase, keyIndex, key);
                }
                operations.add(operation);
                updateExpected(expected, operation);
            }
        }
        check(operations.size() == BATCH_OPERATIONS,
                "generated batch does not contain exactly 10,000 operations");

        long usedBefore = usedHeap();
        long batchStart = System.nanoTime();
        database.batch(operations);
        long batchNanos = System.nanoTime() - batchStart;
        long usedAfterBatch = usedHeap();
        long walAfter = Files.size(path);
        verifyDump(database.dumpSnapshot(), expected);

        List<BatchOperation> invalid = Arrays.asList(
                BatchOperation.put(bytes("invalid-prefix"), bytes("must-not-appear")), null);
        try {
            database.batch(invalid);
            throw new AssertionError("null-containing batch was accepted");
        } catch (IllegalArgumentException expectedFailure) {
            check(database.get(bytes("invalid-prefix")) == null,
                    "rejected batch applied a valid prefix");
            check(Arrays.equals(bytes("new"), database.get(updatedKey)),
                    "rejected batch changed earlier committed data");
        }
        database.put(bytes("still-usable"), bytes("yes"));
        expected.put("still-usable", bytes("yes"));
        database.close();

        long openStart = System.nanoTime();
        DatabaseImpl reopened = new DatabaseImpl();
        reopened.open(path.toString());
        long recoveryOpenNanos = System.nanoTime() - openStart;
        try {
            verifyDump(reopened.dumpSnapshot(), expected);
            check(reopened.get(deletedKey) == null, "deleted key returned after reopen");
            check(reopened.get(emptyKey) != null
                            && reopened.get(emptyKey).length == 0,
                    "empty value was not preserved");
            check(Arrays.equals(boundaryValue, reopened.get(boundaryKey)),
                    "boundary-sized key/value was not preserved");
        } finally {
            reopened.close();
        }

        long expectedWalDelta = walAfter - walBefore;
        double seconds = batchNanos / 1_000_000_000.0;
        System.out.printf("PASS batch-operations=%d final-keys=%d batch-ms=%.3f "
                        + "wal-before=%d wal-after=%d wal-delta=%d database-bytes=%d "
                        + "recovery-open-ms=%.3f used-heap-before=%d used-heap-after=%d%n",
                operations.size(), expected.size(), batchNanos / 1_000_000.0,
                walBefore, walAfter, expectedWalDelta, Files.size(path),
                recoveryOpenNanos / 1_000_000.0, usedBefore, usedAfterBatch);
        System.out.println("PASS duplicate order, mixed updates/deletes, empty value, "
                + "1024-byte key, 1 MiB value, rejected batch atomicity, reopen persistence");
    }

    private static BatchOperation createOperation(int phase, int keyIndex, byte[] key) {
        if (phase == 1 || (phase == 4 && keyIndex % 4 == 0)
                || (phase == 6 && keyIndex % 5 == 0)
                || (phase == 8 && keyIndex % 2 == 0)) {
            return BatchOperation.delete(key);
        }
        byte[] value = (phase == 3 && keyIndex % 3 == 0)
                || (phase == 7 && keyIndex % 7 == 0)
                        ? new byte[0]
                        : bytes("value-" + phase + "-" + keyIndex);
        return BatchOperation.put(key, value);
    }

    private static void updateExpected(Map<String, byte[]> expected,
            BatchOperation operation) {
        String key = text(operation.getKey());
        if (operation.getType() == BatchOperation.Type.DELETE) {
            expected.remove(key);
        } else {
            expected.put(key, operation.getValue());
        }
    }

    private static void verifyDump(List<DatabaseImpl.DumpEntry> dump,
            Map<String, byte[]> expected) {
        check(dump.size() == expected.size(),
                "key count mismatch: actual=" + dump.size() + ", expected=" + expected.size());
        for (DatabaseImpl.DumpEntry entry : dump) {
            byte[] value = expected.get(text(entry.key()));
            check(value != null, "unexpected key in database dump");
            check(Arrays.equals(value, entry.value()), "value mismatch in database dump");
        }
    }

    private static long usedHeap() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
