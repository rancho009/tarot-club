package kvdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Isolated one-million-key insert, recovery, delete, and recovery workload. */
public final class MillionKeyScaleTest {
    private static final int KEY_COUNT = 1_000_000;
    private static final int BATCH_SIZE = 10_000;
    private static final byte[] KEY_PREFIX = "million-key-".getBytes(StandardCharsets.UTF_8);
    private MillionKeyScaleTest() { }

    public static void main(String[] args) throws Exception {
        Path directory = Files.createTempDirectory("tarot-million-keys-");
        Path path = directory.resolve("million.db");
        long maxHeap = Runtime.getRuntime().maxMemory();
        long usedBefore = usedHeap();
        DatabaseImpl database = new DatabaseImpl();
        long initialOpenStart = System.nanoTime();
        database.open(path.toString());
        long initialOpenNanos = System.nanoTime() - initialOpenStart;

        long insertStart = System.nanoTime();
        applyBatches(database, false);
        long insertNanos = System.nanoTime() - insertStart;
        long usedAfterInsert = usedHeap();
        verifyRepresentativeKeys(database, true);
        database.close();
        long insertedSize = Files.size(path);
        long checkInsertStart = System.nanoTime();
        int insertedCount = checkKeyCount(path);
        long insertedRecoveryNanos = System.nanoTime() - checkInsertStart;
        check(insertedCount == KEY_COUNT,
                "expected one million inserted keys, found " + insertedCount);

        long reopenStart = System.nanoTime();
        database = new DatabaseImpl();
        database.open(path.toString());
        long reopenNanos = System.nanoTime() - reopenStart;
        verifyRepresentativeKeys(database, true);

        long deleteStart = System.nanoTime();
        applyBatches(database, true);
        long deleteNanos = System.nanoTime() - deleteStart;
        long usedAfterDelete = usedHeap();
        verifyRepresentativeKeys(database, false);
        database.close();
        long deletedSize = Files.size(path);
        long checkDeleteStart = System.nanoTime();
        int deletedCount = checkKeyCount(path);
        long deletedRecoveryNanos = System.nanoTime() - checkDeleteStart;
        check(deletedCount == 0, "expected empty database after deleting all keys, found "
                + deletedCount);

        long compactStart = System.nanoTime();
        database = new DatabaseImpl();
        database.open(path.toString());
        database.compact();
        long compactNanos = System.nanoTime() - compactStart;
        long compactedSize = Files.size(path);
        check(compactedSize < deletedSize, "compaction did not shrink one-million-key WAL: before="
                + deletedSize + " after=" + compactedSize);
        try {
            verifyRepresentativeKeys(database, false);
        } finally {
            database.close();
        }

        long finalOpenStart = System.nanoTime();
        database = new DatabaseImpl();
        database.open(path.toString());
        long finalOpenNanos = System.nanoTime() - finalOpenStart;
        try {
            verifyRepresentativeKeys(database, false);
        } finally {
            database.close();
        }
        long usedAtEnd = usedHeap();

        printMetric("insert", KEY_COUNT, insertNanos, insertedSize);
        printMetric("delete", KEY_COUNT, deleteNanos, deletedSize);
        System.out.printf("PASS million-key persistence: inserted-count=%d deleted-count=%d "
                        + "initial-open-ms=%.3f insert-check-recovery-ms=%.3f "
                        + "database-reopen-ms=%.3f delete-check-recovery-ms=%.3f "
                        + "compaction-ms=%.3f compacted-file-bytes=%d final-open-ms=%.3f "
                        + "max-heap=%d heap-before=%d heap-after-insert=%d "
                        + "heap-after-delete=%d heap-at-end=%d database=%s%n",
                insertedCount, KEY_COUNT, millis(initialOpenNanos),
                millis(insertedRecoveryNanos), millis(reopenNanos),
                millis(deletedRecoveryNanos), millis(compactNanos), compactedSize,
                millis(finalOpenNanos), maxHeap,
                usedBefore, usedAfterInsert, usedAfterDelete, usedAtEnd, path);
    }

    private static void applyBatches(DatabaseImpl database, boolean delete)
            throws IOException {
        for (int start = 0; start < KEY_COUNT; start += BATCH_SIZE) {
            List<BatchOperation> batch = new ArrayList<>(BATCH_SIZE);
            for (int offset = start; offset < start + BATCH_SIZE; offset++) {
                byte[] key = key(offset);
                batch.add(delete ? BatchOperation.delete(key)
                        : BatchOperation.put(key, value(offset)));
            }
            database.batch(batch);
            if ((start + BATCH_SIZE) % 100_000 == 0) {
                System.out.printf("progress %s=%d/%d%n",
                        delete ? "deleted" : "inserted", start + BATCH_SIZE, KEY_COUNT);
            }
        }
    }

    private static void verifyRepresentativeKeys(DatabaseImpl database, boolean present)
            throws IOException {
        for (int i = 0; i < KEY_COUNT; i += 997) {
            byte[] actual = database.get(key(i));
            if (present) {
                check(java.util.Arrays.equals(value(i), actual),
                        "value mismatch at key index " + i);
            } else {
                check(actual == null, "deleted key remains at index " + i);
            }
        }
        int last = KEY_COUNT - 1;
        byte[] actualLast = database.get(key(last));
        check(present ? java.util.Arrays.equals(value(last), actualLast) : actualLast == null,
                "last key state mismatch");
    }

    private static int checkKeyCount(Path databasePath) throws Exception {
        String executable = Path.of(System.getProperty("java.home"), "bin",
                System.getProperty("os.name").toLowerCase().contains("win")
                        ? "java.exe" : "java").toString();
        Process process = new ProcessBuilder(executable, "-Xmx2g", "-cp",
                System.getProperty("java.class.path"), "kvdb.Main", "check",
                databasePath.toString()).redirectErrorStream(true).start();
        if (!process.waitFor(10, TimeUnit.MINUTES)) {
            process.destroyForcibly();
            throw new IOException("CHECK subprocess timed out");
        }
        byte[] output = process.getInputStream().readAllBytes();
        String text = new String(output, StandardCharsets.UTF_8).trim();
        check(process.exitValue() == 0, "CHECK subprocess failed: " + text);
        String prefix = "CHECK OK keys=";
        check(text.startsWith(prefix), "unexpected CHECK output: " + text);
        return Integer.parseInt(text.substring(prefix.length()));
    }

    private static byte[] key(int index) {
        byte[] key = new byte[KEY_PREFIX.length + 7];
        System.arraycopy(KEY_PREFIX, 0, key, 0, KEY_PREFIX.length);
        int remaining = index;
        for (int position = key.length - 1; position >= KEY_PREFIX.length; position--) {
            key[position] = (byte) ('0' + remaining % 10);
            remaining /= 10;
        }
        return key;
    }

    private static byte[] value(int index) {
        return ByteBuffer.allocate(16).putInt(index).putInt(~index)
                .putLong(0x5441524F54000000L ^ index).array();
    }

    private static long usedHeap() {
        Runtime runtime = Runtime.getRuntime();
        return runtime.totalMemory() - runtime.freeMemory();
    }

    private static double millis(long nanos) { return nanos / 1_000_000.0; }

    private static void printMetric(String operation, int count, long nanos, long fileSize) {
        double seconds = nanos / 1_000_000_000.0;
        System.out.printf("%s: operations=%d elapsed-seconds=%.3f ops-per-second=%.1f file-bytes=%d%n",
                operation, count, seconds, count / seconds, fileSize);
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
