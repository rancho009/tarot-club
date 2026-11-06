package kvdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.zip.CRC32C;

/** Focused verification for concurrency, batch validation, and WAL semantics. */
public final class ConcurrencyAndWalValidationTest {
    private ConcurrencyAndWalValidationTest() { }

    public static void main(String[] args) throws Exception {
        runConcurrency(4);
        runConcurrency(8);
        testNullBatchOperations();
        testValidCrcInvalidCommit();
        System.out.println("ALL CONCURRENCY / WAL VALIDATION TESTS PASSED");
    }

    private static void runConcurrency(int threadCount) throws Exception {
        Path directory = Files.createTempDirectory("tarot-concurrency-");
        Path databasePath = directory.resolve("shared.db");
        CyclicBarrier start = new CyclicBarrier(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<Integer>> results = new ArrayList<>();
        try {
            for (int worker = 0; worker < threadCount; worker++) {
                int workerId = worker;
                results.add(executor.submit(() -> {
                    Database database = new DatabaseImpl();
                    database.open(databasePath.toString());
                    try {
                        database.put(key(workerId, "batch-deleted"), bytes("present-before-batch"));
                        start.await();
                        int calls = 1;
                        byte[] key = key(workerId, "single");
                        byte[] value = value(workerId, -1);
                        database.put(key, value);
                        calls++;
                        check(Arrays.equals(value, database.get(key)), "concurrent GET mismatch");
                        calls++;
                        database.delete(key);
                        calls++;
                        check(database.get(key) == null, "deleted key remained visible");
                        calls++;
                        for (int iteration = 0; iteration < 40; iteration++) {
                            byte[] firstKey = key(workerId, "batch-a-" + iteration);
                            byte[] secondKey = key(workerId, "batch-b-" + iteration);
                            byte[] firstValue = value(workerId, iteration * 2);
                            byte[] secondValue = value(workerId, iteration * 2 + 1);
                            database.batch(List.of(
                                    BatchOperation.put(firstKey, firstValue),
                                    BatchOperation.put(secondKey, secondValue),
                                    BatchOperation.delete(key(workerId, "batch-deleted"))));
                            calls++;
                            check(Arrays.equals(firstValue, database.get(firstKey)),
                                    "first batch value mismatch");
                            check(Arrays.equals(secondValue, database.get(secondKey)),
                                    "second batch value mismatch");
                            calls += 2;
                        }
                        return calls;
                    } finally {
                        database.close();
                    }
                }));
            }
            int operations = 0;
            List<String> threadErrors = new ArrayList<>();
            for (int worker = 0; worker < results.size(); worker++) {
                try {
                    operations += results.get(worker).get();
                } catch (ExecutionException e) {
                    threadErrors.add("thread " + worker + ": " + e.getCause());
                }
            }
            if (!threadErrors.isEmpty()) {
                throw new AssertionError("Worker exceptions: " + String.join("; ", threadErrors));
            }
            check(operations == threadCount * 125, "unexpected operation count: " + operations);

            Database reopened = new DatabaseImpl();
            reopened.open(databasePath.toString());
            try {
                for (int worker = 0; worker < threadCount; worker++) {
                    check(reopened.get(key(worker, "single")) == null,
                            "deleted key returned after reopen");
                    check(reopened.get(key(worker, "batch-deleted")) == null,
                            "batch delete did not persist");
                    check(Arrays.equals(value(worker, 78),
                                    reopened.get(key(worker, "batch-a-39"))),
                            "last first batch value missing after reopen");
                    check(Arrays.equals(value(worker, 79),
                                    reopened.get(key(worker, "batch-b-39"))),
                            "last second batch value missing after reopen");
                }
            } finally {
                reopened.close();
            }
            System.out.printf("PASS %d-thread shared-path concurrency: %d API operations, 0 exceptions%n",
                    threadCount, operations);
        } finally {
            executor.shutdownNow();
        }
    }

    private static void testNullBatchOperations() throws Exception {
        Path directory = Files.createTempDirectory("tarot-null-batch-");
        Path path = directory.resolve("null.db");
        byte[] existingKey = bytes("committed");
        byte[] existingValue = bytes("preserved");
        Database database = new DatabaseImpl();
        database.open(path.toString());
        try {
            database.put(existingKey, existingValue);
            List<List<BatchOperation>> invalidCases = List.of(
                    Arrays.asList((BatchOperation) null),
                    Arrays.asList(BatchOperation.put(bytes("prefix"), bytes("bad")), null),
                    Arrays.asList(null, BatchOperation.put(bytes("suffix"), bytes("bad"))));
            for (List<BatchOperation> invalid : invalidCases) {
                expectInvalid(() -> database.batch(invalid));
                check(Arrays.equals(existingValue, database.get(existingKey)),
                        "existing value changed after rejected batch");
                check(database.get(bytes("prefix")) == null, "valid prefix was persisted");
                check(database.get(bytes("suffix")) == null, "valid suffix was persisted");
            }
            database.put(bytes("still-usable"), bytes("yes"));
        } finally {
            database.close();
        }
        Database reopened = new DatabaseImpl();
        reopened.open(path.toString());
        try {
            check(Arrays.equals(existingValue, reopened.get(existingKey)),
                    "prior committed value lost after reopen");
            check(reopened.get(bytes("prefix")) == null, "prefix appeared after reopen");
            check(reopened.get(bytes("suffix")) == null, "suffix appeared after reopen");
            check(Arrays.equals(bytes("yes"), reopened.get(bytes("still-usable"))),
                    "database unusable after rejected batch");
        } finally {
            reopened.close();
        }
        System.out.println("PASS direct API null batch validation: 3 cases; no partial effects");
    }

    private static void testValidCrcInvalidCommit() throws Exception {
        Path directory = Files.createTempDirectory("tarot-invalid-commit-");
        Path fixture = directory.resolve("invalid-commit.db");
        Path committedPrefix = directory.resolve("committed-prefix.db");
        byte[] priorKey = bytes("prior");
        byte[] priorValue = bytes("safe");
        byte[] baseRecord = RecordCodec.encode(RecordType.PUT, 1,
                putPayload(priorKey, priorValue));
        byte[] beginRecord = RecordCodec.encode(RecordType.BEGIN, 2, intPayload(2));
        byte[] firstOperation = RecordCodec.encode(RecordType.PUT, 2,
                putPayload(bytes("batch-one"), bytes("one")));
        byte[] secondOperation = RecordCodec.encode(RecordType.DELETE, 2,
                deletePayload(bytes("batch-two")));
        CRC32C transactionCrc = new CRC32C();
        transactionCrc.update(firstOperation, 0, firstOperation.length);
        transactionCrc.update(secondOperation, 0, secondOperation.length);
        ByteBuffer commitPayload = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        commitPayload.putInt(3); // Invalid: BEGIN and actual operation count are both 2.
        commitPayload.putInt((int) transactionCrc.getValue());
        byte[] invalidCommit = RecordCodec.encode(RecordType.COMMIT, 2, commitPayload.array());

        byte[] wal = concatenate(baseRecord, beginRecord, firstOperation, secondOperation,
                invalidCommit);
        byte[] prefix = baseRecord.clone();
        Files.write(fixture, wal);
        Files.write(committedPrefix, prefix);
        byte[] originalFixture = Files.readAllBytes(fixture);

        independentlyVerifyAllRecordCrcs(wal);
        Database rejected = new DatabaseImpl();
        boolean failedClosed = false;
        try {
            rejected.open(fixture.toString());
        } catch (IOException expected) {
            failedClosed = expected.getMessage().contains("operation count mismatch");
        } finally {
            rejected.close();
        }
        check(failedClosed, "recovery did not reject semantic COMMIT count mismatch");
        check(Arrays.equals(originalFixture, Files.readAllBytes(fixture)),
                "recovery modified invalid fixture bytes");

        Database prefixDatabase = new DatabaseImpl();
        prefixDatabase.open(committedPrefix.toString());
        try {
            check(Arrays.equals(priorValue, prefixDatabase.get(priorKey)),
                    "prior valid committed record did not recover");
            check(prefixDatabase.get(bytes("batch-one")) == null,
                    "invalid batch PUT became visible");
            check(prefixDatabase.get(bytes("batch-two")) == null,
                    "invalid batch DELETE became visible");
        } finally {
            prefixDatabase.close();
        }
        System.out.println("PASS valid-CRC invalid COMMIT: independently verified CRC32C; "
                + "recovery rejected count mismatch and preserved fixture bytes");
    }

    private static void independentlyVerifyAllRecordCrcs(byte[] wal) throws IOException {
        int offset = 0;
        while (offset < wal.length) {
            ByteBuffer header = ByteBuffer.wrap(wal, offset, RecordCodec.HEADER_SIZE)
                    .order(ByteOrder.BIG_ENDIAN);
            int payloadLength = header.getInt(offset + 16);
            int storedCrc = header.getInt(offset + 20);
            CRC32C independent = new CRC32C();
            independent.update(wal, offset, 20);
            independent.update(wal, offset + RecordCodec.HEADER_SIZE, payloadLength);
            check((int) independent.getValue() == storedCrc,
                    "independent outer CRC32C verification failed");
            RecordCodec.decode(ByteBuffer.wrap(wal, offset,
                    RecordCodec.HEADER_SIZE + payloadLength));
            offset += RecordCodec.HEADER_SIZE + payloadLength;
        }
        check(offset == wal.length, "fixture has trailing bytes");
    }

    private static byte[] putPayload(byte[] key, byte[] value) {
        ByteBuffer buffer = ByteBuffer.allocate(8 + key.length + value.length)
                .order(ByteOrder.BIG_ENDIAN);
        return buffer.putInt(key.length).putInt(value.length).put(key).put(value).array();
    }

    private static byte[] deletePayload(byte[] key) {
        ByteBuffer buffer = ByteBuffer.allocate(4 + key.length).order(ByteOrder.BIG_ENDIAN);
        return buffer.putInt(key.length).put(key).array();
    }

    private static byte[] intPayload(int value) {
        return ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(value).array();
    }

    private static byte[] concatenate(byte[]... records) {
        int length = Arrays.stream(records).mapToInt(record -> record.length).sum();
        ByteBuffer buffer = ByteBuffer.allocate(length);
        for (byte[] record : records) buffer.put(record);
        return buffer.array();
    }

    private static byte[] key(int worker, String suffix) {
        return bytes("worker-" + worker + "/" + suffix);
    }

    private static byte[] value(int worker, int sequence) {
        return bytes("value-" + worker + "-" + sequence);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static void expectInvalid(CheckedRunnable runnable) throws Exception {
        try {
            runnable.run();
            throw new AssertionError("null-containing batch was accepted");
        } catch (IllegalArgumentException expected) {
            // Expected validation result.
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    @FunctionalInterface
    private interface CheckedRunnable { void run() throws Exception; }
}
