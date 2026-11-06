package kvdb;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/** Compaction replacement, refresh, failure, and process-death checks. */
public final class CompactionTest {
    private CompactionTest() { }

    public static void main(String[] args) throws Exception {
        testEmptyDatabase();
        testLiveOverwrittenAndDeletedRecordsAcrossInstances();
        testFailedCandidatePreservesOriginal();
        testConcurrentWritesAndCompaction();
        testProcessTerminationDuringCandidateWrite();
        testProcessTerminationAfterReplacement();
        testGenerationBumpedBeforeReplacementState();
        System.out.println("ALL SAFE COMPACTION TESTS PASSED");
    }

    private static void testEmptyDatabase() throws Exception {
        Path path = tempDatabase("tarot-compact-empty-");
        DatabaseImpl database = new DatabaseImpl();
        database.open(path.toString());
        database.compact();
        check(Files.size(path) == 0, "empty compaction should leave an empty WAL");
        database.close();
        DatabaseImpl reopened = new DatabaseImpl();
        reopened.open(path.toString());
        try {
            check(reopened.get(bytes("missing")) == null,
                    "empty database returned a value after compact/reopen");
        } finally {
            reopened.close();
        }
        System.out.println("PASS empty compaction and reopen");
    }

    private static void testLiveOverwrittenAndDeletedRecordsAcrossInstances()
            throws Exception {
        Path path = tempDatabase("tarot-compact-live-");
        DatabaseImpl first = new DatabaseImpl();
        DatabaseImpl second = new DatabaseImpl();
        first.open(path.toString());
        second.open(path.toString());
        for (int version = 0; version < 24; version++) {
            List<BatchOperation> updates = new ArrayList<>(40);
            for (int key = 0; key < 40; key++) {
                updates.add(BatchOperation.put(key(key), value(key, version)));
            }
            first.batch(updates);
        }
        for (int key = 30; key < 40; key++) {
            first.delete(key(key));
        }
        long before = Files.size(path);
        first.compact();
        long after = Files.size(path);
        check(after < before / 2, "compaction did not substantially reduce file size");
        for (int key = 0; key < 30; key++) {
            check(Arrays.equals(value(key, 23), second.get(key(key))),
                    "stale instance did not refresh compacted live value");
        }
        for (int key = 30; key < 40; key++) {
            check(second.get(key(key)) == null, "deleted key returned after compaction");
        }
        second.put(bytes("after-compaction"), bytes("works"));
        check(Arrays.equals(bytes("works"), first.get(bytes("after-compaction"))),
                "writer could not append after generation refresh");
        first.close();
        second.close();

        DatabaseImpl reopened = new DatabaseImpl();
        reopened.open(path.toString());
        try {
            check(reopened.checkIntegrity() == 31, "wrong key count after compact/reopen");
            for (int key = 0; key < 30; key++) {
                check(Arrays.equals(value(key, 23), reopened.get(key(key))),
                        "live value missing after compact/reopen");
            }
        } finally {
            reopened.close();
        }
        System.out.printf("PASS live/overwrite/delete compaction: before=%d after=%d bytes%n",
                before, after);
    }

    private static void testFailedCandidatePreservesOriginal() throws Exception {
        Path path = tempDatabase("tarot-compact-fail-");
        DatabaseImpl writer = new DatabaseImpl();
        writer.open(path.toString());
        writer.put(bytes("one"), new byte[4096]);
        writer.put(bytes("two"), new byte[4096]);
        writer.close();
        long originalSize = Files.size(path);

        DatabaseImpl limited = new DatabaseImpl();
        limited.open(path.toString(), new DatabaseConfig(4096));
        try {
            try {
                limited.compact();
                throw new AssertionError("compaction candidate exceeded limit but succeeded");
            } catch (IOException expected) {
                check(expected.getMessage().contains("Maximum WAL file size exceeded"),
                        "unexpected compaction failure: " + expected.getMessage());
            }
            check(Files.size(path) == originalSize,
                    "failed candidate changed the authoritative file size");
            check(limited.get(bytes("one")).length == 4096,
                    "original database unreadable after candidate failure");
        } finally {
            limited.close();
        }
        DatabaseImpl reopened = new DatabaseImpl();
        reopened.open(path.toString());
        try {
            check(reopened.get(bytes("two")).length == 4096,
                    "original data lost after failed compaction");
        } finally {
            reopened.close();
        }
        System.out.println("PASS candidate size-limit failure preserved original database");
    }

    private static void testConcurrentWritesAndCompaction() throws Exception {
        Path path = tempDatabase("tarot-compact-concurrent-");
        DatabaseImpl compactor = new DatabaseImpl();
        DatabaseImpl writer = new DatabaseImpl();
        compactor.open(path.toString());
        writer.open(path.toString());
        for (int version = 0; version < 8; version++) {
            List<BatchOperation> batch = new ArrayList<>(500);
            for (int key = 0; key < 500; key++) {
                batch.add(BatchOperation.put(key(key), value(key, version)));
            }
            compactor.batch(batch);
        }
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch start = new CountDownLatch(1);
        Future<?> writes = executor.submit(() -> {
            try {
                start.await();
                for (int index = 0; index < 100; index++) {
                    writer.put(bytes("concurrent-" + index), bytes("value-" + index));
                }
            } catch (Exception e) {
                throw new AssertionError("concurrent writer failed", e);
            }
        });
        try {
            start.countDown();
            compactor.compact();
            writes.get();
            for (int index = 0; index < 100; index++) {
                check(Arrays.equals(bytes("value-" + index),
                                compactor.get(bytes("concurrent-" + index))),
                        "concurrent write lost across compaction");
            }
            check(compactor.get(key(499)) != null, "base key missing after concurrent compact");
        } finally {
            executor.shutdownNow();
            writer.close();
            compactor.close();
        }
        System.out.println("PASS concurrent writes serialized safely with compaction");
    }

    private static void testProcessTerminationDuringCandidateWrite() throws Exception {
        Path path = tempDatabase("tarot-compact-crash-");
        DatabaseImpl setup = new DatabaseImpl();
        setup.open(path.toString());
        final int keyCount = 80_000;
        for (int phase = 0; phase < 4; phase++) {
            for (int start = 0; start < keyCount; start += 10_000) {
                List<BatchOperation> operations = new ArrayList<>(10_000);
                int end = Math.min(start + 10_000, keyCount);
                for (int key = start; key < end; key++) {
                    operations.add(BatchOperation.put(key(key), smallValue(key, phase)));
                }
                setup.batch(operations);
            }
        }
        for (int start = keyCount / 2; start < keyCount; start += 10_000) {
            List<BatchOperation> deletions = new ArrayList<>(10_000);
            for (int key = start; key < start + 10_000; key++) {
                deletions.add(BatchOperation.delete(key(key)));
            }
            setup.batch(deletions);
        }
        setup.close();
        long originalSize = Files.size(path);

        String java = Path.of(System.getProperty("java.home"), "bin",
                System.getProperty("os.name").toLowerCase().contains("win")
                        ? "java.exe" : "java").toString();
        Process child = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
                CompactionWorker.class.getName(), path.toString())
                .redirectErrorStream(true).start();
        boolean killedDuringBuild = false;
        try (BufferedReader output = new BufferedReader(new InputStreamReader(
                child.getInputStream(), StandardCharsets.UTF_8))) {
            check("COMPACTION_START".equals(output.readLine()),
                    "compaction child failed to start");
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (System.nanoTime() < deadline && child.isAlive()) {
                try (DirectoryStream<Path> candidates = Files.newDirectoryStream(
                        path.getParent(), path.getFileName() + ".compact.*.tmp")) {
                    for (Path candidate : candidates) {
                        if (Files.size(candidate) > 0 && Files.size(path) == originalSize) {
                            child.destroyForcibly();
                            check(child.waitFor(30, TimeUnit.SECONDS),
                                    "compaction child did not terminate");
                            killedDuringBuild = true;
                            break;
                        }
                    }
                }
                if (!child.isAlive() || killedDuringBuild) break;
                Thread.sleep(1);
            }
            if (!killedDuringBuild) {
                String finalLine = output.readLine();
                if (child.isAlive()) child.waitFor(30, TimeUnit.SECONDS);
                throw new AssertionError("did not catch compaction before replacement; child="
                        + finalLine);
            }
        } finally {
            if (child.isAlive()) child.destroyForcibly();
        }

        check(Files.size(path) == originalSize,
                "killed candidate writer changed the authoritative WAL");
        DatabaseImpl recovered = new DatabaseImpl();
        recovered.open(path.toString());
        try {
            check(Arrays.equals(smallValue(0, 3), recovered.get(key(0))),
                    "live record lost after compaction process termination");
            check(recovered.get(key(keyCount - 1)) == null,
                    "deleted key returned after compaction process termination");
            check(Files.size(path) == originalSize,
                    "recovery modified the intact original WAL");
        } finally {
            recovered.close();
        }
        try (DirectoryStream<Path> candidates = Files.newDirectoryStream(
                path.getParent(), path.getFileName() + ".compact.*.tmp")) {
            check(!candidates.iterator().hasNext(),
                    "reopen did not clean the orphaned compaction candidate");
        }
        System.out.println("PASS process terminated during candidate write; old WAL recovered intact");
    }

    private static void testProcessTerminationAfterReplacement() throws Exception {
        Path path = tempDatabase("tarot-compact-after-move-");
        DatabaseImpl setup = new DatabaseImpl();
        setup.open(path.toString());
        for (int version = 0; version < 6; version++) {
            List<BatchOperation> batch = new ArrayList<>(1000);
            for (int key = 0; key < 1000; key++) {
                batch.add(BatchOperation.put(key(key), smallValue(key, version)));
            }
            setup.batch(batch);
        }
        List<BatchOperation> deletions = new ArrayList<>(500);
        for (int key = 500; key < 1000; key++) {
            deletions.add(BatchOperation.delete(key(key)));
        }
        setup.batch(deletions);
        setup.close();
        long originalSize = Files.size(path);

        String java = Path.of(System.getProperty("java.home"), "bin",
                System.getProperty("os.name").toLowerCase().contains("win")
                        ? "java.exe" : "java").toString();
        Process child = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
                CompactionWorker.class.getName(), path.toString(), "hold")
                .redirectErrorStream(true).start();
        try (BufferedReader output = new BufferedReader(new InputStreamReader(
                child.getInputStream(), StandardCharsets.UTF_8))) {
            check("COMPACTION_START".equals(output.readLine()),
                    "post-replacement child failed to start");
            check("COMPACTION_DONE".equals(output.readLine()),
                    "child did not complete atomic replacement");
            check(Files.size(path) < originalSize / 2,
                    "compacted file did not shrink before child termination");
            child.destroyForcibly();
            check(child.waitFor(30, TimeUnit.SECONDS), "post-replacement child did not terminate");
        } finally {
            if (child.isAlive()) child.destroyForcibly();
        }

        DatabaseImpl recovered = new DatabaseImpl();
        recovered.open(path.toString());
        try {
            check(recovered.checkIntegrity() == 500,
                    "post-replacement process termination lost keys");
            check(Arrays.equals(smallValue(1, 5), recovered.get(key(1))),
                    "post-replacement recovery returned incorrect value");
            check(recovered.get(key(999)) == null,
                    "deleted key returned after post-replacement termination");
        } finally {
            recovered.close();
        }
        System.out.println("PASS process terminated after atomic replacement; compacted WAL recovered");
    }

    private static void testGenerationBumpedBeforeReplacementState() throws Exception {
        Path path = tempDatabase("tarot-compact-generation-");
        DatabaseImpl setup = new DatabaseImpl();
        setup.open(path.toString());
        setup.put(bytes("stable"), bytes("old-file-is-still-authoritative"));
        setup.close();
        DatabaseImpl stale = new DatabaseImpl();
        stale.open(path.toString());

        Path lockPath = path.resolveSibling(path.getFileName() + ".lock");
        try (FileChannel channel = FileChannel.open(lockPath,
                StandardOpenOption.READ, StandardOpenOption.WRITE)) {
            ByteBuffer slots = ByteBuffer.allocate(32).order(ByteOrder.BIG_ENDIAN);
            while (slots.hasRemaining()) {
                if (channel.read(slots, slots.position()) < 0) {
                    throw new IOException("Incomplete generation test metadata");
                }
            }
            slots.flip();
            long firstGeneration = slots.getLong();
            long firstComplement = slots.getLong();
            long secondGeneration = slots.getLong();
            long secondComplement = slots.getLong();
            int activeSlot = secondGeneration >= firstGeneration ? 1 : 0;
            long current = activeSlot == 1 ? secondGeneration : firstGeneration;
            long activeComplement = activeSlot == 1 ? secondComplement : firstComplement;
            check(activeComplement == ~current, "invalid initial generation marker");
            long next = current + 1;
            ByteBuffer update = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN)
                    .putLong(next).putLong(~next);
            update.flip();
            long position = (long) (1 - activeSlot) * 16;
            while (update.hasRemaining()) {
                position += channel.write(update, position);
            }
            channel.force(true);
        }
        try {
            check(Arrays.equals(bytes("old-file-is-still-authoritative"),
                    stale.get(bytes("stable"))),
                    "generation-only crash state failed to recover old database");
        } finally {
            stale.close();
        }
        System.out.println("PASS durable generation bump before replacement recovers old WAL");
    }

    private static Path tempDatabase(String prefix) throws IOException {
        return Files.createTempDirectory(prefix).resolve("database.db");
    }

    private static byte[] key(int index) {
        return bytes("key-" + index);
    }

    private static byte[] value(int key, int version) {
        byte[] value = new byte[4096];
        ByteBufferWriter.put(value, key, version);
        Arrays.fill(value, 8, value.length, (byte) (key * 31 + version));
        return value;
    }

    private static byte[] smallValue(int key, int version) {
        byte[] value = new byte[64];
        ByteBufferWriter.put(value, key, version);
        Arrays.fill(value, 8, value.length, (byte) (key * 31 + version));
        return value;
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    private static final class ByteBufferWriter {
        private ByteBufferWriter() { }
        static void put(byte[] target, int key, int version) {
            target[0] = (byte) (key >>> 24);
            target[1] = (byte) (key >>> 16);
            target[2] = (byte) (key >>> 8);
            target[3] = (byte) key;
            target[4] = (byte) (version >>> 24);
            target[5] = (byte) (version >>> 16);
            target[6] = (byte) (version >>> 8);
            target[7] = (byte) version;
        }
    }
}
