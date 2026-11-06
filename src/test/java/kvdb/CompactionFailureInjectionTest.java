package kvdb;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/** Deterministic failure checks at each compaction publication boundary. */
public final class CompactionFailureInjectionTest {
    private CompactionFailureInjectionTest() { }

    public static void main(String[] args) throws Exception {
        runBeforeReplacement(Point.CREATE);
        runBeforeReplacement(Point.WRITE);
        runBeforeReplacement(Point.VALIDATE);
        runBeforeReplacement(Point.REPLACE);
        runAfterReplacement();
        System.out.println("ALL COMPACTION FAILURE INJECTION TESTS PASSED");
    }

    private static void runBeforeReplacement(Point point) throws Exception {
        Path path = fixture("tarot-compact-inject-" + point.name().toLowerCase());
        byte[] original = Files.readAllBytes(path);
        DatabaseImpl database = new DatabaseImpl(new InjectAt(point));
        database.open(path.toString());
        IOException failure = expectIOException(database::compact,
                point + " failure was not raised");
        if (point == Point.VALIDATE) {
            check(failure instanceof Recovery.CorruptWalException,
                    "candidate validator did not reject the injected corruption: " + failure);
        } else {
            check(failure instanceof AccessDeniedException,
                    point + " did not preserve the injected filesystem failure type");
        }
        check(Arrays.equals(original, Files.readAllBytes(path)),
                point + " failure changed the authoritative WAL");
        verifyState(database, "after " + point);
        database.close();
        assertNoCandidates(path);
        verifyAfterReopen(path, false);
        System.out.println("PASS compaction failure at " + point
                + ": original WAL preserved, live values retained, deletes not resurrected, candidate cleaned");
    }

    private static void runAfterReplacement() throws Exception {
        Path path = fixture("tarot-compact-inject-after-replace");
        long originalSize = Files.size(path);
        DatabaseImpl database = new DatabaseImpl(new InjectAt(Point.AFTER_REPLACE));
        database.open(path.toString());
        IOException failure = expectIOException(database::compact,
                "post-replacement injected failure was not raised");
        check(failure instanceof AccessDeniedException,
                "post-replacement failure lost its injected exception type");
        check(Files.size(path) < originalSize / 2,
                "replacement did not publish the compacted WAL before injected failure");
        verifyState(database, "after replacement failure and generation refresh");
        database.close();
        assertNoCandidates(path);
        verifyAfterReopen(path, true);
        System.out.println("PASS failure immediately after replacement: compacted WAL recovered; live data retained; deletes not resurrected");
    }

    private static Path fixture(String prefix) throws Exception {
        Path directory = Files.createTempDirectory(prefix);
        Path path = directory.resolve("data.db");
        Database database = new DatabaseImpl();
        database.open(path.toString());
        for (int version = 0; version < 8; version++) {
            for (int key = 0; key < 8; key++) {
                database.put(bytes("live-" + key), value(key, version));
                database.put(bytes("deleted-" + key), value(key, version));
            }
        }
        for (int key = 0; key < 8; key++) database.delete(bytes("deleted-" + key));
        database.close();
        return path;
    }

    private static void verifyState(DatabaseImpl database, String phase) throws Exception {
        for (int key = 0; key < 8; key++) {
            check(Arrays.equals(value(key, 7), database.get(bytes("live-" + key))),
                    phase + ": live value mismatch for " + key);
            check(database.get(bytes("deleted-" + key)) == null,
                    phase + ": deleted key resurrected for " + key);
        }
    }

    private static void verifyAfterReopen(Path path, boolean compacted) throws Exception {
        DatabaseImpl recovered = new DatabaseImpl();
        recovered.open(path.toString());
        try {
            verifyState(recovered, "reopen");
            check(recovered.dumpSnapshot().size() == 8,
                    "wrong live key count after compaction failure/replacement");
        } finally {
            recovered.close();
        }
        if (compacted) check(Files.size(path) < 16L * 1024,
                "post-replacement compacted WAL is unexpectedly large");
    }

    private static void assertNoCandidates(Path database) throws Exception {
        try (DirectoryStream<Path> candidates = Files.newDirectoryStream(
                database.getParent(), "data.db.compact.*.tmp")) {
            check(!candidates.iterator().hasNext(), "orphan candidate remained after handled failure");
        }
    }

    private static IOException expectIOException(CheckedRunnable action, String message)
            throws Exception {
        try {
            action.run();
        } catch (IOException expected) {
            return expected;
        }
        throw new AssertionError(message);
    }

    private static byte[] value(int key, int version) {
        byte[] value = new byte[1024];
        Arrays.fill(value, (byte) ('a' + key));
        value[0] = (byte) version;
        return value;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    private enum Point { CREATE, WRITE, VALIDATE, REPLACE, AFTER_REPLACE }

    private static final class InjectAt implements DatabaseFaultInjector {
        private final Point point;
        InjectAt(Point point) { this.point = point; }

        private void fail(Point at) throws IOException {
            if (point == at) throw new AccessDeniedException("injected-" + at);
        }

        @Override public void beforeCandidateCreate(Path candidate) throws IOException {
            fail(Point.CREATE);
        }

        @Override public void beforeCandidateRecord(Path candidate, long recordNumber)
                throws IOException {
            if (point == Point.WRITE && recordNumber == 1) fail(Point.WRITE);
        }

        @Override public void beforeCandidateValidation(Path candidate) throws IOException {
            if (point == Point.VALIDATE) {
                try (java.nio.channels.FileChannel channel = java.nio.channels.FileChannel.open(
                        candidate, java.nio.file.StandardOpenOption.READ,
                        java.nio.file.StandardOpenOption.WRITE)) {
                    java.nio.ByteBuffer checksumByte = java.nio.ByteBuffer.allocate(1);
                    if (channel.read(checksumByte, 20) != 1) {
                        throw new IOException("candidate header disappeared before validation");
                    }
                    checksumByte.flip();
                    checksumByte.put(0, (byte) (checksumByte.get(0) ^ 1));
                    checksumByte.position(0);
                    while (checksumByte.hasRemaining()) channel.write(checksumByte, 20);
                    channel.force(true);
                }
            }
        }

        @Override public void beforeCandidateReplacement(Path database, Path candidate)
                throws IOException {
            fail(Point.REPLACE);
        }

        @Override public void afterCandidateReplacement(Path database) throws IOException {
            fail(Point.AFTER_REPLACE);
        }
    }

    @FunctionalInterface
    private interface CheckedRunnable { void run() throws Exception; }
}
