package kvdb;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/** Verifies that compaction reclaims space under the configured WAL limit. */
public final class FileSizeLimitAfterCompactionTest {
    private static final long LIMIT = 16_384;
    private FileSizeLimitAfterCompactionTest() { }

    public static void main(String[] args) throws Exception {
        Path path = Files.createTempDirectory("tarot-limit-compact-").resolve("limited.db");
        byte[] key = "repeated".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] value = new byte[512];
        Arrays.fill(value, (byte) 0x3c);
        int acknowledgements = 0;
        DatabaseImpl database = new DatabaseImpl();
        database.open(path.toString(), new DatabaseConfig(LIMIT));
        try {
            while (true) {
                value[0] = (byte) acknowledgements;
                try {
                    database.put(key, value);
                    acknowledgements++;
                } catch (IOException expectedLimit) {
                    check(expectedLimit.getMessage().contains("Maximum WAL file size exceeded"),
                            "unexpected configured-limit failure: " + expectedLimit.getMessage());
                    break;
                }
            }
        } finally {
            database.close();
        }
        check(acknowledgements > 1, "did not reach the configured file limit");
        long before = Files.size(path);
        check(before <= LIMIT, "rejected write changed file beyond configured limit");

        DatabaseImpl reopened = new DatabaseImpl();
        reopened.open(path.toString(), new DatabaseConfig(LIMIT));
        byte[] lastAcknowledged = value.clone();
        lastAcknowledged[0] = (byte) (acknowledgements - 1);
        check(Arrays.equals(lastAcknowledged, reopened.get(key)),
                "previously acknowledged value missing after rejected write/reopen");
        reopened.compact();
        long after = Files.size(path);
        check(after < before, "compaction did not reduce configured-limit database");

        byte[] newValue = new byte[8192];
        Arrays.fill(newValue, (byte) 0x7d);
        reopened.put("after-space-reclaimed".getBytes(
                java.nio.charset.StandardCharsets.UTF_8), newValue);
        check(Files.size(path) <= LIMIT, "write exceeded limit after compaction");
        reopened.close();

        DatabaseImpl finalOpen = new DatabaseImpl();
        finalOpen.open(path.toString(), new DatabaseConfig(LIMIT));
        try {
            check(Arrays.equals(lastAcknowledged, finalOpen.get(key)),
                    "old acknowledged value lost after compact/reopen");
            check(Arrays.equals(newValue, finalOpen.get("after-space-reclaimed".getBytes(
                    java.nio.charset.StandardCharsets.UTF_8))),
                    "post-compaction write did not recover");
        } finally {
            finalOpen.close();
        }
        System.out.printf("PASS configured limit=%d acknowledged-writes=%d "
                        + "bytes-before-compaction=%d bytes-after-compaction=%d bytes-after-write=%d%n",
                LIMIT, acknowledgements, before, after, Files.size(path));
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
