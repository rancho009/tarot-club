
package kvdb;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DatabaseCrashRecoveryTest {

    public static void main(String[] args) throws Exception {

        Path original = Files.createTempFile("kvdb-original-", ".db");
        List<Path> testFiles = new ArrayList<>();

        byte[] k1 = "one".getBytes();
        byte[] k2 = "two".getBytes();
        byte[] k3 = "three".getBytes();

        try {
            // Create one complete batch.
            Database db = new DatabaseImpl();
            db.open(original.toString());

            db.batch(List.of(
                    BatchOperation.put(k1, "first".getBytes()),
                    BatchOperation.put(k2, "second".getBytes()),
                    BatchOperation.put(k3, "third".getBytes())));

            db.close();

            byte[] log = Files.readAllBytes(original);

            // Find the end offset of every record.
            List<Integer> ends = new ArrayList<>();
            List<Integer> types = new ArrayList<>();

            int offset = 0;

            while (offset < log.length) {
                int payloadLength = ByteBuffer.wrap(log, offset + 16, 4)
                        .order(ByteOrder.BIG_ENDIAN)
                        .getInt();

                int type = Byte.toUnsignedInt(log[offset + 5]);

                offset += RecordCodec.HEADER_SIZE + payloadLength;

                ends.add(offset);
                types.add(type);
            }

            check(types.size() == 5, "Unexpected record count");
            check(types.get(0) == RecordType.BEGIN.getCode(),
                    "BEGIN missing");
            check(types.get(4) == RecordType.COMMIT.getCode(),
                    "COMMIT missing");

            // Test 1: Crash immediately after BEGIN.
            testFiles.add(testPrefix(log, ends.get(0), "begin"));

            // Test 2: Crash after first PUT, before remaining operations.
            testFiles.add(testPrefix(log, ends.get(1), "mid"));

            // Test 3: Complete COMMIT exists.
            // Recovery may apply the entire transaction.
            testFiles.add(testPrefix(log, ends.get(4), "commit"));

            checkAbsent(testFiles.get(0), k1, k2, k3);
            System.out.println("PASS: Recovery after BEGIN");

            checkAbsent(testFiles.get(1), k1, k2, k3);
            System.out.println("PASS: Recovery after partial batch");

            db = new DatabaseImpl();
            db.open(testFiles.get(2).toString());

            check(Arrays.equals(db.get(k1), "first".getBytes()),
                    "Committed batch key one missing");
            check(Arrays.equals(db.get(k2), "second".getBytes()),
                    "Committed batch key two missing");
            check(Arrays.equals(db.get(k3), "third".getBytes()),
                    "Committed batch key three missing");

            db.close();

            System.out.println("PASS: Recovery after complete COMMIT");
            System.out.println("\nALL CRASH RECOVERY TESTS PASSED!");

        } finally {
            Files.deleteIfExists(original);

            for (Path file : testFiles) {
                Files.deleteIfExists(file);
            }
        }
    }

    private static Path testPrefix(
            byte[] log, int length, String name) throws Exception {

        Path file = Files.createTempFile("kvdb-" + name, ".db");
        Files.write(file, Arrays.copyOf(log, length));
        return file;
    }

    private static void checkAbsent(
            Path file, byte[]... keys) throws Exception {

        Database db = new DatabaseImpl();
        db.open(file.toString());

        for (byte[] key : keys) {
            if (db.get(key) != null) {
                db.close();
                throw new AssertionError(
                        "Partial transaction became visible");
            }
        }

        db.close();
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
