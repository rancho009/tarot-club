
package kvdb;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Arrays;
import java.io.IOException;

public class DatabaseDiskFullTest {

    public static void main(String[] args) throws Exception {

        Path path = Files.createTempFile("kvdb-disk-", ".db");

        byte[] safeKey = "safe".getBytes();
        byte[] safeValue = "ok".getBytes();

        byte[] newKey1 = "new1".getBytes();
        byte[] newKey2 = "new2".getBytes();

        try {
            Database db = new DatabaseImpl();

            // Deliberately small maximum file size.
            db.open(path.toString(), new DatabaseConfig(100));

            // First committed write should fit.
            db.put(safeKey, safeValue);

            check(Arrays.equals(db.get(safeKey), safeValue),
                    "Initial write failed");

            System.out.println("PASS: Initial durable write");

            // This batch cannot fit within the remaining file capacity.
            byte[] largeValue = new byte[20];

            boolean failedAsExpected = false;

            try {
                db.batch(List.of(
                        BatchOperation.put(newKey1, largeValue),
                        BatchOperation.put(newKey2, largeValue)));
            } catch (IOException expected) {
                failedAsExpected = true;
            }

            check(failedAsExpected,
                    "Oversized batch should fail");

            System.out.println("PASS: Oversized batch rejected");

            // Close and reopen to test recovery.
            db.close();

            db = new DatabaseImpl();
            db.open(path.toString(), new DatabaseConfig(100));

            check(Arrays.equals(db.get(safeKey), safeValue),
                    "Previously committed data was lost");

            check(db.get(newKey1) == null,
                    "Partial batch operation one was recovered");

            check(db.get(newKey2) == null,
                    "Partial batch operation two was recovered");

            System.out.println("PASS: Previous data preserved");
            System.out.println("PASS: Failed batch not recovered");

            db.close();

            System.out.println("\nALL DISK-FULL TESTS PASSED!");

        } finally {
            Files.deleteIfExists(path);
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
