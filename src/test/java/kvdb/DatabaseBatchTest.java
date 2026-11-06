
package kvdb;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

public class DatabaseBatchTest {

    public static void main(String[] args) throws Exception {

        Path path = Files.createTempFile("kvdb-batch-", ".db");

        try {
            Database db = new DatabaseImpl();
            db.open(path.toString());

            byte[] key1 = "one".getBytes();
            byte[] key2 = "two".getBytes();
            byte[] key3 = "three".getBytes();

            db.batch(List.of(
                    BatchOperation.put(key1, "first".getBytes()),
                    BatchOperation.put(key2, "second".getBytes()),
                    BatchOperation.put(key3, "third".getBytes())));

            check(Arrays.equals(db.get(key1), "first".getBytes()),
                    "First batch value failed");
            check(Arrays.equals(db.get(key2), "second".getBytes()),
                    "Second batch value failed");
            check(Arrays.equals(db.get(key3), "third".getBytes()),
                    "Third batch value failed");

            System.out.println("PASS: Batch PUT");

            db.batch(List.of(
                    BatchOperation.delete(key1),
                    BatchOperation.put(key2, "updated".getBytes()),
                    BatchOperation.put("four".getBytes(), "fourth".getBytes())));

            check(db.get(key1) == null, "Batch DELETE failed");
            check(Arrays.equals(db.get(key2), "updated".getBytes()),
                    "Batch overwrite failed");
            check(Arrays.equals(db.get("four".getBytes()),
                    "fourth".getBytes()), "Batch insertion failed");

            System.out.println("PASS: Mixed batch operations");

            db.close();

            db = new DatabaseImpl();
            db.open(path.toString());

            check(db.get(key1) == null, "Delete recovery failed");
            check(Arrays.equals(db.get(key2), "updated".getBytes()),
                    "Batch recovery failed");
            check(Arrays.equals(db.get(key3), "third".getBytes()),
                    "Earlier batch recovery failed");

            System.out.println("PASS: Batch recovery after reopen");

            db.close();

            System.out.println("\nALL BATCH TESTS PASSED!");

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
