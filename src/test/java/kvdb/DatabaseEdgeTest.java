package kvdb;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class DatabaseEdgeTest {

    public static void main(String[] args) throws Exception {
        Path path = Files.createTempFile("kvdb-edge-", ".db");

        try {
            Database db = new DatabaseImpl();
            db.open(path.toString());

            byte[] key = "name".getBytes();
            byte[] first = "Ranjeet".getBytes();
            byte[] second = "Manvith".getBytes();

            // 1. Overwrite
            db.put(key, first);
            db.put(key, second);
            check(Arrays.equals(db.get(key), second),
                    "Overwrite failed");
            System.out.println("PASS: Overwrite");

            // 2. Empty value
            db.put("empty".getBytes(), new byte[0]);
            check(db.get("empty".getBytes()).length == 0,
                    "Empty value failed");
            System.out.println("PASS: Empty value");

            // 3. Missing key
            check(db.get("missing".getBytes()) == null,
                    "Missing key should return null");
            System.out.println("PASS: Missing key");

            // 4. Multiple keys
            db.put("one".getBytes(), "1".getBytes());
            db.put("two".getBytes(), "2".getBytes());
            db.put("three".getBytes(), "3".getBytes());

            check(db.get("one".getBytes())[0] == '1', "Key one failed");
            check(db.get("two".getBytes())[0] == '2', "Key two failed");
            check(db.get("three".getBytes())[0] == '3', "Key three failed");
            System.out.println("PASS: Multiple keys");

            db.close();

            // 5. Persistence after reopening
            db = new DatabaseImpl();
            db.open(path.toString());

            check(Arrays.equals(db.get(key), second),
                    "Overwrite persistence failed");
            check(db.get("empty".getBytes()).length == 0,
                    "Empty value persistence failed");

            System.out.println("PASS: Edge-case persistence");

            db.close();

            System.out.println("\nALL EDGE TESTS PASSED!");

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