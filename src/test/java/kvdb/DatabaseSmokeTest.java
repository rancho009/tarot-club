package kvdb;



import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class DatabaseSmokeTest {

    public static void main(String[] args) throws Exception {

        Path path = Files.createTempFile("tarot-kvdb-", ".db");

        byte[] key = "student".getBytes();
        byte[] value = "database-persistence-works".getBytes();

        try {
            // Test 1: Write and read.
            Database db = new DatabaseImpl();
            db.open(path.toString());

            db.put(key, value);

            check(
                    Arrays.equals(db.get(key), value),
                    "Immediate read failed");

            System.out.println("PASS: PUT and GET");

            // Test 2: Close and reopen.
            db.close();

            db = new DatabaseImpl();
            db.open(path.toString());

            check(
                    Arrays.equals(db.get(key), value),
                    "Data was lost after reopening");

            System.out.println("PASS: Persistence after reopening");

            // Test 3: Delete.
            db.delete(key);

            check(
                    db.get(key) == null,
                    "Deleted key still exists");

            System.out.println("PASS: DELETE");

            // Test 4: Delete persistence.
            db.close();

            db = new DatabaseImpl();
            db.open(path.toString());

            check(
                    db.get(key) == null,
                    "Deleted key returned after reopening");

            System.out.println("PASS: DELETE persistence");

            db.close();

            System.out.println("\nALL DATABASE SMOKE TESTS PASSED!");

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
