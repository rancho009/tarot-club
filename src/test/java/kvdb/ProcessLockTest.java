package kvdb;

import java.nio.file.Files;
import java.nio.file.Path;

public class ProcessLockTest {

    public static void main(String[] args) throws Exception {

        Path path = Files.createTempFile("kvdb-lock-", ".db");

        Database first = new DatabaseImpl();
        Database second = new DatabaseImpl();

        try {
            // Process simulation: first instance acquires the lock.
            first.open(path.toString());
            System.out.println("PASS: First database opened");

            // A second instance in the same JVM must be rejected.
            boolean rejected = false;

            try {
                second.open(path.toString());
            } catch (Exception expected) {
                rejected = true;
            }

            check(rejected, "Second database should be rejected");

            System.out.println("PASS: Second database rejected");

            first.close();
            System.out.println("PASS: First database closed");

            // Lock should now be available.
            second.open(path.toString());

            System.out.println("PASS: Second database opened after release");

            second.close();

            System.out.println("\nALL PROCESS LOCK TESTS PASSED!");

        } finally {
            first.close();
            second.close();
            Files.deleteIfExists(path);
            Files.deleteIfExists(
                    path.resolveSibling(path.getFileName() + ".lock"));
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}