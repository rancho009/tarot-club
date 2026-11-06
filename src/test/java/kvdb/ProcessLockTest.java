package kvdb;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProcessLockTest {

    public static void main(String[] args) throws Exception {
        Path path = Files.createTempFile("kvdb-lock-", ".db");
        try {
            testSameJvm(path);
            testSubprocessCoordination(path);
            testWaitsForOtherProcessLock(path);
            testNormalCloseReleasesFileLock(path);
            testProcessTerminationReleasesFileLock(path);
            System.out.println("ALL MULTI-PROCESS LOCK TESTS PASSED!");
        } finally {
            Files.deleteIfExists(path);
            Files.deleteIfExists(path.resolveSibling(path.getFileName() + ".lock"));
        }
    }

    private static void testSameJvm(Path path) throws Exception {
        Database first = new DatabaseImpl();
        Database second = new DatabaseImpl();
        try {
            first.open(path.toString());
            second.open(path.toString());
            CountDownLatch start = new CountDownLatch(1);
            Thread a = new Thread(() -> runWrites(first, "thread-a", start));
            Thread b = new Thread(() -> runWrites(second, "thread-b", start));
            a.start();
            b.start();
            start.countDown();
            a.join();
            b.join();
            check(Arrays.equals(first.get("thread-abatch".getBytes(StandardCharsets.UTF_8)),
                    "done".getBytes(StandardCharsets.UTF_8)), "Thread A write missing");
            check(Arrays.equals(second.get("thread-bbatch".getBytes(StandardCharsets.UTF_8)),
                    "done".getBytes(StandardCharsets.UTF_8)), "Thread B write missing");
            System.out.println("PASS: Same-JVM instances and threads coordinate");
        } finally {
            first.close();
            second.close();
        }
    }

    private static void runWrites(Database db, String prefix, CountDownLatch start) {
        try {
            start.await();
            for (int i = 0; i < 20; i++) {
                db.put((prefix + i).getBytes(StandardCharsets.UTF_8), new byte[] {(byte) i});
            }
            db.batch(List.of(
                    BatchOperation.put((prefix + "batch").getBytes(StandardCharsets.UTF_8),
                            "done".getBytes(StandardCharsets.UTF_8)),
                    BatchOperation.delete((prefix + "0").getBytes(StandardCharsets.UTF_8))));
        } catch (Exception e) {
            throw new AssertionError("Concurrent writer failed", e);
        }
    }

    private static void testSubprocessCoordination(Path path) throws Exception {
        Database parent = new DatabaseImpl();
        Process child = null;
        try {
            parent.open(path.toString());
            child = startWorker("coordinate", path.toString());
            BufferedReader output = new BufferedReader(new InputStreamReader(
                    child.getInputStream(), StandardCharsets.UTF_8));
            BufferedWriter input = new BufferedWriter(new OutputStreamWriter(
                    child.getOutputStream(), StandardCharsets.UTF_8));
            check("OPENED".equals(output.readLine()), "Child could not open shared database");
            parent.put("parent-process".getBytes(StandardCharsets.UTF_8),
                    "parent".getBytes(StandardCharsets.UTF_8));
            input.write("GO\n");
            input.flush();
            check("DONE".equals(output.readLine()), "Child did not finish its operations");
            check(child.waitFor(10, TimeUnit.SECONDS) && child.exitValue() == 0,
                    "Child process failed");
            check(Arrays.equals(parent.get("child-process".getBytes(StandardCharsets.UTF_8)),
                    "child".getBytes(StandardCharsets.UTF_8)), "Child write not visible in parent");
            check(Arrays.equals(parent.get("parent-process".getBytes(StandardCharsets.UTF_8)),
                    "parent".getBytes(StandardCharsets.UTF_8)), "Parent write missing");
            System.out.println("PASS: Separate processes open and coordinate database operations");
        } finally {
            if (child != null && child.isAlive()) child.destroyForcibly().waitFor();
            parent.close();
        }
    }

    private static void testProcessTerminationReleasesFileLock(Path path) throws Exception {
        Process locker = startWorker("hold-lock", path.resolveSibling(
                path.getFileName() + ".lock").toString());
        try {
            BufferedReader output = new BufferedReader(new InputStreamReader(
                    locker.getInputStream(), StandardCharsets.UTF_8));
            check("LOCKED".equals(output.readLine()), "Worker did not acquire OS file lock");
            locker.destroyForcibly();
            check(locker.waitFor(10, TimeUnit.SECONDS), "Lock holder did not terminate");
            Database db = new DatabaseImpl();
            try {
                db.open(path.toString());
                db.put("after-kill".getBytes(StandardCharsets.UTF_8), new byte[] {1});
            } finally {
                db.close();
            }
            System.out.println("PASS: OS lock released after holder process termination");
        } finally {
            if (locker.isAlive()) locker.destroyForcibly().waitFor();
        }
    }

    private static void testNormalCloseReleasesFileLock(Path path) throws Exception {
        Database first = new DatabaseImpl();
        first.open(path.toString());
        first.close();
        Database second = new DatabaseImpl();
        try {
            second.open(path.toString());
            second.put("after-close".getBytes(StandardCharsets.UTF_8), new byte[] {2});
            System.out.println("PASS: OS lock released after normal close");
        } finally {
            second.close();
        }
    }

    private static void testWaitsForOtherProcessLock(Path path) throws Exception {
        Database db = new DatabaseImpl();
        db.open(path.toString());
        Process locker = startWorker("hold-lock", path.resolveSibling(
                path.getFileName() + ".lock").toString());
        var executor = Executors.newSingleThreadExecutor();
        try {
            BufferedReader output = new BufferedReader(new InputStreamReader(
                    locker.getInputStream(), StandardCharsets.UTF_8));
            BufferedWriter input = new BufferedWriter(new OutputStreamWriter(
                    locker.getOutputStream(), StandardCharsets.UTF_8));
            check("LOCKED".equals(output.readLine()), "Worker did not acquire OS file lock");
            Future<?> write = executor.submit(() -> {
                try {
                    db.put("waited-write".getBytes(StandardCharsets.UTF_8), new byte[] {7});
                } catch (Exception e) {
                    throw new AssertionError("Write after lock release failed", e);
                }
            });
            Thread.sleep(300);
            check(!write.isDone(), "Database operation did not wait for process lock");
            input.write("RELEASE\n");
            input.flush();
            write.get(10, TimeUnit.SECONDS);
            check(locker.waitFor(10, TimeUnit.SECONDS) && locker.exitValue() == 0,
                    "Lock holder did not exit cleanly");
            System.out.println("PASS: Database operation waits for another process lock");
        } finally {
            executor.shutdownNow();
            if (locker.isAlive()) locker.destroyForcibly().waitFor();
            db.close();
        }
    }

    private static Process startWorker(String mode, String argument) throws Exception {
        String java = Path.of(System.getProperty("java.home"), "bin",
                isWindows() ? "java.exe" : "java").toString();
        return new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
                ProcessLockWorker.class.getName(), mode, argument)
                .redirectErrorStream(true).start();
    }

    private static boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().contains("win");
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
