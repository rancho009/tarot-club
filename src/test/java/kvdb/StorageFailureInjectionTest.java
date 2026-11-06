package kvdb;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/** Controlled WAL write, short-write, force, permission, and size-limit checks. */
public final class StorageFailureInjectionTest {
    private StorageFailureInjectionTest() { }

    public static void main(String[] args) throws Exception {
        testPartialWalWriteIsTruncatedOnRecovery();
        testForceFailureIsNeverAcknowledged();
        testPermissionDeniedWriteIsNeverAcknowledged();
        System.out.println("ALL STORAGE FAILURE INJECTION TESTS PASSED");
    }

    private static void testPartialWalWriteIsTruncatedOnRecovery() throws Exception {
        Path path = fixture("tarot-partial-write-");
        long prefixSize = Files.size(path);
        DatabaseFaultInjector injector = new DatabaseFaultInjector() {
            private boolean thrown;

            @Override public int maxWalWriteChunk(int requestedBytes) {
                return Math.min(9, requestedBytes);
            }

            @Override public void afterWalWrite(long position, int bytesWritten)
                    throws IOException {
                if (!thrown) {
                    thrown = true;
                    throw new IOException("injected partial WAL write failure");
                }
            }
        };

        DatabaseImpl failed = new DatabaseImpl(injector);
        failed.open(path.toString());
        int acknowledgements = 0;
        try {
            failed.put(bytes("partial"), bytes("must-not-be-acknowledged"));
            acknowledgements++;
            throw new AssertionError("partial WAL write returned success");
        } catch (IOException expected) {
            check(expected.getMessage().contains("partial WAL write"),
                    "unexpected partial-write failure: " + expected);
        } finally {
            failed.close();
        }
        check(acknowledgements == 0, "partial write was falsely acknowledged");
        check(Files.size(path) == prefixSize + 9,
                "fault injector did not leave the expected short record prefix");

        DatabaseImpl recovered = new DatabaseImpl();
        recovered.open(path.toString());
        try {
            check(Arrays.equals(bytes("prior-value"), recovered.get(bytes("prior"))),
                    "prior acknowledged record was lost after short write");
            check(recovered.get(bytes("partial")) == null,
                    "incomplete, unacknowledged record became visible");
            check(Files.size(path) == prefixSize,
                    "recovery did not truncate precisely to the valid WAL boundary");
            recovered.put(bytes("after-recovery"), bytes("usable"));
        } finally {
            recovered.close();
        }
        System.out.println("PASS partial WAL write: 9-byte prefix rejected, ACK count=0, recovery truncated tail and retained prior data");
    }

    private static void testForceFailureIsNeverAcknowledged() throws Exception {
        Path path = fixture("tarot-force-failure-");
        DatabaseFaultInjector injector = new DatabaseFaultInjector() {
            @Override public void beforeWalForce() throws IOException {
                throw new IOException("injected force failure");
            }
        };
        DatabaseImpl failed = new DatabaseImpl(injector);
        failed.open(path.toString());
        int acknowledgements = 0;
        try {
            failed.put(bytes("force-uncertain"), bytes("maybe-visible-after-recovery"));
            acknowledgements++;
            throw new AssertionError("force failure returned success");
        } catch (IOException expected) {
            check(expected.getMessage().contains("force failure"),
                    "unexpected force error: " + expected);
        } finally {
            failed.close();
        }
        check(acknowledgements == 0, "force-failed write was falsely acknowledged");

        DatabaseImpl recovered = new DatabaseImpl();
        recovered.open(path.toString());
        try {
            check(Arrays.equals(bytes("prior-value"), recovered.get(bytes("prior"))),
                    "prior acknowledged data lost after force failure");
            byte[] uncertain = recovered.get(bytes("force-uncertain"));
            check(uncertain == null || Arrays.equals(
                            bytes("maybe-visible-after-recovery"), uncertain),
                    "uncertain unacknowledged value recovered with incorrect bytes");
            recovered.put(bytes("after-force-failure"), bytes("usable"));
        } finally {
            recovered.close();
        }
        System.out.println("PASS injected force failure: ACK count=0; recovery preserved prior data and accepted only absent-or-complete uncertain write");
    }

    private static void testPermissionDeniedWriteIsNeverAcknowledged() throws Exception {
        Path path = fixture("tarot-permission-denied-");
        long originalSize = Files.size(path);
        DatabaseFaultInjector injector = new DatabaseFaultInjector() {
            @Override public void beforeWalWrite(long position, int requestedBytes)
                    throws IOException {
                throw new AccessDeniedException(path.toString(), null,
                        "injected write permission denial");
            }
        };
        DatabaseImpl failed = new DatabaseImpl(injector);
        failed.open(path.toString());
        int acknowledgements = 0;
        try {
            failed.put(bytes("denied"), bytes("no-ack"));
            acknowledgements++;
            throw new AssertionError("permission-denied write returned success");
        } catch (AccessDeniedException expected) {
            check(expected.getReason().contains("permission denial"),
                    "permission error reason was not preserved");
        } finally {
            failed.close();
        }
        check(acknowledgements == 0, "permission-denied write was falsely acknowledged");
        check(Files.size(path) == originalSize,
                "permission-denied write changed WAL size");

        DatabaseImpl recovered = new DatabaseImpl();
        recovered.open(path.toString());
        try {
            check(Arrays.equals(bytes("prior-value"), recovered.get(bytes("prior"))),
                    "permission-denied write damaged prior data");
            check(recovered.get(bytes("denied")) == null,
                    "permission-denied value became visible");
        } finally {
            recovered.close();
        }
        System.out.println("PASS injected AccessDeniedException: unchanged WAL and ACK count=0");
    }

    private static Path fixture(String prefix) throws Exception {
        Path path = Files.createTempDirectory(prefix).resolve("data.db");
        Database database = new DatabaseImpl();
        database.open(path.toString());
        database.put(bytes("prior"), bytes("prior-value"));
        database.close();
        return path;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
