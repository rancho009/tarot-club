
package kvdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

public class WALTest {

    public static void main(String[] args) throws Exception {
        testOpenAndAppend();
        testMultipleRecords();
        testReopenAndAppend();
        testMaximumFileSize();
        testOperationsAfterClose();

        System.out.println("All WAL tests passed!");
    }

    private static void testOpenAndAppend() throws Exception {
        Path path = Files.createTempFile("wal-test", ".db");

        try {
            try (WAL wal = new WAL()) {
                wal.open(path);

                byte[] payload = { 10, 20, 30 };

                long offset = wal.append(
                        RecordType.PUT, 1L, payload);

                check(offset == 0,
                        "First record must start at offset 0");

                wal.force();

                check(wal.size() == 27,
                        "Unexpected file size");
            }

            byte[] fileBytes = Files.readAllBytes(path);

            RecordCodec.DecodedRecord decoded = RecordCodec.decode(ByteBuffer.wrap(fileBytes));

            check(decoded.getType() == RecordType.PUT,
                    "Wrong record type");

            check(decoded.getTransactionId() == 1L,
                    "Wrong transaction ID");

        } finally {
            Files.deleteIfExists(path);
        }
    }

    private static void testMultipleRecords() throws Exception {
        Path path = Files.createTempFile("wal-test", ".db");

        try {
            try (WAL wal = new WAL()) {
                wal.open(path);

                long first = wal.append(
                        RecordType.PUT, 1L,
                        new byte[] { 1, 2 });

                long second = wal.append(
                        RecordType.DELETE, 2L,
                        new byte[] { 3, 4, 5 });

                check(first == 0,
                        "Incorrect first offset");

                check(second == 26,
                        "Incorrect second offset");

                wal.force();

                check(wal.size() == 53,
                        "Unexpected size after two records");
            }

        } finally {
            Files.deleteIfExists(path);
        }
    }

    private static void testReopenAndAppend() throws Exception {
        Path path = Files.createTempFile("wal-test", ".db");

        try {
            try (WAL firstWal = new WAL()) {
                firstWal.open(path);

                firstWal.append(
                        RecordType.PUT, 1L,
                        new byte[] { 1 });
            }

            try (WAL secondWal = new WAL()) {
                secondWal.open(path);

                long offset = secondWal.append(
                        RecordType.DELETE, 2L,
                        new byte[] { 2 });

                check(offset == 25,
                        "Reopened WAL did not append at end");

                secondWal.force();
            }

        } finally {
            Files.deleteIfExists(path);
        }
    }

    private static void testMaximumFileSize() throws Exception {
        Path path = Files.createTempFile("wal-test", ".db");

        try {
            try (WAL wal = new WAL()) {
                wal.open(path, 25);

                wal.append(
                        RecordType.PUT, 1L,
                        new byte[] { 1 });

                try {
                    wal.append(
                            RecordType.PUT, 2L,
                            new byte[] { 2 });

                    throw new AssertionError(
                            "WAL accepted data beyond maximum size");

                } catch (IOException expected) {
                    // Expected.
                }
            }

        } finally {
            Files.deleteIfExists(path);
        }
    }

    private static void testOperationsAfterClose() throws Exception {
        Path path = Files.createTempFile("wal-test", ".db");

        WAL wal = new WAL();

        try {
            wal.open(path);
            wal.close();

            try {
                wal.append(
                        RecordType.PUT, 1L,
                        new byte[] { 1 });

                throw new AssertionError(
                        "Append after close was accepted");

            } catch (IOException expected) {
                // Expected.
            }

        } finally {
            wal.close();
            Files.deleteIfExists(path);
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
