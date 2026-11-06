package kvdb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;

public final class MainProtocolTest {
    private MainProtocolTest() { }

    public static void main(String[] args) throws Exception {
        testServeProtocolAndDurableAck();
        testConfiguredSizeLimitResponse();
        testOneShotCommands();
        testLargeValueInBatch();
        System.out.println("ALL JAVA CLI PROTOCOL TESTS PASSED!");
    }

    private static void testServeProtocolAndDurableAck() throws Exception {
        Path path = Files.createTempFile("tarot-cli-protocol-", ".db");
        byte[] keyOne = new byte[] {0, 1, (byte) 0xff};
        byte[] valueOne = "one\tline\nvalue".getBytes(StandardCharsets.UTF_8);
        byte[] keyTwo = "two".getBytes(StandardCharsets.UTF_8);
        byte[] valueTwo = new byte[] {9, 0, 8};
        String input = "PUT\tp1\t" + b64(keyOne) + "\t" + b64(valueOne) + "\n"
                + "GET\tg1\t" + b64(keyOne) + "\n"
                + "BATCH\tb1\t2\n"
                + "PUT\t" + b64(keyTwo) + "\t" + b64(valueTwo) + "\n"
                + "DEL\t" + b64(keyOne) + "\n"
                + "CHECK\nDUMP\nQUIT\n";
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            int result = Main.run(new String[] {"serve", path.toString()},
                    new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)),
                    new PrintStream(output, true, StandardCharsets.UTF_8),
                    new PrintStream(new ByteArrayOutputStream()));
            String text = output.toString(StandardCharsets.UTF_8);
            check(result == 0, "serve returned a failure status");
            check(text.contains("READY"), "server READY banner missing");
            check(text.contains("ACK p1"), "PUT ACK missing");
            check(text.contains("VALUE\tg1\t" + b64(valueOne)), "GET response mismatch");
            check(text.contains("ACK b1"), "BATCH ACK missing");
            check(text.contains("CHECK\tOK\t1"), "CHECK response mismatch");
            check(text.contains("ENTRY\t" + b64(keyTwo) + "\t" + b64(valueTwo)),
                    "DUMP entry missing");
            check(!text.contains("ENTRY\t" + b64(keyOne) + "\t"),
                    "Deleted key included in DUMP");

            DatabaseImpl reopened = new DatabaseImpl();
            try {
                reopened.open(path.toString());
                check(reopened.get(keyOne) == null, "Delete was not durable after ACK");
                check(Arrays.equals(reopened.get(keyTwo), valueTwo),
                        "Batch value was not durable after ACK");
            } finally {
                reopened.close();
            }
            System.out.println("PASS: stdin protocol, atomic batch, dump/check, durable ACK");
        } finally {
            Files.deleteIfExists(path);
            Files.deleteIfExists(path.resolveSibling(path.getFileName() + ".lock"));
        }
    }

    private static void testConfiguredSizeLimitResponse() throws Exception {
        Path path = Files.createTempFile("tarot-cli-limit-", ".db");
        String firstKey = b64(new byte[] {1});
        String secondKey = b64(new byte[] {2});
        String value = b64(new byte[] {3});
        String input = "PUT\tfit\t" + firstKey + "\t" + value + "\n"
                + "PUT\ttoo-big\t" + secondKey + "\t" + value + "\nQUIT\n";
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            int result = Main.run(new String[] {"serve", path.toString(), "35"},
                    new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)),
                    new PrintStream(output, true, StandardCharsets.UTF_8),
                    new PrintStream(new ByteArrayOutputStream()));
            String text = output.toString(StandardCharsets.UTF_8);
            check(result == 0, "size-limit serve returned a failure status");
            check(text.contains("ACK fit"), "fitting write was not acknowledged");
            check(text.contains("ERROR\ttoo-big\t"), "size-limit failure response missing");
            check(!text.contains("ACK too-big"), "oversized write was acknowledged");
            System.out.println("PASS: configured file-size limit does not produce false ACK");
        } finally {
            Files.deleteIfExists(path);
            Files.deleteIfExists(path.resolveSibling(path.getFileName() + ".lock"));
        }
    }

    private static void testOneShotCommands() throws Exception {
        Path path = Files.createTempFile("tarot-cli-oneshot-", ".db");
        byte[] key = "cli-key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "cli-value".getBytes(StandardCharsets.UTF_8);
        try {
            check(run(new String[] {"open", path.toString()}, "").contains("OPEN OK"),
                    "open command failed");
            check(run(new String[] {"put", path.toString(), "p1", b64(key), b64(value)}, "")
                    .contains("ACK p1"), "put command failed");
            check(run(new String[] {"get", path.toString(), "g1", b64(key)}, "")
                    .contains("VALUE\tg1\t" + b64(value)), "get command failed");
            String batchInput = "PUT\t" + b64("batch-key".getBytes(StandardCharsets.UTF_8))
                    + "\t" + b64("batch-value".getBytes(StandardCharsets.UTF_8)) + "\n";
            check(run(new String[] {"batch", path.toString(), "b1", "1"}, batchInput)
                    .contains("ACK b1"), "batch command failed");
            check(run(new String[] {"del", path.toString(), "d1", b64(key)}, "")
                    .contains("ACK d1"), "del command failed");
            check(run(new String[] {"dump", path.toString()}, "")
                            .contains("KEY: \"batch-key\" | VALUE: \"batch-value\""),
                    "human dump command failed");
            check(run(new String[] {"dump64", path.toString()}, "").contains("DUMP_DONE\t1"),
                    "legacy dump command failed");
            check(run(new String[] {"check", path.toString()}, "").contains("CHECK OK keys=1"),
                    "check command failed");
            System.out.println("PASS: open/put/get/del/batch/dump/check command dispatch");
        } finally {
            Files.deleteIfExists(path);
            Files.deleteIfExists(path.resolveSibling(path.getFileName() + ".lock"));
        }
    }

    private static void testLargeValueInBatch() throws Exception {
        Path path = Files.createTempFile("tarot-cli-large-", ".db");
        byte[] key = "large-value".getBytes(StandardCharsets.UTF_8);
        byte[] value = new byte[1024 * 1024];
        for (int i = 0; i < value.length; i++) value[i] = (byte) (i * 31);
        String input = "BATCH\tlarge-batch\t2\n"
                + "PUT\t" + b64(key) + "\t" + b64(value) + "\n"
                + "PUT\t" + b64("empty".getBytes(StandardCharsets.UTF_8)) + "\t\n"
                + "QUIT\n";
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            int result = Main.run(new String[] {"serve", path.toString()},
                    new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)),
                    new PrintStream(output, true, StandardCharsets.UTF_8),
                    new PrintStream(new ByteArrayOutputStream()));
            check(result == 0, "large batch serve failed");
            check(output.toString(StandardCharsets.UTF_8).contains("ACK large-batch"),
                    "large batch did not receive ACK");
            DatabaseImpl reopened = new DatabaseImpl();
            try {
                reopened.open(path.toString());
                check(Arrays.equals(reopened.get(key), value),
                        "1 MiB batch value changed over protocol");
                check(reopened.get("empty".getBytes(StandardCharsets.UTF_8)).length == 0,
                        "empty batch value was not preserved");
            } finally {
                reopened.close();
            }
            System.out.println("PASS: 1 MiB value and large batch framing");
        } finally {
            Files.deleteIfExists(path);
            Files.deleteIfExists(path.resolveSibling(path.getFileName() + ".lock"));
        }
    }

    private static String run(String[] args, String input) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        int result = Main.run(args, new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)),
                new PrintStream(output, true, StandardCharsets.UTF_8),
                new PrintStream(new ByteArrayOutputStream()));
        check(result == 0, "CLI command returned exit code " + result);
        return output.toString(StandardCharsets.UTF_8);
    }

    private static String b64(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
