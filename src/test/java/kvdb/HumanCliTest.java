package kvdb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;

/** Functional coverage for the human-readable CLI boundary and batch files. */
public final class HumanCliTest {
    private HumanCliTest() { }

    public static void main(String[] args) throws Exception {
        testHumanCrudAndEscaping();
        testReadableDumpAndBinaryFallback();
        testHumanBatchParsingAndAtomicity();
        testBatchLimitsAndSizes();
        testLegacyBase64Commands();
        System.out.println("ALL HUMAN CLI TESTS PASSED!");
    }

    private static void testHumanCrudAndEscaping() throws Exception {
        Path directory = Files.createTempDirectory("tarot-human-cli-");
        Path database = directory.resolve("human.db");
        String db = database.toString();
        String special = "quote \" slash \\ newline\n tab\t return\r control\u0001";

        check(call("open", db).successOutput().contains("OPEN OK"), "OPEN output mismatch");
        check(call("put", db, "key with spaces", "value with spaces").successOutput()
                .contains("PUT OK"), "PUT with spaces failed");
        check(call("get", db, "key with spaces").successOutput()
                .contains("VALUE \"value with spaces\""), "GET with spaces failed");
        check(call("put", db, "key with spaces", "updated value").successOutput()
                .contains("PUT OK"), "PUT update failed");
        check(call("get", db, "key with spaces").successOutput()
                .contains("VALUE \"updated value\""), "updated GET failed");
        check(call("put", db, "unicode Ω", "こんにちは 🌟").exitCode() == 0,
                "Unicode PUT failed");
        check(call("get", db, "unicode Ω").successOutput()
                .contains("VALUE \"こんにちは 🌟\""), "Unicode GET failed");
        check(call("put", db, "special", special).exitCode() == 0,
                "special-character PUT failed");
        check(call("get", db, "special").successOutput().contains(
                "VALUE \"quote \\\" slash \\\\ newline\\n tab\\t return\\r control\\u0001\""),
                "special-character output was not escaped");
        check(call("put", db, "empty", "").exitCode() == 0, "empty value PUT failed");
        check(call("get", db, "empty").successOutput().contains("VALUE \"\""),
                "empty value output mismatch");
        check(call("get", db, "missing").successOutput().contains("NOT FOUND"),
                "missing key response mismatch");
        check(call("del", db, "key with spaces").successOutput().contains("DELETE OK"),
                "DEL failed");
        check(call("get", db, "key with spaces").successOutput().contains("NOT FOUND"),
                "deleted key remained visible");
        check(call("delete", db, "unicode Ω").successOutput().contains("DELETE OK"),
                "DELETE alias failed");
        check(call("put", db, "", "invalid").exitCode() != 0,
                "zero-length key was accepted");
        check(call("put", db).exitCode() != 0, "invalid PUT arguments were accepted");
        check(call("get", db).exitCode() != 0, "invalid GET arguments were accepted");

        DatabaseImpl reopened = new DatabaseImpl();
        try {
            reopened.open(db);
            check(Arrays.equals(reopened.get("special".getBytes(StandardCharsets.UTF_8)),
                    special.getBytes(StandardCharsets.UTF_8)), "special value changed after reopen");
            check(reopened.get("key with spaces".getBytes(StandardCharsets.UTF_8)) == null,
                    "delete did not survive reopen");
        } finally {
            reopened.close();
        }
        System.out.println("PASS: human CRUD, spaces, Unicode, escaping, empty/missing values, reopen");
    }

    private static void testReadableDumpAndBinaryFallback() throws Exception {
        Path directory = Files.createTempDirectory("tarot-human-dump-");
        Path database = directory.resolve("dump.db");
        String db = database.toString();
        String special = "line\n tab\t quote \" slash \\";
        check(call("put", db, "Ranjeet", "Rajeev").exitCode() == 0, "DUMP seed PUT failed");
        check(call("put", db, "College", "MITE").exitCode() == 0, "DUMP seed PUT failed");
        check(call("put", db, "ключ", "日本語 🌸").exitCode() == 0,
                "Unicode DUMP seed PUT failed");
        check(call("put", db, "special", special).exitCode() == 0,
                "special DUMP seed PUT failed");
        check(call("put", db, "empty", "").exitCode() == 0, "empty DUMP seed PUT failed");

        DatabaseImpl binaryWriter = new DatabaseImpl();
        try {
            binaryWriter.open(db);
            binaryWriter.put(new byte[] {(byte) 0xff}, new byte[] {0, (byte) 0xff});
            binaryWriter.put(new byte[] {(byte) 0xfe},
                    "binary key with text value".getBytes(StandardCharsets.UTF_8));
            binaryWriter.put("binary value".getBytes(StandardCharsets.UTF_8),
                    new byte[] {(byte) 0xff});
        } finally {
            binaryWriter.close();
        }

        Result dump = call("dump", db);
        String output = dump.successOutput();
        check(output.contains("KEY: \"Ranjeet\" | VALUE: \"Rajeev\""),
                "readable DUMP row missing");
        check(output.contains("KEY: \"College\" | VALUE: \"MITE\""),
                "readable DUMP row missing");
        check(output.contains("KEY: \"ключ\" | VALUE: \"日本語 🌸\""),
                "Unicode DUMP row missing");
        check(output.contains("KEY: \"special\" | VALUE: \"line\\n tab\\t quote \\\" slash \\\\\""),
                "DUMP escaping mismatch");
        check(output.contains("KEY: \"empty\" | VALUE: \"\""), "empty DUMP value mismatch");
        check(output.contains("KEY: base64:/w== | VALUE: base64:AP8="),
                "invalid UTF-8 did not use explicit Base64 fallback");
        check(output.contains("KEY: base64:/g== | VALUE: \"binary key with text value\""),
                "invalid UTF-8 key fallback mismatch");
        check(output.contains("KEY: \"binary value\" | VALUE: base64:/w=="),
                "invalid UTF-8 value fallback mismatch");
        check(output.contains("DUMP OK entries=8"), "DUMP count mismatch after reopen");
        System.out.println("PASS: readable DUMP, UTF-8, escaping, empty value, binary fallback, reopen");
    }

    private static void testHumanBatchParsingAndAtomicity() throws Exception {
        Path directory = Files.createTempDirectory("tarot-human-batch-");
        Path database = directory.resolve("batch.db");
        Path batchFile = directory.resolve("valid.batch");
        String db = database.toString();
        check(call("put", db, "old key", "old").exitCode() == 0, "batch seed PUT failed");

        String rows = "PUT\tspace key\tvalue with spaces\n"
                + "PUT\tключ\t日本語 🌸\n"
                + "PUT\tline\\tkey\tfirst\\nsecond\\rthird\\\\slash\n"
                + "PUT\tempty\t\n"
                + "PUT\tduplicate\tfirst\n"
                + "PUT\tduplicate\tlast\n"
                + "DELETE\told key\n";
        Files.writeString(batchFile, rows, StandardCharsets.UTF_8);
        Result applied = call("batch", db, batchFile.toString());
        check(applied.exitCode() == 0
                        && applied.successOutput().contains("BATCH OK operations=7"),
                "human batch was not applied");
        check(call("get", db, "space key").successOutput().contains("\"value with spaces\""),
                "spaces were not preserved in batch");
        check(call("get", db, "ключ").successOutput().contains("\"日本語 🌸\""),
                "Unicode was not preserved in batch");
        check(call("get", db, "line\tkey").successOutput()
                .contains("VALUE \"first\\nsecond\\rthird\\\\slash\""),
                "batch escapes were not decoded");
        check(call("get", db, "empty").successOutput().contains("VALUE \"\""),
                "empty batch value was not preserved");
        check(call("get", db, "duplicate").successOutput().contains("\"last\""),
                "duplicate-key ordering changed");
        check(call("get", db, "old key").successOutput().contains("NOT FOUND"),
                "batch DELETE failed");

        Path invalid = directory.resolve("invalid-final-row.batch");
        Files.writeString(invalid, "PUT\tshould-not-appear\tvalid prefix\nUNKNOWN\tbad\n",
                StandardCharsets.UTF_8);
        Result rejected = call("batch", db, invalid.toString());
        check(rejected.exitCode() != 0 && rejected.errorOutput().contains("row 2"),
                "invalid final batch row was not rejected with its line number");
        check(call("get", db, "should-not-appear").successOutput().contains("NOT FOUND"),
                "valid batch prefix was applied after a later invalid row");

        Path malformedEscape = directory.resolve("bad-escape.batch");
        Files.writeString(malformedEscape, "PUT\tbad\tvalue\\q\n", StandardCharsets.UTF_8);
        check(call("batch", db, malformedEscape.toString()).exitCode() != 0,
                "unsupported escape was accepted");

        Path invalidUtf8 = directory.resolve("invalid-utf8.batch");
        byte[] validPrefix = "PUT\tutf8-prefix\tvalue\n".getBytes(StandardCharsets.UTF_8);
        byte[] invalidFile = Arrays.copyOf(validPrefix, validPrefix.length + 1);
        invalidFile[invalidFile.length - 1] = (byte) 0xff;
        Files.write(invalidUtf8, invalidFile);
        check(call("batch", db, invalidUtf8.toString()).exitCode() != 0,
                "invalid UTF-8 batch file was accepted");
        check(call("get", db, "utf8-prefix").successOutput().contains("NOT FOUND"),
                "valid prefix from invalid UTF-8 batch was applied");

        DatabaseImpl reopened = new DatabaseImpl();
        try {
            reopened.open(db);
            check(Arrays.equals(reopened.get("duplicate".getBytes(StandardCharsets.UTF_8)),
                    "last".getBytes(StandardCharsets.UTF_8)),
                    "batch ordering did not survive reopen");
            check(reopened.get("should-not-appear".getBytes(StandardCharsets.UTF_8)) == null,
                    "failed batch prefix survived reopen");
        } finally {
            reopened.close();
        }
        System.out.println("PASS: human batch format, escapes, duplicate ordering, mixed operations, atomic rejection");
    }

    private static void testBatchLimitsAndSizes() throws Exception {
        Path directory = Files.createTempDirectory("tarot-human-batch-limits-");
        Path database = directory.resolve("limits.db");
        String db = database.toString();

        Path tooLongKey = directory.resolve("long-key.batch");
        Files.writeString(tooLongKey, "PUT\t" + "k".repeat(1025) + "\tvalue\n",
                StandardCharsets.UTF_8);
        check(call("batch", db, tooLongKey.toString()).exitCode() != 0,
                "oversized batch key was accepted");

        Path tooLongValue = directory.resolve("long-value.batch");
        Files.writeString(tooLongValue, "PUT\tkey\t" + "v".repeat(1_048_577) + "\n",
                StandardCharsets.UTF_8);
        check(call("batch", db, tooLongValue.toString()).exitCode() != 0,
                "oversized batch value was accepted");

        Path maximum = directory.resolve("maximum.batch");
        StringBuilder allowed = new StringBuilder(200_000);
        for (int i = 0; i < 10_000; i++) {
            allowed.append("PUT\tmax-").append(i).append("\tv\n");
        }
        Files.writeString(maximum, allowed, StandardCharsets.UTF_8);
        Result maximumResult = call("batch", db, maximum.toString());
        check(maximumResult.exitCode() == 0
                        && maximumResult.successOutput().contains("operations=10000"),
                "maximum 10000-operation batch was rejected");
        check(call("get", db, "max-9999").successOutput().contains("VALUE \"v\""),
                "maximum batch final operation missing");

        Path overLimit = directory.resolve("over-limit.batch");
        StringBuilder excessive = new StringBuilder("PUT\ttoo-many-prefix\tv\n");
        for (int i = 1; i < 10_001; i++) {
            excessive.append("PUT\tover-").append(i).append("\tv\n");
        }
        Files.writeString(overLimit, excessive, StandardCharsets.UTF_8);
        Result excessiveResult = call("batch", db, overLimit.toString());
        check(excessiveResult.exitCode() != 0
                        && excessiveResult.errorOutput().contains("exceeds 10000"),
                "batch above 10000 operations was not rejected");
        check(call("get", db, "too-many-prefix").successOutput().contains("NOT FOUND"),
                "over-limit batch prefix was partially applied");
        System.out.println("PASS: key/value size validation, 10000-operation limit, over-limit atomic rejection");
    }

    private static void testLegacyBase64Commands() throws Exception {
        Path directory = Files.createTempDirectory("tarot-legacy-cli-");
        String db = directory.resolve("legacy.db").toString();
        String key = Base64.getEncoder().encodeToString(new byte[] {0, (byte) 0xff});
        String value = Base64.getEncoder().encodeToString(new byte[] {7, 0, (byte) 0xfe});
        check(call("put64", db, "legacy-put", key, value).successOutput()
                .contains("ACK legacy-put"), "put64 compatibility failed");
        check(call("get64", db, "legacy-get", key).successOutput()
                .contains("VALUE\tlegacy-get\t" + value), "get64 compatibility failed");
        String batchKey = Base64.getEncoder().encodeToString(new byte[] {1, 2});
        String batchValue = Base64.getEncoder().encodeToString(new byte[] {3, 4});
        Result legacyBatch = callWithInput(new String[] {"batch64", db, "legacy-batch", "1"},
                "PUT\t" + batchKey + "\t" + batchValue + "\n");
        check(legacyBatch.exitCode() == 0
                        && legacyBatch.successOutput().contains("ACK legacy-batch"),
                "batch64 compatibility failed");
        check(call("del64", db, "legacy-delete", key).successOutput()
                .contains("ACK legacy-delete"), "del64 compatibility failed");
        check(call("get", db, "legacy-missing").successOutput().contains("NOT FOUND"),
                "human GET missing-key output failed");
        check(call("get64", db, "legacy-batch-get", batchKey).successOutput()
                .contains("VALUE\tlegacy-batch-get\t" + batchValue),
                "batch64 value was not retrievable through legacy GET");
        System.out.println("PASS: explicit legacy Base64 CLI aliases");
    }

    private static Result call(String... args) {
        return callWithInput(args, "");
    }

    private static Result callWithInput(String[] args, String input) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ByteArrayOutputStream error = new ByteArrayOutputStream();
        int exitCode = Main.run(args,
                new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)),
                new PrintStream(output, true, StandardCharsets.UTF_8),
                new PrintStream(error, true, StandardCharsets.UTF_8));
        return new Result(exitCode, output.toString(StandardCharsets.UTF_8),
                error.toString(StandardCharsets.UTF_8));
    }

    private record Result(int exitCode, String successOutput, String errorOutput) { }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
