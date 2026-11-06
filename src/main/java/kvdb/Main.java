package kvdb;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;

/** Command-line interface and line-framed integration protocol for TAROT. */
public final class Main {
    private static final Base64.Decoder DECODER = Base64.getDecoder();
    private static final Base64.Encoder ENCODER = Base64.getEncoder();

    private Main() { }

    public static void main(String[] args) {
        PrintStream utf8Output = new PrintStream(
                new FileOutputStream(FileDescriptor.out), true, StandardCharsets.UTF_8);
        PrintStream utf8Error = new PrintStream(
                new FileOutputStream(FileDescriptor.err), true, StandardCharsets.UTF_8);
        int exitCode = run(args, System.in, utf8Output, utf8Error);
        if (exitCode != 0) System.exit(exitCode);
    }

    static int run(String[] args, InputStream input, PrintStream output, PrintStream error) {
        if (args.length == 0) {
            error.println(usage());
            return 2;
        }
        try {
            return switch (args[0]) {
                case "open" -> runOpen(args, output);
                case "put" -> runPut(args, output);
                case "put64" -> runLegacyPut(args, output);
                case "get" -> runGet(args, output);
                case "get64" -> runLegacyGet(args, output);
                case "del" -> runDelete(args, output);
                case "del64" -> runLegacyDelete(args, output);
                case "delete" -> runHumanDelete(args, output);
                case "batch" -> runBatch(args, input, output);
                case "batch64" -> runLegacyBatch(args, input, output);
                case "dump" -> runDump(args, output);
                case "dump64" -> runLegacyDump(args, output);
                case "check" -> runCheck(args, output);
                case "compact" -> runCompact(args, output);
                case "serve" -> runServe(args, input, output);
                default -> {
                    error.println(usage());
                    yield 2;
                }
            };
        } catch (Exception e) {
            if ("serve".equals(args[0])) {
                printError(output, args.length > 2 ? args[2] : "-", e);
            } else {
                error.println("ERROR: " + message(e));
            }
            return 1;
        }
    }

    private static int runOpen(String[] args, PrintStream out) throws IOException {
        requireCount(args, 2, 3, "open <db-path> [max-file-bytes]");
        Long maxBytes = args.length == 3 ? parseMaxBytes(args, 2) : null;
        DatabaseImpl db = open(args[1], maxBytes);
        db.close();
        out.println("OPEN OK");
        return 0;
    }

    private static int runPut(String[] args, PrintStream out) throws IOException {
        if (args.length == 5) return runLegacyPut(args, out);
        requireCount(args, 4, 4, "put <db-path> <key> <value>");
        byte[] key = encodeUtf8(args[2]);
        byte[] value = encodeUtf8(args[3]);
        try (DatabaseImpl db = open(args[1], null)) {
            db.put(key, value);
            out.println("PUT OK");
        }
        return 0;
    }

    private static int runLegacyPut(String[] args, PrintStream out) throws IOException {
        requireCount(args, 5, 5, "put <db-path> <op-id> <key-base64> <value-base64>");
        validateOpId(args[2]);
        try (DatabaseImpl db = open(args[1], null)) {
            db.put(decode(args[3]), decode(args[4]));
            out.println("ACK " + args[2]);
        }
        return 0;
    }

    private static int runGet(String[] args, PrintStream out) throws IOException {
        if (args.length == 4) return runLegacyGet(args, out);
        requireCount(args, 3, 3, "get <db-path> <key>");
        try (DatabaseImpl db = open(args[1], null)) {
            byte[] value = db.get(encodeUtf8(args[2]));
            out.println(value == null ? "NOT FOUND" : "VALUE " + readable(value));
        }
        return 0;
    }

    private static int runLegacyGet(String[] args, PrintStream out) throws IOException {
        requireCount(args, 4, 4, "get <db-path> <op-id> <key-base64>");
        validateOpId(args[2]);
        try (DatabaseImpl db = open(args[1], null)) {
            printGet(out, args[2], db.get(decode(args[3])));
        }
        return 0;
    }

    private static int runDelete(String[] args, PrintStream out) throws IOException {
        if (args.length == 4) return runLegacyDelete(args, out);
        return runHumanDelete(args, out);
    }

    private static int runHumanDelete(String[] args, PrintStream out) throws IOException {
        requireCount(args, 3, 3, "del <db-path> <key>");
        try (DatabaseImpl db = open(args[1], null)) {
            db.delete(encodeUtf8(args[2]));
            out.println("DELETE OK");
        }
        return 0;
    }

    private static int runLegacyDelete(String[] args, PrintStream out) throws IOException {
        requireCount(args, 4, 4, "del <db-path> <op-id> <key-base64>");
        validateOpId(args[2]);
        try (DatabaseImpl db = open(args[1], null)) {
            db.delete(decode(args[3]));
            out.println("ACK " + args[2]);
        }
        return 0;
    }

    private static int runBatch(String[] args, InputStream input, PrintStream out)
            throws IOException {
        if (args.length == 4) return runLegacyBatch(args, input, out);
        requireCount(args, 3, 3, "batch <db-path> <batch-file>");
        List<BatchOperation> operations = readHumanBatch(Path.of(args[2]));
        try (DatabaseImpl db = open(args[1], null)) {
            db.batch(operations);
            out.println("BATCH OK operations=" + operations.size());
        }
        return 0;
    }

    private static int runLegacyBatch(String[] args, InputStream input, PrintStream out)
            throws IOException {
        requireCount(args, 4, 4, "batch <db-path> <op-id> <count> (operations on stdin)");
        validateOpId(args[2]);
        List<BatchOperation> operations = readBatch(input, args[3]);
        try (DatabaseImpl db = open(args[1], null)) {
            db.batch(operations);
            out.println("ACK " + args[2]);
        }
        return 0;
    }

    private static int runDump(String[] args, PrintStream out) throws IOException {
        requireCount(args, 2, 2, "dump <db-path>");
        try (DatabaseImpl db = open(args[1], null)) {
            printReadableDump(out, db.dumpSnapshot());
        }
        return 0;
    }

    private static int runLegacyDump(String[] args, PrintStream out) throws IOException {
        requireCount(args, 2, 2, "dump64 <db-path>");
        try (DatabaseImpl db = open(args[1], null)) {
            printDump(out, db.dumpSnapshot());
        }
        return 0;
    }

    private static int runCheck(String[] args, PrintStream out) throws IOException {
        requireCount(args, 2, 2, "check <db-path>");
        try (DatabaseImpl db = open(args[1], null)) {
            int count = db.checkIntegrity();
            out.println("CHECK OK keys=" + count);
        }
        return 0;
    }

    private static int runCompact(String[] args, PrintStream out) throws IOException {
        requireCount(args, 2, 2, "compact <db-path>");
        try (DatabaseImpl db = open(args[1], null)) {
            db.compact();
            out.println("COMPACT OK");
        }
        return 0;
    }

    private static int runServe(String[] args, InputStream input, PrintStream out)
            throws IOException {
        requireCount(args, 2, 3, "serve <db-path> [max-file-bytes]");
        long maxBytes = parseMaxBytes(args, 2);
        DatabaseImpl db = new DatabaseImpl();
        try {
            db.open(args[1], maxBytes == 0 ? new DatabaseConfig() : new DatabaseConfig(maxBytes));
        } catch (Exception e) {
            printError(out, "-", e);
            return 1;
        }
        out.println("READY");
        out.flush();

        try (db; BufferedReader reader = new BufferedReader(
                new InputStreamReader(input, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) continue;
                String[] fields = line.split("\t", -1);
                String opId = fields.length > 1 ? safeOpId(fields[1]) : "-";
                try {
                    if (!serveCommand(db, fields, reader, out)) return 0;
                } catch (Exception e) {
                    printError(out, opId, e);
                }
            }
        }
        return 0;
    }

    private static boolean serveCommand(DatabaseImpl db, String[] fields,
            BufferedReader input, PrintStream out) throws IOException {
        String command = fields[0];
        switch (command) {
            case "PUT" -> {
                requireFields(fields, 4);
                validateOpId(fields[1]);
                db.put(decode(fields[2]), decode(fields[3]));
                out.println("ACK " + fields[1]);
            }
            case "GET" -> {
                requireFields(fields, 3);
                validateOpId(fields[1]);
                printGet(out, fields[1], db.get(decode(fields[2])));
            }
            case "DEL" -> {
                requireFields(fields, 3);
                validateOpId(fields[1]);
                db.delete(decode(fields[2]));
                out.println("ACK " + fields[1]);
            }
            case "BATCH" -> {
                requireFields(fields, 3);
                validateOpId(fields[1]);
                List<BatchOperation> operations = readBatch(input, fields[2]);
                db.batch(operations);
                out.println("ACK " + fields[1]);
            }
            case "DUMP" -> printDump(out, db.dumpSnapshot());
            case "CHECK" -> out.println("CHECK\tOK\t" + db.checkIntegrity());
            case "COMPACT" -> {
                requireFields(fields, 1);
                db.compact();
                out.println("COMPACT OK");
            }
            case "QUIT" -> {
                requireFields(fields, 1);
                out.println("BYE");
                out.flush();
                return false;
            }
            default -> throw new IllegalArgumentException("Unknown command");
        }
        out.flush();
        return true;
    }

    private static DatabaseImpl open(String path, Long maxBytes) throws IOException {
        DatabaseImpl db = new DatabaseImpl();
        try {
            if (maxBytes == null) db.open(path);
            else db.open(path, new DatabaseConfig(maxBytes));
            return db;
        } catch (IOException | RuntimeException e) {
            try { db.close(); } catch (IOException closeError) { e.addSuppressed(closeError); }
            throw e;
        }
    }

    private static long parseMaxBytes(String[] args, int index) {
        if (args.length <= index) return 0;
        long max = Long.parseLong(args[index]);
        if (max <= 0) throw new IllegalArgumentException("max-file-bytes must be positive");
        return max;
    }

    private static List<BatchOperation> readBatch(InputStream input, String countText)
            throws IOException {
        return readBatch(new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8)),
                countText);
    }

    private static List<BatchOperation> readBatch(BufferedReader input, String countText)
            throws IOException {
        int count;
        try {
            count = Integer.parseInt(countText);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid batch operation count", e);
        }
        if (count < 1 || count > 10_000) {
            throw new IllegalArgumentException("Batch count must be between 1 and 10000");
        }
        List<BatchOperation> operations = new ArrayList<>(count);
        String firstError = null;
        for (int i = 0; i < count; i++) {
            String line = input.readLine();
            if (line == null) throw new EOFException("EOF inside batch frame");
            try {
                String[] fields = line.split("\t", -1);
                if (fields.length == 3 && fields[0].equals("PUT")) {
                    operations.add(BatchOperation.put(decode(fields[1]), decode(fields[2])));
                } else if (fields.length == 2 && fields[0].equals("DEL")) {
                    operations.add(BatchOperation.delete(decode(fields[1])));
                } else {
                    throw new IllegalArgumentException("Invalid batch operation frame");
                }
            } catch (RuntimeException e) {
                if (firstError == null) firstError = message(e);
            }
        }
        if (firstError != null) throw new IllegalArgumentException(firstError);
        return operations;
    }

    /** Reads and validates the entire human batch file before any database write begins. */
    private static List<BatchOperation> readHumanBatch(Path path) throws IOException {
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        List<BatchOperation> operations = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                Files.newInputStream(path), decoder))) {
            String line;
            int lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                lineNumber++;
                if (operations.size() == 10_000) {
                    throw new IllegalArgumentException(
                            "Batch file exceeds 10000 operations at line " + lineNumber);
                }
                try {
                    String[] fields = line.split("\t", -1);
                    if (fields.length == 3 && fields[0].equals("PUT")) {
                        String key = unescapeBatchField(fields[1]);
                        String value = unescapeBatchField(fields[2]);
                        operations.add(BatchOperation.put(encodeUtf8(key), encodeUtf8(value)));
                    } else if (fields.length == 2 && fields[0].equals("DELETE")) {
                        operations.add(BatchOperation.delete(
                                encodeUtf8(unescapeBatchField(fields[1]))));
                    } else {
                        throw new IllegalArgumentException("Malformed batch row");
                    }
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(
                            "Invalid batch row " + lineNumber + ": " + message(e), e);
                }
            }
        }
        if (operations.isEmpty()) {
            throw new IllegalArgumentException("Batch file must contain 1 to 10000 operations");
        }
        return operations;
    }

    private static String unescapeBatchField(String field) {
        StringBuilder decoded = new StringBuilder(field.length());
        for (int i = 0; i < field.length(); i++) {
            char current = field.charAt(i);
            if (current != '\\') {
                decoded.append(current);
                continue;
            }
            if (++i == field.length()) {
                throw new IllegalArgumentException("Trailing backslash escape");
            }
            decoded.append(switch (field.charAt(i)) {
                case 't' -> '\t';
                case 'n' -> '\n';
                case 'r' -> '\r';
                case '\\' -> '\\';
                default -> throw new IllegalArgumentException(
                        "Unsupported escape \\" + field.charAt(i));
            });
        }
        return decoded.toString();
    }

    private static byte[] encodeUtf8(String value) {
        CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            ByteBuffer encoded = encoder.encode(CharBuffer.wrap(value));
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("Text argument is not valid Unicode", e);
        }
    }

    /** Returns a quoted UTF-8 string or an explicit Base64 representation. */
    private static String readable(byte[] bytes) {
        String text = decodeUtf8(bytes);
        return text == null ? "base64:" + ENCODER.encodeToString(bytes)
                : "\"" + escapeText(text) + "\"";
    }

    private static String decodeUtf8(byte[] bytes) {
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            return decoder.decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }

    private static String escapeText(String text) {
        StringBuilder escaped = new StringBuilder(text.length());
        for (int i = 0; i < text.length(); i++) {
            char current = text.charAt(i);
            switch (current) {
                case '\\' -> escaped.append("\\\\");
                case '"' -> escaped.append("\\\"");
                case '\n' -> escaped.append("\\n");
                case '\r' -> escaped.append("\\r");
                case '\t' -> escaped.append("\\t");
                default -> {
                    if (Character.isISOControl(current)) {
                        escaped.append(String.format("\\u%04x", (int) current));
                    } else {
                        escaped.append(current);
                    }
                }
            }
        }
        return escaped.toString();
    }

    private static byte[] decode(String encoded) {
        try {
            return DECODER.decode(encoded);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid Base64 field", e);
        }
    }

    private static void printGet(PrintStream out, String opId, byte[] value) {
        if (value == null) out.println("NOT_FOUND\t" + opId);
        else out.println("VALUE\t" + opId + "\t" + ENCODER.encodeToString(value));
        out.flush();
    }

    private static void printDump(PrintStream out, List<DatabaseImpl.DumpEntry> entries) {
        List<DatabaseImpl.DumpEntry> sorted = new ArrayList<>(entries);
        sorted.sort(Comparator.comparing(DatabaseImpl.DumpEntry::key, Arrays::compareUnsigned));
        for (DatabaseImpl.DumpEntry entry : sorted) {
            out.println("ENTRY\t" + ENCODER.encodeToString(entry.key()) + "\t"
                    + ENCODER.encodeToString(entry.value()));
        }
        out.println("DUMP_DONE\t" + sorted.size());
        out.flush();
    }

    private static void printReadableDump(
            PrintStream out, List<DatabaseImpl.DumpEntry> entries) {
        List<DatabaseImpl.DumpEntry> sorted = new ArrayList<>(entries);
        sorted.sort(Comparator.comparing(DatabaseImpl.DumpEntry::key, Arrays::compareUnsigned));
        for (DatabaseImpl.DumpEntry entry : sorted) {
            out.println("KEY: " + readable(entry.key()) + " | VALUE: " + readable(entry.value()));
        }
        out.println("DUMP OK entries=" + sorted.size());
        out.flush();
    }

    private static void printError(PrintStream out, String opId, Exception error) {
        String safe = safeOpId(opId);
        out.println("ERROR\t" + safe + "\t"
                + ENCODER.encodeToString(message(error).getBytes(StandardCharsets.UTF_8)));
        out.flush();
    }

    private static String safeOpId(String opId) {
        try {
            validateOpId(opId);
            return opId;
        } catch (RuntimeException e) {
            return "-";
        }
    }

    private static void validateOpId(String opId) {
        if (opId == null || !opId.matches("[A-Za-z0-9_.:-]{1,128}")) {
            throw new IllegalArgumentException("Operation ID must be 1-128 safe ASCII characters");
        }
    }

    private static void requireFields(String[] fields, int expected) {
        if (fields.length != expected) throw new IllegalArgumentException("Wrong field count");
    }

    private static void requireCount(String[] args, int min, int max, String form) {
        if (args.length < min || args.length > max) {
            throw new IllegalArgumentException("Usage: " + form);
        }
    }

    private static String message(Exception error) {
        return error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
    }

    private static String usage() {
        return "Usage: open|put|get|del|batch|dump|check|compact|serve ...; see README.md for CLI and protocol";
    }
}
