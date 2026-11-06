package kvdb;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class ProcessLockWorker {
    public static void main(String[] args) throws Exception {
        if ("hold-lock".equals(args[0])) {
            holdLock(Path.of(args[1]));
            return;
        }
        if (!"coordinate".equals(args[0])) {
            throw new IllegalArgumentException("Unknown worker mode: " + args[0]);
        }

        DatabaseImpl db = new DatabaseImpl();
        try {
            db.open(args[1]);
            System.out.println("OPENED");
            System.out.flush();
            String command = new BufferedReader(new InputStreamReader(
                    System.in, StandardCharsets.UTF_8)).readLine();
            if (!"GO".equals(command)) throw new IllegalStateException("Expected GO");
            db.put("child-process".getBytes(StandardCharsets.UTF_8),
                    "child".getBytes(StandardCharsets.UTF_8));
            db.batch(List.of(
                    BatchOperation.put("child-batch".getBytes(StandardCharsets.UTF_8),
                            "committed".getBytes(StandardCharsets.UTF_8)),
                    BatchOperation.delete("child-process".getBytes(StandardCharsets.UTF_8))));
            db.put("child-process".getBytes(StandardCharsets.UTF_8),
                    "child".getBytes(StandardCharsets.UTF_8));
            System.out.println("DONE");
            System.out.flush();
        } finally {
            db.close();
        }
    }

    private static void holdLock(Path lockPath) throws Exception {
        try (FileChannel channel = FileChannel.open(lockPath,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                FileLock fileLock = channel.lock()) {
            if (!fileLock.isValid()) throw new IllegalStateException("Lock is invalid");
            System.out.println("LOCKED");
            System.out.flush();
            new BufferedReader(new InputStreamReader(
                    System.in, StandardCharsets.UTF_8)).readLine();
        }
    }
}
