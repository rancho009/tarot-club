package kvdb;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/** Child process used to terminate compaction while its candidate is being written. */
public final class CompactionWorker {
    private CompactionWorker() { }

    public static void main(String[] args) throws Exception {
        DatabaseImpl database = new DatabaseImpl();
        database.open(args[0]);
        try {
            System.out.println("COMPACTION_START");
            System.out.flush();
            database.compact();
            System.out.println("COMPACTION_DONE");
            System.out.flush();
            if (args.length > 1 && "hold".equals(args[1])) {
                new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))
                        .readLine();
            }
        } finally {
            database.close();
        }
    }
}
