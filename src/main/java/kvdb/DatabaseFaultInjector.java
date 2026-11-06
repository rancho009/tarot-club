package kvdb;

import java.io.IOException;
import java.nio.file.Path;

/** Package-private deterministic I/O fault points for isolated tests. */
interface DatabaseFaultInjector {
    DatabaseFaultInjector NONE = new DatabaseFaultInjector() { };

    default int maxWalWriteChunk(int requestedBytes) { return requestedBytes; }

    default void beforeWalWrite(long position, int requestedBytes) throws IOException { }

    default void afterWalWrite(long position, int bytesWritten) throws IOException { }

    default void beforeWalForce() throws IOException { }

    default void beforeCandidateCreate(Path candidate) throws IOException { }

    default void beforeCandidateRecord(Path candidate, long recordNumber)
            throws IOException { }

    default void beforeCandidateValidation(Path candidate) throws IOException { }

    default void beforeCandidateReplacement(Path database, Path candidate)
            throws IOException { }

    default void afterCandidateReplacement(Path database) throws IOException { }
}
