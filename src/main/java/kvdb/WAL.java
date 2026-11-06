
package kvdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class WAL implements AutoCloseable {

    private FileChannel channel;
    private long maxFileBytes;
    private boolean failed;
    private boolean closed = true;

    public WAL() {
    }

    public synchronized void open(Path path) throws IOException {
        open(path, Long.MAX_VALUE);
    }

    public synchronized void open(Path path, long maxFileBytes)
            throws IOException {

        if (!closed) {
            throw new IOException("WAL is already open");
        }

        if (path == null) {
            throw new IllegalArgumentException("Path cannot be null");
        }

        if (maxFileBytes <= 0) {
            throw new IllegalArgumentException(
                    "Maximum file size must be positive");
        }

        channel = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);

        this.maxFileBytes = maxFileBytes;
        this.failed = false;
        this.closed = false;
    }

    /**
     * Appends one encoded record.
     *
     * @return the byte offset at which the record begins.
     *         This method does not force the record to disk.
     */
    public synchronized long append(
            RecordType type,
            long transactionId,
            byte[] payload) throws IOException {

        ensureUsable();

        byte[] record = RecordCodec.encode(
                type, transactionId, payload);

        long offset = channel.size();

        if (record.length > maxFileBytes - offset) {
            throw new IOException("Maximum WAL file size exceeded");
        }

        ByteBuffer buffer = ByteBuffer.wrap(record);

        try {
            writeFully(buffer, offset);
            return offset;
        } catch (IOException e) {
            // A partial record may have reached disk.
            // Do not allow further writes in this session.
            failed = true;
            throw e;
        }
    }

    /**
     * Forces all previously appended records to stable storage.
     * Call this before acknowledging a successful operation.
     */
    public synchronized void force() throws IOException {
        ensureUsable();

        try {
            channel.force(true);
        } catch (IOException e) {
            failed = true;
            throw e;
        }
    }

    /**
     * Returns the current physical file size.
     */
    public synchronized long size() throws IOException {
        ensureUsable();
        return channel.size();
    }

    private void writeFully(ByteBuffer buffer, long offset)
            throws IOException {

        long position = offset;

        while (buffer.hasRemaining()) {
            int written = channel.write(buffer, position);

            if (written < 0) {
                throw new IOException("Unexpected end of WAL file");
            }

            if (written == 0) {
                Thread.onSpinWait();
                continue;
            }

            position += written;
        }
    }

    private void ensureUsable() throws IOException {
        if (closed || channel == null || !channel.isOpen()) {
            throw new IOException("WAL is not open");
        }

        if (failed) {
            throw new IOException(
                    "WAL is in a failed state; close and recover it");
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }

        IOException failure = null;

        try {
            if (!failed && channel.isOpen()) {
                channel.force(true);
            }
        } catch (IOException e) {
            failure = e;
        }

        try {
            channel.close();
        } catch (IOException e) {
            if (failure == null) {
                failure = e;
            } else {
                failure.addSuppressed(e);
            }
        } finally {
            channel = null;
            closed = true;
        }

        if (failure != null) {
            throw failure;
        }
    }

}
