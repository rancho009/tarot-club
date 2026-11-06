package kvdb;

import java.io.IOException;
import java.util.List;

public interface Database extends AutoCloseable {

    void open(String path) throws IOException;

    void open(String path, DatabaseConfig config) throws IOException;

    void put(byte[] key, byte[] value) throws IOException;

    byte[] get(byte[] key) throws IOException;

    void delete(byte[] key) throws IOException;

    void batch(List<BatchOperation> operations) throws IOException;

    void compact() throws IOException;

    @Override
    void close() throws IOException;
}
