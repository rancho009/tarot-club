
package kvdb;

public final class DatabaseConfig {

    public static final long DEFAULT_MAX_FILE_BYTES = 1024L * 1024L * 1024L; // 1 GB

    private final long maxFileBytes;

    public DatabaseConfig() {
        this(DEFAULT_MAX_FILE_BYTES);
    }

    public DatabaseConfig(long maxFileBytes) {
        if (maxFileBytes <= 0) {
            throw new IllegalArgumentException(
                    "Maximum file size must be positive");
        }

        this.maxFileBytes = maxFileBytes;
    }

    public long getMaxFileBytes() {
        return maxFileBytes;
    }
}
