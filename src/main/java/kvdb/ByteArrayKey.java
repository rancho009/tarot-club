
package kvdb;

import java.util.Arrays;

/**
 * Immutable wrapper for byte array keys.
 * Provides content-based equality and hashing.
 */
public final class ByteArrayKey {

    private final byte[] key;
    private final int hashCode;

    public ByteArrayKey(byte[] key) {

        if (key == null) {
            throw new IllegalArgumentException(
                "Key cannot be null"
            );
        }

        if (key.length < 1 || key.length > 1024) {
            throw new IllegalArgumentException(
                "Key length must be between 1 and 1024 bytes"
            );
        }

        this.key = Arrays.copyOf(key, key.length);
        this.hashCode = Arrays.hashCode(this.key);
    }

    /**
     * Returns a defensive copy of the key.
     */
    public byte[] bytes() {
        return Arrays.copyOf(key, key.length);
    }

    @Override
    public boolean equals(Object obj) {

        if (this == obj) {
            return true;
        }

        if (!(obj instanceof ByteArrayKey)) {
            return false;
        }

        ByteArrayKey other = (ByteArrayKey) obj;

        return Arrays.equals(this.key, other.key);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return Arrays.toString(key);
    }
}
