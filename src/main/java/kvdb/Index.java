
package kvdb;

import java.util.HashMap;
import java.util.Map;

/**
 * In-memory index for TAROT KV database.
 * Maps keys to their value locations in the database file.
 */
public final class Index {

    private final Map<ByteArrayKey, ValueLocation> index;

    /**
     * Creates an empty index.
     */
    public Index() {
        this.index = new HashMap<>();
    }

    /**
     * Inserts a new key or updates an existing key.
     */
    public void put(ByteArrayKey key, ValueLocation location) {

        if (key == null) {
            throw new IllegalArgumentException(
                "Key cannot be null"
            );
        }

        if (location == null) {
            throw new IllegalArgumentException(
                "ValueLocation cannot be null"
            );
        }

        index.put(key, location);
    }

    /**
     * Retrieves the location associated with a key.
     * Returns null if the key does not exist.
     */
    public ValueLocation get(ByteArrayKey key) {

        if (key == null) {
            throw new IllegalArgumentException(
                "Key cannot be null"
            );
        }

        return index.get(key);
    }

    /**
     * Removes a key from the index.
     */
    public void remove(ByteArrayKey key) {

        if (key == null) {
            throw new IllegalArgumentException(
                "Key cannot be null"
            );
        }

        index.remove(key);
    }

    /**
     * Returns the number of stored keys.
     */
    public int size() {
        return index.size();
    }

    /**
     * Removes all entries from the index.
     */
    public void clear() {
        index.clear();
    }
}
