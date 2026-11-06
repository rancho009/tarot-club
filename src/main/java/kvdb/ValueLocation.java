
package kvdb;

/**
 * Stores the location of a value inside the database file.
 */
public final class ValueLocation {

    private final long offset;
    private final int length;

    /**
     * Constructor to initialize value location.
     */
    public ValueLocation(long offset, int length) {

        if (offset < 0) {
            throw new IllegalArgumentException(
                "Offset cannot be negative"
            );
        }

        if (length < 0) {
            throw new IllegalArgumentException(
                "Length cannot be negative"
            );
        }

        this.offset = offset;
        this.length = length;
    }

    /**
     * Returns the starting position of the value.
     */
    public long offset() {
        return offset;
    }

    /**
     * Returns the length of the value in bytes.
     */
    public int length() {
        return length;
    }

    @Override
    public String toString() {
        return "ValueLocation{" +
                "offset=" + offset +
                ", length=" + length +
                '}';
    }
}
