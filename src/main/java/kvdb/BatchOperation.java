package kvdb;

public final class BatchOperation {

    public enum Type {
        PUT, DELETE
    }

    private final Type type;
    private final byte[] key;
    private final byte[] value;

    private BatchOperation(Type type, byte[] key, byte[] value) {
        if (type == null || key == null) {
            throw new IllegalArgumentException("Type and key cannot be null");
        }

        if (key.length < 1 || key.length > 1024) {
            throw new IllegalArgumentException("Key must be 1–1024 bytes");
        }

        if (type == Type.PUT && value == null) {
            throw new IllegalArgumentException("PUT value cannot be null");
        }

        if (type == Type.PUT && value.length > 1024 * 1024) {
            throw new IllegalArgumentException("Value exceeds 1 MB");
        }

        this.type = type;
        this.key = key.clone();
        this.value = value == null ? null : value.clone();
    }

    public static BatchOperation put(byte[] key, byte[] value) {
        return new BatchOperation(Type.PUT, key, value);
    }

    public static BatchOperation delete(byte[] key) {
        return new BatchOperation(Type.DELETE, key, null);
    }

    public Type getType() {
        return type;
    }

    public byte[] getKey() {
        return key.clone();
    }

    public byte[] getValue() {
        return value == null ? null : value.clone();
    }
}