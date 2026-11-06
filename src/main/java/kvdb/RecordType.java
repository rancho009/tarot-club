
package kvdb;

public enum RecordType {
    PUT(1),
    DELETE(2),
    BEGIN(3),
    COMMIT(4);

    private final byte code;

    RecordType(int code) {
        this.code = (byte) code;
    }

    public byte getCode() {
        return code;
    }

    public static RecordType fromCode(byte code) {
        for (RecordType type : values()) {
            if (type.code == code) {
                return type;
            }
        }

        throw new IllegalArgumentException(
                "Unknown record type: " + Byte.toUnsignedInt(code));
    }
}
