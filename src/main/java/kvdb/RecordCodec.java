
package kvdb;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32C;

public final class RecordCodec {

    private static final int MAGIC = 0x4B564442; // KVDB
    private static final byte VERSION = 1;

    public static final int HEADER_SIZE = 24;

    // Maximum PUT payload:
    // 4-byte key length + 4-byte value length
    // + 1024-byte key + 1 MB value.
    public static final int MAX_PAYLOAD_SIZE = 8 + 1024 + (1024 * 1024);

    private RecordCodec() {
        // Utility class.
    }

    public static byte[] encode(
            RecordType type,
            long transactionId,
            byte[] payload) {

        if (type == null || payload == null) {
            throw new IllegalArgumentException(
                    "Record type and payload cannot be null");
        }

        if (payload.length > MAX_PAYLOAD_SIZE) {
            throw new IllegalArgumentException(
                    "Payload exceeds maximum allowed size");
        }

        ByteBuffer buffer = ByteBuffer
                .allocate(HEADER_SIZE + payload.length)
                .order(ByteOrder.BIG_ENDIAN);

        // First 20 bytes of header.
        buffer.putInt(MAGIC);
        buffer.put(VERSION);
        buffer.put(type.getCode());
        buffer.putShort((short) 0); // Reserved flags.
        buffer.putLong(transactionId);
        buffer.putInt(payload.length);

        // Calculate CRC32C over first 20 header bytes + payload.
        CRC32C crc = new CRC32C();
        crc.update(buffer.array(), 0, 20);
        crc.update(payload, 0, payload.length);

        // Checksum occupies the final 4 header bytes.
        buffer.putInt((int) crc.getValue());

        // Append payload.
        buffer.put(payload);

        return buffer.array();
    }

    public static DecodedRecord decode(ByteBuffer source)
            throws IOException {

        if (source == null) {
            throw new IllegalArgumentException(
                    "Source buffer cannot be null");
        }

        // Work on a duplicate so a failed decode
        // does not change the original buffer position.
        ByteBuffer buffer = source.duplicate()
                .order(ByteOrder.BIG_ENDIAN);

        if (buffer.remaining() < HEADER_SIZE) {
            throw new EOFException("Incomplete record header");
        }

        int startPosition = buffer.position();

        int magic = buffer.getInt();
        byte version = buffer.get();
        byte typeCode = buffer.get();
        short flags = buffer.getShort();
        long transactionId = buffer.getLong();
        int payloadLength = buffer.getInt();
        int storedChecksum = buffer.getInt();

        if (magic != MAGIC) {
            throw new IOException("Invalid record magic");
        }

        if (version != VERSION) {
            throw new IOException("Unsupported record version");
        }

        if (flags != 0) {
            throw new IOException("Unsupported record flags");
        }

        RecordType type;

        try {
            type = RecordType.fromCode(typeCode);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid record type", e);
        }

        if (payloadLength < 0 ||
                payloadLength > MAX_PAYLOAD_SIZE) {
            throw new IOException("Invalid payload length");
        }

        if (buffer.remaining() < payloadLength) {
            throw new EOFException("Incomplete record payload");
        }

        byte[] payload = new byte[payloadLength];
        buffer.get(payload);

        // Validate checksum.
        CRC32C crc = new CRC32C();

        ByteBuffer checksumHeader = source.duplicate();
        checksumHeader.position(startPosition);
        checksumHeader.limit(startPosition + 20);

        crc.update(checksumHeader);

        crc.update(payload, 0, payload.length);

        int calculatedChecksum = (int) crc.getValue();
        if (calculatedChecksum != storedChecksum) {
            throw new IOException("Record checksum mismatch");
        }

        // Advance the original buffer only after successful decoding.
        source.position(buffer.position());

        return new DecodedRecord(
                type,
                transactionId,
                payload);
    }

    public static final class DecodedRecord {

        private final RecordType type;
        private final long transactionId;
        private final byte[] payload;

        private DecodedRecord(
                RecordType type,
                long transactionId,
                byte[] payload) {

            this.type = type;
            this.transactionId = transactionId;
            this.payload = payload.clone();
        }

        public RecordType getType() {
            return type;
        }

        public long getTransactionId() {
            return transactionId;
        }

        public byte[] getPayload() {
            return payload.clone();
        }
    }
}
