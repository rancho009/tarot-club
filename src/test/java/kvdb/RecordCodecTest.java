
package kvdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class RecordCodecTest {

    public static void main(String[] args) throws Exception {
        testPutRecord();
        testDeleteRecord();
        testEmptyPayload();
        testCorruptedChecksum();
        testInvalidMagic();
        testIncompleteRecord();
        testDirectBuffer();

        System.out.println("All RecordCodec tests passed!");
    }

    private static void testPutRecord() throws Exception {
        byte[] payload = { 0, 0, 0, 1, 0, 0, 0, 2, 65, 66, 67 };

        byte[] encoded = RecordCodec.encode(
                RecordType.PUT, 1001L, payload);

        RecordCodec.DecodedRecord decoded = RecordCodec.decode(ByteBuffer.wrap(encoded));

        check(decoded.getType() == RecordType.PUT,
                "PUT type mismatch");

        check(decoded.getTransactionId() == 1001L,
                "Transaction ID mismatch");

        check(Arrays.equals(payload, decoded.getPayload()),
                "PUT payload mismatch");
    }

    private static void testDeleteRecord() throws Exception {
        byte[] payload = { 0, 0, 0, 1, 65 };

        byte[] encoded = RecordCodec.encode(
                RecordType.DELETE, 1002L, payload);

        RecordCodec.DecodedRecord decoded = RecordCodec.decode(ByteBuffer.wrap(encoded));

        check(decoded.getType() == RecordType.DELETE,
                "DELETE type mismatch");

        check(Arrays.equals(payload, decoded.getPayload()),
                "DELETE payload mismatch");
    }

    private static void testEmptyPayload() throws Exception {
        byte[] encoded = RecordCodec.encode(
                RecordType.BEGIN, 1003L, new byte[0]);

        RecordCodec.DecodedRecord decoded = RecordCodec.decode(ByteBuffer.wrap(encoded));

        check(decoded.getType() == RecordType.BEGIN,
                "Empty payload record type mismatch");

        check(decoded.getPayload().length == 0,
                "Expected empty payload");
    }

    private static void testCorruptedChecksum() throws Exception {
        byte[] encoded = RecordCodec.encode(
                RecordType.PUT, 1004L, new byte[] { 1, 2, 3 });

        encoded[encoded.length - 1] ^= 1;

        expectFailure(encoded, "Corrupted checksum was accepted");
    }

    private static void testInvalidMagic() throws Exception {
        byte[] encoded = RecordCodec.encode(
                RecordType.PUT, 1005L, new byte[] { 1 });

        encoded[0] ^= 1;

        expectFailure(encoded, "Invalid magic was accepted");
    }

    private static void testIncompleteRecord() throws Exception {
        byte[] encoded = RecordCodec.encode(
                RecordType.PUT, 1006L, new byte[] { 1, 2, 3 });

        byte[] truncated = Arrays.copyOf(
                encoded, encoded.length - 1);

        expectFailure(truncated, "Incomplete record was accepted");
    }

    private static void testDirectBuffer() throws Exception {
        byte[] encoded = RecordCodec.encode(
                RecordType.PUT, 1007L, new byte[] { 5, 6, 7 });

        ByteBuffer direct = ByteBuffer.allocateDirect(encoded.length);
        direct.put(encoded);
        direct.flip();

        RecordCodec.DecodedRecord decoded = RecordCodec.decode(direct);

        check(decoded.getType() == RecordType.PUT,
                "Direct buffer decoding failed");
    }

    private static void expectFailure(byte[] data, String message)
            throws Exception {
        try {
            RecordCodec.decode(ByteBuffer.wrap(data));
            throw new AssertionError(message);
        } catch (IOException expected) {
            // Expected failure.
        }
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
