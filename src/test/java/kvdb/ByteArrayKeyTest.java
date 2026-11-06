
package kvdb;

public class ByteArrayKeyTest {

    public static void main(String[] args) {

        byte[] key1 = {10, 20, 30};
        byte[] key2 = {10, 20, 30};

        ByteArrayKey a = new ByteArrayKey(key1);
        ByteArrayKey b = new ByteArrayKey(key2);

        System.out.println("Keys equal: " + a.equals(b));

        System.out.println(
            "Hash codes equal: " +
            (a.hashCode() == b.hashCode())
        );

        key1[0] = 99;

        System.out.println(
            "Original key modification safe: " +
            a.equals(b)
        );
    }
}
