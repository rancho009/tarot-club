import java.util.Random;

public class RandomMillionTest {
    public static void main(String[] args) {

        Random random = new Random(42);

        for (int i = 0; i < 1_000_000; i++) {
            String key = "key-" + i;
            String value = "value-" + random.nextInt(1_000_000);

            System.out.println(key + " = " + value);
        }
    }
}