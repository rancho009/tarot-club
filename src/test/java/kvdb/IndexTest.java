
package kvdb;

public class IndexTest {

    private static int passed = 0;

    private static void check(String testName, boolean condition) {
        if (!condition) {
            throw new AssertionError("FAILED: " + testName);
        }

        passed++;
        System.out.println("PASSED: " + testName);
    }

    private static void expectException(
            String testName, Runnable action) {

        try {
            action.run();
            throw new AssertionError("FAILED: " + testName);
        } catch (IllegalArgumentException e) {
            passed++;
            System.out.println("PASSED: " + testName);
        }
    }

    public static void main(String[] args) {

        System.out.println("===== INDEX FUNCTIONAL TEST =====");

        Index index = new Index();

        // 1. Insert a key
        ByteArrayKey key1 =
                new ByteArrayKey(new byte[]{1, 2, 3});

        ValueLocation location1 =
                new ValueLocation(100, 20);

        index.put(key1, location1);

        check("Insert key", index.size() == 1);

        // 2. Search using an equivalent key
        ByteArrayKey searchKey =
                new ByteArrayKey(new byte[]{1, 2, 3});

        ValueLocation result = index.get(searchKey);

        check("Search existing key",
                result != null &&
                result.offset() == 100 &&
                result.length() == 20);

        // 3. Search missing key
        ByteArrayKey missingKey =
                new ByteArrayKey(new byte[]{9, 8, 7});

        check("Search missing key",
                index.get(missingKey) == null);

        // 4. Update existing key
        ValueLocation location2 =
                new ValueLocation(200, 40);

        index.put(key1, location2);

        result = index.get(searchKey);

        check("Update existing key",
                result != null &&
                result.offset() == 200 &&
                result.length() == 40);

        check("Update does not increase size",
                index.size() == 1);

        // 5. Insert another key
        ByteArrayKey key2 =
                new ByteArrayKey(new byte[]{4, 5, 6});

        index.put(key2, new ValueLocation(300, 50));

        check("Insert second key", index.size() == 2);

        // 6. Remove a key
        index.remove(key1);

        check("Remove existing key",
                index.get(key1) == null);

        check("Size after removal", index.size() == 1);

        // 7. Clear the index
        index.clear();

        check("Clear index",
                index.size() == 0);

        // 8. Null validation
        expectException("Reject null key in put",
                () -> index.put(null, location1));

        expectException("Reject null location in put",
                () -> index.put(key1, null));

        expectException("Reject null key in get",
                () -> index.get(null));

        expectException("Reject null key in remove",
                () -> index.remove(null));

        // Final result
        System.out.println("--------------------------------");
        System.out.println("TOTAL TESTS PASSED: " + passed);
        System.out.println("ALL INDEX TESTS COMPLETED!");
    }
}
