
import kvdb.ValueLocation;

public class TestValueLocation {
    public static void main(String[] args) {

        ValueLocation location = new ValueLocation(1024, 500);

        System.out.println("Offset: " + location.offset());
        System.out.println("Length: " + location.length());
        System.out.println(location);
    }
}
