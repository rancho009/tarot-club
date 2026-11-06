package kvdb;

public class ProcessLockWorker {
    public static void main(String[] args) throws Exception {
        DatabaseImpl db = new DatabaseImpl();

        try {
            db.open(args[0]);
            System.out.println("OPENED");
            System.out.flush();

            Thread.sleep(15000);
            db.close();
        } catch (Exception e) {
            System.out.println("REJECTED: " + e.getMessage());
            System.exit(2);
        }
    }
}