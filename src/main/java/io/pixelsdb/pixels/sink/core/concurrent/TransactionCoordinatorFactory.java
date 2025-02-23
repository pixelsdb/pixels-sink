package io.pixelsdb.pixels.sink.core.concurrent;

public class TransactionCoordinatorFactory {
    private static TransactionCoordinator instance;

    public static synchronized TransactionCoordinator getCoordinator() {
        if (instance == null) {
            instance = new TransactionCoordinator();
        }
        return instance;
    }

    public static synchronized void reset() {
        instance = null;
    }
}
