package io.pixelsdb.pixels.sink;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestUtils {
    public static ExecutorService synchronousExecutor() {
        return Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable);
            thread.setDaemon(true);
            return thread;
        });
    }
}
