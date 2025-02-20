package io.pixelsdb.pixels.sink.monitor;

import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import java.lang.InterruptedException;

public class MonitorThreadManager {
    private final ExecutorService executor = Executors.newFixedThreadPool(PixelsSinkConstants.MONITOR_NUM);

    public void startMonitor(Runnable monitor) {
        executor.submit(monitor);
    }

    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
