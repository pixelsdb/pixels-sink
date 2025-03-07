package io.pixelsdb.pixels.sink.monitor;

import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
public class MonitorThreadManager {
    private final List<Runnable> monitors = new CopyOnWriteArrayList<>();
    private final ExecutorService executor = Executors.newFixedThreadPool(PixelsSinkConstants.MONITOR_NUM);

    public void startMonitor(Runnable monitor) {
        monitors.add(monitor);
        executor.submit(monitor);
    }

    public void shutdown() {
        stopMonitors();
        shutdownExecutor();
        awaitTermination();
    }

    private void stopMonitors() {
        monitors.forEach(monitor -> {
            if (monitor instanceof StoppableMonitor) {
                ((StoppableMonitor) monitor).stopMonitor();
            }
        });
    }

    private void shutdownExecutor() {
        executor.shutdown();
    }

    private void awaitTermination() {
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
    }
}
