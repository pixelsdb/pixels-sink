/*
 * Copyright 2025 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
