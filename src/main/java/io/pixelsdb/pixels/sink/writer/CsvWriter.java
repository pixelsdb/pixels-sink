/*
 * Copyright 2018-2025 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pixelsdb.pixels.sink.writer;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.PixelsSinkDefaultConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
public class CsvWriter implements Closeable {
    private final FileWriter writer;
    private static final Logger log = LoggerFactory.getLogger(CsvWriter.class);
    private Long recordCnt = 0L;

    private final ReentrantLock lock = new ReentrantLock();
    private ScheduledExecutorService scheduler;

    public CsvWriter(PixelsSinkConfig pixelsSinkConfig, String tableName) throws IOException {
        String outputDirectory = pixelsSinkConfig.getCsvSinkPath()  + File.separator + pixelsSinkConfig.getCaptureDatabase();
        log.info("Writer outputDirectory is " + outputDirectory);
        File directory = new File(outputDirectory);

        if (!directory.exists() && !directory.mkdirs()) {
            throw new IOException("Failed to create directories: " + outputDirectory);
        }

        String filePath = outputDirectory + File.separator + tableName + ".csv";
        File file = new File(filePath);
        if (!file.exists() && !file.createNewFile()) {
            throw new IOException("Failed to create file: " + filePath);
        }
        this.scheduler = Executors.newScheduledThreadPool(1);
        this.writer = new FileWriter(file, true);
        this.scheduler.scheduleAtFixedRate(this::flush, 3, 3, TimeUnit.SECONDS);
    }

    public void writeToCsv(Map<String, Object> message) throws IOException {

        lock.lock();
        try {
            recordCnt+=message.size();
            writer.append(message.values().stream().map(String::valueOf).collect(Collectors.joining("|")));
        } catch (IOException e) {
            throw e;
        } finally {

        }

        writer.append("\n");
        if(recordCnt >= PixelsSinkDefaultConfig.CSV_RECORD_FLUSH) {
            writer.flush();
        }
    }
    private void flush() {
        lock.lock();
        try {
            writer.flush();
        } catch (IOException ignored) {
        } finally {
            lock.unlock();
        }
    }
    @Override
    public void close() throws IOException {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
        }

        lock.lock();
        try {
            writer.close();
        } finally {
            lock.unlock();
        }
    }
}