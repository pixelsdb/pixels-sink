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
package io.pixelsdb.pixels.sink.sink;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.PixelsSinkDefaultConfig;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.proto.SinkProto;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class CsvWriter implements PixelsSinkWriter {
    private static final Logger log = LoggerFactory.getLogger(CsvWriter.class);
    private Long recordCnt = 0L;

    @Getter
    private static final PixelsSinkMode pixelsSinkMode = PixelsSinkMode.CSV;
    private final ReentrantLock lock = new ReentrantLock();
    private final ConcurrentMap<String, FileChannel> tableWriters = new ConcurrentHashMap<>();
    private final ScheduledExecutorService flushScheduler;
    private final Path baseOutputPath;
    private final String databaseName;
    private final boolean enableHeader;
    private final ReentrantLock globalLock = new ReentrantLock();
    private final ReentrantLock writeLock = new ReentrantLock(true);
    private final AtomicInteger writeCounter = new AtomicInteger(0);


    private final String CSV_DELIMITER = "|";

    public CsvWriter(PixelsSinkConfig config) throws IOException {
        this.databaseName = config.getCaptureDatabase();
        this.baseOutputPath = Paths.get(config.getCsvSinkPath(), databaseName);
        this.enableHeader = config.isSinkCsvEnableHeader();

        if (!Files.exists(baseOutputPath)) {
            Files.createDirectories(baseOutputPath);
        }

        this.flushScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("csv-flusher-%d").build()
        );
        this.flushScheduler.scheduleAtFixedRate(this::flush, 3, 3, TimeUnit.SECONDS);
    }

    @Override
    public void flush() {
        writeLock.lock();
        try {
            for (FileChannel channel : tableWriters.values()) {
                try {
                    channel.force(true);
                } catch (IOException e) {
                    log.error("Failed to flush channel {}", channel, e);
                }
            }
            writeCounter.set(0);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean write(Map<String, Object> message) throws IOException {
        return true;
    }

    @Override
    public boolean write(RowChangeEvent event) {
        final String tableName = event.getTable();
        if (event.getOp() == SinkProto.OperationType.DELETE) {
            return true;
        }
        Map<String, Object> message = event.getAfterData();
        writeLock.lock();
        try {
            FileChannel channel = getOrCreateChannel(event);
            String csvLine = convertToCSV(message);

            ByteBuffer buffer = ByteBuffer.wrap((csvLine + "\n").getBytes(StandardCharsets.UTF_8));
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }

            if (writeCounter.incrementAndGet() % PixelsSinkDefaultConfig.SINK_CSV_RECORD_FLUSH == 0) {
                channel.force(false);
            }
            return true;
        } catch (IOException e) {
            log.error("Write failed for table {}: {}", tableName, e.getMessage());
            return false;
        } finally {
            writeLock.unlock();
        }
    }

    private FileChannel getOrCreateChannel(RowChangeEvent event) throws IOException {
        String tableName = event.getTable();
        return tableWriters.computeIfAbsent(tableName, key -> {
            try {
                Path tablePath = baseOutputPath.resolve(tableName + ".csv");
                FileChannel channel = FileChannel.open(tablePath,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND,
                        StandardOpenOption.WRITE);

                if (channel.size() == 0) {
                    String header = String.join(CSV_DELIMITER, getHeaderFields(event));
                    channel.write(ByteBuffer.wrap((header + "\n").getBytes()));
                }
                return channel;
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to create channel for " + tableName, e);
            }
        });
    }

    private String convertToCSV(Map<String, Object> message) {
        return message.values().stream()
                .map(obj -> {
                    if (obj == null) return "";
                    return obj.toString();
                })
                .collect(Collectors.joining(CSV_DELIMITER));
    }

    private List<String> getHeaderFields(RowChangeEvent event) {
        return event.getSchema().getFieldNames();
    }

    @Override
    public void close() throws IOException {
        flushScheduler.shutdown();
        try {
            if (!flushScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                flushScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        globalLock.lock();
        try {
            for (FileChannel channel : tableWriters.values()) {
                channel.close();
            }
            tableWriters.clear();
        } finally {
            globalLock.unlock();
        }
    }
}