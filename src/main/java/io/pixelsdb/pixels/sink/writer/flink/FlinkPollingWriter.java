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
 */
package io.pixelsdb.pixels.sink.writer.flink;

import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.writer.AbstractBucketedWriter;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * FlinkPollingWriter is a PixelsSinkWriter implementation designed for a long-polling pattern.
 * It maintains in-memory blocking queues per table, acting as a buffer between the upstream
 * data source (producer) and the gRPC service (consumer).
 * This class is thread-safe and integrates FlushRateLimiter to control ingress traffic.
 * It also manages the lifecycle of the gRPC server.
 */
public class FlinkPollingWriter extends AbstractBucketedWriter<Void> implements PixelsSinkWriter
{

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkPollingWriter.class);
    // Core data structure: A thread-safe map from table name to a thread-safe blocking queue.
    private final Map<TableBucketKey, BlockingQueue<SinkProto.RowRecord>> tableQueues;
    // The gRPC server instance managed by this writer.
    private final PollingRpcServer pollingRpcServer;

    /**
     * Constructor for FlinkPollingWriter.
     * Initializes the data structures, rate limiter, and starts the gRPC server.
     */
    public FlinkPollingWriter()
    {
        this.tableQueues = new ConcurrentHashMap<>();

        // --- START: New logic to initialize and start the gRPC server ---
        try
        {
            // 1. Get configuration
            PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
            int rpcPort = config.getSinkFlinkServerPort();
            // 2. Create the gRPC service implementation first, passing a reference to this writer.
            PixelsPollingServiceImpl service = new PixelsPollingServiceImpl(this);

            // 3. Create the PollingRpcServer instance with the service and port.
            LOGGER.info("Attempting to start gRPC Polling Server on port {}...", rpcPort);
            this.pollingRpcServer = new PollingRpcServer(service, rpcPort);
            // 4. Start the server.
            this.pollingRpcServer.start();
            LOGGER.info("gRPC Polling Server successfully started and is managed by FlinkPollingWriter.");
        } catch (IOException e)
        {
            // If the server fails to start, the writer cannot function.
            // Throw a RuntimeException to fail the Flink task initialization.
            LOGGER.error("Failed to start gRPC server during FlinkPollingWriter initialization.", e);
            throw new RuntimeException("Could not start gRPC server", e);
        }
        // --- END: New logic ---
    }

    /**
     * [Producer side] Receives row change events from the data source, applies rate limiting,
     * converts them, and places them into the in-memory queue.
     *
     * @param event The row change event
     * @return always returns true, unless an interruption occurs.
     */
    @Override
    public boolean writeRow(RowChangeEvent event)
    {
        if (event == null)
        {
            LOGGER.warn("Received a null RowChangeEvent, skipping.");
            return false;
        }

        try
        {
            writeRowChangeEvent(event, null);
            return true;
        } catch (Exception e)
        {
            LOGGER.error(
                    "Failed to process and write row for table: {}",
                    event.getFullTableName(),
                    e
            );
            return false;
        }
    }

    /**
     * [Consumer side] The gRPC service calls this method to pull data.
     * Implements long-polling logic: if the queue is empty, it blocks for a specified timeout.
     * batchSize acts as an upper limit on the number of records pulled to prevent oversized RPC responses.
     *
     * @param tableName The name of the table to pull data from
     * @param bucketId
     * @param batchSize The maximum number of records to pull
     * @param timeout   The maximum time to wait for data
     * @param unit      The time unit for the timeout
     * @return A list of RowRecords, which will be empty if no data is available before the timeout.
     * @throws InterruptedException if the thread is interrupted while waiting
     */
    public List<SinkProto.RowRecord> pollRecords(
            SchemaTableName tableName,
            int bucketId,
            int batchSize,
            long timeout,
            TimeUnit unit
    ) throws InterruptedException
    {
        List<SinkProto.RowRecord> records = new ArrayList<>(batchSize);
        TableBucketKey key = new TableBucketKey(tableName, bucketId);

        BlockingQueue<SinkProto.RowRecord> queue = tableQueues.get(key);

        if (queue == null)
        {
            unit.sleep(timeout);
            return records;
        }

        SinkProto.RowRecord first = queue.poll(timeout, unit);
        if (first == null)
        {
            return records;
        }

        records.add(first);
        queue.drainTo(records, batchSize - 1);

        LOGGER.debug(
                "Polled {} records for table {}, bucket {}",
                records.size(), tableName, bucketId
        );
        return records;
    }

    /**
     * This implementation does not involve transactions, so this method is a no-op.
     */
    @Override
    public boolean writeTrans(SinkProto.TransactionMetadata transactionMetadata)
    {
        return true;
    }

    /**
     * This implementation uses an in-memory queue, so data is immediately available. flush is a no-op.
     */
    @Override
    public void flush()
    {
        // No-op
    }

    /**
     * Cleans up resources on close. This is where we stop the gRPC server.
     */
    @Override
    public void close() throws IOException
    {
        LOGGER.info("Closing FlinkPollingWriter...");
        if (this.pollingRpcServer != null)
        {
            LOGGER.info("Attempting to shut down the gRPC Polling Server...");
            this.pollingRpcServer.stop();
            LOGGER.info("gRPC Polling Server shut down.");
        }
        LOGGER.info("Clearing all table queues.");
        tableQueues.clear();
        LOGGER.info("FlinkPollingWriter closed.");
    }

    @Override
    protected void emit(RowChangeEvent event, int bucketId, Void unused)
    {
        TableBucketKey key =
                new TableBucketKey(event.getSchemaTableName(), bucketId);

        BlockingQueue<SinkProto.RowRecord> queue =
                tableQueues.computeIfAbsent(
                        key,
                        k -> new LinkedBlockingQueue<>(PixelsSinkConstants.MAX_QUEUE_SIZE)
                );

        try
        {
            queue.put(event.getRowRecord());
            LOGGER.debug(
                    "Enqueued row for table {}, bucket {}, queueSize={}",
                    event.getFullTableName(), bucketId, queue.size()
            );
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted while enqueueing row for " + event.getFullTableName(),
                    e
            );
        }
    }

    record TableBucketKey(SchemaTableName table, int bucketId)
    {
    }
}
