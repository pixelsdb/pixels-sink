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
package io.pixelsdb.pixels.sink.source.storage;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.core.utils.Pair;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.conversion.sinkproto.RowRecordConverter;
import io.pixelsdb.pixels.sink.conversion.sinkproto.TransactionMetadataConverter;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.pipeline.TablePipelineManager;
import io.pixelsdb.pixels.sink.pipeline.TransactionPipeline;
import io.pixelsdb.pixels.sink.provider.ProtoType;
import io.pixelsdb.pixels.sink.source.SinkSource;
import io.pixelsdb.pixels.sink.util.EtcdFileRegistry;
import io.pixelsdb.pixels.sink.util.DataTransform;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import io.pixelsdb.pixels.sink.util.concurrent.DecodeExecutors;
import io.pixelsdb.pixels.sink.util.concurrent.OrderedBatchDecoder;
import io.pixelsdb.pixels.sink.util.rateLimiter.FlushRateLimiter;
import io.pixelsdb.pixels.sink.util.rateLimiter.FlushRateLimiterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractSinkStorageSource implements SinkSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSinkStorageSource.class);
    private static final int DECODE_BATCH_SIZE = 64;
    private static final long DECODE_BATCH_WAIT_MILLIS = 5;
    private static final long DECODE_SHUTDOWN_TIMEOUT_SECONDS = 30;
    protected static final int RECORD_HEADER_SIZE = Integer.BYTES * 2;
    protected final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean abortRequested = new AtomicBoolean(false);
    private final CountDownLatch stopped = new CountDownLatch(1);
    private volatile Thread sourceThread;

    protected final String topic;
    protected final String baseDir;
    protected final EtcdFileRegistry etcdFileRegistry;
    protected final List<String> files;
    protected final CompletableFuture<ByteBuffer> POISON_PILL = new CompletableFuture<>();
    protected final Map<Integer, Thread> consumerThreads = new ConcurrentHashMap<>();
    protected final Map<Integer, BlockingQueue<Pair<CompletableFuture<ByteBuffer>, Integer>>> queueMap = new ConcurrentHashMap<>();
    protected final boolean storageLoopEnabled;
    protected final FlushRateLimiter sourceRateLimiter;
    protected final TablePipelineManager tablePipelineManager = new TablePipelineManager();
    protected final TransactionPipeline transactionPipeline = new TransactionPipeline();
    protected final RowRecordConverter rowRecordConverter;
    protected final TransactionMetadataConverter transactionMetadataConverter =
            new TransactionMetadataConverter();
    protected final boolean freshnessTimestamp;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final ExecutorService decodeExecutor;
    protected int loopId = 0;
    protected List<PhysicalReader> readers = new ArrayList<>();

    protected AbstractSinkStorageSource()
    {
        PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();
        this.topic = pixelsSinkConfig.getSinkProtoData();
        this.baseDir = pixelsSinkConfig.getSinkProtoDir();
        this.etcdFileRegistry = new EtcdFileRegistry(topic, baseDir);
        this.files = this.etcdFileRegistry.listAllFiles();
        this.storageLoopEnabled = pixelsSinkConfig.isSinkStorageLoop();
        this.rowRecordConverter = new RowRecordConverter(TableMetadataRegistry.Instance());
        this.freshnessTimestamp = pixelsSinkConfig.isSinkMonitorFreshnessTimestamp();
        this.sourceRateLimiter = FlushRateLimiterFactory.getNewInstance();
        this.decodeExecutor = DecodeExecutors.newFixedCallerRuns(
                pixelsSinkConfig.getSourceDecodeThreads(),
                "storage-proto-decoder");
    }

    protected void beginProcessing()
    {
        if (!started.compareAndSet(false, true))
        {
            throw new IllegalStateException("Storage source has already been started");
        }
        sourceThread = Thread.currentThread();
        running.set(true);
        transactionPipeline.start();
    }

    ProtoType getProtoType(int key)
    {
        return key == -1 ? ProtoType.TRANS : ProtoType.ROW;
    }

    protected static Pair<Integer, ByteBuffer> readRecord(
            PhysicalReader reader, long offset, long fileLength) throws IOException
    {
        long remaining = fileLength - offset;
        if (remaining < RECORD_HEADER_SIZE)
        {
            throw new IOException(
                    "Truncated sink proto header at offset " + offset +
                            " in " + reader.getPath());
        }

        int key = reader.readInt(ByteOrder.BIG_ENDIAN);
        int valueLength = reader.readInt(ByteOrder.BIG_ENDIAN);
        long availablePayload = remaining - RECORD_HEADER_SIZE;
        if (valueLength < 0 || valueLength > availablePayload)
        {
            throw new IOException(
                    "Invalid sink proto payload length " + valueLength +
                            " at offset " + offset + " in " + reader.getPath());
        }

        return new Pair<>(key, reader.readFully(valueLength));
    }

    protected void submitRecord(int key, ByteBuffer valueBuffer, int recordLoopId)
            throws InterruptedException
    {
        BlockingQueue<Pair<CompletableFuture<ByteBuffer>, Integer>> queue =
                queueMap.computeIfAbsent(
                        key,
                        ignored -> new LinkedBlockingQueue<>(PixelsSinkConstants.MAX_QUEUE_SIZE)
                );

        consumerThreads.computeIfAbsent(key, ignored ->
        {
            ProtoType protoType = getProtoType(key);
            Thread thread = new Thread(() -> consumeQueue(key, queue, protoType));
            thread.setName("consumer-" + key);
            thread.start();
            return thread;
        });

        if (getProtoType(key) == ProtoType.ROW)
        {
            sourceRateLimiter.acquire(1);
        }

        queue.put(new Pair<>(
                CompletableFuture.completedFuture(valueBuffer),
                recordLoopId
        ));
    }

    protected void clean()
    {
        boolean interrupted = false;
        try
        {
            running.set(false);
            if (!abortRequested.get())
            {
                for (BlockingQueue<Pair<CompletableFuture<ByteBuffer>, Integer>> queue
                        : queueMap.values())
                {
                    try
                    {
                        queue.put(new Pair<>(POISON_PILL, loopId));
                    } catch (InterruptedException e)
                    {
                        interrupted = true;
                        abortRequested.set(true);
                        break;
                    }
                }
            }
            if (abortRequested.get())
            {
                consumerThreads.values().forEach(Thread::interrupt);
            }

            for (Thread thread : consumerThreads.values())
            {
                while (thread.isAlive())
                {
                    try
                    {
                        thread.join();
                    } catch (InterruptedException e)
                    {
                        interrupted = true;
                        abortRequested.set(true);
                        consumerThreads.values().forEach(Thread::interrupt);
                    }
                }
            }

            if (abortRequested.get())
            {
                decodeExecutor.shutdownNow();
            } else
            {
                shutdownDecodeExecutor();
            }

            for (PhysicalReader reader : readers)
            {
                try
                {
                    reader.close();
                } catch (IOException e)
                {
                    LOGGER.warn("Failed to close reader", e);
                }
            }
            if (abortRequested.get())
            {
                tablePipelineManager.abort();
                transactionPipeline.abort();
            } else
            {
                tablePipelineManager.close();
                transactionPipeline.close();
            }
        } finally
        {
            sourceThread = null;
            stopped.countDown();
            if (interrupted)
            {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void shutdownDecodeExecutor()
    {
        DecodeExecutors.shutdownGracefully(
                decodeExecutor, DECODE_SHUTDOWN_TIMEOUT_SECONDS, LOGGER);
    }

    protected void consumeQueue(int key, BlockingQueue<Pair<CompletableFuture<ByteBuffer>, Integer>> queue, ProtoType protoType)
    {
        boolean stopAfterBatch = false;
        try
        {
            while (!stopAfterBatch)
            {
                List<Pair<CompletableFuture<ByteBuffer>, Integer>> batch =
                        new ArrayList<>(DECODE_BATCH_SIZE);
                Pair<CompletableFuture<ByteBuffer>, Integer> first = queue.take();
                if (isPoisonPill(first))
                {
                    break;
                }
                batch.add(first);

                long deadline = System.nanoTime() +
                        TimeUnit.MILLISECONDS.toNanos(DECODE_BATCH_WAIT_MILLIS);
                while (batch.size() < DECODE_BATCH_SIZE)
                {
                    long remainingNanos = deadline - System.nanoTime();
                    if (remainingNanos <= 0)
                    {
                        break;
                    }

                    Pair<CompletableFuture<ByteBuffer>, Integer> next =
                            queue.poll(remainingNanos, TimeUnit.NANOSECONDS);
                    if (next == null)
                    {
                        break;
                    }
                    if (isPoisonPill(next))
                    {
                        stopAfterBatch = true;
                        break;
                    }
                    batch.add(next);
                }

                processBatch(key, batch, protoType);
            }
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    private boolean isPoisonPill(
            Pair<CompletableFuture<ByteBuffer>, Integer> record)
    {
        return record.getLeft() == POISON_PILL;
    }

    private void processBatch(
            int key,
            List<Pair<CompletableFuture<ByteBuffer>, Integer>> batch,
            ProtoType protoType) throws InterruptedException
    {
        for (int i = 0; i < batch.size(); ++i)
        {
            metricsFacade.recordDebeziumEvent();
        }

        switch (protoType)
        {
            case ROW ->
            {
                List<RowChangeEvent> events = OrderedBatchDecoder.decodeInOrder(
                        decodeExecutor,
                        batch,
                        record -> decodeRowChangeSourceRecord(key, record));
                for (RowChangeEvent event : events)
                {
                    if (event != null)
                    {
                        metricsFacade.recordSerdRowChange();
                        tablePipelineManager.route(event);
                    }
                }
            }
            case TRANS ->
            {
                List<SinkProto.TransactionMetadata> transactions =
                        OrderedBatchDecoder.decodeInOrder(
                        decodeExecutor,
                        batch,
                        this::decodeTransactionSourceRecord);
                for (SinkProto.TransactionMetadata transaction : transactions)
                {
                    if (transaction != null)
                    {
                        metricsFacade.recordSerdTxChange();
                        transactionPipeline.publish(transaction);
                    }
                }
            }
        }
    }

    private RowChangeEvent decodeRowChangeSourceRecord(
            int key,
            Pair<CompletableFuture<ByteBuffer>, Integer> record)
    {
        try
        {
            return convertRowChangeSourceRecord(
                    key, record.getLeft().get(), record.getRight());
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e)
        {
            LOGGER.warn("Failed to read storage row record", e.getCause());
            return null;
        }
    }

    private SinkProto.TransactionMetadata decodeTransactionSourceRecord(
            Pair<CompletableFuture<ByteBuffer>, Integer> record)
    {
        try
        {
            return transactionMetadataConverter.convert(
                    record.getLeft().get(), record.getRight());
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            return null;
        } catch (Exception e)
        {
            LOGGER.warn("Failed to convert storage transaction metadata", e);
            return null;
        }
    }

    protected ByteBuffer copyToHeap(ByteBuffer directBuffer)
    {
        ByteBuffer duplicate = directBuffer.duplicate();
        ByteBuffer heapBuffer = ByteBuffer.allocate(duplicate.remaining());
        heapBuffer.put(duplicate);
        heapBuffer.flip();
        return heapBuffer;
    }

    protected RowChangeEvent convertRowChangeSourceRecord(
            int key, ByteBuffer dataBuffer, int loopId)
    {
        try
        {
            SinkProto.RowRecord.Builder builder =
                    rowRecordConverter.parse(dataBuffer).toBuilder();
            if (freshnessTimestamp)
            {
                DataTransform.updateRecordTimestamp(builder, System.currentTimeMillis() * 1000);
            }
            if (builder.hasTransaction())
            {
                SinkProto.TransactionInfo transaction = builder.getTransaction();
                builder.setTransaction(transaction.toBuilder()
                        .setId(transaction.getId() + "_" + loopId)
                        .build());
            }
            return rowRecordConverter.convert(builder.build());
        } catch (Exception e)
        {
            LOGGER.warn("Failed to convert storage row record", e);
            return null;
        }
    }

    @Override
    public boolean isRunning()
    {
        return running.get();
    }

    @Override
    public void close()
    {
        running.set(false);
        if (!started.get() || Thread.currentThread() == sourceThread)
        {
            return;
        }

        boolean interrupted = false;
        while (true)
        {
            try
            {
                stopped.await();
                break;
            } catch (InterruptedException e)
            {
                interrupted = true;
            }
        }
        if (interrupted)
        {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void abort()
    {
        abortRequested.set(true);
        running.set(false);
        consumerThreads.values().forEach(Thread::interrupt);
        decodeExecutor.shutdownNow();
        tablePipelineManager.abort();
        transactionPipeline.abort();

        Thread thread = sourceThread;
        if (thread != null && thread != Thread.currentThread())
        {
            thread.interrupt();
        }
    }
}
