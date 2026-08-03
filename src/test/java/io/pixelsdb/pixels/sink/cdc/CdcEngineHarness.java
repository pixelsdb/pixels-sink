/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.sink.cdc;

import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.source.engine.PixelsDebeziumConsumer;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongConsumer;

final class CdcEngineHarness implements AutoCloseable
{
    static final int ORDER_SEQUENCE_BASE = 100;

    private static final long INTERLEAVING_DELAY_MILLIS = 400L;

    private final RecordingRetinaWriter writer;
    private final PixelsDebeziumConsumer consumer;
    private final InterleavingConsumer interleavingConsumer;
    private final DebeziumEngine<RecordChangeEvent<SourceRecord>> engine;
    private final ExecutorService executor;
    private Future<?> engineFuture;

    CdcEngineHarness(
            Properties connectorProperties,
            RecordingRetinaWriter writer)
    {
        this.writer = writer;
        this.consumer = new PixelsDebeziumConsumer(writer);
        this.interleavingConsumer = new InterleavingConsumer(consumer);
        this.engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(connectorProperties)
                .notifying(interleavingConsumer)
                .build();
        this.executor = Executors.newSingleThreadExecutor(runnable ->
        {
            Thread thread = new Thread(runnable, "cdc-validation-engine");
            thread.setDaemon(true);
            return thread;
        });
    }

    static void initializeSinkConfig(
            Path stateDirectory,
            String dialect,
            String connectorClass,
            String topicPrefix) throws IOException
    {
        Path configFile = stateDirectory.resolve("pixels-sink.properties");
        Files.writeString(configFile, """
                sink.monitor.enable=false
                sink.monitor.report.enable=false
                sink.mode=none
                sink.debezium.dialect=%s
                debezium.connector.class=%s
                debezium.topic.prefix=%s
                sink.datasource.decode.threads=4
                """.formatted(dialect, connectorClass, topicPrefix),
                StandardCharsets.UTF_8);
        PixelsSinkConfigFactory.reset();
        PixelsSinkConfigFactory.initialize(configFile.toString());
    }

    static Properties commonConnectorProperties(
            Path stateDirectory,
            String connectorClass,
            String topicPrefix)
    {
        Properties properties = new Properties();
        properties.setProperty("name", "pixels-" + topicPrefix);
        properties.setProperty("connector.class", connectorClass);
        properties.setProperty("topic.prefix", topicPrefix);
        properties.setProperty(
                "offset.storage",
                "org.apache.kafka.connect.storage.FileOffsetBackingStore");
        properties.setProperty(
                "offset.storage.file.filename",
                stateDirectory.resolve("offsets.dat").toString());
        properties.setProperty("offset.flush.interval.ms", "0");
        properties.setProperty("provide.transaction.metadata", "true");
        properties.setProperty("include.schema.changes", "false");
        properties.setProperty("tombstones.on.delete", "false");
        properties.setProperty("snapshot.mode", "initial");
        properties.setProperty("decimal.handling.mode", "precise");
        properties.setProperty("binary.handling.mode", "bytes");
        properties.setProperty(
                "time.precision.mode", "adaptive_time_microseconds");
        properties.setProperty("record.processing.order", "ORDERED");
        properties.setProperty(
                "record.processing.with.serial.consumer", "true");
        properties.setProperty("record.processing.threads", "4");
        return properties;
    }

    void start()
    {
        consumer.start();
        engineFuture = executor.submit(engine);
    }

    List<Long> sourceSequenceOrder()
    {
        return interleavingConsumer.sourceSequenceOrder();
    }

    List<Long> decodeAccessOrder()
    {
        return interleavingConsumer.decodeAccessOrder();
    }

    boolean interleavingDelayApplied()
    {
        return interleavingConsumer.interleavingDelayApplied();
    }

    void assertHealthy()
    {
        if (engineFuture == null || !engineFuture.isDone())
        {
            return;
        }
        try
        {
            engineFuture.get();
        } catch (Exception e)
        {
            throw new AssertionError("Embedded Debezium Engine failed", e);
        }
    }

    @Override
    public void close() throws Exception
    {
        Exception failure = null;
        try
        {
            if (engineFuture == null || !engineFuture.isDone())
            {
                engine.close();
            }
        } catch (IOException e)
        {
            failure = e;
        }
        executor.shutdown();
        try
        {
            if (!executor.awaitTermination(30, TimeUnit.SECONDS))
            {
                executor.shutdownNow();
            }
        } catch (InterruptedException e)
        {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
            failure = e;
        }
        try
        {
            consumer.close();
        } catch (RuntimeException e)
        {
            if (failure == null)
            {
                failure = e;
            } else
            {
                failure.addSuppressed(e);
            }
        }
        writer.close();
        PixelsSinkConfigFactory.reset();
        if (failure != null)
        {
            throw failure;
        }
    }

    private static final class InterleavingConsumer
            implements DebeziumEngine.ChangeConsumer<RecordChangeEvent<SourceRecord>>
    {
        private final PixelsDebeziumConsumer delegate;
        private final List<Long> sourceSequenceOrder =
                Collections.synchronizedList(new ArrayList<>());
        private final List<Long> decodeAccessOrder =
                Collections.synchronizedList(new ArrayList<>());
        private final AtomicBoolean delayedBatchClaimed = new AtomicBoolean();

        private InterleavingConsumer(PixelsDebeziumConsumer delegate)
        {
            this.delegate = delegate;
        }

        @Override
        public void handleBatch(
                List<RecordChangeEvent<SourceRecord>> records,
                DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>>
                        committer) throws InterruptedException
        {
            List<Long> sequences = records.stream()
                    .map(RecordChangeEvent::record)
                    .map(InterleavingConsumer::sequenceOf)
                    .filter(sequence -> sequence >= ORDER_SEQUENCE_BASE)
                    .toList();
            boolean interleaveThisBatch = sequences.size() > 1 &&
                    delayedBatchClaimed.compareAndSet(false, true);
            if (!interleaveThisBatch)
            {
                delegate.handleBatch(records, committer);
                return;
            }

            sourceSequenceOrder.addAll(sequences);
            Map<RecordChangeEvent<SourceRecord>,
                    RecordChangeEvent<SourceRecord>> originals =
                    new IdentityHashMap<>();
            AtomicBoolean delayAssigned = new AtomicBoolean();
            List<RecordChangeEvent<SourceRecord>> wrapped =
                    new ArrayList<>(records.size());
            for (RecordChangeEvent<SourceRecord> record : records)
            {
                SourceRecord sourceRecord = record.record();
                long sequence = sequenceOf(sourceRecord);
                RecordChangeEvent<SourceRecord> forwarded = record;
                if (sequence >= ORDER_SEQUENCE_BASE)
                {
                    boolean delayed = delayAssigned.compareAndSet(false, true);
                    SourceRecord delayedRecord = withDelayedEnvelope(
                            sourceRecord,
                            sequence,
                            delayed ? INTERLEAVING_DELAY_MILLIS : 0L,
                            decodeAccessOrder::add);
                    forwarded = () -> delayedRecord;
                }
                wrapped.add(forwarded);
                originals.put(forwarded, record);
            }
            delegate.handleBatch(
                    wrapped, forwardingCommitter(committer, originals));
        }

        List<Long> sourceSequenceOrder()
        {
            synchronized (sourceSequenceOrder)
            {
                return List.copyOf(sourceSequenceOrder);
            }
        }

        List<Long> decodeAccessOrder()
        {
            synchronized (decodeAccessOrder)
            {
                return List.copyOf(decodeAccessOrder);
            }
        }

        boolean interleavingDelayApplied()
        {
            return delayedBatchClaimed.get();
        }

        private static long sequenceOf(SourceRecord record)
        {
            if (record == null || !(record.value() instanceof Struct envelope))
            {
                return -1;
            }
            if (envelope.schema().field("after") == null)
            {
                return -1;
            }
            Object afterObject = envelope.get("after");
            if (!(afterObject instanceof Struct after) ||
                    after.schema().field("sequence_no") == null)
            {
                return -1;
            }
            Object sequence = after.get("sequence_no");
            return sequence instanceof Number number
                    ? number.longValue()
                    : -1;
        }

        private static SourceRecord withDelayedEnvelope(
                SourceRecord sourceRecord,
                long sequence,
                long delayMillis,
                LongConsumer observed)
        {
            Struct envelope = (Struct) sourceRecord.value();
            Struct delayedEnvelope = new DelayedEnvelope(
                    envelope, sequence, delayMillis, observed);
            return sourceRecord.newRecord(
                    sourceRecord.topic(),
                    sourceRecord.kafkaPartition(),
                    sourceRecord.keySchema(),
                    sourceRecord.key(),
                    sourceRecord.valueSchema(),
                    delayedEnvelope,
                    sourceRecord.timestamp(),
                    sourceRecord.headers());
        }

        private static DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>>
        forwardingCommitter(
                DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>>
                        delegate,
                Map<RecordChangeEvent<SourceRecord>,
                        RecordChangeEvent<SourceRecord>> originals)
        {
            return new DebeziumEngine.RecordCommitter<>()
            {
                @Override
                public void markProcessed(RecordChangeEvent<SourceRecord> record)
                        throws InterruptedException
                {
                    delegate.markProcessed(originals.getOrDefault(record, record));
                }

                @Override
                public void markProcessed(
                        RecordChangeEvent<SourceRecord> record,
                        DebeziumEngine.Offsets offsets)
                        throws InterruptedException
                {
                    delegate.markProcessed(
                            originals.getOrDefault(record, record), offsets);
                }

                @Override
                public void markBatchFinished() throws InterruptedException
                {
                    delegate.markBatchFinished();
                }

                @Override
                public DebeziumEngine.Offsets buildOffsets()
                {
                    return delegate.buildOffsets();
                }
            };
        }
    }

    private static final class DelayedEnvelope extends Struct
    {
        private final long sequence;
        private final long delayMillis;
        private final LongConsumer observed;
        private final AtomicBoolean afterObserved = new AtomicBoolean();

        private DelayedEnvelope(
                Struct delegate,
                long sequence,
                long delayMillis,
                LongConsumer observed)
        {
            super(delegate.schema());
            this.sequence = sequence;
            this.delayMillis = delayMillis;
            this.observed = observed;
            for (Field field : delegate.schema().fields())
            {
                put(field, delegate.get(field));
            }
        }

        @Override
        public Struct getStruct(String fieldName)
        {
            Struct value = super.getStruct(fieldName);
            if ("after".equals(fieldName) &&
                    value != null &&
                    afterObserved.compareAndSet(false, true))
            {
                delay();
                observed.accept(sequence);
            }
            return value;
        }

        private void delay()
        {
            if (delayMillis <= 0)
            {
                return;
            }
            try
            {
                Thread.sleep(delayMillis);
            } catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Interrupted while forcing decode interleaving", e);
            }
        }
    }
}
