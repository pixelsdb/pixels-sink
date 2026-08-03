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
package io.pixelsdb.pixels.sink.source.engine;


import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.conversion.debezium.DebeziumRowConverter;
import io.pixelsdb.pixels.sink.conversion.debezium.DebeziumTransactionConverter;
import io.pixelsdb.pixels.sink.conversion.debezium.connect.DebeziumConnectRowConverter;
import io.pixelsdb.pixels.sink.conversion.debezium.connect.DebeziumConnectTransactionConverter;
import io.pixelsdb.pixels.sink.conversion.debezium.dialect.DebeziumSourceAdapter;
import io.pixelsdb.pixels.sink.conversion.debezium.support.DebeziumRecordUtil;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.pipeline.TablePipelineManager;
import io.pixelsdb.pixels.sink.pipeline.TransactionPipeline;
import io.pixelsdb.pixels.sink.source.engine.adapter.DebeziumSourceAdapterSelector;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import io.pixelsdb.pixels.sink.util.concurrent.StreamOrderedDecoder;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * @package: io.pixelsdb.pixels.source
 * @className: PixelsDebeziumConsumer
 * @author: AntiO2
 * @date: 2025/9/25 12:51
 */
public class PixelsDebeziumConsumer
        implements DebeziumEngine.ChangeConsumer<RecordChangeEvent<SourceRecord>>, AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PixelsDebeziumConsumer.class);
    private static final Object TRANSACTION_STREAM_KEY = new Object();
    private static final ConnectEventClassifier CLASSIFIER = new ConnectEventClassifier();

    private final String checkTransactionTopic;
    private final DebeziumSourceAdapter connectorAdapter;
    private final DebeziumRowConverter<SourceRecord> rowConverter;
    private final DebeziumTransactionConverter<SourceRecord> transactionConverter;
    private final TransactionPipeline transactionPipeline;
    private final TablePipelineManager tablePipelineManager;
    private final StreamOrderedDecoder decodePipeline;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();

    private record PendingRecord(
            RecordChangeEvent<SourceRecord> record,
            CompletableFuture<Void> completion)
    {
    }

    public PixelsDebeziumConsumer()
    {
        this(null, false);
    }

    public PixelsDebeziumConsumer(PixelsSinkWriter writer)
    {
        this(Objects.requireNonNull(writer, "writer is null"), true);
    }

    private PixelsDebeziumConsumer(PixelsSinkWriter writer, boolean writerInjected)
    {
        this.checkTransactionTopic = pixelsSinkConfig.getDebeziumTopicPrefix() + ".transaction";
        this.connectorAdapter = DebeziumSourceAdapterSelector.configured();
        this.rowConverter = new DebeziumConnectRowConverter(
                TableMetadataRegistry.Instance(), connectorAdapter);
        this.transactionConverter = new DebeziumConnectTransactionConverter(connectorAdapter);
        this.transactionPipeline = writerInjected
                ? new TransactionPipeline(writer)
                : new TransactionPipeline();
        this.tablePipelineManager = writerInjected
                ? new TablePipelineManager(writer)
                : new TablePipelineManager();
        this.decodePipeline = new StreamOrderedDecoder(
                pixelsSinkConfig.getSourceDecodeThreads(),
                "debezium-decoder");
    }

    public void start()
    {
        transactionPipeline.start();
        decodePipeline.start();
    }


    public void handleBatch(List<RecordChangeEvent<SourceRecord>> event,
                            DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>> committer) throws InterruptedException
    {
        List<PendingRecord> pendingRecords = new ArrayList<>(event.size());
        for (RecordChangeEvent<SourceRecord> record : event)
        {
            SourceRecord sourceRecord = record.record();
            if (sourceRecord == null)
            {
                pendingRecords.add(new PendingRecord(
                        record, CompletableFuture.completedFuture(null)));
                continue;
            }

            metricsFacade.recordDebeziumEvent();
            DebeziumRecordType recordType =
                    CLASSIFIER.classify(sourceRecord, checkTransactionTopic);
            logSourceRecord(sourceRecord, recordType);
            CompletableFuture<Void> completion = switch (recordType)
            {
                case ROW -> submitRow(sourceRecord);
                case TRANSACTION -> submitTransaction(sourceRecord);
                case TOMBSTONE, UNKNOWN_CONTROL ->
                {
                    LOGGER.debug("Skipping Debezium {} event from topic {}",
                            recordType, sourceRecord.topic());
                    yield CompletableFuture.completedFuture(null);
                }
            };
            pendingRecords.add(new PendingRecord(record, completion));
        }

        for (PendingRecord pending : pendingRecords)
        {
            awaitCompletion(pending.completion());
            committer.markProcessed(pending.record());
        }
        committer.markBatchFinished();
    }

    private CompletableFuture<Void> submitRow(SourceRecord record)
    {
        return decodePipeline.submit(
                CLASSIFIER.tableOf(record),
                record,
                rowConverter::convert,
                result -> publishRow(record, result));
    }

    private CompletableFuture<Void> submitTransaction(SourceRecord record)
    {
        return decodePipeline.submit(
                TRANSACTION_STREAM_KEY,
                record,
                transactionConverter::convert,
                result -> publishTransaction(record, result));
    }

    private void publishRow(
            SourceRecord record,
            StreamOrderedDecoder.DecodeResult<RowChangeEvent> result)
    {
        if (result.failure() != null)
        {
            LOGGER.warn("Skipping invalid Debezium ROW event from topic {}",
                    record.topic(), result.failure());
            return;
        }
        if (result.value() == null)
        {
            return;
        }
        metricsFacade.recordSerdRowChange();
        tablePipelineManager.route(result.value());
    }

    private void publishTransaction(
            SourceRecord record,
            StreamOrderedDecoder.DecodeResult<SinkProto.TransactionMetadata> result)
    {
        if (result.failure() != null)
        {
            LOGGER.warn("Skipping invalid Debezium TRANSACTION event from topic {}",
                    record.topic(), result.failure());
            return;
        }
        if (result.value() == null)
        {
            return;
        }
        metricsFacade.recordSerdTxChange();
        transactionPipeline.publish(result.value());
    }

    private void awaitCompletion(CompletableFuture<Void> completion)
            throws InterruptedException
    {
        try
        {
            completion.get();
        } catch (ExecutionException e)
        {
            throw new IllegalStateException(
                    "Debezium decode pipeline failed before publishing an event",
                    e.getCause());
        }
    }

    private void logSourceRecord(SourceRecord record, DebeziumRecordType recordType)
    {
        if (!LOGGER.isDebugEnabled())
        {
            return;
        }
        Struct value = record.value() instanceof Struct struct ? struct : null;
        Struct source = value == null ? null :
                asStruct(DebeziumRecordUtil.getFieldSafely(value, "source"));
        Struct transaction = value == null ? null :
                asStruct(DebeziumRecordUtil.getFieldSafely(value, "transaction"));
        String rawTransactionId = recordType == DebeziumRecordType.TRANSACTION
                ? DebeziumRecordUtil.getStringSafely(value, "id")
                : DebeziumRecordUtil.getStringSafely(transaction, "id");
        String canonicalTransactionId =
                connectorAdapter.normalizeTransactionId(rawTransactionId);

        LOGGER.debug("Debezium SourceRecord topic={}, sourcePartition={}, sourceOffset={}, " +
                        "keySchema={}, key={}, valueSchema={}, category={}, transaction.id={}, " +
                        "source.gtid={}, source.file={}, source.pos={}, source.row={}",
                record.topic(), record.sourcePartition(), record.sourceOffset(),
                schemaName(record.keySchema()), record.key(), schemaName(record.valueSchema()),
                recordType, canonicalTransactionId,
                DebeziumRecordUtil.getStringSafely(source, "gtid"),
                DebeziumRecordUtil.getStringSafely(source, "file"),
                DebeziumRecordUtil.getStringSafely(source, "pos"),
                DebeziumRecordUtil.getStringSafely(source, "row"));
    }

    private static Struct asStruct(Object value)
    {
        return value instanceof Struct struct ? struct : null;
    }

    private static String schemaName(org.apache.kafka.connect.data.Schema schema)
    {
        return schema == null ? "" : String.valueOf(schema.name());
    }

    @Override
    public void close()
    {
        decodePipeline.close();
        tablePipelineManager.close();
        transactionPipeline.close();
    }

    public void abort()
    {
        decodePipeline.abort();
        tablePipelineManager.abort();
        transactionPipeline.abort();
    }
}
