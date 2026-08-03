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
package io.pixelsdb.pixels.sink.conversion.debezium.connect;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.conversion.debezium.DebeziumRowConverter;
import io.pixelsdb.pixels.sink.conversion.debezium.dialect.DebeziumSourceAdapter;
import io.pixelsdb.pixels.sink.conversion.debezium.support.DebeziumEnvelopeNormalizer;
import io.pixelsdb.pixels.sink.conversion.debezium.support.DebeziumRecordUtil;
import io.pixelsdb.pixels.sink.conversion.debezium.support.DebeziumRowRecordAssembler;
import io.pixelsdb.pixels.sink.conversion.debezium.support.DebeziumRowValueConverter;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts Kafka Connect {@link SourceRecord} Debezium envelopes to row events.
 */
public final class DebeziumConnectRowConverter implements DebeziumRowConverter<SourceRecord>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumConnectRowConverter.class);

    private final TableMetadataRegistry tableMetadataRegistry;
    private final DebeziumSourceAdapter configuredAdapter;
    private final DebeziumRowRecordAssembler rowRecordAssembler = new DebeziumRowRecordAssembler();

    public DebeziumConnectRowConverter(
            TableMetadataRegistry tableMetadataRegistry,
            DebeziumSourceAdapter configuredAdapter)
    {
        this.tableMetadataRegistry = tableMetadataRegistry;
        this.configuredAdapter = configuredAdapter;
    }

    @Override
    public RowChangeEvent convert(SourceRecord sourceRecord) throws SinkException
    {
        if (!(sourceRecord.value() instanceof Struct value))
        {
            throw new SinkException("Debezium row value must be a Struct");
        }
        String op = DebeziumRecordUtil.getStringSafely(value, "op");
        SinkProto.OperationType operationType = DebeziumRecordUtil.getOperationType(op);
        return buildRowRecord(value, operationType);
    }

    private RowChangeEvent buildRowRecord(
            Struct value, SinkProto.OperationType opType) throws SinkException
    {
        DebeziumSourceAdapter adapter;
        SinkProto.SourceInfo sourceInfo;
        try
        {
            Struct source = value.getStruct("source");
            if (source == null)
            {
                throw new SinkException("Missing source field in row record");
            }
            adapter = configuredAdapter == null
                    ? DebeziumEnvelopeNormalizer.adapterForSource(source)
                    : configuredAdapter;
            sourceInfo = DebeziumEnvelopeNormalizer.normalizeSource(source, adapter);
        } catch (DataException | IllegalArgumentException e)
        {
            LOGGER.warn("Missing source field in row record");
            throw new SinkException(e);
        }

        TableMetadata metadata = tableMetadataRegistry.getMetadata(
                sourceInfo.getDb(), sourceInfo.getTable());
        TypeDescription typeDescription = metadata.getTypeDescription();
        DebeziumRowValueConverter rowDataParser = new DebeziumRowValueConverter(typeDescription);

        SinkProto.TransactionInfo transactionInfo = null;
        try
        {
            Struct transaction = value.getStruct("transaction");
            if (transaction != null)
            {
                transactionInfo = DebeziumEnvelopeNormalizer.normalizeTransaction(
                        transaction, adapter);
            }
        } catch (DataException e)
        {
            LOGGER.warn("Missing transaction field in row record");
        }

        SinkProto.RowValue beforeValue = null;
        if (DebeziumRecordUtil.hasBeforeValue(opType))
        {
            Struct before = value.getStruct("before");
            if (before == null)
            {
                throw new SinkException("Missing before image for " + opType);
            }
            SinkProto.RowValue.Builder beforeBuilder = SinkProto.RowValue.newBuilder();
            rowDataParser.parse(before, beforeBuilder);
            beforeValue = beforeBuilder.build();
        }

        SinkProto.RowValue afterValue = null;
        if (DebeziumRecordUtil.hasAfterValue(opType))
        {
            Struct after = value.getStruct("after");
            if (after == null)
            {
                throw new SinkException("Missing after image for " + opType);
            }
            SinkProto.RowValue.Builder afterBuilder = SinkProto.RowValue.newBuilder();
            rowDataParser.parse(after, afterBuilder);
            afterValue = afterBuilder.build();
        }

        return rowRecordAssembler.assemble(
                opType, sourceInfo, transactionInfo, beforeValue, afterValue,
                typeDescription, metadata);
    }
}
