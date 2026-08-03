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
package io.pixelsdb.pixels.sink.conversion.debezium.avro;

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
import org.apache.avro.generic.GenericRecord;

public final class DebeziumAvroRowConverter implements DebeziumRowConverter<GenericRecord>
{
    private final TableMetadataRegistry tableMetadataRegistry;
    private final DebeziumSourceAdapter configuredAdapter;
    private final DebeziumRowRecordAssembler rowRecordAssembler =
            new DebeziumRowRecordAssembler();

    public DebeziumAvroRowConverter(
            TableMetadataRegistry tableMetadataRegistry,
            DebeziumSourceAdapter configuredAdapter)
    {
        this.tableMetadataRegistry = tableMetadataRegistry;
        this.configuredAdapter = configuredAdapter;
    }

    @Override
    public RowChangeEvent convert(GenericRecord record) throws SinkException
    {
        SinkProto.OperationType operation = DebeziumRecordUtil.getOperationType(
                DebeziumRecordUtil.getStringSafely(record, "op"));
        Object sourceObject = record.get("source");
        if (!(sourceObject instanceof GenericRecord source))
        {
            throw new SinkException("Missing source field in row record");
        }
        DebeziumSourceAdapter adapter = configuredAdapter == null
                ? DebeziumEnvelopeNormalizer.adapterForSource(source)
                : configuredAdapter;
        SinkProto.SourceInfo sourceInfo =
                DebeziumEnvelopeNormalizer.normalizeSource(source, adapter);
        TableMetadata metadata = tableMetadataRegistry.getMetadata(
                sourceInfo.getDb(), sourceInfo.getTable());
        TypeDescription schema = metadata.getTypeDescription();
        DebeziumRowValueConverter rowValueConverter =
                new DebeziumRowValueConverter(schema);

        SinkProto.RowValue before = parseRowValue(
                record.get("before"), operation, true, rowValueConverter);
        SinkProto.RowValue after = parseRowValue(
                record.get("after"), operation, false, rowValueConverter);
        SinkProto.TransactionInfo transaction = record.get("transaction") == null
                ? null
                : DebeziumEnvelopeNormalizer.normalizeTransaction(
                record.get("transaction"), adapter);
        return rowRecordAssembler.assemble(
                operation, sourceInfo, transaction, before, after, schema, metadata);
    }

    private SinkProto.RowValue parseRowValue(
            Object value,
            SinkProto.OperationType operation,
            boolean before,
            DebeziumRowValueConverter converter) throws SinkException
    {
        boolean required = before
                ? DebeziumRecordUtil.hasBeforeValue(operation)
                : DebeziumRecordUtil.hasAfterValue(operation);
        if (!required)
        {
            return null;
        }
        if (!(value instanceof GenericRecord row))
        {
            throw new SinkException(
                    "Missing " + (before ? "before" : "after") + " image for " + operation);
        }
        SinkProto.RowValue.Builder builder = SinkProto.RowValue.newBuilder();
        converter.parse(row, builder);
        return builder.build();
    }
}
