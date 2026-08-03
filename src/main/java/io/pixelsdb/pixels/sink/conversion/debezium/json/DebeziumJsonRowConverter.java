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
package io.pixelsdb.pixels.sink.conversion.debezium.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

public final class DebeziumJsonRowConverter implements DebeziumRowConverter<byte[]>
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final TableMetadataRegistry tableMetadataRegistry;
    private final DebeziumSourceAdapter configuredAdapter;
    private final DebeziumRowRecordAssembler rowRecordAssembler =
            new DebeziumRowRecordAssembler();

    public DebeziumJsonRowConverter(
            TableMetadataRegistry tableMetadataRegistry,
            DebeziumSourceAdapter configuredAdapter)
    {
        this.tableMetadataRegistry = tableMetadataRegistry;
        this.configuredAdapter = configuredAdapter;
    }

    @Override
    public RowChangeEvent convert(byte[] data) throws Exception
    {
        JsonNode rootNode = OBJECT_MAPPER.readTree(data);
        JsonNode payloadNode = rootNode.path("payload");
        SinkProto.OperationType operation = DebeziumRecordUtil.getOperationType(
                payloadNode.path("op").asText(""));

        if (!payloadNode.hasNonNull("source"))
        {
            throw new SinkException("Missing source field in row record");
        }

        JsonNode sourceNode = payloadNode.get("source");
        DebeziumSourceAdapter adapter = configuredAdapter == null
                ? DebeziumEnvelopeNormalizer.adapterForSource(sourceNode)
                : configuredAdapter;
        SinkProto.SourceInfo sourceInfo =
                DebeziumEnvelopeNormalizer.normalizeSource(sourceNode, adapter);
        TableMetadata metadata = tableMetadataRegistry.getMetadata(
                sourceInfo.getDb(), sourceInfo.getTable());
        TypeDescription schema = metadata.getTypeDescription();
        DebeziumRowValueConverter rowValueConverter =
                new DebeziumRowValueConverter(schema);

        SinkProto.TransactionInfo transaction = payloadNode.hasNonNull("transaction")
                ? DebeziumEnvelopeNormalizer.normalizeTransaction(
                payloadNode.get("transaction"), adapter)
                : null;
        SinkProto.RowValue before = parseBefore(
                payloadNode, operation, rowValueConverter);
        SinkProto.RowValue after = parseAfter(
                payloadNode, operation, rowValueConverter);
        return rowRecordAssembler.assemble(
                operation, sourceInfo, transaction, before, after, schema, metadata);
    }

    private SinkProto.RowValue parseBefore(
            JsonNode payload,
            SinkProto.OperationType operation,
            DebeziumRowValueConverter converter) throws SinkException
    {
        if (!DebeziumRecordUtil.hasBeforeValue(operation))
        {
            return null;
        }
        JsonNode before = payload.get("before");
        if (before == null || before.isNull())
        {
            throw new SinkException("Missing before image for " + operation);
        }
        SinkProto.RowValue.Builder builder = SinkProto.RowValue.newBuilder();
        converter.parse(before, builder);
        return builder.build();
    }

    private SinkProto.RowValue parseAfter(
            JsonNode payload,
            SinkProto.OperationType operation,
            DebeziumRowValueConverter converter) throws SinkException
    {
        if (!DebeziumRecordUtil.hasAfterValue(operation))
        {
            return null;
        }
        JsonNode after = payload.get("after");
        if (after == null || after.isNull())
        {
            throw new SinkException("Missing after image for " + operation);
        }
        SinkProto.RowValue.Builder builder = SinkProto.RowValue.newBuilder();
        converter.parse(after, builder);
        return builder.build();
    }
}
