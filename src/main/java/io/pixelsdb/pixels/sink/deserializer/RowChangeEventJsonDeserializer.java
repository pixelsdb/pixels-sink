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

package io.pixelsdb.pixels.sink.deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.monitor.MetricsFacade;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class RowChangeEventJsonDeserializer implements Deserializer<RowChangeEvent> {
    private static final Logger logger = LoggerFactory.getLogger(RowChangeEventJsonDeserializer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public RowChangeEvent deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            logger.debug("Received empty message from topic: {}", topic);
            return null;
        }
        MetricsFacade.getInstance().addRawData(data.length);
        try {
            JsonNode rootNode = objectMapper.readTree(data);
            JsonNode schemaNode = rootNode.path("schema");
            JsonNode payloadNode = rootNode.path("payload");

            SinkProto.OperationType opType = parseOperationType(payloadNode);
            TypeDescription schema = getSchema(schemaNode, opType);

            return buildRowRecord(payloadNode, schema, opType);
        } catch (Exception e) {
            logger.error("Failed to deserialize message from topic {}: {}", topic, e.getMessage());
            return DeserializerUtil.buildErrorEvent(topic, data, e);
        }
    }

    private SinkProto.OperationType parseOperationType(JsonNode payloadNode) {
        String opCode = payloadNode.path("op").asText("");
        return DeserializerUtil.getOperationType(opCode);
    }

    // TODO: cache schema
    private TypeDescription getSchema(JsonNode schemaNode, SinkProto.OperationType opType) {
        switch (opType) {
            case DELETE:
                return SchemaDeserializer.parseFromBeforeOrAfter(schemaNode, "before");
            case INSERT:
            case UPDATE:
            case SNAPSHOT:
                return SchemaDeserializer.parseFromBeforeOrAfter(schemaNode, "after");
            case UNRECOGNIZED:
                throw new IllegalArgumentException("Operation type is unknown. Check op");
        }
        return null;
    }

    private RowChangeEvent buildRowRecord(JsonNode payloadNode,
                                          TypeDescription schema,
                                          SinkProto.OperationType opType) {

        SinkProto.RowRecord.Builder builder = SinkProto.RowRecord.newBuilder();

        builder.setOp(parseOperationType(payloadNode))
                .setTsMs(payloadNode.path("ts_ms").asLong())
                .setTsUs(payloadNode.path("ts_us").asLong())
                .setTsNs(payloadNode.path("ts_ns").asLong());

        Map<String, Object> beforeData = parseDataFields(payloadNode, schema, opType, "before");
        Map<String, Object> afterData = parseDataFields(payloadNode, schema, opType, "after");
        if (payloadNode.has("source")) {
            builder.setSource(parseSourceInfo(payloadNode.get("source")));
        }

        if (payloadNode.hasNonNull("transaction")) {
            builder.setTransaction(parseTransactionInfo(payloadNode.get("transaction")));
        }

        // RowChangeEvent event = new RowChangeEvent(builder.build(), schema, opType, beforeData, afterData);
        RowChangeEvent event = new RowChangeEvent(builder.build());
        event.initIndexKey();
        return event;
    }

    private Map<String, Object> parseDataFields(JsonNode payloadNode,
                                                TypeDescription schema,
                                                SinkProto.OperationType opType,
                                                String dataField) {
        RowDataParser parser = new RowDataParser(schema);

        JsonNode dataNode = payloadNode.get(dataField);
        if (dataNode != null && !dataNode.isNull()) {
            return parser.parse(dataNode, opType);
        }
        return null;
    }

    private JsonNode resolveDataNode(JsonNode payloadNode, SinkProto.OperationType opType) {
        return opType == SinkProto.OperationType.DELETE ?
                payloadNode.get("before") :
                payloadNode.get("after");
    }


    private SinkProto.SourceInfo parseSourceInfo(JsonNode sourceNode) {
        return SinkProto.SourceInfo.newBuilder()
                .setVersion(sourceNode.path("version").asText())
                .setConnector(sourceNode.path("connector").asText())
                .setName(sourceNode.path("name").asText())
                .setTsMs(sourceNode.path("ts_ms").asLong())
                .setSnapshot(sourceNode.path("snapshot").asText())
                .setDb(sourceNode.path("db").asText())
                .setSequence(sourceNode.path("sequence").asText())
                .setTsUs(sourceNode.path("ts_us").asLong())
                .setTsNs(sourceNode.path("ts_ns").asLong())
                .setSchema(sourceNode.path("schema").asText())
                .setTable(sourceNode.path("table").asText())
                .setTxId(sourceNode.path("txId").asLong())
                .setLsn(sourceNode.path("lsn").asLong())
                .setXmin(sourceNode.path("xmin").asLong())
                .build();
    }

    private SinkProto.TransactionInfo parseTransactionInfo(JsonNode txNode) {
        return SinkProto.TransactionInfo.newBuilder()
                .setId(txNode.path("id").asText())
                .setTotalOrder(txNode.path("total_order").asLong())
                .setDataCollectionOrder(txNode.path("data_collection_order").asLong())
                .build();
    }


    private boolean hasAfterData(SinkProto.OperationType op) {
        return op != SinkProto.OperationType.DELETE;
    }

    private boolean hasBeforeData(SinkProto.OperationType op) {
        return op == SinkProto.OperationType.DELETE || op == SinkProto.OperationType.UPDATE;
    }
}

