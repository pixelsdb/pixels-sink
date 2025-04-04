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

import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.proto.RowRecordMessage;
import io.pixelsdb.pixels.sink.proto.SinkProto;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RowChangeEventAvroDeserializer implements Deserializer<RowChangeEvent> {

    private final AvroKafkaDeserializer<GenericRecord> avroDeserializer = new AvroKafkaDeserializer<>();
    private final TableMetadataRegistry tableMetadataRegistry = TableMetadataRegistry.Instance();
    private final PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> enrichedConfig = new HashMap<>(configs);
        enrichedConfig.put(SerdeConfig.REGISTRY_URL, config.getRegistryUrl());
        avroDeserializer.configure(enrichedConfig, isKey);
    }

    @Override
    public RowChangeEvent deserialize(String topic, byte[] data) {
        try {
            GenericRecord avroRecord = avroDeserializer.deserialize(topic, data);
            Schema avroSchema = avroRecord.getSchema();
            registerSchema(topic, avroSchema);
            return convertToRowChangeEvent(avroRecord, avroSchema);
        } catch (Exception e) {
            throw new SerializationException("Avro deserialization failed", e);
        }
    }

    private void registerSchema(String topic, Schema avroSchema) {
//        TypeDescription schemaDesc = schemaCache.computeIfAbsent(topic, () ->
//                SchemaDeserializer.parseFromAvroSchema(avroSchema)
//        );
//
//        TableMetadata metadata = new TableMetadata(
//                avroSchema.getFullName(),
//                schemaDesc,
//                System.currentTimeMillis()
//        );
//
//        metadataRegistry.register(topic, metadata);
    }

    private RowChangeEvent convertToRowChangeEvent(GenericRecord avroRecord, Schema schema) {
        SinkProto.OperationType op = parseOperationType(avroRecord);
        long tsNs = (Long) avroRecord.get("ts_ns");
        RowRecordMessage.RowRecord.Builder recordBuilder = RowRecordMessage.RowRecord.newBuilder()
                .setOp(avroRecord.get("op").toString())
                .setTsMs((Long) avroRecord.get("ts_ms"))
                .setTsUs((Long) avroRecord.get("ts_us"))
                .setTsNs(tsNs);

        parseRowData(avroRecord.get("before"), recordBuilder.getBeforeBuilder());
        parseRowData(avroRecord.get("after"), recordBuilder.getAfterBuilder());

        if (avroRecord.get("source") != null) {
            parseSourceInfo((GenericRecord) avroRecord.get("source"), recordBuilder.getSourceBuilder());
        }

        if (avroRecord.get("transaction") != null) {
            parseTransactionInfo((GenericRecord) avroRecord.get("transaction"),
                    recordBuilder.getTransactionBuilder());
        }

        return new RowChangeEvent(
                recordBuilder.build(),
                SchemaDeserializer.parseFromAvroSchema(schema),
                op,
                convertToDataMap(avroRecord.get("before")),
                convertToDataMap(avroRecord.get("after"))
        );
    }

    // 辅助方法实现
    private SinkProto.OperationType parseOperationType(GenericRecord record) {
        String op = record.get("op").toString().toUpperCase();
        try {
            return SinkProto.OperationType.valueOf(op);
        } catch (IllegalArgumentException e) {
            return SinkProto.OperationType.UNRECOGNIZED;
        }
    }

    private void parseRowData(Object data, RowRecordMessage.RowData.Builder builder) {
        if (data instanceof GenericRecord) {
            GenericRecord rowData = (GenericRecord) data;
            builder.setId((Long) rowData.get("id"))
                    .setName(rowData.get("name").toString());

            // 处理attributes map
            Map<CharSequence, CharSequence> avroMap =
                    (Map<CharSequence, CharSequence>) rowData.get("attributes");
            avroMap.forEach((k, v) -> builder.putAttributes(k.toString(), v.toString()));
        }
    }

    private void parseSourceInfo(GenericRecord source, RowRecordMessage.SourceInfo.Builder builder) {
        builder.setVersion(getStringSafely(source, "version"))
                .setConnector(getStringSafely(source, "connector"))
                .setName(getStringSafely(source, "name"))
                .setTsMs(getLongSafely(source, "ts_ms"))
                .setSnapshot(getStringSafely(source, "snapshot"))
                .setDb(getStringSafely(source, "db"))
                .setSequence(getStringSafely(source, "sequence"))
                .setSchema(getStringSafely(source, "schema"))
                .setTable(getStringSafely(source, "table"))
                .setTxId(getLongSafely(source, "tx_id"))
                .setLsn(getLongSafely(source, "lsn"))
                .setXmin(getLongSafely(source, "xmin"));
    }

    private void parseTransactionInfo(GenericRecord transaction,
                                      RowRecordMessage.TransactionInfo.Builder builder) {
        builder.setId(getStringSafely(transaction, "id"))
                .setTotalOrder(getLongSafely(transaction, "total_order"))
                .setDataCollectionOrder(getLongSafely(transaction, "data_collection_order"));
    }

    private Map<String, Object> convertToDataMap(Object data) {
        if (!(data instanceof GenericRecord)) return null;

        GenericRecord record = (GenericRecord) data;
        Map<String, Object> result = new LinkedHashMap<>();
        record.getSchema().getFields().forEach(field -> {
            Object value = record.get(field.name());
            result.put(field.name(), convertAvroValue(value));
        });
        return result;
    }

    private Object convertAvroValue(Object value) {
        if (value instanceof GenericRecord) {
            return convertToDataMap(value);
        } else if (value instanceof String) {
            return value.toString();
        } else if (value instanceof List) {
            return ((List<?>) value).stream()
                    .map(this::convertAvroValue)
                    .collect(Collectors.toList());
        } else if (value instanceof Map) {
            Map<Object, Object> converted = new LinkedHashMap<>();
            ((Map<?, ?>) value).forEach((k, v) ->
                    converted.put(k.toString(), convertAvroValue(v)));
            return converted;
        }
        return value;
    }

    // 安全取值方法
    private String getStringSafely(GenericRecord record, String field) {
        Object value = record.get(field);
        return value != null ? value.toString() : "";
    }

    private long getLongSafely(GenericRecord record, String field) {
        Object value = record.get(field);
        return value instanceof Number ? ((Number) value).longValue() : 0L;
    }


    private void parseRowData(GenericRecord record, String fieldName, RowRecordMessage.RowData.Builder builder) {
        if (record.get(fieldName) != null) {
            GenericRecord data = (GenericRecord) record.get(fieldName);
            builder.setId((Long) data.get("id"))
                    .setName(data.get("name").toString());

            // 解析attributes
            Map<CharSequence, CharSequence> avroMap = (Map<CharSequence, CharSequence>) data.get("attributes");
            avroMap.forEach((k, v) -> builder.putAttributes(k.toString(), v.toString()));
        }
    }

    private Map<String, Object> parseDataMap(Object data) {
        if (data == null) return null;

        GenericRecord record = (GenericRecord) data;
        Map<String, Object> result = new HashMap<>();
        record.getSchema().getFields().forEach(field -> {
            result.put(field.name(), record.get(field.name()));
        });
        return result;
    }
}