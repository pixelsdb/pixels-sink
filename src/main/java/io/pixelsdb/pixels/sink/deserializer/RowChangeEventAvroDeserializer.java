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
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.monitor.MetricsFacade;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.HashMap;
import java.util.Map;

public class RowChangeEventAvroDeserializer implements Deserializer<RowChangeEvent> {

    private final AvroKafkaDeserializer<GenericRecord> avroDeserializer = new AvroKafkaDeserializer<>();
    private final TableMetadataRegistry tableMetadataRegistry = TableMetadataRegistry.Instance();
    private final PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> enrichedConfig = new HashMap<>(configs);
        enrichedConfig.put(SerdeConfig.REGISTRY_URL, config.getRegistryUrl());
        enrichedConfig.put(SerdeConfig.CHECK_PERIOD_MS, SerdeConfig.CHECK_PERIOD_MS_DEFAULT);
        avroDeserializer.configure(enrichedConfig, isKey);
    }

    @Override
    public RowChangeEvent deserialize(String topic, byte[] data) {
        try {
            MetricsFacade.getInstance().addRawData(data.length);
            GenericRecord avroRecord = avroDeserializer.deserialize(topic, data);
            Schema avroSchema = avroRecord.getSchema();
            RowChangeEvent rowChangeEvent = convertToRowChangeEvent(avroRecord, avroSchema);
            return rowChangeEvent;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
            // throw new SerializationException("Avro deserialization failed", e);
            // TODO: 这里有些Row Change Event 不是完整的结构。
        }
    }

    private void registerSchema(String topic, Schema avroSchema) {

    }

    private RowChangeEvent convertToRowChangeEvent(GenericRecord avroRecord, Schema schema) {
        SinkProto.OperationType op = parseOperationType(avroRecord);
        SinkProto.RowRecord.Builder recordBuilder = SinkProto.RowRecord.newBuilder()
                .setOp(DeserializerUtil.getOperationType(avroRecord.get("op").toString()))
                .setTsMs(DeserializerUtil.getLongSafely(avroRecord, "ts_ms"));

        if (avroRecord.get("source") != null) {
            //TODO: 这里看下怎么处理，如果没有source信息，其实可以通过topic推出schema和table信息。
            parseSourceInfo((GenericRecord) avroRecord.get("source"), recordBuilder.getSourceBuilder());
        }

        String sourceSchema = recordBuilder.getSource().getSchema();
        String sourceTable = recordBuilder.getSource().getTable();
        TypeDescription typeDescription = tableMetadataRegistry.parseTypeDescription(avroRecord, sourceSchema, sourceTable);
        // TableMetadata tableMetadata = tableMetadataRegistry.loadTableMetadata(sourceSchema, sourceTable);

        parseRowData(avroRecord.get("before"), recordBuilder.getBeforeBuilder(), typeDescription);
        parseRowData(avroRecord.get("after"), recordBuilder.getAfterBuilder(), typeDescription);

        if (avroRecord.get("transaction") != null) {
            parseTransactionInfo((GenericRecord) avroRecord.get("transaction"),
                    recordBuilder.getTransactionBuilder());
        }

        return new RowChangeEvent(recordBuilder.build());
    }

    private SinkProto.OperationType parseOperationType(GenericRecord record) {
        String op = DeserializerUtil.getStringSafely(record, "op");
        try {
            return DeserializerUtil.getOperationType(op);
        } catch (IllegalArgumentException e) {
            return SinkProto.OperationType.UNRECOGNIZED;
        }
    }

    private void parseRowData(Object data, SinkProto.RowValue.Builder builder, TypeDescription typeDescription) {
        if (data instanceof GenericRecord rowData) {
            RowDataParser rowDataParser = new RowDataParser(typeDescription); // TODO make it static?
            rowDataParser.parse(rowData, builder);
        }
    }

    private void parseSourceInfo(GenericRecord source, SinkProto.SourceInfo.Builder builder) {
        builder.setVersion(DeserializerUtil.getStringSafely(source, "version"))
                .setConnector(DeserializerUtil.getStringSafely(source, "connector"))
                .setName(DeserializerUtil.getStringSafely(source, "name"))
                .setTsMs(DeserializerUtil.getLongSafely(source, "ts_ms"))
                .setSnapshot(DeserializerUtil.getStringSafely(source, "snapshot"))
                .setDb(DeserializerUtil.getStringSafely(source, "db"))
                .setSequence(DeserializerUtil.getStringSafely(source, "sequence"))
                .setSchema(DeserializerUtil.getStringSafely(source, "schema"))
                .setTable(DeserializerUtil.getStringSafely(source, "table"))
                .setTxId(DeserializerUtil.getLongSafely(source, "tx_id"))
                .setLsn(DeserializerUtil.getLongSafely(source, "lsn"))
                .setXmin(DeserializerUtil.getLongSafely(source, "xmin"));
    }

    private void parseTransactionInfo(GenericRecord transaction,
                                      SinkProto.TransactionInfo.Builder builder) {
        builder.setId(DeserializerUtil.getStringSafely(transaction, "id"))
                .setTotalOrder(DeserializerUtil.getLongSafely(transaction, "total_order"))
                .setDataCollectionOrder(DeserializerUtil.getLongSafely(transaction, "data_collection_order"));
    }

}