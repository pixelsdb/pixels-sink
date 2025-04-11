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
import io.pixelsdb.pixels.sink.proto.TransactionMetadataValue;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class TransactionAvroMessageDeserializer implements Deserializer<TransactionMetadataValue.TransactionMetadata> {
    private static final Logger logger = LoggerFactory.getLogger(TransactionAvroMessageDeserializer.class);
    private final AvroKafkaDeserializer<GenericRecord> avroDeserializer = new AvroKafkaDeserializer<>();
    private final PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> enrichedConfig = new HashMap<>(configs);
        enrichedConfig.put(SerdeConfig.REGISTRY_URL, config.getRegistryUrl());
        enrichedConfig.put(SerdeConfig.CHECK_PERIOD_MS, SerdeConfig.CHECK_PERIOD_MS_DEFAULT);
        avroDeserializer.configure(enrichedConfig, isKey);
    }

    @Override
    public TransactionMetadataValue.TransactionMetadata deserialize(String topic, byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        try {
            GenericRecord avroRecord = avroDeserializer.deserialize(topic, bytes);
            return convertToTransactionMetadata(avroRecord);
        } catch (Exception e) {
            logger.error("Avro deserialization failed for topic {}: {}", topic, e.getMessage());
            throw new SerializationException("Failed to deserialize Avro message", e);
        }
    }

    private TransactionMetadataValue.TransactionMetadata convertToTransactionMetadata(GenericRecord record) {
        TransactionMetadataValue.TransactionMetadata.Builder builder =
                TransactionMetadataValue.TransactionMetadata.newBuilder();
        builder.setStatus(DeserializerUtil.getStringSafely(record, "status"))
                .setId(DeserializerUtil.getStringSafely(record, "id"))
                .setEventCount(DeserializerUtil.getLongSafely(record, "event_count"))
                .setTsMs(DeserializerUtil.getLongSafely(record, "ts_ms"));

        if (record.get("data_collections") != null) {
            Iterable<?> collections = (Iterable<?>) record.get("data_collections");
            for (Object item : collections) {
                if (item instanceof GenericRecord collectionRecord) {
                    TransactionMetadataValue.TransactionMetadata.DataCollection.Builder collectionBuilder =
                            TransactionMetadataValue.TransactionMetadata.DataCollection.newBuilder();
                    collectionBuilder.setDataCollection(DeserializerUtil.getStringSafely(collectionRecord, "data_collection"));
                    collectionBuilder.setEventCount(DeserializerUtil.getLongSafely(collectionRecord, "event_count"));
                    builder.addDataCollections(collectionBuilder);
                }
            }
        }

        return builder.build();
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
