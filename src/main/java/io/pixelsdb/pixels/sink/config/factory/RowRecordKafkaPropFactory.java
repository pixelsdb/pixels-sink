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
package io.pixelsdb.pixels.sink.config.factory;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import io.apicurio.registry.serde.SerdeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.Properties;

public class RowRecordKafkaPropFactory implements KafkaPropFactory
{
    static Properties getCommonKafkaProperties(PixelsSinkConfig config)
    {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, config.getKeyDeserializer());
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaProperties.put(
                PixelsSinkConstants.KAFKA_VALUE_FORMAT, config.getKafkaValueFormat());
        return kafkaProperties;
    }

    @Override
    public Properties createKafkaProperties(PixelsSinkConfig config)
    {
        Properties kafkaProperties = getCommonKafkaProperties(config);
        String dialect = config.resolveDebeziumSourceDialect();
        if (!dialect.isBlank())
        {
            kafkaProperties.put(PixelsSinkConstants.SINK_DEBEZIUM_DIALECT, dialect);
        }
        if (config.getRegistryUrl() != null && !config.getRegistryUrl().isBlank())
        {
            kafkaProperties.put(SerdeConfig.REGISTRY_URL, config.getRegistryUrl());
        }
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId());

        kafkaProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        kafkaProperties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
        kafkaProperties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000");
        kafkaProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");

        return kafkaProperties;
    }
}
