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
package io.pixelsdb.pixels.sink.source.kafka;

import io.apicurio.registry.serde.SerdeConfig;
import io.pixelsdb.pixels.sink.TestConfig;
import io.pixelsdb.pixels.sink.config.KafkaValueFormat;
import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.source.kafka.serde.KafkaRecordConverter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
class AvroConsumerTest
{
    private static final int MAX_POLL_CYCLES = 100;

    @Test
    void shouldConsumeRowChangeEvents() throws Exception
    {
        TestConfig.initializeIntegrationConfig();
        KafkaConsumer<String, byte[]> consumer = rowConsumer();
        consumer.subscribe(Collections.singletonList(topic()));

        try (KafkaRecordConverter<RowChangeEvent> converter =
                     KafkaRecordConverter.forRow(avroConverterProperties()))
        {
            int recordCount = 0;
            for (int i = 0; i < MAX_POLL_CYCLES; ++i)
            {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, byte[]> record : records)
                {
                    RowChangeEvent event = converter.convert(record.topic(), record.value());
                    if (event != null)
                    {
                        recordCount++;
                    }
                }
            }
            assertTrue(recordCount > 0, "No row-change records were consumed");
        } finally
        {
            consumer.close();
        }
    }

    private static KafkaConsumer<String, byte[]> rowConsumer()
    {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(SerdeConfig.REGISTRY_URL, registryUrl());
        props.put(SerdeConfig.AUTO_REGISTER_ARTIFACT, "true");
        props.put(SerdeConfig.CHECK_PERIOD_MS, "30000");
        return new KafkaConsumer<>(props);
    }

    private static Properties avroConverterProperties()
    {
        Properties properties = new Properties();
        properties.put(PixelsSinkConstants.KAFKA_VALUE_FORMAT, KafkaValueFormat.AVRO);
        properties.put(SerdeConfig.REGISTRY_URL, registryUrl());
        properties.put(SerdeConfig.AUTO_REGISTER_ARTIFACT, "true");
        properties.put(SerdeConfig.CHECK_PERIOD_MS, "30000");
        return properties;
    }

    private static String topic()
    {
        return requiredProperty("pixels.sink.test.topic");
    }

    private static String registryUrl()
    {
        return requiredProperty("pixels.sink.test.registry.url");
    }

    private static String bootstrapServers()
    {
        return requiredProperty("pixels.sink.test.bootstrap.servers");
    }

    private static String groupId()
    {
        return System.getProperty(
                "pixels.sink.test.group.id", "pixels-sink-test-" + UUID.randomUUID());
    }

    private static String requiredProperty(String key)
    {
        String value = System.getProperty(key);
        Assumptions.assumeTrue(value != null && !value.isBlank(),
                "Set -D" + key + " to run this integration test");
        return value;
    }
}
