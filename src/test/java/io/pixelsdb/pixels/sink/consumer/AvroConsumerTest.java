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

package io.pixelsdb.pixels.sink.consumer;

import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.client.RegistryClientFactory;
import io.apicurio.registry.serde.SerdeConfig;
import io.apicurio.registry.serde.avro.AvroKafkaDeserializer;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.deserializer.RowChangeEventAvroDeserializer;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AvroConsumerTest {

    private static final String TOPIC = "oltp_server.pixels_realtime_crud.customer";
    private static final String REGISTRY_URL = "http://localhost:8080/apis/registry/v2";
    private static final String BOOTSTRAP_SERVERS = "localhost:29092";
    private static final String GROUP_ID = "avro-consumer-test-group-1";

    private static KafkaConsumer<String, RowChangeEvent> getRowChangeEventAvroKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, RowChangeEventAvroDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(SerdeConfig.REGISTRY_URL, REGISTRY_URL);
        props.put(SerdeConfig.AUTO_REGISTER_ARTIFACT, "true");
        props.put(SerdeConfig.CHECK_PERIOD_MS, "30000");

        KafkaConsumer<String, RowChangeEvent> consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    private static void processRecord(RowChangeEvent event) {
        RetinaProto.RowValue.Builder builder = RetinaProto.RowValue.newBuilder();
        for (SinkProto.ColumnValue value : event.getRowRecord().getAfter().getValuesList()) {
            builder.addValues(value.getValue());
        }
        builder.build();
    }

    private static KafkaConsumer<String, GenericRecord> getStringGenericRecordKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroKafkaDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(SerdeConfig.REGISTRY_URL, REGISTRY_URL);
        props.put(SerdeConfig.AUTO_REGISTER_ARTIFACT, "true");
        props.put(SerdeConfig.CHECK_PERIOD_MS, "30000");

        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    private static RowChangeEvent convertToRowChangeEvent(GenericRecord record, Schema schema) {
        return new RowChangeEvent(SinkProto.RowRecord.newBuilder().build());
    }

    @Test
    public void avroConsumerTest() {
        KafkaConsumer<String, GenericRecord> consumer = getStringGenericRecordKafkaConsumer();
        consumer.subscribe(Collections.singletonList(TOPIC));

        RegistryClient registryClient = RegistryClientFactory.create(REGISTRY_URL);

        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    processRecord(record, registryClient);
                }
            }
        } finally {
            consumer.close();
        }
    }
    private static void processRecord(ConsumerRecord<String, GenericRecord> record, RegistryClient registryClient) {
        try {
            GenericRecord avroRecord = record.value();
            Schema schema = avroRecord.getSchema();

            String schemaId = getSchemaIdFromRegistry(registryClient, schema);
            System.out.println("Schema ID: " + schemaId);

            RowChangeEvent event = convertToRowChangeEvent(avroRecord, schema);

            System.out.println("Successfully processed message:");
            System.out.println("Topic: " + record.topic());
            System.out.println("Partition: " + record.partition());
            System.out.println("Offset: " + record.offset());
            System.out.println("Event: " + event);

        } catch (Exception e) {
            System.err.println("Error processing message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static String getSchemaIdFromRegistry(RegistryClient client, Schema schema) {
        String schemaContent = schema.toString();
        try {
            return "";
        } catch (Exception e) {
            throw new RuntimeException("Schema not found in registry: " + schema.getFullName(), e);
        }
    }

    @Test
    public void sinkConsumerTest() throws IOException {
        PixelsSinkConfigFactory.initialize("/home/anti/work/pixels-sink/src/main/resources/pixels-sink.local.properties");
        KafkaConsumer<String, RowChangeEvent> consumer = getRowChangeEventAvroKafkaConsumer();
        consumer.subscribe(Collections.singletonList(TOPIC));

        try {
            while (true) {
                ConsumerRecords<String, RowChangeEvent> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, RowChangeEvent> record : records) {
                    processRecord(record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}