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
package io.pixelsdb.pixels.sink.config;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LegacyKafkaValueFormatMigrationTest
{
    @Test
    void shouldDefaultToJsonWhenNoFormatOrLegacyKeys()
    {
        assertEquals("json", PixelsSinkConfig.resolveKafkaValueFormat(new Properties()));
    }

    @Test
    void shouldInferJsonFromLegacyDeserializerClass()
    {
        Properties props = new Properties();
        props.setProperty(
                "value.deserializer",
                "io.pixelsdb.pixels.sink.conversion.debezium.RowChangeEventJsonDeserializer");
        props.setProperty(
                "transaction.topic.value.deserializer",
                "io.pixelsdb.pixels.sink.conversion.debezium.TransactionMetadataJsonDeserializer");
        assertEquals("json", PixelsSinkConfig.resolveKafkaValueFormat(props));
    }

    @Test
    void shouldInferAvroFromLegacyDeserializerClass()
    {
        Properties props = new Properties();
        props.setProperty(
                "value.deserializer",
                "io.pixelsdb.pixels.sink.source.kafka.serde.RowChangeEventAvroDeserializer");
        assertEquals("avro", PixelsSinkConfig.resolveKafkaValueFormat(props));
    }

    @Test
    void shouldPreferExplicitFormatAndIgnoreLegacyKeys()
    {
        Properties props = new Properties();
        props.setProperty("sink.kafka.value.format", "avro");
        props.setProperty(
                "value.deserializer",
                "io.pixelsdb.pixels.sink.conversion.debezium.RowChangeEventJsonDeserializer");
        assertEquals("avro", PixelsSinkConfig.resolveKafkaValueFormat(props));
    }

    @Test
    void shouldFailFastOnConflictingLegacyFormats()
    {
        Properties props = new Properties();
        props.setProperty(
                "value.deserializer",
                "io.pixelsdb.pixels.sink.conversion.debezium.RowChangeEventJsonDeserializer");
        props.setProperty(
                "transaction.topic.value.deserializer",
                "io.pixelsdb.pixels.sink.conversion.debezium.TransactionMetadataAvroDeserializer");
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> PixelsSinkConfig.resolveKafkaValueFormat(props));
        assertTrue(error.getMessage().contains("Conflicting"));
    }

    @Test
    void shouldFailFastOnUnrecognizedLegacyClass()
    {
        Properties props = new Properties();
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> PixelsSinkConfig.resolveKafkaValueFormat(props));
        assertTrue(error.getMessage().contains("Unable to migrate"));
    }
}
