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
package io.pixelsdb.pixels.sink.source.kafka.serde;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KafkaRecordConverterTest
{
    @Test
    void shouldCreateJsonRowAndTransactionConverters()
    {
        Properties properties = jsonProperties();
        try (KafkaRecordConverter<RowChangeEvent> row = KafkaRecordConverter.forRow(properties);
             KafkaRecordConverter<SinkProto.TransactionMetadata> tx =
                     KafkaRecordConverter.forTransaction(properties))
        {
            assertNotNull(row);
            assertNotNull(tx);
        }
    }

    @Test
    void shouldReturnNullForEmptyOrNullRowPayload()
    {
        try (KafkaRecordConverter<RowChangeEvent> converter =
                     KafkaRecordConverter.forRow(jsonProperties()))
        {
            assertNull(converter.convert("topic", null));
            assertNull(converter.convert("topic", new byte[0]));
        }
    }

    @Test
    void shouldSwallowInvalidRowPayload()
    {
        try (KafkaRecordConverter<RowChangeEvent> converter =
                     KafkaRecordConverter.forRow(jsonProperties()))
        {
            assertNull(converter.convert("topic", "not-json".getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Test
    void shouldPropagateInvalidTransactionPayload()
    {
        try (KafkaRecordConverter<SinkProto.TransactionMetadata> converter =
                     KafkaRecordConverter.forTransaction(jsonProperties()))
        {
            assertThrows(RuntimeException.class,
                    () -> converter.convert("topic", "not-json".getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Test
    void shouldRejectTransactionConverterWithoutDialect()
    {
        Properties properties = new Properties();
        properties.put(PixelsSinkConstants.KAFKA_VALUE_FORMAT, "json");

        IllegalStateException error = assertThrows(
                IllegalStateException.class,
                () -> KafkaRecordConverter.forTransaction(properties));
        assertTrue(error.getMessage().contains(PixelsSinkConstants.SINK_DEBEZIUM_DIALECT));
    }

    private static Properties jsonProperties()
    {
        Properties properties = new Properties();
        properties.put(PixelsSinkConstants.KAFKA_VALUE_FORMAT, "json");
        properties.put(PixelsSinkConstants.SINK_DEBEZIUM_DIALECT, "postgresql");
        return properties;
    }
}
