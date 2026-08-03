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
import io.pixelsdb.pixels.sink.config.KafkaValueFormat;
import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import io.pixelsdb.pixels.sink.conversion.debezium.avro.DebeziumAvroPayloadDecoder;
import io.pixelsdb.pixels.sink.conversion.debezium.avro.DebeziumAvroRowConverter;
import io.pixelsdb.pixels.sink.conversion.debezium.avro.DebeziumAvroTransactionConverter;
import io.pixelsdb.pixels.sink.conversion.debezium.dialect.DebeziumSourceAdapter;
import io.pixelsdb.pixels.sink.conversion.debezium.dialect.DebeziumSourceAdapterRegistry;
import io.pixelsdb.pixels.sink.conversion.debezium.json.DebeziumJsonRowConverter;
import io.pixelsdb.pixels.sink.conversion.debezium.json.DebeziumJsonTransactionConverter;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Assembles Debezium row/tx converters for Kafka by {@code sink.kafka.value.format}.
 */
public final class KafkaRecordConverter<T> implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRecordConverter.class);

    private enum ErrorPolicy
    {
        SWALLOW,
        PROPAGATE
    }

    @FunctionalInterface
    private interface ConvertFn<T>
    {
        T convert(String topic, byte[] data) throws Exception;
    }

    private final ConvertFn<T> convertFn;
    private final AutoCloseable resource;
    private final ErrorPolicy errorPolicy;

    private KafkaRecordConverter(
            ConvertFn<T> convertFn, AutoCloseable resource, ErrorPolicy errorPolicy)
    {
        this.convertFn = convertFn;
        this.resource = resource;
        this.errorPolicy = errorPolicy;
    }

    public static KafkaRecordConverter<RowChangeEvent> forRow(Properties properties)
    {
        String format = resolveFormat(properties);
        DebeziumSourceAdapter adapter = resolveAdapter(properties);
        return switch (format)
        {
            case KafkaValueFormat.JSON ->
            {
                DebeziumJsonRowConverter converter = new DebeziumJsonRowConverter(
                        TableMetadataRegistry.Instance(), adapter);
                yield new KafkaRecordConverter<>(
                        (topic, data) -> converter.convert(data), null, ErrorPolicy.SWALLOW);
            }
            case KafkaValueFormat.AVRO ->
            {
                DebeziumAvroPayloadDecoder decoder = new DebeziumAvroPayloadDecoder();
                decoder.configure(toConfigMap(properties), false);
                DebeziumAvroRowConverter converter = new DebeziumAvroRowConverter(
                        TableMetadataRegistry.Instance(), adapter);
                yield new KafkaRecordConverter<>(
                        (topic, data) -> converter.convert(decoder.decode(topic, data)),
                        decoder,
                        ErrorPolicy.SWALLOW);
            }
            default -> throw new IllegalArgumentException(
                    "Unsupported Kafka value format: " + format);
        };
    }

    public static KafkaRecordConverter<SinkProto.TransactionMetadata> forTransaction(
            Properties properties)
    {
        String format = resolveFormat(properties);
        // Transaction envelopes lack source.connector; dialect must be configured up front.
        DebeziumSourceAdapter adapter = requireAdapter(properties);
        return switch (format)
        {
            case KafkaValueFormat.JSON ->
            {
                DebeziumJsonTransactionConverter converter =
                        new DebeziumJsonTransactionConverter(adapter);
                yield new KafkaRecordConverter<>(
                        (topic, data) -> converter.convert(data), null, ErrorPolicy.PROPAGATE);
            }
            case KafkaValueFormat.AVRO ->
            {
                DebeziumAvroPayloadDecoder decoder = new DebeziumAvroPayloadDecoder();
                decoder.configure(toConfigMap(properties), false);
                DebeziumAvroTransactionConverter converter =
                        new DebeziumAvroTransactionConverter(adapter);
                yield new KafkaRecordConverter<>(
                        (topic, data) -> converter.convert(decoder.decode(topic, data)),
                        decoder,
                        ErrorPolicy.PROPAGATE);
            }
            default -> throw new IllegalArgumentException(
                    "Unsupported Kafka value format: " + format);
        };
    }

    public T convert(String topic, byte[] data)
    {
        if (data == null || data.length == 0)
        {
            return null;
        }
        try
        {
            return convertFn.convert(topic, data);
        } catch (RuntimeException e)
        {
            if (errorPolicy == ErrorPolicy.SWALLOW)
            {
                LOGGER.warn("Failed to convert Kafka record from topic {}", topic, e);
                return null;
            }
            throw e;
        } catch (Exception e)
        {
            if (errorPolicy == ErrorPolicy.SWALLOW)
            {
                LOGGER.warn("Failed to convert Kafka record from topic {}", topic, e);
                return null;
            }
            throw new RuntimeException("Failed to convert Kafka record from " + topic, e);
        }
    }

    @Override
    public void close()
    {
        if (resource != null)
        {
            try
            {
                resource.close();
            } catch (Exception e)
            {
                LOGGER.warn("Failed to close Kafka record converter resource", e);
            }
        }
    }

    private static String resolveFormat(Properties properties)
    {
        Object configured = properties.get(PixelsSinkConstants.KAFKA_VALUE_FORMAT);
        return KafkaValueFormat.resolve(configured == null ? null : configured.toString());
    }

    private static DebeziumSourceAdapter resolveAdapter(Properties properties)
    {
        Object dialect = properties.get(PixelsSinkConstants.SINK_DEBEZIUM_DIALECT);
        if (dialect == null || dialect.toString().isBlank())
        {
            return null;
        }
        return DebeziumSourceAdapterRegistry.resolve(dialect.toString());
    }

    private static DebeziumSourceAdapter requireAdapter(Properties properties)
    {
        DebeziumSourceAdapter adapter = resolveAdapter(properties);
        if (adapter == null)
        {
            throw new IllegalStateException(
                    PixelsSinkConstants.SINK_DEBEZIUM_DIALECT +
                            " is required for Kafka transaction decoding" +
                            " (mysql or postgresql)");
        }
        return adapter;
    }

    private static Map<String, Object> toConfigMap(Properties properties)
    {
        Map<String, Object> configuration = new HashMap<>();
        properties.forEach((key, value) -> configuration.put(key.toString(), value));
        return configuration;
    }
}
