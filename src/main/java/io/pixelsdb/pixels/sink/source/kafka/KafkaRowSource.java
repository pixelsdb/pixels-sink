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
package io.pixelsdb.pixels.sink.source.kafka;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.pipeline.TablePipelineManager;
import io.pixelsdb.pixels.sink.source.kafka.serde.KafkaRecordConverter;
import io.pixelsdb.pixels.sink.util.DataTransform;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public final class KafkaRowSource implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaRowSource.class);

    private final Properties kafkaProperties;
    private final String topic;
    private final String tableName;
    private final TablePipelineManager tablePipelineManager;
    private final KafkaRecordConverter<RowChangeEvent> converter;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private KafkaConsumer<String, byte[]> consumer;

    public KafkaRowSource(
            Properties kafkaProperties,
            String topic,
            TablePipelineManager tablePipelineManager)
    {
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
        this.kafkaProperties = new Properties();
        this.kafkaProperties.putAll(kafkaProperties);
        this.kafkaProperties.put(
                ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId() + "-" + topic);
        this.kafkaProperties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        this.kafkaProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        this.topic = topic;
        this.tableName = DataTransform.extractTableName(topic);
        this.tablePipelineManager = tablePipelineManager;
        this.converter = KafkaRecordConverter.forRow(this.kafkaProperties);
    }

    @Override
    public void run()
    {
        try
        {
            consumer = new KafkaConsumer<>(kafkaProperties);
            consumer.subscribe(Collections.singleton(topic));
            while (running.get())
            {
                try
                {
                    ConsumerRecords<String, byte[]> records =
                            consumer.poll(Duration.ofSeconds(5));
                    records.forEach(record ->
                    {
                        byte[] value = record.value();
                        if (value == null)
                        {
                            return;
                        }
                        metricsFacade.addRawData(value.length);
                        try
                        {
                            RowChangeEvent event = converter.convert(topic, value);
                            if (event != null)
                            {
                                metricsFacade.recordSerdRowChange();
                                tablePipelineManager.route(event);
                            }
                        } catch (RuntimeException e)
                        {
                            LOGGER.warn(
                                    "Failed to convert Kafka record from topic {}: {}",
                                    topic, e.getMessage());
                        }
                    });
                } catch (InterruptException e)
                {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        } catch (WakeupException e)
        {
            LOGGER.debug("Consumer wakeup triggered for {}", tableName);
        } catch (Exception e)
        {
            LOGGER.error("Kafka source failed for {}", tableName, e);
        } finally
        {
            converter.close();
            if (consumer != null)
            {
                consumer.close(Duration.ofSeconds(5));
            }
        }
    }

    void requestStop()
    {
        running.set(false);
        if (consumer != null)
        {
            consumer.wakeup();
        }
    }
}
