/*
 * Copyright 2018-2025 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pixelsdb.pixels.sink;

import io.pixelsdb.pixels.sink.config.CommandLineConfig;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.deserializer.DebeziumJsonMessageDeserializer;
import io.pixelsdb.pixels.sink.monitor.TopicMonitor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class PixelsSinkApp {

    public static void main(String[] args) throws IOException {
        CommandLineConfig cmdLineConfig = new CommandLineConfig(args);

        PixelsSinkConfig pixelsSinkConfig = new PixelsSinkConfig(cmdLineConfig.getConfigPath());

        Properties kafkaProperties = getProperties(pixelsSinkConfig);
        TopicMonitor topicMonitor = new TopicMonitor(pixelsSinkConfig, kafkaProperties);
        topicMonitor.start();
    }

    private static Properties getProperties(PixelsSinkConfig config) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, config.getKeyDeserializer());
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, config.getValueDeserializer());
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return kafkaProperties;
    }
}
