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
import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import io.pixelsdb.pixels.sink.config.factory.KafkaPropFactorySelector;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.core.concurrent.TransactionCoordinatorFactory;
import io.pixelsdb.pixels.sink.monitor.MonitorThreadManager;
import io.pixelsdb.pixels.sink.monitor.TopicMonitor;
import io.pixelsdb.pixels.sink.monitor.TransactionMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

public class PixelsSinkApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(PixelsSinkApp.class);
    private static MonitorThreadManager manager;
    private static volatile boolean running = true;

    public static void main(String[] args) throws IOException {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running = false;
            if (manager != null) {
                manager.shutdown();
            }
            TransactionCoordinatorFactory.reset();
            LOGGER.info("Pixels Sink Server shutdown complete");
        }));

        init(args);

        PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();
        KafkaPropFactorySelector kafkaPropFactorySelector = new KafkaPropFactorySelector();

        Properties transactionKafkaProperties = kafkaPropFactorySelector
                .getFactory(PixelsSinkConstants.TRANSACTION_KAFKA_PROP_FACTORY)
                .createKafkaProperties(pixelsSinkConfig);
        TransactionMonitor transactionMonitor = new TransactionMonitor(pixelsSinkConfig, transactionKafkaProperties);

        Properties topicKafkaProperties = kafkaPropFactorySelector
                .getFactory(PixelsSinkConstants.ROW_RECORD_KAFKA_PROP_FACTORY)
                .createKafkaProperties(pixelsSinkConfig);
        TopicMonitor topicMonitor = new TopicMonitor(pixelsSinkConfig, topicKafkaProperties);

        manager = new MonitorThreadManager();
        manager.startMonitor(transactionMonitor);
        manager.startMonitor(topicMonitor);

    }

    private static void init(String[] args) throws IOException {
        CommandLineConfig cmdLineConfig = new CommandLineConfig(args);
        PixelsSinkConfigFactory.initialize(cmdLineConfig.getConfigPath());
    }
}
