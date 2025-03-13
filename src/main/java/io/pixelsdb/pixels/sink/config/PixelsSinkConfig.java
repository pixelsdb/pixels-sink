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
package io.pixelsdb.pixels.sink.config;

import io.pixelsdb.pixels.sink.sink.PixelsSinkMode;
import lombok.Getter;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Getter
public class PixelsSinkConfig {
    private final Properties properties;

    private final Long transactionTimeout;
    private final PixelsSinkMode pixelsSinkMode;
    private final short remotePort;
    private final int batchSize;
    private final int timeoutMs;
    private final int flushIntervalMs;
    private final int maxRetries;
    private final boolean sinkCsvEnableHeader;

    public PixelsSinkConfig(String configFilePath) throws IOException {
        properties = new Properties();
        if (configFilePath != null && !configFilePath.isEmpty()) {
            try (InputStream input = new FileInputStream(configFilePath)) {
                properties.load(input);
            } catch (FileNotFoundException e) {
                throw new FileNotFoundException("Configuration file not found: " + configFilePath);
            } catch (IOException e) {
                throw new IOException("Error reading configuration file: " + configFilePath, e);
            }
        } else {
            try (InputStream input = getClass().getClassLoader().getResourceAsStream(PixelsSinkDefaultConfig.PROPERTIES_PATH)) {
                if (input == null) {
                    throw new FileNotFoundException("Resource file not found: " + configFilePath);
                }
                properties.load(input);
            }
        }
        this.transactionTimeout = Long.valueOf(properties.getProperty("transaction.timeout", TransactionConfig.DEFAULT_TRANSACTION_TIME_OUT));
        this.pixelsSinkMode = PixelsSinkMode.fromValue(properties.getProperty("sink.mode", PixelsSinkDefaultConfig.SINK_MODE));

        String remotePortStr = properties.getProperty("sink.remote.port");
        this.remotePort = (remotePortStr != null) ? Short.parseShort(remotePortStr) : PixelsSinkDefaultConfig.SINK_REMOTE_PORT;
        this.batchSize = parseInt(properties.getProperty("sink.batch.size"), PixelsSinkDefaultConfig.SINK_BATCH_SIZE);
        this.timeoutMs = parseInt(properties.getProperty("sink.timeout.ms"), PixelsSinkDefaultConfig.SINK_TIMEOUT_MS);
        this.flushIntervalMs = parseInt(properties.getProperty("sink.flush.interval.ms"), PixelsSinkDefaultConfig.SINK_FLUSH_INTERVAL_MS);
        this.maxRetries = parseInt(properties.getProperty("sink.max.retries"), PixelsSinkDefaultConfig.SINK_MAX_RETRIES);
        this.sinkCsvEnableHeader = parseBoolean(properties.getProperty("sink.csv.enable_header"), PixelsSinkDefaultConfig.SINK_CSV_ENABLE_HEADER);
    }

    public String getTopicPrefix() {
        return properties.getProperty("topic.prefix");
    }

    public String getCaptureDatabase() {
        return properties.getProperty("consumer.capture_database");
    }

    public String[] getIncludeTables() {
        String includeTables = properties.getProperty("consumer.include_tables", "");
        return includeTables.isEmpty() ? new String[0] : includeTables.split(",");
    }

    public String getBootstrapServers() {
        return properties.getProperty("bootstrap.servers");
    }

    public String getGroupId() {
        return properties.getProperty("group.id");
    }

    public String getKeyDeserializer() {
        return properties.getProperty("key.deserializer", PixelsSinkDefaultConfig.KEY_DESERIALIZER);
    }
    public String getValueDeserializer() {
        return properties.getProperty("value.deserializer", PixelsSinkDefaultConfig.VALUE_DESERIALIZER);
    }

    public String getCsvSinkPath() {
        return properties.getProperty("sink.csv.path", PixelsSinkDefaultConfig.CSV_SINK_PATH);
    }

    public String getTransactionTopicSuffix() {
        return properties.getProperty("transaction.topic.suffix", TransactionConfig.DEFAULT_TRANSACTION_TOPIC_SUFFIX);
    }

    public String getTransactionTopicValueDeserializer() {
        return properties.getProperty("transaction.topic.key.deserializer", TransactionConfig.DEFAULT_TRANSACTION_TOPIC_KEY_DESERIALIZER);
    }

    public String getTransactionTopicGroupId() {
        return properties.getProperty("transaction.topic.group_id", TransactionConfig.DEFAULT_TRANSACTION_TOPIC_GROUP_ID);
    }

    public String getSinkRemoteHost() {
        return properties.getProperty("sink.remote.host", PixelsSinkDefaultConfig.SINK_REMOTE_HOST);
    }

    private int parseInt(String valueStr, int defaultValue) {
        return (valueStr != null) ? Integer.parseInt(valueStr) : defaultValue;
    }

    private boolean parseBoolean(String valueStr, boolean defaultValue) {
        return (valueStr != null) ? Boolean.parseBoolean(valueStr) : defaultValue;
    }
}