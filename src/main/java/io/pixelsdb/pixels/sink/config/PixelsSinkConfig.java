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
package io.pixelsdb.pixels.sink.config;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.sink.sink.PixelsSinkMode;
import lombok.Getter;

import java.io.IOException;
import java.util.Objects;

@Getter
public class PixelsSinkConfig {
    private ConfigFactory config;

    private Long transactionTimeout;
    private PixelsSinkMode pixelsSinkMode;
    private short remotePort;
    private int batchSize;
    private int timeoutMs;
    private int flushIntervalMs;
    private int maxRetries;
    private boolean sinkCsvEnableHeader;
    private boolean monitorEnabled;
    private short monitorPort;
    private boolean rpcEnable;
    private int mockRpcDelay;
    public PixelsSinkConfig(String configFilePath) throws IOException {
        this.config = ConfigFactory.Instance();
        this.config.loadProperties(configFilePath);
        parseProps();
    }

    public PixelsSinkConfig(ConfigFactory config) {
        this.config = config;
        parseProps();
    }

    private void parseProps() {
        this.pixelsSinkMode = PixelsSinkMode.fromValue(getProperty("sink.mode", PixelsSinkDefaultConfig.SINK_MODE));
        this.transactionTimeout = Long.valueOf(getProperty("transaction.timeout", TransactionConfig.DEFAULT_TRANSACTION_TIME_OUT));
        this.remotePort = parseShort(getProperty("sink.remote.port"), PixelsSinkDefaultConfig.SINK_REMOTE_PORT);
        this.batchSize = parseInt(getProperty("sink.batch.size"), PixelsSinkDefaultConfig.SINK_BATCH_SIZE);
        this.timeoutMs = parseInt(getProperty("sink.timeout.ms"), PixelsSinkDefaultConfig.SINK_TIMEOUT_MS);
        this.flushIntervalMs = parseInt(getProperty("sink.flush.interval.ms"), PixelsSinkDefaultConfig.SINK_FLUSH_INTERVAL_MS);
        this.maxRetries = parseInt(getProperty("sink.max.retries"), PixelsSinkDefaultConfig.SINK_MAX_RETRIES);
        this.sinkCsvEnableHeader = parseBoolean(getProperty("sink.csv.enable_header"), PixelsSinkDefaultConfig.SINK_CSV_ENABLE_HEADER);
        this.monitorEnabled = parseBoolean(getProperty("sink.monitor.enabled"), PixelsSinkDefaultConfig.SINK_MONITOR_ENABLED);
        this.monitorPort = parseShort(getProperty("sink.monitor.port"), PixelsSinkDefaultConfig.SINK_MONITOR_PORT);
        this.rpcEnable = parseBoolean(getProperty("sink.rpc.enable"), PixelsSinkDefaultConfig.SINK_RPC_ENABLED);
        this.mockRpcDelay = parseInt(getProperty("sink.rpc.mock.delay"), PixelsSinkDefaultConfig.MOCK_RPC_DELAY);
    }

    public String getTopicPrefix() {
        return getProperty("topic.prefix");
    }

    public String getCaptureDatabase() {
        return getProperty("consumer.capture_database");
    }

    public String[] getIncludeTables() {
        String includeTables = getProperty("consumer.include_tables", "");
        return includeTables.isEmpty() ? new String[0] : includeTables.split(",");
    }

    public String getBootstrapServers() {
        return getProperty("bootstrap.servers");
    }

    public String getGroupId() {
        return getProperty("group.id");
    }

    public String getKeyDeserializer() {
        return getProperty("key.deserializer", PixelsSinkDefaultConfig.KEY_DESERIALIZER);
    }
    public String getValueDeserializer() {
        return getProperty("value.deserializer", PixelsSinkDefaultConfig.VALUE_DESERIALIZER);
    }

    public String getCsvSinkPath() {
        return getProperty("sink.csv.path", PixelsSinkDefaultConfig.CSV_SINK_PATH);
    }

    public String getTransactionTopicSuffix() {
        return getProperty("transaction.topic.suffix", TransactionConfig.DEFAULT_TRANSACTION_TOPIC_SUFFIX);
    }

    public String getTransactionTopicValueDeserializer() {
        return getProperty("transaction.topic.value.deserializer", TransactionConfig.DEFAULT_TRANSACTION_TOPIC_VALUE_DESERIALIZER);
    }

    public String getTransactionTopicGroupId() {
        return getProperty("transaction.topic.group_id", TransactionConfig.DEFAULT_TRANSACTION_TOPIC_GROUP_ID);
    }

    public String getSinkRemoteHost() {
        return getProperty("sink.remote.host", PixelsSinkDefaultConfig.SINK_REMOTE_HOST);
    }

    private short parseShort(String valueStr, short defaultValue) {
        return (valueStr != null) ? Short.parseShort(valueStr) : defaultValue;
    }

    private int parseInt(String valueStr, int defaultValue) {
        return (valueStr != null) ? Integer.parseInt(valueStr) : defaultValue;
    }

    private boolean parseBoolean(String valueStr, boolean defaultValue) {
        return (valueStr != null) ? Boolean.parseBoolean(valueStr) : defaultValue;
    }

    public String getProperty(String key) {
        return config.getProperty(key);
    }

    public String getProperty(String key, String defaultValue) {
        String value = config.getProperty(key);
        if (Objects.isNull(value)) {
            return defaultValue;
        }
        return value;
    }

    public String getRegistryUrl() {
        return getProperty("sink.registry.url", "");
    }

}