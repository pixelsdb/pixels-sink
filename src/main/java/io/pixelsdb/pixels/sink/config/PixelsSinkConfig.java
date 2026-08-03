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
package io.pixelsdb.pixels.sink.config;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.sink.writer.PixelsSinkMode;
import io.pixelsdb.pixels.sink.writer.retina.RetinaServiceProxy;
import io.pixelsdb.pixels.sink.writer.retina.TransactionMode;
import lombok.Getter;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

@Getter
public class PixelsSinkConfig
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PixelsSinkConfig.class);
    private static final String LEGACY_VALUE_DESERIALIZER = "value.deserializer";
    private static final String LEGACY_TX_VALUE_DESERIALIZER =
            "transaction.topic.value.deserializer";
    private static final String KAFKA_VALUE_FORMAT_KEY = "sink.kafka.value.format";

    private final ConfigFactory config;

    @ConfigKey(value = "transaction.timeout", defaultValue = TransactionConfig.DEFAULT_TRANSACTION_TIME_OUT)
    private Long transactionTimeout;

    @ConfigKey(value = "sink.mode", defaultValue = PixelsSinkDefaultConfig.SINK_MODE)
    private PixelsSinkMode pixelsSinkMode;

    @ConfigKey(value = "sink.retina.mode", defaultValue = PixelsSinkDefaultConfig.SINK_RETINA_MODE)
    private RetinaServiceProxy.RetinaWriteMode retinaWriteMode;

    @ConfigKey(value = "sink.retina.client", defaultValue = "1")
    private int retinaClientNum;

    @ConfigKey(value = "sink.retina.log.queue", defaultValue = "true")
    private boolean retinaLogQueueEnabled;

    @ConfigKey(value = "sink.retina.rpc.limit", defaultValue = "1000")
    private int retinaRpcLimit;

    @ConfigKey(value = "sink.retina.trans.limit", defaultValue = "1000")
    private int retinaTransLimit;

    @ConfigKey(value = "sink.trans.mode", defaultValue = TransactionConfig.DEFAULT_TRANSACTION_MODE)
    private TransactionMode transactionMode;

    @ConfigKey(value = "sink.commit.method", defaultValue = "async")
    private String commitMethod;

    @ConfigKey(value = "sink.commit.batch.size", defaultValue = "500")
    private int commitBatchSize;

    @ConfigKey(value = "sink.commit.batch.worker", defaultValue = "16")
    private int commitBatchWorkers;

    @ConfigKey(value = "sink.commit.batch.delay", defaultValue = "200")
    private int commitBatchDelay;

    @ConfigKey(value = "sink.remote.port", defaultValue = "9090")
    private short remotePort;

    @ConfigKey(value = "sink.flink.server.port", defaultValue = "9091")
    private int sinkFlinkServerPort;

    @ConfigKey(value = "sink.timeout.ms", defaultValue = "30000")
    private int timeoutMs;

    @ConfigKey(value = "sink.flush.interval.ms", defaultValue = "1000")
    private int flushIntervalMs;

    @ConfigKey(value = "sink.flush.batch.size", defaultValue = "100")
    private int flushBatchSize;

    @ConfigKey(value = "sink.max.retries", defaultValue = "3")
    private int maxRetries;

    @ConfigKey(value = "sink.csv.enable_header", defaultValue = "false")
    private boolean sinkCsvEnableHeader;

    @ConfigKey(value = "sink.monitor.enable", defaultValue = "false")
    private boolean monitorEnabled;

    @ConfigKey(value = "sink.monitor.port", defaultValue = "9464")
    private short monitorPort;

    @ConfigKey(value = "sink.monitor.report.enable", defaultValue = "true")
    private boolean monitorReportEnabled;

    @ConfigKey(value = "sink.monitor.report.interval", defaultValue = "5000")
    private short monitorReportInterval;

    @ConfigKey(value = "sink.monitor.freshness.interval", defaultValue = "1000")
    private int freshnessReportInterval;

    @ConfigKey(value = "sink.monitor.freshness.file", defaultValue = "/tmp/sinkFreshness.csv")
    private String monitorFreshnessReportFile;

    @ConfigKey(value = "sink.monitor.report.file", defaultValue = "/tmp/sink.csv")
    private String monitorReportFile;

    @ConfigKey(value = "sink.rpc.enable", defaultValue = "false")
    private boolean rpcEnable;

    @ConfigKey(value = "sink.trans.batch.size", defaultValue = "100")
    private int transBatchSize;

    @ConfigKey(value = "sink.retina.trans.request.batch", defaultValue = "false")
    private boolean retinaTransRequestBatch;

    @ConfigKey(value = "sink.retina.trans.request.batch.size", defaultValue = "100")
    private int retinaTransRequestBatchSize;

    private boolean retinaEmbedded = false;

    @ConfigKey("topic.prefix")
    private String topicPrefix;
    @ConfigKey("debezium.topic.prefix")
    private String debeziumTopicPrefix;

    /**
     * Sink-side CDC dialect for envelope normalization: {@code mysql} or {@code postgresql}.
     * Independent of {@code debezium.connector.class}, which is only for Debezium Engine.
     */
    @ConfigKey(value = "sink.debezium.dialect", defaultValue = "")
    private String debeziumDialect;

    @ConfigKey(value = "debezium.connector.class", defaultValue = "")
    private String debeziumConnectorClass;

    @ConfigKey("consumer.capture_database")
    private String captureDatabase;

    @ConfigKey(value = "consumer.include_tables", defaultValue = "")
    private String includeTablesRaw;

    @ConfigKey("bootstrap.servers")
    private String bootstrapServers;

    @ConfigKey("group.id")
    private String groupId;

    @ConfigKey(value = "key.deserializer", defaultClass = StringDeserializer.class)
    private String keyDeserializer;

    /**
     * Kafka envelope wire format: {@code json} or {@code avro}.
     */
    @ConfigKey(value = "sink.kafka.value.format", defaultValue = KafkaValueFormat.JSON)
    private String kafkaValueFormat;

    @ConfigKey(value = "sink.csv.path", defaultValue = PixelsSinkDefaultConfig.CSV_SINK_PATH)
    private String csvSinkPath;

    @ConfigKey(value = "transaction.topic.suffix", defaultValue = TransactionConfig.DEFAULT_TRANSACTION_TOPIC_SUFFIX)
    private String transactionTopicSuffix;

    @ConfigKey(value = "transaction.topic.group_id",
            defaultValue = TransactionConfig.DEFAULT_TRANSACTION_TOPIC_GROUP_ID)
    private String transactionTopicGroupId;

    @ConfigKey(value = "sink.remote.host", defaultValue = PixelsSinkDefaultConfig.SINK_REMOTE_HOST)
    private String sinkRemoteHost;

    @ConfigKey("sink.registry.url")
    private String registryUrl;

    @ConfigKey(value = "sink.datasource", defaultValue = PixelsSinkDefaultConfig.DATA_SOURCE)
    private String dataSource;

    /**
     * Engine wire format. Currently only {@code connect} is implemented.
     */
    @ConfigKey(value = "sink.datasource.engine.format", defaultValue = "connect")
    private String engineFormat;

    @ConfigKey(value = "sink.datasource.decode.threads",
            defaultValue = PixelsSinkDefaultConfig.SOURCE_DECODE_THREADS)
    private int sourceDecodeThreads;

    @ConfigKey(value = "sink.datasource.rate.limit", defaultValue = "-1")
    private int sourceRateLimit;

    @ConfigKey(value = "sink.datasource.rate.limit.type", defaultValue = "-1")
    private String rateLimiterType;

    private boolean enableSourceRateLimit;

    @ConfigKey(value = "sink.proto.dir")
    private String sinkProtoDir;
    @ConfigKey(value = "sink.proto.data", defaultValue = "data")
    private String sinkProtoData;

    @ConfigKey(value = "sink.proto.maxRecords", defaultValue = PixelsSinkDefaultConfig.MAX_RECORDS_PER_FILE)
    private int maxRecordsPerFile;

    @ConfigKey(value = "sink.storage.loop", defaultValue = "false")
    private boolean sinkStorageLoop;

    @ConfigKey(value = "sink.storage.mode", defaultValue = PixelsSinkDefaultConfig.STORAGE_MODE)
    private String sinkStorageMode;

    @ConfigKey(value = "sink.monitor.freshness.level", defaultValue = "row") // row or txn or embed
    private String sinkMonitorFreshnessLevel;
    @ConfigKey(value = "sink.monitor.freshness.embed.warmup", defaultValue = "10")
    private Integer sinkMonitorFreshnessEmbedWarmupSeconds;

    @ConfigKey(value = "sink.monitor.freshness.embed.static", defaultValue = "false")
    private boolean sinkMonitorFreshnessEmbedStatic;

    @ConfigKey(value = "sink.monitor.freshness.embed.snapshot", defaultValue = "false")
    private boolean sinkMonitorFreshnessEmbedSnapshot;

    @ConfigKey(value = "sink.monitor.freshness.embed.tablelist", defaultValue = "")
    private List<String> sinkMonitorFreshnessEmbedTableList;

    @ConfigKey(value = "sink.monitor.freshness.embed.delay", defaultValue = "0")
    private Integer sinkMonitorFreshnessEmbedDelay;

    @ConfigKey(value = "sink.monitor.freshness.verbose", defaultValue = "false")
    private boolean sinkMonitorFreshnessVerbose;

    @ConfigKey(value = "sink.monitor.freshness.timestamp", defaultValue = "false")
    private boolean sinkMonitorFreshnessTimestamp;

    @ConfigKey(value = "sink.query.url")
    private String sinkQueryUrl;

    @ConfigKey(value = "sink.query.user")
    private String sinkQueryUser;

    @ConfigKey(value = "sink.query.password")
    private String sinkQueryPassword;

    @ConfigKey(value = "sink.query.parallel", defaultValue = "1")
    private int sinkQueryParallel;

    public PixelsSinkConfig(String configFilePath) throws IOException
    {
        this.config = ConfigFactory.Instance();
        this.config.loadProperties(configFilePath);
        init();
    }

    public PixelsSinkConfig(ConfigFactory config)
    {
        this.config = config;
        init();
    }

    public String[] getIncludeTables()
    {
        return includeTablesRaw.isEmpty() ? new String[0] : includeTablesRaw.split(",");
    }

    /**
     * Token used to select a {@code DebeziumSourceAdapter}.
     * Prefers {@code sink.debezium.dialect}; falls back to {@code debezium.connector.class}
     * so Engine-only configs keep working without duplicating the dialect.
     */
    public String resolveDebeziumSourceDialect()
    {
        if (debeziumDialect != null && !debeziumDialect.isBlank())
        {
            return debeziumDialect.trim();
        }
        if (debeziumConnectorClass != null && !debeziumConnectorClass.isBlank())
        {
            return debeziumConnectorClass.trim();
        }
        return "";
    }

    private void init()
    {
        Properties props = this.config.extractPropertiesByPrefix("", false);
        ConfigLoader.load(props, this);

        if (this.sourceDecodeThreads <= 0)
        {
            throw new IllegalArgumentException(
                    "sink.datasource.decode.threads must be positive");
        }
        this.enableSourceRateLimit = this.sourceRateLimit >= 0;
        this.kafkaValueFormat = resolveKafkaValueFormat(props);
        this.engineFormat = EngineValueFormat.resolve(this.engineFormat);
    }

    /**
     * Migrates legacy {@code value.deserializer} /
     * {@code transaction.topic.value.deserializer} class names to
     * {@code sink.kafka.value.format}. Package-visible for unit tests.
     */
    static String resolveKafkaValueFormat(Properties props)
    {
        String explicitFormat = trimToNull(props.getProperty(KAFKA_VALUE_FORMAT_KEY));
        String rowDeserializer = trimToNull(props.getProperty(LEGACY_VALUE_DESERIALIZER));
        String txDeserializer = trimToNull(props.getProperty(LEGACY_TX_VALUE_DESERIALIZER));
        boolean hasLegacy = rowDeserializer != null || txDeserializer != null;

        if (explicitFormat != null)
        {
            if (hasLegacy)
            {
                LOGGER.warn(
                        "Ignoring legacy {} / {} because {} is set to '{}'",
                        LEGACY_VALUE_DESERIALIZER,
                        LEGACY_TX_VALUE_DESERIALIZER,
                        KAFKA_VALUE_FORMAT_KEY,
                        explicitFormat);
            }
            return KafkaValueFormat.resolve(explicitFormat);
        }

        if (!hasLegacy)
        {
            return KafkaValueFormat.resolve(null);
        }

        String inferredRow = inferFormatFromDeserializer(rowDeserializer);
        String inferredTx = inferFormatFromDeserializer(txDeserializer);
        if (inferredRow == null && inferredTx == null)
        {
            throw new IllegalArgumentException(
                    "Unable to migrate legacy Kafka deserializer classes to " +
                            KAFKA_VALUE_FORMAT_KEY + ": value.deserializer=" +
                            rowDeserializer + ", transaction.topic.value.deserializer=" +
                            txDeserializer);
        }
        if (inferredRow != null && inferredTx != null && !inferredRow.equals(inferredTx))
        {
            throw new IllegalArgumentException(
                    "Conflicting legacy Kafka deserializer formats: value.deserializer=" +
                            rowDeserializer + " (" + inferredRow +
                            "), transaction.topic.value.deserializer=" +
                            txDeserializer + " (" + inferredTx + ")");
        }
        String inferred = inferredRow != null ? inferredRow : inferredTx;
        LOGGER.warn(
                "Migrating legacy Kafka deserializer class names to {}={}",
                KAFKA_VALUE_FORMAT_KEY, inferred);
        return inferred;
    }

    private static String inferFormatFromDeserializer(String className)
    {
        if (className == null)
        {
            return null;
        }
        String simpleName = className;
        int lastDot = className.lastIndexOf('.');
        if (lastDot >= 0 && lastDot + 1 < className.length())
        {
            simpleName = className.substring(lastDot + 1);
        }
        String lower = simpleName.toLowerCase(Locale.ROOT);
        boolean json = lower.contains("json");
        boolean avro = lower.contains("avro");
        if (json == avro)
        {
            return null;
        }
        return json ? KafkaValueFormat.JSON : KafkaValueFormat.AVRO;
    }

    private static String trimToNull(String value)
    {
        if (value == null)
        {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}