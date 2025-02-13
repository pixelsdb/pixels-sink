package io.pixelsdb.pixels.sink;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.monitor.TopicMonitor;
import io.pixelsdb.pixels.sink.writer.CsvWriter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableConsumerTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(TableConsumerTask.class);
    private PixelsSinkConfig pixelsSinkConfig;
    private Properties kafkaProperties;
    private String topic;
    private CsvWriter writer; // bind a writer
    private String tableName;
    // TODO writer should be an abstract class. I will implement it later
    public TableConsumerTask(PixelsSinkConfig pixelsSinkConfig, Properties kafkaProperties, String topic) throws IOException {
        this.pixelsSinkConfig = pixelsSinkConfig;
        this.kafkaProperties = kafkaProperties;
        this.topic = topic;
        this.kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, pixelsSinkConfig.getGroupId() + "-" + topic);
        this.kafkaProperties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        this.kafkaProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        this.tableName = extractTableName(topic);
        this.writer = new CsvWriter(pixelsSinkConfig, tableName);
    }

    @Override
    public void run() {

        KafkaConsumer<String, Map<String, Object>> consumer = new KafkaConsumer<>(kafkaProperties);
        consumer.subscribe(Collections.singleton(topic));
        TopicPartition partition = new TopicPartition(topic, 0);  // partition 0
        consumer.poll(1);
        log.info("Poll Success");
        consumer.seek(partition, 0);  // 设置偏移量为 0

        while (true) {
            ConsumerRecords<String, Map<String, Object>> records = consumer.poll(Duration.ofSeconds(5));
            log.info("{} Consumer poll returned {} records", tableName, records.count());
            if (!records.isEmpty()) {
                records.forEach(record -> {
                    try {
                        writer.writeToCsv(record.value());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
            }
            // TODO stop singal
        }
    }

    private String extractTableName(String topic) {
        String[] parts = topic.split("\\.");
        return parts[parts.length - 1];
    }
}
