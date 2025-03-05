package io.pixelsdb.pixels.sink;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.core.concurrent.TransactionCoordinator;
import io.pixelsdb.pixels.sink.core.concurrent.TransactionCoordinatorFactory;
import io.pixelsdb.pixels.sink.core.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.writer.CsvWriter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TableConsumerTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(TableConsumerTask.class);
    private static final TransactionCoordinator transactionCoordinator = TransactionCoordinatorFactory.getCoordinator();
    private final PixelsSinkConfig pixelsSinkConfig;
    private final Properties kafkaProperties;
    private final String topic;
    private final CsvWriter writer; // bind a writer
    private final String tableName;
    private TypeDescription schema; // assume schema will not change here
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
        KafkaConsumer<String, RowChangeEvent> consumer = new KafkaConsumer<>(kafkaProperties);
        consumer.subscribe(Collections.singleton(topic));
        TopicPartition partition = new TopicPartition(topic, 0);  // partition 0
        consumer.poll(1);
        log.info("Poll Success");
        consumer.seek(partition, 0);

        while (true) {
            ConsumerRecords<String, RowChangeEvent> records = consumer.poll(Duration.ofSeconds(5));
            if (!records.isEmpty()) {
                log.info("{} Consumer poll returned {} records", tableName, records.count());
                records.forEach(record -> {
                    transactionCoordinator.processRowEvent(record.value());
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
