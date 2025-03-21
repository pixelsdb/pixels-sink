package io.pixelsdb.pixels.sink.consumer;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.concurrent.TransactionCoordinator;
import io.pixelsdb.pixels.sink.concurrent.TransactionCoordinatorFactory;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.sink.PixelsSinkWriter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class TableConsumerTask implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(TableConsumerTask.class);
    private static final TransactionCoordinator transactionCoordinator = TransactionCoordinatorFactory.getCoordinator();
    private final Properties kafkaProperties;
    private final String topic;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final String tableName;
    private TypeDescription schema; // assume schema will not change here
    private KafkaConsumer<String, RowChangeEvent> consumer;
    private PixelsSinkWriter sinkWriter;

    public TableConsumerTask(Properties kafkaProperties, String topic) throws IOException {
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
        this.kafkaProperties = kafkaProperties;
        this.topic = topic;
        this.kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId() + "-" + topic);
        this.kafkaProperties.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
        this.kafkaProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        this.tableName = extractTableName(topic);
    }

    @Override
    public void run() {
        try {
            consumer = new KafkaConsumer<>(kafkaProperties);
            consumer.subscribe(Collections.singleton(topic));

            TopicPartition partition = new TopicPartition(topic, 0);
            consumer.poll(Duration.ofSeconds(1));
            consumer.seek(partition, 0);

            while (running.get()) {
                ConsumerRecords<String, RowChangeEvent> records = consumer.poll(Duration.ofSeconds(5));
                if (!records.isEmpty()) {
                    log.info("{} Consumer poll returned {} records", tableName, records.count());
                    records.forEach(record -> {
                        RowChangeEvent event = record.value();
                        transactionCoordinator.processRowEvent(event);
                    });
                }
            }
        } catch (WakeupException e) {
            // shutdown normally
            log.info("Consumer wakeup triggered for {}", tableName);
        } finally {
            if (consumer != null) {
                consumer.close(Duration.ofSeconds(5));
                log.info("Kafka consumer closed for {}", tableName);
            }
        }
    }

    public void shutdown() {
        running.set(false);
        log.info("Shutting down consumer for table: {}", tableName);
        if (consumer != null) {
            consumer.wakeup();
        }
        try {
            sinkWriter.close();
        } catch (IOException e) {
            log.error("Error closing writer for {}: {}", tableName, e.getMessage());
        }
    }

    private String extractTableName(String topic) {
        String[] parts = topic.split("\\.");
        return parts[parts.length - 1];
    }
}
