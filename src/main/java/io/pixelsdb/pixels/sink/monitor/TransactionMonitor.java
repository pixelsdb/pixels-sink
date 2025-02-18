package io.pixelsdb.pixels.sink.monitor;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.pojo.TransactionMessageDTO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import io.pixelsdb.pixels.sink.processer.TransactionMessageProcessor;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class TransactionMonitor implements Runnable{
    private final String transactionTopic;
    private final KafkaConsumer<String, TransactionMessageDTO> consumer;

    public TransactionMonitor(PixelsSinkConfig pixelsSinkConfig, Properties kafkaProperties) {
        this.transactionTopic = pixelsSinkConfig.getTopicPrefix() + "." + pixelsSinkConfig.getTransactionTopicSuffix();
        this.consumer = new KafkaConsumer<String, TransactionMessageDTO>(kafkaProperties);
    }


    @Override
    public void run() {
        consumer.subscribe(Collections.singletonList(transactionTopic));
        while(true) {
            ConsumerRecords<String, TransactionMessageDTO> records = consumer.poll(Duration.ofSeconds(5));
            for(ConsumerRecord<String, TransactionMessageDTO> record: records) {
                TransactionMessageDTO transaction = record.value();
            }
        }
    }

    public void shutdown() {
        consumer.close();
    }
}
