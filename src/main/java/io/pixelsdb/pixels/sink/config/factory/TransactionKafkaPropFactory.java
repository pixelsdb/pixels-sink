package io.pixelsdb.pixels.sink.config.factory;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

import static io.pixelsdb.pixels.sink.config.factory.RowRecordKafkaPropFactory.getCommonKafkaProperties;

public class TransactionKafkaPropFactory implements KafkaPropFactory{
    @Override
    public Properties createKafkaProperties(PixelsSinkConfig config) {
        Properties kafkaProperties =  getCommonKafkaProperties(config);
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, config.getTransactionTopicValueDeserializer());
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, config.getTransactionTopicGroupId());
        return kafkaProperties;
    }
}
