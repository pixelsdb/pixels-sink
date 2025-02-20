package io.pixelsdb.pixels.sink.config.factory;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class RowRecordKafkaPropFactory implements KafkaPropFactory{
    @Override
    public Properties createKafkaProperties(PixelsSinkConfig config) {
        Properties kafkaProperties = getCommonKafkaProperties(config);
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, config.getValueDeserializer());
        kafkaProperties.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId());
        return kafkaProperties;
    }

    static Properties getCommonKafkaProperties(PixelsSinkConfig config) {
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, config.getKeyDeserializer());
        kafkaProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return kafkaProperties;
    }
}
