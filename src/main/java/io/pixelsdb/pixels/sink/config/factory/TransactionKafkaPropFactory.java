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
