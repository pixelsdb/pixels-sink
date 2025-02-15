package io.pixelsdb.pixels.sink.deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pixelsdb.pixels.sink.dto.TransactionMessageDTO;
import io.pixelsdb.pixels.sink.dto.enums.TransactionStatusEnum;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TransactionMessageDeserializer implements Deserializer<TransactionMessageDTO> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionMessageDeserializer.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Deserializer.super.configure(configs, isKey);
    }

    @Override
    public TransactionMessageDTO deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            JsonNode rootNode = OBJECT_MAPPER.readTree(data);
            JsonNode payloadNode = rootNode.path("payload");

            TransactionMessageDTO message = new TransactionMessageDTO();

            // 解析基础字段
            message.setStatus(parseStatus(payloadNode));
            message.setId(payloadNode.path("id").asText());
            message.setEventCount(payloadNode.path("event_count").asInt());
            message.setTsMs(payloadNode.path("ts_ms").asLong());

            // 解析数据集合
            message.setDataCollections(parseDataCollections(payloadNode));

            return message;
        } catch (IOException e) {
            String errorMsg = "反序列化事务消息失败";
            LOGGER.error("{}: {}", errorMsg, new String(data), e);
            throw new SerializationException(errorMsg, e);
        }
    }
    private TransactionStatusEnum parseStatus(JsonNode payloadNode) {
        String statusStr = payloadNode.path("status").asText();
        TransactionStatusEnum status = TransactionStatusEnum.fromValue(statusStr);
        if (status == null) {
            throw new SerializationException("Illegal transaction status: " + statusStr);
        }
        return status;
    }

    private List<TransactionMessageDTO.DataCollectionInfo> parseDataCollections(JsonNode payloadNode) {
        List<TransactionMessageDTO.DataCollectionInfo> collections = new ArrayList<>();
        JsonNode dataCollectionsNode = payloadNode.path("data_collections");

        dataCollectionsNode.forEach(collectionNode -> {
            TransactionMessageDTO.DataCollectionInfo info = new TransactionMessageDTO.DataCollectionInfo();
            info.setDataCollection(collectionNode.path("data_collection").asText());
            info.setEventCount(collectionNode.path("event_count").asInt());
            collections.add(info);
        });
        return collections;
    }

    @Override
    public TransactionMessageDTO deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public TransactionMessageDTO deserialize(String topic, Headers headers, ByteBuffer data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public void close() {
        Deserializer.super.close();
    }
}
