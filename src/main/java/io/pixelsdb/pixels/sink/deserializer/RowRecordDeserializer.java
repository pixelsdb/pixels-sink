package io.pixelsdb.pixels.sink.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.pixelsdb.pixels.sink.proto.RowRecordMessage;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class RowRecordDeserializer implements Deserializer<RowRecordMessage.RowRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RowRecordDeserializer.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonFormat.Parser PROTO_PARSER = JsonFormat.parser().ignoringUnknownFields();

    @Override
    public RowRecordMessage.RowRecord deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) {
            return null;
        }
        try {
            Map<String, Object> rawMessage = OBJECT_MAPPER.readValue(data, Map.class);
            return parseRowRecord(rawMessage);
        } catch (IOException e) {
            LOGGER.error("Failed to deserialize row record message", e);
            throw new RuntimeException("Deserialization error", e);
        }
    }

    static RowRecordMessage.RowRecord parseRowRecord(Map<String, Object> rawMessage) throws IOException {
        RowRecordMessage.RowRecord.Builder builder = RowRecordMessage.RowRecord.newBuilder();
        String json = OBJECT_MAPPER.writeValueAsString(rawMessage.get("payload"));
        //TODO optimize
        return getRowRecord(json, builder);
    }

    private static RowRecordMessage.RowRecord getRowRecord(String json, RowRecordMessage.RowRecord.Builder builder) throws InvalidProtocolBufferException {
        PROTO_PARSER.merge(json, builder);
        return builder.build();
    }
}