package io.pixelsdb.pixels.sink.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class DebeziumJsonMessageDeserializer implements Deserializer<Map<String, Object>> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Map<String, Object> deserialize(String topic, byte[] data) {
        try {
            Map<String, Object> message = objectMapper.readValue(data, Map.class);
            // TODO: Watch Schema Change
            Map<String, Object> payload = (Map<String, Object>) message.get("payload");
            if(payload == null) {
                return null;
            }
            Map<String, Object> after = (Map<String, Object>) payload.get("after");
            if(after == null) {
                return null;
            }
            return after;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
