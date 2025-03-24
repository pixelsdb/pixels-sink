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
            return after;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
