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
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.pixelsdb.pixels.sink.SinkProto;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class RowRecordDeserializer implements Deserializer<SinkProto.RowRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RowRecordDeserializer.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonFormat.Parser PROTO_PARSER = JsonFormat.parser().ignoringUnknownFields();

    static SinkProto.RowRecord parseRowRecord(Map<String, Object> rawMessage) throws IOException {
        SinkProto.RowRecord.Builder builder = SinkProto.RowRecord.newBuilder();
        String json = OBJECT_MAPPER.writeValueAsString(rawMessage.get("payload"));
        //TODO optimize
        return getRowRecord(json, builder);
    }

    private static SinkProto.RowRecord getRowRecord(String json, SinkProto.RowRecord.Builder builder) throws InvalidProtocolBufferException {
        PROTO_PARSER.merge(json, builder);
        return builder.build();
    }

    @Override
    public SinkProto.RowRecord deserialize(String topic, byte[] data) {
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
}