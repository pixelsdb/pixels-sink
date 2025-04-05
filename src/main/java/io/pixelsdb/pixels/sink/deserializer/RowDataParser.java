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

import com.fasterxml.jackson.databind.JsonNode;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.pojo.enums.OperationType;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class RowDataParser {
    private final TypeDescription schema;

    public RowDataParser(TypeDescription schema) {
        this.schema = schema;
    }

    public Map<String, Object> parse(JsonNode dataNode, OperationType operation) {
        if (dataNode.isNull() && operation == OperationType.DELETE) {
            return parseDeleteRecord();
        }
        return parseNode(dataNode, schema);
    }

    private Map<String, Object> parseNode(JsonNode node, TypeDescription schema) {
        Map<String, Object> result = new HashMap<>();
        for (int i = 0; i < schema.getFieldNames().size(); i++) {
            String fieldName = schema.getFieldNames().get(i);
            TypeDescription fieldType = schema.getChildren().get(i);
            result.put(fieldName, parseValue(node.get(fieldName), fieldType));
        }
        return result;
    }

    private Object parseValue(JsonNode valueNode, TypeDescription type) {
        if (valueNode.isNull()) return null;

        switch (type.getCategory()) {
            case INT:
                return valueNode.asInt();
            case LONG:
                return valueNode.asLong();
            case STRING:
                return valueNode.asText().trim();
            case DECIMAL:
                return parseDecimal(valueNode, type);
            case DATE:
                return parseDate(valueNode);
            case STRUCT:
                return parseNode(valueNode, type);
            case BINARY:
                return parseBinary(valueNode);
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    private Map<String, Object> parseDeleteRecord() {
        return Collections.singletonMap("__deleted", true);
    }

    BigDecimal parseDecimal(JsonNode node, TypeDescription type) {
        byte[] bytes = Base64.getDecoder().decode(node.asText());
        int scale = type.getScale();
        return new BigDecimal(new BigInteger(bytes), scale);
    }

    private LocalDate parseDate(JsonNode node) {
        return LocalDate.ofEpochDay(node.asLong());
    }

    private byte[] parseBinary(JsonNode node) {
        try {
            return node.binaryValue();
        } catch (IOException e) {
            throw new RuntimeException("Binary parsing failed", e);
        }
    }
}