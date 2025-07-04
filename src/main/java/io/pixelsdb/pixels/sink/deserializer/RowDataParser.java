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
import io.pixelsdb.pixels.core.PixelsProto;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.SinkProto;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
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

    public Map<String, Object> parse(JsonNode dataNode, SinkProto.OperationType operation) {
        if (dataNode.isNull() && operation == SinkProto.OperationType.DELETE) {
            return parseDeleteRecord();
        }
        return parseNode(dataNode, schema);
    }

    public void parse(GenericRecord record, SinkProto.RowValue.Builder builder) {
        for (int i = 0; i < schema.getFieldNames().size(); i++) {
            String fieldName = schema.getFieldNames().get(i);
            TypeDescription fieldType = schema.getChildren().get(i);
            builder.addValues(parseValue(record, fieldName, fieldType).build());
            // result.put(fieldName, parseValue(node.get(fieldName), fieldType));
        }
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

    private SinkProto.ColumnValue.Builder parseValue(GenericRecord record, String filedName, TypeDescription filedType) {

        SinkProto.ColumnValue.Builder columnValueBuilder = SinkProto.ColumnValue.newBuilder();
        columnValueBuilder.setName(filedName);
        switch (filedType.getCategory()) {
            case INT: {
                int value = (int) record.get(filedName);
                columnValueBuilder.setValue(RetinaProto.ColumnValue.newBuilder().setNumberVal(Integer.toString(value)));
                columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.INT));
                break;
            }

            case LONG: {
                long value = (long) record.get(filedName);
                columnValueBuilder.setValue(RetinaProto.ColumnValue.newBuilder().setNumberVal(Long.toString(value)));
                columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.LONG));
                break;
            }

            case STRING: {
                String value = (String) record.get(filedName).toString();
                columnValueBuilder.setValue(RetinaProto.ColumnValue.newBuilder().setStringVal(value));
                columnValueBuilder.setType(PixelsProto.Type.newBuilder().setKind(PixelsProto.Type.Kind.STRING));
                break;
            }
            case DECIMAL: {
                columnValueBuilder.setType(PixelsProto.Type.newBuilder()
                        .setKind(PixelsProto.Type.Kind.DECIMAL)
                        .setDimension(filedType.getDimension())
                        .setScale(filedType.getScale())
                        .build());
                columnValueBuilder.setValue(RetinaProto.ColumnValue.newBuilder().setNumberVal(
                        new String(((ByteBuffer) record.get(filedName)).array())));
                break;
            }
//            case DATE:
//                return parseDate(valueNode);
//            case STRUCT:
//                return parseNode(valueNode, type);
//            case BINARY:
//                return parseBinary(valueNode);
            default:
                throw new IllegalArgumentException("Unsupported type: " + filedType.getCategory());
        }
        return columnValueBuilder;
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