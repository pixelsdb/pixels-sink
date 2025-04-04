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

import java.util.Iterator;

public class SchemaDeserializer {
    public static TypeDescription parseFromBeforeOrAfter(JsonNode schemaNode, String fieldName) {
        JsonNode beforeAfterSchema = findSchemaField(schemaNode, fieldName);
        if (beforeAfterSchema == null) {
            throw new IllegalArgumentException("Field '" + fieldName + "' not found in schema");
        }
        return parseStruct(beforeAfterSchema.get("fields"));
    }

    private static JsonNode findSchemaField(JsonNode schemaNode, String targetField) {
        Iterator<JsonNode> fields = schemaNode.get("fields").elements();
        while (fields.hasNext()) {
            JsonNode field = fields.next();
            if (targetField.equals(field.get("field").asText())) {
                return field;
            }
        }
        return null;
    }

    static TypeDescription parseStruct(JsonNode fields) {
        TypeDescription structType = TypeDescription.createStruct();
        fields.forEach(field -> {
            String name = field.get("field").asText();
            TypeDescription fieldType = parseFieldType(field);
            structType.addField(name, fieldType);
        });
        return structType;
    }

    static TypeDescription parseFieldType(JsonNode fieldNode) {
        if (!fieldNode.has("type")) {
            throw new IllegalArgumentException("Field is missing required 'type' property");
        }
        String typeName = fieldNode.get("type").asText();
        String logicalType = fieldNode.has("name") ? fieldNode.get("name").asText() : null;

        if (logicalType != null) {
            switch (logicalType) {
                case "org.apache.kafka.connect.data.Decimal":
                    int precision = Integer.parseInt(fieldNode.get("parameters").get("connect.decimal.precision").asText());
                    int scale = Integer.parseInt(fieldNode.get("parameters").get("scale").asText());
                    return TypeDescription.createDecimal(precision, scale);
                case "io.debezium.time.Date":
                    return TypeDescription.createDate();
            }
        }

        switch (typeName) {
            case "int64":
                return TypeDescription.createLong();
            case "int32":
                return TypeDescription.createInt();
            case "string":
                return TypeDescription.createString();
            case "struct":
                return parseStruct(fieldNode.get("fields"));
            default:
                throw new IllegalArgumentException("Unsupported type: " + typeName);
        }
    }

    /**
     * 将 Avro Schema 转换为 Pixels 类型描述
     *
     * @param avroSchema Avro Schema 对象
     * @return Pixels 类型描述树
     */
    public static TypeDescription parseFromAvroSchema(Schema avroSchema) {
        return parseAvroType(avroSchema, new HashMap<>());
    }

    // 递归解析方法（带缓存）
    private static TypeDescription parseAvroType(Schema schema, Map<String, TypeDescription> cache) {
        // 处理缓存命中
        String schemaKey = schema.getFullName() + ":" + schema.hashCode();
        if (cache.containsKey(schemaKey)) {
            return cache.get(schemaKey);
        }

        TypeDescription typeDesc;
        switch (schema.getType()) {
            case RECORD:
                typeDesc = parseAvroRecord(schema, cache);
                break;
            case UNION:
                typeDesc = parseAvroUnion(schema, cache);
                break;
            case ARRAY:
                typeDesc = parseAvroArray(schema, cache);
                break;
            case MAP:
                typeDesc = parseAvroMap(schema, cache);
                break;
            default:
                typeDesc = parseAvroPrimitive(schema);
        }

        cache.put(schemaKey, typeDesc);
        return typeDesc;
    }

    private static TypeDescription parseAvroRecord(Schema schema, Map<String, TypeDescription> cache) {
        TypeDescription structType = TypeDescription.createStruct();
        for (Schema.Field field : schema.getFields()) {
            TypeDescription fieldType = parseAvroType(field.schema(), cache);
            structType.addField(field.name(), fieldType);
        }
        return structType;
    }

    private static TypeDescription parseAvroUnion(Schema schema, Map<String, TypeDescription> cache) {
        // 找到第一个非 null 类型
        for (Schema type : schema.getTypes()) {
            if (type.getType() != Schema.Type.NULL) {
                return parseAvroType(type, cache);
            }
        }
        throw new IllegalArgumentException("Invalid union type: " + schema);
    }

    private static TypeDescription parseAvroArray(Schema schema, Map<String, TypeDescription> cache) {
        TypeDescription elementType = parseAvroType(schema.getElementType(), cache);
        return TypeDescription.createArray(elementType);
    }

    private static TypeDescription parseAvroMap(Schema schema, Map<String, TypeDescription> cache) {
        TypeDescription valueType = parseAvroType(schema.getValueType(), cache);
        return TypeDescription.createMap(TypeDescription.createString(), valueType);
    }

    private static TypeDescription parseAvroPrimitive(Schema schema) {
        // 处理逻辑类型
        String logicalType = schema.getLogicalType() != null ?
                schema.getLogicalType().getName() : null;

        if (logicalType != null) {
            switch (logicalType) {
                case "decimal":
                    return TypeDescription.createDecimal(
                            schema.getObjectProp("precision").intValue(),
                            schema.getObjectProp("scale").intValue()
                    );
                case "date":
                    return TypeDescription.createDate();
                case "timestamp-millis":
                    return TypeDescription.createTimestamp();
                case "uuid":
                    return TypeDescription.createString(); // Pixels 无 UUID 类型
            }
        }

        // 基本类型映射
        switch (schema.getType()) {
            case LONG:
                return TypeDescription.createLong();
            case INT:
                return TypeDescription.createInt();
            case STRING:
                return TypeDescription.createString();
            case BOOLEAN:
                return TypeDescription.createBoolean();
            case FLOAT:
                return TypeDescription.createFloat();
            case DOUBLE:
                return TypeDescription.createDouble();
            case BYTES:
                return TypeDescription.createBinary();
            default:
                throw new IllegalArgumentException("Unsupported Avro type: " + schema);
        }
    }
}
}
