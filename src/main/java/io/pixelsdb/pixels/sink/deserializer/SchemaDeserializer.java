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
}
