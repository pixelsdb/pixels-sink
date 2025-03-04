package io.pixelsdb.pixels.sink.deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pixelsdb.pixels.core.TypeDescription;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;

public class SchemaDeserializerTest {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    public void setUp() throws IOException, URISyntaxException {
        // String schemaStr = schemaNode.toString();

    }

    private String loadSchemaFromFile(String filename) throws IOException, URISyntaxException {
        ClassLoader classLoader = getClass().getClassLoader();
        return new String(Files.readAllBytes(Paths.get(Objects.requireNonNull(classLoader.getResource(filename)).toURI())));
    }

    @Test
    public void testParseNationStruct() throws IOException, URISyntaxException {
        String jsonSchema = loadSchemaFromFile("records/nation.json");
        JsonNode rootNode = objectMapper.readTree(jsonSchema);
        JsonNode schemaNode = rootNode.get("schema");

        TypeDescription typeDesc = SchemaDeserializer.parseFromBeforeOrAfter(schemaNode, "after");

        Assertions.assertEquals(TypeDescription.Category.STRUCT, typeDesc.getCategory());
        Assertions.assertEquals(4, typeDesc.getChildren().size());
        Assertions.assertEquals("n_nationkey", typeDesc.getFieldNames().get(0));
        Assertions.assertEquals(TypeDescription.Category.INT, typeDesc.getChildren().get(0).getCategory());
        Assertions.assertEquals("n_name", typeDesc.getFieldNames().get(1));
        Assertions.assertEquals(TypeDescription.Category.STRING, typeDesc.getChildren().get(1).getCategory());
        Assertions.assertEquals("n_regionkey", typeDesc.getFieldNames().get(2));
        Assertions.assertEquals(TypeDescription.Category.INT, typeDesc.getChildren().get(2).getCategory());
        Assertions.assertEquals("n_comment", typeDesc.getFieldNames().get(3));
        Assertions.assertEquals(TypeDescription.Category.STRING, typeDesc.getChildren().get(3).getCategory());
    }

    @Test
    public void testParseDecimalType() throws IOException {
        String jsonSchema = "[{"
                + "\"type\": \"bytes\","
                + "\"name\": \"org.apache.kafka.connect.data.Decimal\","
                + "\"parameters\": {"
                + "  \"scale\": \"2\","
                + "  \"connect.decimal.precision\": \"15\""
                + "},"
                + "\"field\": \"price\""
                + "}]";

        JsonNode rootNode = objectMapper.readTree(jsonSchema);

        TypeDescription typeDesc = SchemaDeserializer.parseStruct(rootNode);
        TypeDescription filedType = typeDesc.getChildren().get(0);
        Assertions.assertEquals(TypeDescription.Category.DECIMAL, filedType.getCategory());
        Assertions.assertEquals(15, filedType.getPrecision());
        Assertions.assertEquals(2, filedType.getScale());
    }

    @Test
    public void testParseDateType() throws IOException {

        String jsonSchema = "[{"
                + "\"type\": \"int32\","
                + "\"name\": \"io.debezium.time.Date\","
                + "\"field\": \"created_at\""
                + "}]";
        JsonNode rootNode = objectMapper.readTree(jsonSchema);
        TypeDescription typeDesc = SchemaDeserializer.parseStruct(rootNode);
        TypeDescription filedType = typeDesc.getChildren().get(0);
        Assertions.assertEquals(TypeDescription.Category.DATE, filedType.getCategory());
    }

    // 测试未知类型异常
    @Test
    public void testParseInvalidType() throws IOException {
        String jsonSchema = "[{"
                + "\"type\": \"unknown_type\","
                + "\"field\": \"invalid_field\""
                + "}]";
        JsonNode rootNode = objectMapper.readTree(jsonSchema);
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> {
                    SchemaDeserializer.parseStruct(rootNode);
                }
        );
    }

    @Test
    public void testMissingRequiredField() throws IOException {
        String jsonSchema = "{"
                + "\"type\": \"struct\","
                + "\"fields\": ["
                + "  {\"field\": \"id\"}" // lack type
                + "]"
                + "}";
        JsonNode rootNode = objectMapper.readTree(jsonSchema);
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> {
                    SchemaDeserializer.parseFieldType(rootNode);
                }
        );
    }
}