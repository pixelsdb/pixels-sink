package io.pixelsdb.pixels.sink.deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pixelsdb.pixels.core.TypeDescription;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class SchemaDeserializerTest {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    public void setUp() throws IOException, URISyntaxException {

        // String schemaStr = schemaNode.toString();

    }

    private String loadSchemaFromFile(String filename) throws IOException, URISyntaxException {
        // 通过 ClassLoader 读取资源文件
        ClassLoader classLoader = getClass().getClassLoader();
        return new String(Files.readAllBytes(Paths.get(classLoader.getResource(filename).toURI())));
    }

    @Test
    public void testParseNationStruct() throws IOException, URISyntaxException {
        String jsonSchema = loadSchemaFromFile("records/nation.json");
        JsonNode rootNode = objectMapper.readTree(jsonSchema);
        JsonNode schemaNode = rootNode.get("schema");

        TypeDescription typeDesc = SchemaDeserializer.parseFromBeforeOrAfter(schemaNode, "after");

        assertEquals(TypeDescription.Category.STRUCT, typeDesc.getCategory());
        assertEquals(4, typeDesc.getChildren().size());
        assertEquals("n_nationkey", typeDesc.getFieldNames().get(0));
        assertEquals(TypeDescription.Category.INT, typeDesc.getChildren().get(0).getCategory());
        assertEquals("n_name", typeDesc.getFieldNames().get(1));
        assertEquals(TypeDescription.Category.STRING, typeDesc.getChildren().get(1).getCategory());
        assertEquals("n_regionkey", typeDesc.getFieldNames().get(2));
        assertEquals(TypeDescription.Category.INT, typeDesc.getChildren().get(2).getCategory());
        assertEquals("n_comment", typeDesc.getFieldNames().get(3));
        assertEquals(TypeDescription.Category.STRING, typeDesc.getChildren().get(3).getCategory());
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
        assertEquals(TypeDescription.Category.DECIMAL, filedType.getCategory());
        assertEquals(15, filedType.getPrecision());
        assertEquals(2, filedType.getScale());
    }
//
//    @Test
//    public void testParseNestedStruct() throws IOException {
//        String jsonSchema = "{"
//                + "\"type\": \"struct\","
//                + "\"fields\": ["
//                + "  {\"type\": \"int32\", \"field\": \"order_id\"},"
//                + "  {"
//                + "    \"type\": \"struct\","
//                + "    \"field\": \"customer\","
//                + "    \"fields\": ["
//                + "      {\"type\": \"string\", \"field\": \"name\"},"
//                + "      {\"type\": \"string\", \"field\": \"email\"}"
//                + "    ]"
//                + "  }"
//                + "]"
//                + "}";
//
//        TypeDescription typeDesc = SchemaDeserializer.deserialize(jsonSchema);
//
//        assertEquals(TypeDescription.Category.STRUCT, typeDesc.getCategory());
//        TypeDescription customerType = typeDesc.getChildren().get(1);
//        assertEquals(TypeDescription.Category.STRUCT, customerType.getCategory());
//        assertEquals("name", customerType.getFieldNames().get(0));
//        assertEquals("email", customerType.getFieldNames().get(1));
//    }
//
//    // 测试 Date 类型解析
//    @Test
//    public void testParseDateType() throws IOException {
//        String jsonSchema = "{"
//                + "\"type\": \"int32\","
//                + "\"name\": \"io.debezium.time.Date\","
//                + "\"field\": \"created_at\""
//                + "}";
//
//        TypeDescription typeDesc = SchemaDeserializer.deserialize(jsonSchema);
//        assertEquals(Category.DATE, typeDesc.getCategory());
//    }
//
//    // 测试未知类型异常
//    @Test(expected = IllegalArgumentException.class)
//    public void testParseInvalidType() throws IOException {
//        String jsonSchema = "{"
//                + "\"type\": \"unknown_type\","
//                + "\"field\": \"invalid_field\""
//                + "}";
//        SchemaDeserializer.deserialize(jsonSchema);
//    }
//
//    // 测试缺失必要字段异常
//    @Test(expected = IllegalArgumentException.class)
//    public void testMissingRequiredField() throws IOException {
//        String jsonSchema = "{"
//                + "\"type\": \"struct\","
//                + "\"fields\": ["
//                + "  {\"field\": \"id\"}" // 缺少 "type" 字段
//                + "]"
//                + "}";
//        SchemaDeserializer.deserialize(jsonSchema);
//    }
}