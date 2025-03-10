package io.pixelsdb.pixels.sink.deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.pixelsdb.pixels.core.TypeDescription;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RowDataParserTest {
    // BqQ= 17
    // JbR7 24710.35
    @ParameterizedTest
    @CsvSource({
            // encodedValue, expectedValue, precision, scale
            "BqQ=, 17.00, 15, 2",
            "JbR7, 24710.35, 15, 2",
    })
    void testParseDecimalValid(String encodedValue, String expectedValue, int precision, int scale) {
        JsonNode node = new TextNode(encodedValue);
        TypeDescription type = TypeDescription.createDecimal(precision, scale);
        RowDataParser rowDataParser = new RowDataParser(type);
        BigDecimal result = rowDataParser.parseDecimal(node, type);
        assertEquals(new BigDecimal(expectedValue), result);
    }

}
