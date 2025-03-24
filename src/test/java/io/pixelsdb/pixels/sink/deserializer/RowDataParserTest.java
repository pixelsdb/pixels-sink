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
