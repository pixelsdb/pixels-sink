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

import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

class RowChangeEventDeserializerTest {

    private final Deserializer<RowChangeEvent> deserializer = new RowChangeEventJsonDeserializer();

    private String loadSchemaFromFile(String filename) throws IOException, URISyntaxException {
        ClassLoader classLoader = getClass().getClassLoader();
        return new String(Files.readAllBytes(Paths.get(
                Objects.requireNonNull(classLoader.getResource(filename)).toURI()
        )));
    }

    //   @ParameterizedTest
//    @EnumSource(value = OperationType.class, names = {"INSERT", "UPDATE"})
//        //, , "SNAPSHOT"
//    void shouldParseValidOperations(OperationType opType) throws Exception {
//        String jsonData = loadSchemaFromFile("records/" + opType.name().toLowerCase() + ".json");
//        RowChangeEvent event = deserializer.deserialize("test_topic", jsonData.getBytes());
//
//        assertNotNull(event);
//        assertEquals(opType, event.getOp());
//        assertEquals("region", event.getTable());
//
//        Map<String, Object> data = opType == OperationType.DELETE ?
//                event.getBeforeData() : event.getAfterData();
//        assertNotNull(data);
//    }

    @Test
    void shouldHandleDeleteOperation() throws Exception {
        String jsonData = loadSchemaFromFile("records/delete.json");
        RowChangeEvent event = deserializer.deserialize("test_topic", jsonData.getBytes());

        assertTrue(event.isDelete());
        assertNotNull(event.getBeforeData());
        assertNull(event.getAfterData());
    }


    @Test
    void shouldHandleEmptyData() {
        RowChangeEvent event = deserializer.deserialize("empty_topic", new byte[0]);
        assertNull(event);
    }
}

