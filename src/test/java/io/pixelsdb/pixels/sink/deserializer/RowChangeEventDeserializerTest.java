package io.pixelsdb.pixels.sink.deserializer;

import io.pixelsdb.pixels.sink.core.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.pojo.enums.OperationType;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

class RowChangeEventDeserializerTest {

    private final Deserializer<RowChangeEvent> deserializer = new RowChangeEventDeserializer();

    private String loadSchemaFromFile(String filename) throws IOException, URISyntaxException {
        ClassLoader classLoader = getClass().getClassLoader();
        return new String(Files.readAllBytes(Paths.get(
                Objects.requireNonNull(classLoader.getResource(filename)).toURI()
        )));
    }


    @ParameterizedTest
    @EnumSource(value = OperationType.class, names = {"INSERT", "UPDATE"})
        //, , "SNAPSHOT"
    void shouldParseValidOperations(OperationType opType) throws Exception {
        String jsonData = loadSchemaFromFile("records/" + opType.name().toLowerCase() + ".json");
        RowChangeEvent event = deserializer.deserialize("test_topic", jsonData.getBytes());

        assertNotNull(event);
        assertEquals(opType, event.getOp());
        assertEquals("region", event.getTable());

        Map<String, Object> data = opType == OperationType.DELETE ?
                event.getBeforeData() : event.getAfterData();
        assertNotNull(data);
    }

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

