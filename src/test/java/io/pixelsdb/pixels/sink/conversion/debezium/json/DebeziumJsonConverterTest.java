/*
 * Copyright 2026 PixelsDB.
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
 */
package io.pixelsdb.pixels.sink.conversion.debezium.json;

import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.TestConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.conversion.debezium.dialect.MySqlSourceAdapter;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DebeziumJsonConverterTest
{
    private static final SchemaTableName REGION_TABLE =
            new SchemaTableName("pixels_realtime_crud", "region");
    private static final SchemaTableName NATION_TABLE =
            new SchemaTableName("pixels_realtime_crud", "nation");

    private static TableMetadata previousRegionMetadata;
    private static TableMetadata previousNationMetadata;
    private static boolean hadRegionMetadata;
    private static boolean hadNationMetadata;

    private final DebeziumJsonRowConverter rowConverter =
            new DebeziumJsonRowConverter(TableMetadataRegistry.Instance(), null);
    private final DebeziumJsonTransactionConverter transactionConverter =
            new DebeziumJsonTransactionConverter(MySqlSourceAdapter.INSTANCE);

    @BeforeAll
    static void setUpConfig() throws Exception
    {
        TestConfig.initializeUnitConfig();
        Map<SchemaTableName, TableMetadata> registry = metadataRegistry();
        hadRegionMetadata = registry.containsKey(REGION_TABLE);
        previousRegionMetadata = registry.get(REGION_TABLE);
        hadNationMetadata = registry.containsKey(NATION_TABLE);
        previousNationMetadata = registry.get(NATION_TABLE);
        // MySQL delete/update fixtures use uppercase column names.
        registry.put(REGION_TABLE, tableMetadata(2, "region",
                List.of("R_REGIONKEY", "R_NAME", "R_COMMENT"),
                List.of("int", "string", "string")));
        registry.put(NATION_TABLE, tableMetadata(3, "nation",
                List.of("n_nationkey", "n_name", "n_regionkey", "n_comment"),
                List.of("int", "string", "int", "string")));
    }

    @AfterAll
    static void resetConfig() throws ReflectiveOperationException
    {
        Map<SchemaTableName, TableMetadata> registry = metadataRegistry();
        restore(registry, REGION_TABLE, hadRegionMetadata, previousRegionMetadata);
        restore(registry, NATION_TABLE, hadNationMetadata, previousNationMetadata);
        PixelsSinkConfigFactory.reset();
    }

    @Test
    void shouldHandleDeleteOperation() throws Exception
    {
        RowChangeEvent event = convertFixture("records/mysql-region-delete.json");
        assertTrue(event.isDelete());
    }

    @Test
    void shouldHandleUpdateOperation() throws Exception
    {
        RowChangeEvent event = convertFixture("records/mysql-region-update.json");
        assertTrue(event.isUpdate());
        assertEquals("region", event.getTable());
        assertTrue(event.hasBeforeData());
        assertTrue(event.hasAfterData());
    }

    @Test
    void shouldHandlePostgresInsert() throws Exception
    {
        Map<SchemaTableName, TableMetadata> registry = metadataRegistry();
        TableMetadata mysqlRegion = registry.get(REGION_TABLE);
        // Postgres insert fixture uses lowercase column names.
        registry.put(REGION_TABLE, tableMetadata(2, "region",
                List.of("r_regionkey", "r_name", "r_comment"),
                List.of("int", "string", "string")));
        try
        {
            RowChangeEvent event = convertFixture("records/postgresql-region-insert.json");
            assertTrue(event.isInsert());
            assertEquals("public.region", event.getFullTableName());
            assertEquals("779", event.getTransaction().getId());
            assertTrue(event.hasAfterData());
        } finally
        {
            registry.put(REGION_TABLE, mysqlRegion);
        }
    }

    @Test
    void shouldHandlePostgresSnapshotNation() throws Exception
    {
        RowChangeEvent event = convertFixture("records/postgresql-nation-snapshot.json");
        assertTrue(event.isSnapshot());
        assertEquals("public.nation", event.getFullTableName());
        assertTrue(event.hasAfterData());
    }

    @Test
    void shouldConvertTransactionMetadata() throws Exception
    {
        String json = """
                {
                  "payload": {
                    "status": "END",
                    "id": "mysql-tx-1",
                    "event_count": 1,
                    "ts_ms": 1750000000000,
                    "data_collections": [
                      {"data_collection": "pixels_realtime_crud.region", "event_count": 1}
                    ]
                  }
                }
                """;

        SinkProto.TransactionMetadata transaction = transactionConverter.convert(
                json.getBytes(StandardCharsets.UTF_8));

        assertEquals(SinkProto.TransactionStatus.END, transaction.getStatus());
        assertEquals("mysql-tx-1", transaction.getId());
        assertEquals("pixels_realtime_crud.region",
                transaction.getDataCollections(0).getDataCollection());
    }

    private RowChangeEvent convertFixture(String filename) throws Exception
    {
        return rowConverter.convert(loadFixture(filename).getBytes(StandardCharsets.UTF_8));
    }

    private String loadFixture(String filename) throws IOException, URISyntaxException
    {
        ClassLoader classLoader = getClass().getClassLoader();
        return Files.readString(Paths.get(
                Objects.requireNonNull(classLoader.getResource(filename)).toURI()),
                StandardCharsets.UTF_8);
    }

    private static TableMetadata tableMetadata(
            long id, String name, List<String> columnNames, List<String> columnTypes)
            throws Exception
    {
        Table table = new Table();
        table.setId(id);
        table.setName(name);
        List<Column> columns = new java.util.ArrayList<>(columnNames.size());
        for (int i = 0; i < columnNames.size(); i++)
        {
            columns.add(column(columnNames.get(i), columnTypes.get(i)));
        }
        return new TableMetadata(table, null, columns);
    }

    private static Column column(String name, String type)
    {
        Column column = new Column();
        column.setName(name);
        column.setType(type);
        return column;
    }

    private static void restore(
            Map<SchemaTableName, TableMetadata> registry,
            SchemaTableName key,
            boolean hadPrevious,
            TableMetadata previous)
    {
        if (hadPrevious)
        {
            registry.put(key, previous);
        }
        else
        {
            registry.remove(key);
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<SchemaTableName, TableMetadata> metadataRegistry()
            throws ReflectiveOperationException
    {
        Field registry = TableMetadataRegistry.class.getDeclaredField("registry");
        registry.setAccessible(true);
        return (Map<SchemaTableName, TableMetadata>) registry.get(TableMetadataRegistry.Instance());
    }
}
