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
package io.pixelsdb.pixels.sink.conversion.debezium.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.TestConfig;
import io.pixelsdb.pixels.sink.conversion.debezium.dialect.DebeziumSourceAdapterRegistry;
import io.pixelsdb.pixels.sink.conversion.debezium.dialect.MySqlSourceAdapter;
import io.pixelsdb.pixels.sink.conversion.debezium.dialect.PostgresSourceAdapter;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DebeziumEnvelopeNormalizerTest
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final SchemaTableName MYSQL_TABLE =
            new SchemaTableName("cdc_verify", "binlog_test");
    private static TableMetadata previousMySqlMetadata;
    private static boolean hadMySqlMetadata;

    @BeforeAll
    static void setUpConfig() throws Exception
    {
        TestConfig.initializeUnitConfig();
        Map<SchemaTableName, TableMetadata> registry = metadataRegistry();
        hadMySqlMetadata = registry.containsKey(MYSQL_TABLE);
        previousMySqlMetadata = registry.get(MYSQL_TABLE);
        registry.put(MYSQL_TABLE, tableMetadata(1, "binlog_test"));
    }

    @AfterAll
    static void resetConfig() throws ReflectiveOperationException
    {
        Map<SchemaTableName, TableMetadata> registry = metadataRegistry();
        if (hadMySqlMetadata)
        {
            registry.put(MYSQL_TABLE, previousMySqlMetadata);
        }
        else
        {
            registry.remove(MYSQL_TABLE);
        }
        PixelsSinkConfigFactory.reset();
    }

    @Test
    void shouldNormalizeMySqlSourceWithoutSchema() throws SinkException
    {
        Struct source = mysqlSource();

        SinkProto.SourceInfo normalized = DebeziumEnvelopeNormalizer.normalizeSource(
                source, MySqlSourceAdapter.INSTANCE);

        assertEquals("cdc_verify", normalized.getDb());
        assertEquals("", normalized.getSchema());
        assertEquals("binlog_test", normalized.getTable());
        assertEquals("cdc_verify.binlog_test", rowChangeEventFor(normalized).getFullTableName());
    }

    @Test
    void shouldNormalizePostgresSourceStruct()
    {
        Schema sourceSchema = SchemaBuilder.struct()
                .field("connector", Schema.STRING_SCHEMA)
                .field("db", Schema.STRING_SCHEMA)
                .field("schema", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA)
                .build();
        Struct source = new Struct(sourceSchema)
                .put("connector", "postgresql")
                .put("db", "pixels_realtime_crud")
                .put("schema", "public")
                .put("table", "region");

        SinkProto.SourceInfo normalized = DebeziumEnvelopeNormalizer.normalizeSource(
                source, PostgresSourceAdapter.INSTANCE);

        assertEquals("pixels_realtime_crud", normalized.getDb());
        assertEquals("public", normalized.getSchema());
        assertEquals("region", normalized.getTable());
        assertEquals("779",
                PostgresSourceAdapter.INSTANCE.normalizeTransactionId("779"));
    }

    @Test
    void shouldRejectConfiguredConnectorMismatch()
    {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> DebeziumEnvelopeNormalizer.normalizeSource(
                        mysqlSource(), PostgresSourceAdapter.INSTANCE));

        assertTrue(error.getMessage().contains("source.connector mysql"));
    }

    @Test
    void shouldResolveConnectorAdaptersThroughSpi()
    {
        assertEquals("mysql",
                DebeziumSourceAdapterRegistry.resolve(
                        "io.debezium.connector.mysql.MySqlConnector").connector());
        assertEquals("postgresql",
                DebeziumSourceAdapterRegistry.resolve("postgresql").connector());
    }

    @Test
    void shouldRejectBlankConnector()
    {
        assertThrows(IllegalArgumentException.class,
                () -> DebeziumSourceAdapterRegistry.resolve(" "));
    }

    @Test
    void shouldRejectUnsupportedConnector()
    {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> DebeziumSourceAdapterRegistry.resolve("oracle"));

        assertTrue(error.getMessage().contains("Unsupported Debezium connector"));
    }

    @Test
    void shouldPreserveMySqlGtidAsOpaqueTransactionId() throws Exception
    {
        String gtid = "8d02ae5e-aeff-ca52-daa1-ab14474cee00:3";
        JsonNode end = OBJECT_MAPPER.readTree("""
                {
                  "status": "END",
                  "id": "%s",
                  "event_count": 4,
                  "ts_ms": 1750000000000,
                  "data_collections": [
                    {"data_collection": "cdc_verify.binlog_test", "event_count": 4}
                  ]
                }
                """.formatted(gtid));

        SinkProto.TransactionMetadata transaction =
                DebeziumEnvelopeNormalizer.normalizeTransactionMetadata(
                        end, MySqlSourceAdapter.INSTANCE);

        assertEquals(SinkProto.TransactionStatus.END, transaction.getStatus());
        assertEquals(gtid, transaction.getId());
        assertEquals(4, transaction.getEventCount());
        assertEquals("cdc_verify.binlog_test",
                transaction.getDataCollections(0).getDataCollection());
    }

    private Struct mysqlSource()
    {
        Schema sourceSchema = SchemaBuilder.struct()
                .field("connector", Schema.STRING_SCHEMA)
                .field("db", Schema.STRING_SCHEMA)
                .field("table", Schema.STRING_SCHEMA)
                .build();
        return new Struct(sourceSchema)
                .put("connector", "mysql")
                .put("db", "cdc_verify")
                .put("table", "binlog_test");
    }

    private static RowChangeEvent rowChangeEventFor(SinkProto.SourceInfo source) throws SinkException
    {
        return new RowChangeEvent(
                SinkProto.RowRecord.newBuilder()
                        .setSource(source)
                        .setOp(SinkProto.OperationType.SNAPSHOT)
                        .setAfter(SinkProto.RowValue.newBuilder().build())
                        .build(),
                TypeDescription.createSchemaFromStrings(new ArrayList<>(), new ArrayList<>()));
    }

    private static TableMetadata tableMetadata(long id, String name) throws Exception
    {
        Table table = new Table();
        table.setId(id);
        table.setName(name);
        return new TableMetadata(table, null, List.of());
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
