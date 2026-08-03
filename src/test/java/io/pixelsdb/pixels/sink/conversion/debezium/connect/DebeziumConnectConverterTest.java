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
package io.pixelsdb.pixels.sink.conversion.debezium.connect;

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
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DebeziumConnectConverterTest
{
    private static final SchemaTableName TABLE =
            new SchemaTableName("cdc_verify", "binlog_test");
    private static final Schema ROW_SCHEMA = SchemaBuilder.struct().optional()
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct()
            .field("connector", Schema.STRING_SCHEMA)
            .field("db", Schema.STRING_SCHEMA)
            .field("table", Schema.STRING_SCHEMA)
            .build();
    private static final Schema TRANSACTION_INFO_SCHEMA = SchemaBuilder.struct().optional()
            .field("id", Schema.STRING_SCHEMA)
            .field("total_order", Schema.INT64_SCHEMA)
            .field("data_collection_order", Schema.INT64_SCHEMA)
            .build();

    private static TableMetadata previousMetadata;
    private static boolean hadMetadata;

    private final DebeziumConnectRowConverter rowConverter =
            new DebeziumConnectRowConverter(
                    TableMetadataRegistry.Instance(), MySqlSourceAdapter.INSTANCE);
    private final DebeziumConnectTransactionConverter transactionConverter =
            new DebeziumConnectTransactionConverter(MySqlSourceAdapter.INSTANCE);

    @BeforeAll
    static void setUpConfig() throws Exception
    {
        TestConfig.initializeUnitConfig();
        Map<SchemaTableName, TableMetadata> registry = metadataRegistry();
        hadMetadata = registry.containsKey(TABLE);
        previousMetadata = registry.get(TABLE);
        registry.put(TABLE, tableMetadata());
    }

    @AfterAll
    static void resetConfig() throws ReflectiveOperationException
    {
        Map<SchemaTableName, TableMetadata> registry = metadataRegistry();
        if (hadMetadata)
        {
            registry.put(TABLE, previousMetadata);
        }
        else
        {
            registry.remove(TABLE);
        }
        PixelsSinkConfigFactory.reset();
    }

    @Test
    void shouldConvertInsertRow() throws Exception
    {
        RowChangeEvent event = rowConverter.convert(insertRecord());

        assertTrue(event.isInsert());
        assertEquals("binlog_test", event.getTable());
        assertTrue(event.hasAfterData());
        assertEquals("gtid:1", event.getTransaction().getId());
    }

    @Test
    void shouldConvertTransactionBoundary()
    {
        Schema collectionSchema = SchemaBuilder.struct()
                .field("data_collection", Schema.STRING_SCHEMA)
                .field("event_count", Schema.INT64_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct()
                .field("status", Schema.STRING_SCHEMA)
                .field("id", Schema.STRING_SCHEMA)
                .field("event_count", Schema.OPTIONAL_INT64_SCHEMA)
                .field("data_collections", SchemaBuilder.array(collectionSchema).optional().build())
                .field("ts_ms", Schema.INT64_SCHEMA)
                .build();
        Struct collection = new Struct(collectionSchema)
                .put("data_collection", "cdc_verify.binlog_test")
                .put("event_count", 1L);
        Struct value = new Struct(schema)
                .put("status", "END")
                .put("id", "gtid:9")
                .put("event_count", 1L)
                .put("data_collections", List.of(collection))
                .put("ts_ms", 1750000000000L);

        SinkProto.TransactionMetadata transaction = transactionConverter.convert(
                new SourceRecord(
                        Map.of("server", "mysql-cdc"),
                        Map.of("file", "binlog.000004", "pos", 152L),
                        "mysql-cdc.transaction",
                        null,
                        null,
                        schema,
                        value));

        assertEquals(SinkProto.TransactionStatus.END, transaction.getStatus());
        assertEquals("gtid:9", transaction.getId());
        assertEquals("cdc_verify.binlog_test",
                transaction.getDataCollections(0).getDataCollection());
    }

    private SourceRecord insertRecord()
    {
        Struct source = new Struct(SOURCE_SCHEMA)
                .put("connector", "mysql")
                .put("db", "cdc_verify")
                .put("table", "binlog_test");
        Struct transaction = new Struct(TRANSACTION_INFO_SCHEMA)
                .put("id", "gtid:1")
                .put("total_order", 1L)
                .put("data_collection_order", 1L);
        Schema envelopeSchema = SchemaBuilder.struct()
                .field("before", ROW_SCHEMA)
                .field("after", ROW_SCHEMA)
                .field("source", SOURCE_SCHEMA)
                .field("transaction", TRANSACTION_INFO_SCHEMA)
                .field("op", Schema.STRING_SCHEMA)
                .build();
        Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", new Struct(ROW_SCHEMA).put("id", 1L).put("name", "row-1"))
                .put("source", source)
                .put("transaction", transaction)
                .put("op", "c");
        return new SourceRecord(
                Map.of("server", "mysql-cdc"),
                Map.of("file", "binlog.000004", "pos", 152L),
                "mysql-cdc.cdc_verify.binlog_test",
                null,
                null,
                envelopeSchema,
                envelope);
    }

    private static TableMetadata tableMetadata() throws Exception
    {
        Table table = new Table();
        table.setId(1);
        table.setName("binlog_test");
        return new TableMetadata(table, null, List.of(
                column("id", "bigint"),
                column("name", "varchar(64)")));
    }

    private static Column column(String name, String type)
    {
        Column column = new Column();
        column.setName(name);
        column.setType(type);
        return column;
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
