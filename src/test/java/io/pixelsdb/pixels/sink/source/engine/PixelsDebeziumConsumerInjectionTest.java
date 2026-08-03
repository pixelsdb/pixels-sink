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
package io.pixelsdb.pixels.sink.source.engine;

import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.KeyColumns;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.TestConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import io.pixelsdb.pixels.sink.writer.retina.RetinaPayloadBuilder;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PixelsDebeziumConsumerInjectionTest
{
    private static final SchemaTableName TABLE =
            new SchemaTableName("cdc_verify", "records");
    private static final Schema ROW_SCHEMA = SchemaBuilder.struct().optional()
            .field("id", Schema.INT64_SCHEMA)
            .field("value", Schema.STRING_SCHEMA)
            .build();
    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct()
            .field("connector", Schema.STRING_SCHEMA)
            .field("db", Schema.STRING_SCHEMA)
            .field("table", Schema.STRING_SCHEMA)
            .build();
    private static final Schema TRANSACTION_SCHEMA = SchemaBuilder.struct().optional()
            .field("id", Schema.STRING_SCHEMA)
            .field("total_order", Schema.INT64_SCHEMA)
            .field("data_collection_order", Schema.INT64_SCHEMA)
            .build();
    private static final Schema ENVELOPE_SCHEMA = SchemaBuilder.struct()
            .field("before", ROW_SCHEMA)
            .field("after", ROW_SCHEMA)
            .field("source", SOURCE_SCHEMA)
            .field("transaction", TRANSACTION_SCHEMA)
            .field("op", Schema.STRING_SCHEMA)
            .build();

    private Map<SchemaTableName, TableMetadata> registry;
    private TableMetadata previousMetadata;

    @BeforeEach
    void setUp() throws Exception
    {
        TestConfig.initializeUnitConfig();
        registry = metadataRegistry();
        previousMetadata = registry.put(TABLE, tableMetadata());
    }

    @AfterEach
    void tearDown()
    {
        if (previousMetadata == null)
        {
            registry.remove(TABLE);
        } else
        {
            registry.put(TABLE, previousMetadata);
        }
        PixelsSinkConfigFactory.reset();
    }

    @Test
    void shouldRouteDecodedRowsToInjectedWriterInOrder() throws Exception
    {
        RecordingWriter writer = new RecordingWriter();
        RecordingCommitter committer = new RecordingCommitter();
        List<RecordChangeEvent<SourceRecord>> records = List.of(
                changeEvent(insertRecord(1)),
                changeEvent(insertRecord(2)),
                changeEvent(insertRecord(3)));

        try (PixelsDebeziumConsumer consumer = new PixelsDebeziumConsumer(writer))
        {
            consumer.start();
            consumer.handleBatch(records, committer);
        }

        assertEquals(List.of(1L, 2L, 3L), writer.rows.stream()
                .map(row -> row.getTransaction().getDataCollectionOrder())
                .toList());
        assertEquals(3, writer.requests.size());
        assertTrue(writer.requests.stream().allMatch(request ->
                request.getHeader().getToken().equals("test-token") &&
                        request.getSchemaName().equals(TABLE.getSchemaName()) &&
                        request.getVirtualNodeId() == 23 &&
                        request.getTableUpdateData(0).getTableName()
                                .equals(TABLE.getTableName())));
        assertEquals(records, committer.processed);
        assertTrue(committer.batchFinished);
    }

    private static RecordChangeEvent<SourceRecord> changeEvent(SourceRecord record)
    {
        return () -> record;
    }

    private static SourceRecord insertRecord(long order)
    {
        Struct source = new Struct(SOURCE_SCHEMA)
                .put("connector", "mysql")
                .put("db", TABLE.getSchemaName())
                .put("table", TABLE.getTableName());
        Struct transaction = new Struct(TRANSACTION_SCHEMA)
                .put("id", "tx-" + order)
                .put("total_order", order)
                .put("data_collection_order", order);
        Struct envelope = new Struct(ENVELOPE_SCHEMA)
                .put("before", null)
                .put("after", new Struct(ROW_SCHEMA)
                        .put("id", order)
                        .put("value", "row-" + order))
                .put("source", source)
                .put("transaction", transaction)
                .put("op", "c");
        return new SourceRecord(
                Map.of("server", "mysql-cdc"),
                Map.of("file", "binlog.000001", "pos", order),
                "mysql-cdc.cdc_verify.records",
                null,
                null,
                ENVELOPE_SCHEMA,
                envelope);
    }

    private static TableMetadata tableMetadata() throws Exception
    {
        Table table = new Table();
        table.setId(1L);
        table.setName(TABLE.getTableName());

        KeyColumns keyColumns = new KeyColumns();
        keyColumns.addKeyColumnIds(1);
        SinglePointIndex index = new SinglePointIndex();
        index.setId(7L);
        index.setTableId(table.getId());
        index.setKeyColumns(keyColumns);

        return new TableMetadata(table, index, List.of(
                column(1L, "id", "bigint"),
                column(2L, "value", "varchar(64)")));
    }

    private static Column column(long id, String name, String type)
    {
        Column column = new Column();
        column.setId(id);
        column.setName(name);
        column.setType(type);
        return column;
    }

    @SuppressWarnings("unchecked")
    private static Map<SchemaTableName, TableMetadata> metadataRegistry()
            throws ReflectiveOperationException
    {
        Field field = TableMetadataRegistry.class.getDeclaredField("registry");
        field.setAccessible(true);
        return (Map<SchemaTableName, TableMetadata>)
                field.get(TableMetadataRegistry.Instance());
    }

    private static final class RecordingCommitter
            implements DebeziumEngine.RecordCommitter<RecordChangeEvent<SourceRecord>>
    {
        private final List<RecordChangeEvent<SourceRecord>> processed = new ArrayList<>();
        private boolean batchFinished;

        @Override
        public void markProcessed(RecordChangeEvent<SourceRecord> record)
        {
            processed.add(record);
        }

        @Override
        public void markBatchFinished()
        {
            batchFinished = true;
        }

        @Override
        public void markProcessed(
                RecordChangeEvent<SourceRecord> record,
                DebeziumEngine.Offsets offsets)
        {
            markProcessed(record);
        }

        @Override
        public DebeziumEngine.Offsets buildOffsets()
        {
            return (key, value) -> { };
        }
    }

    private static final class RecordingWriter implements PixelsSinkWriter
    {
        private final List<io.pixelsdb.pixels.sink.event.RowChangeEvent> rows =
                new CopyOnWriteArrayList<>();
        private final List<RetinaProto.UpdateRecordRequest> requests =
                new CopyOnWriteArrayList<>();

        @Override
        public void flush()
        {
        }

        @Override
        public boolean writeRow(io.pixelsdb.pixels.sink.event.RowChangeEvent row)
        {
            rows.add(row);
            try
            {
                RetinaProto.TableUpdateData update =
                        RetinaPayloadBuilder.buildTableUpdateData(
                                row.getTable(), row.getTimeStamp(), List.of(row));
                requests.add(RetinaPayloadBuilder.buildUpdateRecordRequest(
                        "test-token", row.getSchemaName(), 23, List.of(update)));
                return true;
            } catch (SinkException e)
            {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public boolean writeTrans(SinkProto.TransactionMetadata transaction)
        {
            return true;
        }

        @Override
        public void close() throws IOException
        {
        }
    }
}
