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

import io.pixelsdb.pixels.common.metadata.SchemaTableName;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.pixelsdb.pixels.sink.source.engine.DebeziumRecordType.ROW;
import static io.pixelsdb.pixels.sink.source.engine.DebeziumRecordType.TOMBSTONE;
import static io.pixelsdb.pixels.sink.source.engine.DebeziumRecordType.TRANSACTION;
import static io.pixelsdb.pixels.sink.source.engine.DebeziumRecordType.UNKNOWN_CONTROL;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ConnectEventClassifierTest
{
    private static final String PREFIX = "mysql-cdc";
    private static final String ROW_TOPIC = PREFIX + ".cdc_verify.binlog_test";
    private static final String TRANSACTION_TOPIC = PREFIX + ".transaction";
    private static final Schema ROW_SCHEMA = SchemaBuilder.struct().optional()
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    private static final Schema SOURCE_SCHEMA = SchemaBuilder.struct()
            .field("connector", Schema.STRING_SCHEMA)
            .field("db", Schema.STRING_SCHEMA)
            .field("table", Schema.OPTIONAL_STRING_SCHEMA)
            .field("gtid", Schema.OPTIONAL_STRING_SCHEMA)
            .field("file", Schema.STRING_SCHEMA)
            .field("pos", Schema.INT64_SCHEMA)
            .field("row", Schema.INT32_SCHEMA)
            .build();
    private static final Schema TRANSACTION_INFO_SCHEMA = SchemaBuilder.struct().optional()
            .field("id", Schema.STRING_SCHEMA)
            .field("total_order", Schema.INT64_SCHEMA)
            .field("data_collection_order", Schema.INT64_SCHEMA)
            .build();

    private final ConnectEventClassifier classifier = new ConnectEventClassifier();

    @Test
    void shouldClassifyMySqlRowOperations()
    {
        assertEquals(ROW, classify(rowRecord("c", null, row(1), "gtid:3", 1)));
        assertEquals(ROW, classify(rowRecord("u", row(1), row(1), "gtid:3", 2)));
        assertEquals(ROW, classify(rowRecord("d", row(1), null, "gtid:3", 3)));
        assertEquals(ROW, classify(rowRecord("r", null, row(1), "", 0)));
    }

    @Test
    void shouldClassifyTransactionBoundaries()
    {
        assertEquals(TRANSACTION, classify(transactionRecord("BEGIN", "gtid:5")));
        assertEquals(TRANSACTION, classify(transactionRecord("END", "gtid:5")));
        assertEquals(UNKNOWN_CONTROL, classify(transactionRecord("COMMIT", "gtid:5")));
    }

    @Test
    void shouldSkipControlEvents()
    {
        assertEquals(TOMBSTONE, classify(sourceRecord(ROW_TOPIC, null, null)));
        assertEquals(UNKNOWN_CONTROL, classify(sourceRecord(
                PREFIX + ".signal", SchemaBuilder.struct()
                        .field("type", Schema.STRING_SCHEMA).build(),
                new Struct(SchemaBuilder.struct()
                        .field("type", Schema.STRING_SCHEMA).build()).put("type", "x"))));
    }

    @Test
    void shouldRejectMalformedRows()
    {
        assertEquals(UNKNOWN_CONTROL,
                classify(rowRecord("u", null, row(1), "gtid:6", 1)));
        assertEquals(UNKNOWN_CONTROL,
                classify(rowRecord("d", null, null, "gtid:6", 1)));
    }

    @Test
    void shouldExtractTableStreamKey()
    {
        SchemaTableName table = classifier.tableOf(rowRecord("c", null, row(1), "gtid:1", 1));
        assertEquals("cdc_verify", table.getSchemaName());
        assertEquals("binlog_test", table.getTableName());
    }

    private DebeziumRecordType classify(SourceRecord record)
    {
        return classifier.classify(record, TRANSACTION_TOPIC);
    }

    private SourceRecord rowRecord(
            String op, Struct before, Struct after, String transactionId, long order)
    {
        Struct transaction = transactionId.isEmpty() ? null : new Struct(TRANSACTION_INFO_SCHEMA)
                .put("id", transactionId)
                .put("total_order", order)
                .put("data_collection_order", order);
        Struct source = new Struct(SOURCE_SCHEMA)
                .put("connector", "mysql")
                .put("db", "cdc_verify")
                .put("table", "binlog_test")
                .put("gtid", transactionId.isEmpty() ? null : transactionId)
                .put("file", "binlog.000004")
                .put("pos", 152L)
                .put("row", (int) order);
        Schema envelopeSchema = SchemaBuilder.struct()
                .name("mysql-cdc.cdc_verify.binlog_test.Envelope")
                .field("before", ROW_SCHEMA)
                .field("after", ROW_SCHEMA)
                .field("source", SOURCE_SCHEMA)
                .field("transaction", TRANSACTION_INFO_SCHEMA)
                .field("op", Schema.STRING_SCHEMA)
                .build();
        Struct envelope = new Struct(envelopeSchema)
                .put("before", before)
                .put("after", after)
                .put("source", source)
                .put("transaction", transaction)
                .put("op", op);
        return sourceRecord(ROW_TOPIC, envelopeSchema, envelope);
    }

    private SourceRecord transactionRecord(String status, String id)
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
        Struct value = new Struct(schema)
                .put("status", status)
                .put("id", id)
                .put("event_count", status.equals("END") ? 4L : null)
                .put("data_collections", null)
                .put("ts_ms", 1750000000000L);
        return sourceRecord(TRANSACTION_TOPIC, schema, value);
    }

    private SourceRecord sourceRecord(String topic, Schema schema, Object value)
    {
        return new SourceRecord(
                Map.of("server", PREFIX),
                Map.of("file", "binlog.000004", "pos", 152L),
                topic,
                null,
                null,
                schema,
                value);
    }

    private Struct row(long id)
    {
        return new Struct(ROW_SCHEMA).put("id", id).put("name", "row-" + id);
    }
}
