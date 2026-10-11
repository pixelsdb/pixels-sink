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
package io.pixelsdb.pixels.sink.writer.retina;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.KeyColumns;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RetinaPayloadBuilderTest
{
    private static final String SCHEMA_NAME = "cdc_verify";
    private static final String TABLE_NAME = "records";
    private static final long TABLE_ID = 41L;
    private static final long INDEX_ID = 71L;
    private static final long TIMESTAMP = 987654321L;

    @Test
    void shouldBuildCompleteUpdateRecordRequest() throws Exception
    {
        TableMetadata metadata = tableMetadata();
        RowChangeEvent insert = event(
                SinkProto.OperationType.INSERT, null, row("1", "insert"), metadata);
        RowChangeEvent snapshot = event(
                SinkProto.OperationType.SNAPSHOT, null, row("2", "snapshot"), metadata);
        RowChangeEvent update = event(
                SinkProto.OperationType.UPDATE, row("3", "before"), row("3", "after"), metadata);
        RowChangeEvent delete = event(
                SinkProto.OperationType.DELETE, row("4", "delete"), null, metadata);

        List<RetinaProto.TableUpdateData> tableUpdates =
                RetinaPayloadBuilder.buildTableUpdateDataList(
                        TABLE_NAME, TIMESTAMP, List.of(insert, snapshot, update, delete));
        RetinaProto.UpdateRecordRequest request =
                RetinaPayloadBuilder.buildUpdateRecordRequest(
                        "fixed-token", SCHEMA_NAME, 23, tableUpdates);

        assertEquals("fixed-token", request.getHeader().getToken());
        assertEquals(SCHEMA_NAME, request.getSchemaName());
        assertEquals(23, request.getVirtualNodeId());
        assertEquals(tableUpdates, request.getTableUpdateDataList());
        assertEquals(4, tableUpdates.size());
        for (RetinaProto.TableUpdateData tableUpdate : tableUpdates)
        {
            assertEquals(TABLE_NAME, tableUpdate.getTableName());
            assertEquals(INDEX_ID, tableUpdate.getPrimaryIndexId());
            assertEquals(TIMESTAMP, tableUpdate.getTimestamp());
        }

        assertEquals(1, tableUpdates.get(0).getInsertDataCount());
        assertRow(tableUpdates.get(0).getInsertData(0).getIndexKeys(0),
                tableUpdates.get(0).getInsertData(0).getColValuesList(), "1", "insert");
        assertEquals(List.of(false, false), tableUpdates.get(0).getInsertData(0).getIsNullList());
        assertEquals(1, tableUpdates.get(1).getInsertDataCount());
        assertRow(tableUpdates.get(1).getInsertData(0).getIndexKeys(0),
                tableUpdates.get(1).getInsertData(0).getColValuesList(), "2", "snapshot");
        assertEquals(List.of(false, false), tableUpdates.get(1).getInsertData(0).getIsNullList());

        assertEquals(1, tableUpdates.get(2).getUpdateDataCount());
        assertRow(tableUpdates.get(2).getUpdateData(0).getIndexKeys(0),
                tableUpdates.get(2).getUpdateData(0).getColValuesList(), "3", "after");
        assertEquals(List.of(false, false), tableUpdates.get(2).getUpdateData(0).getIsNullList());

        assertEquals(1, tableUpdates.get(3).getDeleteDataCount());
        assertEquals(bytes("4"), tableUpdates.get(3).getDeleteData(0).getIndexKeys(0).getKey());
        assertEquals(INDEX_ID, tableUpdates.get(3).getDeleteData(0).getIndexKeys(0).getIndexId());
        assertEquals(TABLE_ID, tableUpdates.get(3).getDeleteData(0).getIndexKeys(0).getTableId());
        assertEquals(TIMESTAMP, tableUpdates.get(3).getDeleteData(0).getIndexKeys(0).getTimestamp());

        assertEquals(request, RetinaProto.UpdateRecordRequest.parseFrom(request.toByteArray()));
    }

    @Test
    void shouldGroupOnlyAdjacentEventsWithTheSameOperation() throws Exception
    {
        TableMetadata metadata = tableMetadata();
        List<RowChangeEvent> events = List.of(
                event(SinkProto.OperationType.INSERT, null, row("1", "first"), metadata),
                event(SinkProto.OperationType.INSERT, null, row("2", "second"), metadata),
                event(SinkProto.OperationType.DELETE, row("3", "third"), null, metadata),
                event(SinkProto.OperationType.DELETE, row("4", "fourth"), null, metadata),
                event(SinkProto.OperationType.INSERT, null, row("5", "fifth"), metadata));

        List<RetinaProto.TableUpdateData> updates =
                RetinaPayloadBuilder.buildTableUpdateDataList(TABLE_NAME, TIMESTAMP, events);

        assertEquals(3, updates.size());
        assertEquals(2, updates.get(0).getInsertDataCount());
        assertEquals(2, updates.get(1).getDeleteDataCount());
        assertEquals(1, updates.get(2).getInsertDataCount());
        assertEquals(bytes("1"), updates.get(0).getInsertData(0).getIndexKeys(0).getKey());
        assertEquals(bytes("2"), updates.get(0).getInsertData(1).getIndexKeys(0).getKey());
        assertEquals(bytes("3"), updates.get(1).getDeleteData(0).getIndexKeys(0).getKey());
        assertEquals(bytes("4"), updates.get(1).getDeleteData(1).getIndexKeys(0).getKey());
        assertEquals(bytes("5"), updates.get(2).getInsertData(0).getIndexKeys(0).getKey());
    }

    @Test
    void shouldRejectMixedOperationsInOneTableUpdateData() throws Exception
    {
        TableMetadata metadata = tableMetadata();
        RowChangeEvent insert = event(
                SinkProto.OperationType.INSERT, null, row("1", "insert"), metadata);
        RowChangeEvent delete = event(
                SinkProto.OperationType.DELETE, row("1", "delete"), null, metadata);

        assertThrows(IllegalArgumentException.class,
                () -> RetinaPayloadBuilder.buildTableUpdateData(
                        TABLE_NAME, TIMESTAMP, List.of(insert, delete)));
    }

    @Test
    void shouldPropagateIsNullAlongsideColValues() throws Exception
    {
        TableMetadata metadata = tableMetadata();
        RowChangeEvent withNull = event(
                SinkProto.OperationType.INSERT,
                null,
                SinkProto.RowValue.newBuilder()
                        .addValues(SinkProto.ColumnValue.newBuilder().setValue(bytes("5")))
                        .addValues(SinkProto.ColumnValue.newBuilder()
                                .setValue(ByteString.EMPTY)
                                .setIsNull(true))
                        .build(),
                metadata);
        RowChangeEvent emptyString = event(
                SinkProto.OperationType.UPDATE,
                row("6", "before"),
                SinkProto.RowValue.newBuilder()
                        .addValues(SinkProto.ColumnValue.newBuilder().setValue(bytes("6")))
                        .addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.EMPTY))
                        .build(),
                metadata);

        List<RetinaProto.TableUpdateData> tableUpdates =
                RetinaPayloadBuilder.buildTableUpdateDataList(
                        TABLE_NAME, TIMESTAMP, List.of(withNull, emptyString));

        assertEquals(List.of(false, true), tableUpdates.get(0).getInsertData(0).getIsNullList());
        assertEquals(bytes("5"), tableUpdates.get(0).getInsertData(0).getColValues(0));
        assertEquals(ByteString.EMPTY, tableUpdates.get(0).getInsertData(0).getColValues(1));

        assertEquals(List.of(false, false), tableUpdates.get(1).getUpdateData(0).getIsNullList());
        assertEquals(ByteString.EMPTY, tableUpdates.get(1).getUpdateData(0).getColValues(1));
    }

    private static void assertRow(
            io.pixelsdb.pixels.index.IndexProto.IndexKey indexKey,
            List<ByteString> columnValues,
            String id,
            String value)
    {
        assertEquals(bytes(id), indexKey.getKey());
        assertEquals(INDEX_ID, indexKey.getIndexId());
        assertEquals(TABLE_ID, indexKey.getTableId());
        assertEquals(TIMESTAMP, indexKey.getTimestamp());
        assertEquals(List.of(bytes(id), bytes(value)), columnValues);
    }

    private static RowChangeEvent event(
            SinkProto.OperationType operation,
            SinkProto.RowValue before,
            SinkProto.RowValue after,
            TableMetadata metadata) throws Exception
    {
        return eventWithRoutingOnly(operation, before, after, metadata);
    }

    private static RowChangeEvent eventWithRoutingOnly(
            SinkProto.OperationType operation,
            SinkProto.RowValue before,
            SinkProto.RowValue after,
            TableMetadata metadata) throws Exception
    {
        SinkProto.RowRecord.Builder record = SinkProto.RowRecord.newBuilder()
                .setOp(operation)
                .setSource(SinkProto.SourceInfo.newBuilder()
                        .setDb(SCHEMA_NAME)
                        .setTable(TABLE_NAME));
        if (before != null)
        {
            record.setBefore(before);
        }
        if (after != null)
        {
            record.setAfter(after);
        }

        RowChangeEvent event = new RowChangeEvent(
                record.build(), metadata.getTypeDescription(), metadata);
        event.initRoutingKey();
        return event;
    }

    private static SinkProto.RowValue row(String id, String value)
    {
        return SinkProto.RowValue.newBuilder()
                .addValues(SinkProto.ColumnValue.newBuilder().setValue(bytes(id)))
                .addValues(SinkProto.ColumnValue.newBuilder().setValue(bytes(value)))
                .build();
    }

    private static TableMetadata tableMetadata() throws Exception
    {
        Table table = new Table();
        table.setId(TABLE_ID);
        table.setName(TABLE_NAME);

        KeyColumns keyColumns = new KeyColumns();
        keyColumns.addKeyColumnIds(1);
        SinglePointIndex index = new SinglePointIndex();
        index.setId(INDEX_ID);
        index.setTableId(TABLE_ID);
        index.setKeyColumns(keyColumns);

        return new TableMetadata(table, index, List.of(
                column(1, "id", "varchar(32)"),
                column(2, "value", "varchar(64)")));
    }

    private static Column column(long id, String name, String type)
    {
        Column column = new Column();
        column.setId(id);
        column.setName(name);
        column.setType(type);
        return column;
    }

    private static ByteString bytes(String value)
    {
        return ByteString.copyFromUtf8(value);
    }

    @Test
    void shouldBindIndexKeyWithPayloadTimestamp() throws Exception
    {
        TableMetadata metadata = tableMetadata();
        RowChangeEvent insert = eventWithRoutingOnly(
                SinkProto.OperationType.INSERT, null, row("1", "insert"), metadata);

        assertEquals(bytes("1"), insert.getAfterRoutingKey());
        assertEquals(null, insert.getAfterKey());

        RetinaProto.TableUpdateData tableUpdate =
                RetinaPayloadBuilder.buildTableUpdateData(
                        TABLE_NAME, TIMESTAMP, List.of(insert));

        assertEquals(TIMESTAMP, tableUpdate.getTimestamp());
        assertEquals(TIMESTAMP, tableUpdate.getInsertData(0).getIndexKeys(0).getTimestamp());
        assertEquals(bytes("1"), tableUpdate.getInsertData(0).getIndexKeys(0).getKey());
        assertEquals(TIMESTAMP, insert.getAfterKey().getTimestamp());
    }

    @Test
    void shouldFoldPostgresTransactionSequences() throws Exception
    {
        String[] sequences = {"UU", "IU", "ID", "UD", "DI", "IUD", "UDI", "IDI", "DID"};
        SinkProto.OperationType[] expected = {SinkProto.OperationType.UPDATE, SinkProto.OperationType.INSERT,
                null, SinkProto.OperationType.DELETE, SinkProto.OperationType.UPDATE, null,
                SinkProto.OperationType.UPDATE, SinkProto.OperationType.INSERT, SinkProto.OperationType.DELETE};
        for (int n = 0; n < sequences.length; n++)
        {
            List<RowChangeEvent> input = transactionSequence(sequences[n], "tx");
            List<RowChangeEvent> net = TransactionBatchCoalescer.coalesce(input);
            if (expected[n] == null) assertEquals(List.of(), net, sequences[n]);
            else
            {
                assertEquals(1, net.size(), sequences[n]);
                assertEquals(expected[n], net.get(0).getOp(), sequences[n]);
                if (net.get(0).hasBeforeData()) assertEquals(input.get(0).getBefore(), net.get(0).getBefore());
                if (net.get(0).hasAfterData()) assertEquals(input.get(input.size() - 1).getAfter(), net.get(0).getAfter());
            }
            List<RetinaProto.TableUpdateData> payload = RetinaPayloadBuilder.buildTableUpdateDataList(TABLE_NAME, TIMESTAMP, input);
            assertEquals(expected[n] == null ? 0 : 1, payload.size(), sequences[n]);
        }
    }

    @Test
    void shouldPreserveTransactionsAndRpcBoundaries() throws Exception
    {
        RowChangeEvent a = transactionSequence("U", "a").get(0);
        RowChangeEvent b = transactionSequence("U", "b").get(0);
        assertEquals(2, RetinaPayloadBuilder.buildTableUpdateDataList(TABLE_NAME, TIMESTAMP, List.of(a, b)).size());
        List<RowChangeEvent> canceled = transactionSequence("ID", "c");
        assertEquals(1, RetinaPayloadBuilder.buildTableUpdateDataList(TABLE_NAME, TIMESTAMP, canceled.subList(0, 1)).get(0).getInsertDataCount());
        assertEquals(1, RetinaPayloadBuilder.buildTableUpdateDataList(TABLE_NAME, TIMESTAMP, canceled.subList(1, 2)).get(0).getDeleteDataCount());
    }

    @Test
    void shouldFoldInterleavedKeysAndLeaveSourceRecordsUnchanged() throws Exception
    {
        List<RowChangeEvent> changes = transactionSequence("UU", "tx");
        RowChangeEvent other = withTransaction(event(SinkProto.OperationType.UPDATE, row("2", "old"), row("2", "new"), tableMetadata()), "tx", 2);
        List<RetinaProto.TableUpdateData> updates = RetinaPayloadBuilder.buildTableUpdateDataList(TABLE_NAME, TIMESTAMP, List.of(changes.get(0), other, changes.get(1)));
        assertEquals(1, updates.size());
        assertEquals(2, updates.get(0).getUpdateDataCount());
        assertEquals(bytes("2"), updates.get(0).getUpdateData(1).getIndexKeys(0).getKey());
        assertEquals(bytes("2"), updates.get(0).getUpdateData(0).getColValues(1));
        assertEquals(bytes("1"), changes.get(0).getAfterData().get(1));
    }

    @Test
    void shouldRejectUnsplitPrimaryKeyChangesAndInvalidSequences() throws Exception
    {
        RowChangeEvent changedKey = withTransaction(event(SinkProto.OperationType.UPDATE, row("1", "old"), row("2", "new"), tableMetadata()), "tx", 1);
        assertThrows(io.pixelsdb.pixels.sink.exception.SinkException.class,
                () -> TransactionBatchCoalescer.coalesce(List.of(changedKey)));
        assertThrows(io.pixelsdb.pixels.sink.exception.SinkException.class,
                () -> TransactionBatchCoalescer.coalesce(transactionSequence("II", "tx")));
    }

    static List<RowChangeEvent> transactionSequence(String sequence, String tx) throws Exception
    {
        List<RowChangeEvent> result = new java.util.ArrayList<>();
        TableMetadata metadata = tableMetadata();
        for (int i = 0; i < sequence.length(); i++)
        {
            char op = sequence.charAt(i);
            result.add(withTransaction(event(op == 'I' ? SinkProto.OperationType.INSERT : op == 'U' ? SinkProto.OperationType.UPDATE : SinkProto.OperationType.DELETE,
                    op == 'I' ? null : row("1", Integer.toString(i)),
                    op == 'D' ? null : row("1", Integer.toString(i + 1)), metadata), tx, i + 1));
        }
        return result;
    }

    @Test
    void canceledTransactionMustNotCombineSeparateRunsOfTheSameKey() throws Exception
    {
        List<RowChangeEvent> events = new java.util.ArrayList<>();
        events.addAll(transactionSequence("U", "a"));
        events.addAll(transactionSequence("ID", "b"));
        events.addAll(transactionSequence("U", "a"));
        assertEquals(2, RetinaPayloadBuilder.buildTableUpdateDataList(TABLE_NAME, TIMESTAMP, events).size());
    }

    @Test
    void shouldCoalesceSplitPrimaryKeyChangeAndKeepFinalNullValue() throws Exception
    {
        TableMetadata metadata = tableMetadata();
        RowChangeEvent deleteOld = withTransaction(event(SinkProto.OperationType.DELETE, row("1", "old"), null, metadata), "tx", 1);
        RowChangeEvent insertNew = withTransaction(event(SinkProto.OperationType.INSERT, null, row("2", "new"), metadata), "tx", 1);
        SinkProto.RowValue finalRow = SinkProto.RowValue.newBuilder()
                .addValues(SinkProto.ColumnValue.newBuilder().setValue(bytes("2")))
                .addValues(SinkProto.ColumnValue.newBuilder().setIsNull(true)).build();
        RowChangeEvent updateNew = withTransaction(event(SinkProto.OperationType.UPDATE, row("2", "new"), finalRow, metadata), "tx", 2);
        List<RetinaProto.TableUpdateData> result = RetinaPayloadBuilder.buildTableUpdateDataList(TABLE_NAME, TIMESTAMP, List.of(deleteOld, insertNew, updateNew));
        assertEquals(2, result.size());
        assertEquals(bytes("1"), result.get(0).getDeleteData(0).getIndexKeys(0).getKey());
        assertEquals(bytes("2"), result.get(1).getInsertData(0).getIndexKeys(0).getKey());
        assertEquals(List.of(false, true), result.get(1).getInsertData(0).getIsNullList());
    }

    @Test
    void concurrentBatchesMustKeepIndependentNetEffects() throws Exception
    {
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(8);
        try
        {
            List<java.util.concurrent.Future<?>> results = new java.util.ArrayList<>();
            for (int i = 0; i < 100; i++)
            {
                final int id = i;
                results.add(executor.submit(() -> {
                    List<RetinaProto.TableUpdateData> result = RetinaPayloadBuilder.buildTableUpdateDataList(
                            TABLE_NAME, TIMESTAMP + id, transactionSequence("UU", "tx" + id));
                    assertEquals(1, result.size());
                    assertEquals(1, result.get(0).getUpdateDataCount());
                    assertEquals(TIMESTAMP + id, result.get(0).getUpdateData(0).getIndexKeys(0).getTimestamp());
                    return null;
                }));
            }
            for (java.util.concurrent.Future<?> result : results) result.get();
        }
        finally { executor.shutdownNow(); }
    }

    private static RowChangeEvent withTransaction(RowChangeEvent event, String tx, long order) throws Exception
    {
        return new RowChangeEvent(event.getRowRecord().toBuilder().setTransaction(
                SinkProto.TransactionInfo.newBuilder().setId(tx).setTotalOrder(order)).build(), event.getSchema(), event.getTableMetadata());
    }
}
