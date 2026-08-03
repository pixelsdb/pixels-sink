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

        RetinaProto.TableUpdateData tableUpdate =
                RetinaPayloadBuilder.buildTableUpdateData(
                        TABLE_NAME, TIMESTAMP, List.of(insert, snapshot, update, delete));
        RetinaProto.UpdateRecordRequest request =
                RetinaPayloadBuilder.buildUpdateRecordRequest(
                        "fixed-token", SCHEMA_NAME, 23, List.of(tableUpdate));

        assertEquals("fixed-token", request.getHeader().getToken());
        assertEquals(SCHEMA_NAME, request.getSchemaName());
        assertEquals(23, request.getVirtualNodeId());
        assertEquals(List.of(tableUpdate), request.getTableUpdateDataList());
        assertEquals(TABLE_NAME, tableUpdate.getTableName());
        assertEquals(INDEX_ID, tableUpdate.getPrimaryIndexId());
        assertEquals(TIMESTAMP, tableUpdate.getTimestamp());

        assertEquals(2, tableUpdate.getInsertDataCount());
        assertRow(tableUpdate.getInsertData(0).getIndexKeys(0),
                tableUpdate.getInsertData(0).getColValuesList(), "1", "insert");
        assertRow(tableUpdate.getInsertData(1).getIndexKeys(0),
                tableUpdate.getInsertData(1).getColValuesList(), "2", "snapshot");

        assertEquals(1, tableUpdate.getUpdateDataCount());
        assertRow(tableUpdate.getUpdateData(0).getIndexKeys(0),
                tableUpdate.getUpdateData(0).getColValuesList(), "3", "after");

        assertEquals(1, tableUpdate.getDeleteDataCount());
        assertEquals(bytes("4"), tableUpdate.getDeleteData(0).getIndexKeys(0).getKey());
        assertEquals(INDEX_ID, tableUpdate.getDeleteData(0).getIndexKeys(0).getIndexId());
        assertEquals(TABLE_ID, tableUpdate.getDeleteData(0).getIndexKeys(0).getTableId());
        assertEquals(TIMESTAMP, tableUpdate.getDeleteData(0).getIndexKeys(0).getTimestamp());

        assertEquals(request, RetinaProto.UpdateRecordRequest.parseFrom(request.toByteArray()));
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
        event.setTimeStamp(TIMESTAMP);
        event.initIndexKey();
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
}
