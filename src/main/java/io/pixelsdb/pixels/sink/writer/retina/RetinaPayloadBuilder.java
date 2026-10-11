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

import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;

import com.google.protobuf.ByteString;
import java.util.HashSet;
import java.util.Set;
import java.util.ArrayList;
import java.util.List;

/**
 * Builds Retina protobuf payloads without performing RPC.
 */
public final class RetinaPayloadBuilder
{
    private RetinaPayloadBuilder()
    {
    }

    /**
     * Folds repeated keys within each source transaction, then groups net operations.
     *
     * <p>Retina processes the operation fields inside one {@code TableUpdateData}
     * in its own fixed order. Therefore, only adjacent events with the same
     * operation and source transaction may share a payload. Within a transaction,
     * independent keys keep their first-occurrence order, with each key reduced
     * to its earliest before image and final after image.</p>
     */
    public static List<RetinaProto.TableUpdateData> buildTableUpdateDataList(
            String tableName,
            long timestamp,
            List<RowChangeEvent> events) throws SinkException
    {
        if (events == null || events.isEmpty())
        {
            throw new IllegalArgumentException("events is empty");
        }

        events = TransactionBatchCoalescer.coalesce(events);
        List<RetinaProto.TableUpdateData> updates = new ArrayList<>();
        int groupStart = 0;
        Set<ByteString> groupKeys = new HashSet<>();
        for (int i = 0; i < events.size(); i++)
        {
            RowChangeEvent event = events.get(i);
            event.initRoutingKey();
            ByteString key = event.isDelete() ? event.getBeforeRoutingKey() : event.getAfterRoutingKey();
            if (i > groupStart && (event.getOp() != events.get(groupStart).getOp()
                    || !event.getTransaction().getId().equals(events.get(groupStart).getTransaction().getId())
                    || groupKeys.contains(key)))
            {
                updates.add(buildTableUpdateData(tableName, timestamp, events.subList(groupStart, i)));
                groupStart = i;
                groupKeys.clear();
            }
            groupKeys.add(key);
        }
        if (groupStart < events.size())
        {
            updates.add(buildTableUpdateData(tableName, timestamp, events.subList(groupStart, events.size())));
        }
        return updates;
    }

    /**
     * Builds one table update from a non-empty, homogeneous operation group.
     */
    public static RetinaProto.TableUpdateData buildTableUpdateData(
            String tableName,
            long timestamp,
            List<RowChangeEvent> events) throws SinkException
    {
        if (events == null || events.isEmpty())
        {
            throw new IllegalArgumentException("events is empty");
        }

        RowChangeEvent firstEvent = events.get(0);
        for (RowChangeEvent event : events)
        {
            if (event.getOp() != firstEvent.getOp())
            {
                throw new IllegalArgumentException(
                        "TableUpdateData cannot contain different operation types");
            }
        }
        RetinaProto.TableUpdateData.Builder builder = RetinaProto.TableUpdateData.newBuilder()
                .setTimestamp(timestamp)
                .setPrimaryIndexId(firstEvent.getTableMetadata().getPrimaryIndexKeyId())
                .setTableName(tableName);
        for (RowChangeEvent event : events)
        {
            // Bind IndexKey once with the same timestamp written into TableUpdateData.
            event.setTimeStamp(timestamp);
            event.bindIndexKey();
            addRowChange(event, builder);
        }
        return builder.build();
    }

    public static RetinaProto.UpdateRecordRequest buildUpdateRecordRequest(
            String token,
            String schemaName,
            int virtualNodeId,
            List<RetinaProto.TableUpdateData> tableUpdates)
    {
        return RetinaProto.UpdateRecordRequest.newBuilder()
                .setHeader(RetinaProto.RequestHeader.newBuilder()
                        .setToken(token)
                        .build())
                .setSchemaName(schemaName)
                .setVirtualNodeId(virtualNodeId)
                .addAllTableUpdateData(tableUpdates)
                .build();
    }

    private static void addRowChange(
            RowChangeEvent event,
            RetinaProto.TableUpdateData.Builder builder) throws SinkException
    {
        switch (event.getOp())
        {
            case SNAPSHOT, INSERT ->
            {
                RetinaProto.InsertData.Builder insertData = RetinaProto.InsertData.newBuilder()
                        .addIndexKeys(event.getAfterKey())
                        .addAllColValues(event.getAfterData())
                        .addAllIsNull(event.getAfterIsNull());
                builder.addInsertData(insertData);
            }
            case UPDATE ->
            {
                RetinaProto.UpdateData.Builder updateData = RetinaProto.UpdateData.newBuilder()
                        .addIndexKeys(event.getAfterKey())
                        .addAllColValues(event.getAfterData())
                        .addAllIsNull(event.getAfterIsNull());
                builder.addUpdateData(updateData);
            }
            case DELETE ->
            {
                RetinaProto.DeleteData.Builder deleteData = RetinaProto.DeleteData.newBuilder()
                        .addIndexKeys(event.getBeforeKey());
                builder.addDeleteData(deleteData);
            }
            case UNRECOGNIZED ->
                    throw new SinkException("Unrecognized op: " + event.getOp());
        }
    }
}
