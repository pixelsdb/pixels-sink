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

import java.util.List;

/**
 * Builds Retina protobuf payloads without performing RPC.
 */
public final class RetinaPayloadBuilder
{
    private RetinaPayloadBuilder()
    {
    }

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
