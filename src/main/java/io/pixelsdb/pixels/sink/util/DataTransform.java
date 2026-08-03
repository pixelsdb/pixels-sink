/*
 * Copyright 2025 PixelsDB.
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
package io.pixelsdb.pixels.sink.util;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.sink.SinkProto;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DataTransform
{
    private static ByteString longToByteString(long value)
    {
        byte[] bytes = ByteBuffer.allocate(Long.BYTES).putLong(value).array();
        return ByteString.copyFrom(bytes);
    }

    public static List<SinkProto.RowRecord> updateRecordTimestamp(List<SinkProto.RowRecord> records, long timestamp)
    {
        if (records == null || records.isEmpty())
        {
            return records;
        }
        SinkProto.ColumnValue timestampColumn = getTimestampColumn(timestamp);
        List<SinkProto.RowRecord> updatedRecords = new ArrayList<>(records.size());
        for (SinkProto.RowRecord record : records)
        {

            updatedRecords.add(updateRecordTimestamp(record, timestampColumn));
        }
        return updatedRecords;
    }

    private static SinkProto.ColumnValue getTimestampColumn(long timestamp)
    {
        ByteString timestampBytes = longToByteString(timestamp);
        return SinkProto.ColumnValue.newBuilder().setValue(timestampBytes).build();
    }

    public static SinkProto.RowRecord updateRecordTimestamp(SinkProto.RowRecord record, long timestamp)
    {
        if (record == null)
        {
            return null;
        }
        SinkProto.ColumnValue timestampColumn = getTimestampColumn(timestamp);
        return updateRecordTimestamp(record, timestampColumn);
    }

    public static void updateRecordTimestamp(SinkProto.RowRecord.Builder recordBuilder, long timestamp)
    {
        switch (recordBuilder.getOp())
        {
            case INSERT:
            case UPDATE:
            case SNAPSHOT:
                if (recordBuilder.hasAfter())
                {
                    SinkProto.RowValue.Builder afterBuilder = recordBuilder.getAfterBuilder();
                    int colCount = afterBuilder.getValuesCount();
                    if (colCount > 0)
                    {
                        afterBuilder.setValues(colCount - 1, getTimestampColumn(timestamp));
                    }
                }
                break;
            case DELETE:
            default:
                break;
        }
    }

    private static SinkProto.RowRecord updateRecordTimestamp(SinkProto.RowRecord.Builder recordBuilder, SinkProto.ColumnValue timestampColumn)
    {
        switch (recordBuilder.getOp())
        {
            case INSERT:
            case UPDATE:
            case SNAPSHOT:
                if (recordBuilder.hasAfter())
                {
                    SinkProto.RowValue.Builder afterBuilder = recordBuilder.getAfterBuilder();
                    int colCount = afterBuilder.getValuesCount();
                    if (colCount > 0)
                    {
                        afterBuilder.setValues(colCount - 1, timestampColumn);
                    }
                }
                break;
            case DELETE:
            default:
                break;
        }
        return recordBuilder.build();
    }

    private static SinkProto.RowRecord updateRecordTimestamp(SinkProto.RowRecord record, SinkProto.ColumnValue timestampColumn)
    {
        SinkProto.RowRecord.Builder recordBuilder = record.toBuilder();
        return updateRecordTimestamp(recordBuilder, timestampColumn);
    }

    public static String extractTableName(String topic)
    {
        String[] parts = topic.split("\\.");
        return parts[parts.length - 1];
    }
}
