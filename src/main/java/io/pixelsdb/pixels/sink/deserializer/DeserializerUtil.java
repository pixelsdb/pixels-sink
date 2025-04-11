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
 *
 */

package io.pixelsdb.pixels.sink.deserializer;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.proto.RowRecordMessage;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericRecord;

import java.util.Arrays;

public class DeserializerUtil {
    static RowChangeEvent buildErrorEvent(String topic, byte[] rawData, Exception error) {
        RowRecordMessage.ErrorInfo errorInfo = RowRecordMessage.ErrorInfo.newBuilder()
                .setMessage(error.getMessage())
                .setStackTrace(Arrays.toString(error.getStackTrace()))
                .setOriginalData(ByteString.copyFrom(rawData))
                .build();

        RowRecordMessage.RowRecord record = RowRecordMessage.RowRecord.newBuilder()
                .setOp("ERROR")
                .setTsMs(System.currentTimeMillis())
                .build();

        return new RowChangeEvent(record) {
            @Override
            public boolean hasError() {
                return true;
            }

            @Override
            public RowRecordMessage.ErrorInfo getErrorInfo() {
                return errorInfo;
            }

            @Override
            public String getTopic() {
                return topic;
            }
        };
    }

    static public String getStringSafely(GenericRecord record, String field) {
        try {
            Object value = record.get(field);
            return value != null ? value.toString() : "";
        } catch (AvroRuntimeException e) {
            return "";
        }
    }

    static public Long getLongSafely(GenericRecord record, String field) {
        try {
            Object value = record.get(field);
            return value instanceof Number ? ((Number) value).longValue() : 0L;
        } catch (AvroRuntimeException e) {
            return 0L;
        }
    }
}
