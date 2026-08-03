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
package io.pixelsdb.pixels.sink.processor;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.TestConfig;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.util.BlockingBoundedQueue;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableProcessorTest
{
    @Test
    void shouldForwardRowsToWriter() throws Exception
    {
        TestConfig.initializeUnitConfig();
        BlockingBoundedQueue<RowChangeEvent> queue = new BlockingBoundedQueue<>(2);
        RecordingWriter writer = new RecordingWriter();
        TableProcessor processor = new TableProcessor(queue, writer);
        RowChangeEvent first = rowEvent();
        RowChangeEvent second = rowEvent();

        try
        {
            processor.run();
            queue.put(first);
            queue.put(second);
            queue.close();
            processor.awaitTermination();

            assertTrue(writer.rowsWritten.await(1, TimeUnit.SECONDS));
            assertEquals(List.of(first, second), writer.rows);
        } finally
        {
            processor.abort();
            queue.abort();
            processor.awaitTermination();
        }
    }

    private static RowChangeEvent rowEvent() throws Exception
    {
        SinkProto.RowRecord record = SinkProto.RowRecord.newBuilder()
                .setOp(SinkProto.OperationType.INSERT)
                .setSource(SinkProto.SourceInfo.newBuilder()
                        .setDb("test_db")
                        .setTable("test_table"))
                .setAfter(SinkProto.RowValue.newBuilder()
                        .addValues(SinkProto.ColumnValue.newBuilder()
                                .setValue(com.google.protobuf.ByteString.EMPTY)))
                .build();
        TypeDescription schema = TypeDescription.createSchemaFromStrings(
                List.of("id"), List.of("int"));
        return new RowChangeEvent(record, schema, null);
    }

    private static final class RecordingWriter implements PixelsSinkWriter
    {
        private final CountDownLatch rowsWritten = new CountDownLatch(2);
        private final List<RowChangeEvent> rows = new CopyOnWriteArrayList<>();

        @Override
        public void flush()
        {
        }

        @Override
        public boolean writeRow(RowChangeEvent rowChangeEvent)
        {
            rows.add(rowChangeEvent);
            rowsWritten.countDown();
            return true;
        }

        @Override
        public boolean writeTrans(SinkProto.TransactionMetadata transactionMetadata)
        {
            return false;
        }

        @Override
        public void close() throws IOException
        {
        }
    }
}
