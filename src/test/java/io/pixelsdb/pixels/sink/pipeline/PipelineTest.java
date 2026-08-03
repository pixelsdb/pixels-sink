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
package io.pixelsdb.pixels.sink.pipeline;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.TestConfig;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import io.pixelsdb.pixels.core.TypeDescription;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PipelineTest
{
    @Test
    void shouldStartAndCloseTablePipeline() throws Exception
    {
        TestConfig.initializeUnitConfig();

        assertDoesNotThrow(() -> {
            try (TablePipeline pipeline = new TablePipeline())
            {
                pipeline.start();
            }
        });
    }

    @Test
    void shouldStartTransactionPipelineOnlyOnce() throws Exception
    {
        TestConfig.initializeUnitConfig();

        assertDoesNotThrow(() -> {
            try (TransactionPipeline pipeline = new TransactionPipeline())
            {
                pipeline.start();
                pipeline.start();
                pipeline.publish(SinkProto.TransactionMetadata.newBuilder()
                        .setId("transaction-1")
                        .setStatus(SinkProto.TransactionStatus.END)
                        .build());
            }
        });
    }

    @Test
    void shouldAbortPipelines() throws Exception
    {
        TestConfig.initializeUnitConfig();

        assertDoesNotThrow(() -> {
            try (TablePipeline tablePipeline = new TablePipeline();
                 TransactionPipeline transactionPipeline = new TransactionPipeline())
            {
                tablePipeline.start();
                transactionPipeline.start();
                tablePipeline.abort();
                transactionPipeline.abort();
            }
        });
    }

    @Test
    void shouldUseInjectedWriterForBothPipelines() throws Exception
    {
        TestConfig.initializeUnitConfig();
        RecordingWriter writer = new RecordingWriter();
        RowChangeEvent row = rowEvent();
        SinkProto.TransactionMetadata transaction = SinkProto.TransactionMetadata.newBuilder()
                .setId("transaction-injected")
                .build();

        try (TablePipeline tablePipeline = new TablePipeline(writer);
             TransactionPipeline transactionPipeline = new TransactionPipeline(writer))
        {
            tablePipeline.start();
            transactionPipeline.start();
            tablePipeline.publish(row);
            transactionPipeline.publish(transaction);
        }

        assertEquals(List.of(row), writer.rows);
        assertEquals(List.of(transaction), writer.transactions);
    }

    private static RowChangeEvent rowEvent() throws Exception
    {
        SinkProto.RowRecord record = SinkProto.RowRecord.newBuilder()
                .setOp(SinkProto.OperationType.INSERT)
                .setSource(SinkProto.SourceInfo.newBuilder()
                        .setDb("test_db")
                        .setTable("test_table"))
                .setAfter(SinkProto.RowValue.newBuilder()
                        .addValues(SinkProto.ColumnValue.newBuilder()))
                .build();
        TypeDescription schema = TypeDescription.createSchemaFromStrings(
                List.of("id"), List.of("int"));
        return new RowChangeEvent(record, schema, null);
    }

    private static final class RecordingWriter implements PixelsSinkWriter
    {
        private final List<RowChangeEvent> rows = new CopyOnWriteArrayList<>();
        private final List<SinkProto.TransactionMetadata> transactions =
                new CopyOnWriteArrayList<>();

        @Override
        public void flush()
        {
        }

        @Override
        public boolean writeRow(RowChangeEvent row)
        {
            rows.add(row);
            return true;
        }

        @Override
        public boolean writeTrans(SinkProto.TransactionMetadata transaction)
        {
            transactions.add(transaction);
            return true;
        }

        @Override
        public void close() throws IOException
        {
        }
    }

}
