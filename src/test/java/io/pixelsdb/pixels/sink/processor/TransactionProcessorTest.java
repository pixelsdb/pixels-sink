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
import io.pixelsdb.pixels.sink.util.BlockingBoundedQueue;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TransactionProcessorTest
{
    @Test
    void shouldForwardTransactionsAndStop() throws Exception
    {
        BlockingBoundedQueue<SinkProto.TransactionMetadata> queue =
                new BlockingBoundedQueue<>(2);
        RecordingWriter writer = new RecordingWriter();
        TransactionProcessor processor = new TransactionProcessor(queue, writer);
        Thread processorThread = new Thread(processor, "transaction-processor-test");
        SinkProto.TransactionMetadata first = SinkProto.TransactionMetadata.newBuilder()
                .setId("transaction-1")
                .build();
        SinkProto.TransactionMetadata second = SinkProto.TransactionMetadata.newBuilder()
                .setId("transaction-2")
                .build();

        try
        {
            processorThread.start();
            queue.put(first);
            queue.put(second);
            queue.close();
            processorThread.join(1000);

            assertTrue(writer.transactionsWritten.await(1, TimeUnit.SECONDS));
            assertEquals(List.of(first, second), writer.transactions);
            assertFalse(processorThread.isAlive());
        } finally
        {
            processor.abort();
            queue.abort();
            processorThread.interrupt();
            processorThread.join(1000);
        }
    }

    private static final class RecordingWriter implements PixelsSinkWriter
    {
        private final CountDownLatch transactionsWritten = new CountDownLatch(2);
        private final List<SinkProto.TransactionMetadata> transactions =
                new CopyOnWriteArrayList<>();

        @Override
        public void flush()
        {
        }

        @Override
        public boolean writeRow(io.pixelsdb.pixels.sink.event.RowChangeEvent rowChangeEvent)
        {
            return false;
        }

        @Override
        public boolean writeTrans(SinkProto.TransactionMetadata transactionMetadata)
        {
            transactions.add(transactionMetadata);
            transactionsWritten.countDown();
            return true;
        }

        @Override
        public void close() throws IOException
        {
        }
    }
}
