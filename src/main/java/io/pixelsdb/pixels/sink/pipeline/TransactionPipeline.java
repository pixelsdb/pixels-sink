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
import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import io.pixelsdb.pixels.sink.processor.TransactionProcessor;
import io.pixelsdb.pixels.sink.util.BlockingBoundedQueue;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriterFactory;

import java.util.Objects;

public final class TransactionPipeline implements AutoCloseable
{
    private final BlockingBoundedQueue<SinkProto.TransactionMetadata> eventQueue;
    private final PixelsSinkWriter writer;
    private final TransactionProcessor processor;
    private final Thread processorThread;

    public TransactionPipeline()
    {
        this(PixelsSinkWriterFactory.getWriter());
    }

    public TransactionPipeline(PixelsSinkWriter writer)
    {
        this.eventQueue = new BlockingBoundedQueue<>(PixelsSinkConstants.MAX_QUEUE_SIZE);
        this.writer = Objects.requireNonNull(writer, "writer is null");
        this.processor = new TransactionProcessor(eventQueue, writer);
        this.processorThread = new Thread(processor, "transaction-processor");
    }

    public void start()
    {
        if (!processorThread.isAlive())
        {
            processorThread.start();
        }
    }

    public void publish(SinkProto.TransactionMetadata transaction)
    {
        eventQueue.put(transaction);
    }

    /**
     * Stops accepting transactions and waits for all pending transactions to
     * be written.
     */
    @Override
    public void close()
    {
        eventQueue.close();
        try
        {
            processorThread.join();
        } catch (InterruptedException e)
        {
            abort();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Discards pending transactions and interrupts processing. Already written
     * transactions are not rolled back.
     */
    public void abort()
    {
        processor.abort();
        eventQueue.abort();
        processorThread.interrupt();
    }
}
