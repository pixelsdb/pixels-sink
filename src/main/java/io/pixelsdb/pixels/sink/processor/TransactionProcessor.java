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
package io.pixelsdb.pixels.sink.processor;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.util.BlockingBoundedQueue;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class TransactionProcessor implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionProcessor.class);
    private final PixelsSinkWriter sinkWriter;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final BlockingBoundedQueue<SinkProto.TransactionMetadata> eventQueue;

    public TransactionProcessor(
            BlockingBoundedQueue<SinkProto.TransactionMetadata> eventQueue)
    {
        this(eventQueue, PixelsSinkWriterFactory.getWriter());
    }

    public TransactionProcessor(
            BlockingBoundedQueue<SinkProto.TransactionMetadata> eventQueue,
            PixelsSinkWriter sinkWriter)
    {
        this.eventQueue = eventQueue;
        this.sinkWriter = sinkWriter;
    }

    @Override
    public void run()
    {
        while (running.get())
        {
            SinkProto.TransactionMetadata transaction = eventQueue.take();
            if (transaction == null)
            {
                running.set(false);
                break;
            }
            sinkWriter.writeTrans(transaction);
        }
        LOGGER.info("Processor thread exited for transaction");
    }

    public void abort()
    {
        LOGGER.info("Aborting transaction processor");
        running.set(false);
    }
}
