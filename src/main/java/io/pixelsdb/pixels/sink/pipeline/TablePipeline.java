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

import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.processor.TableProcessor;
import io.pixelsdb.pixels.sink.util.BlockingBoundedQueue;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriterFactory;

import java.util.Objects;

public final class TablePipeline implements AutoCloseable
{
    private final BlockingBoundedQueue<RowChangeEvent> eventQueue;
    private final PixelsSinkWriter writer;
    private final TableProcessor processor;

    public TablePipeline()
    {
        this(PixelsSinkWriterFactory.getWriter());
    }

    public TablePipeline(PixelsSinkWriter writer)
    {
        this.eventQueue = new BlockingBoundedQueue<>(PixelsSinkConstants.MAX_QUEUE_SIZE);
        this.writer = Objects.requireNonNull(writer, "writer is null");
        this.processor = new TableProcessor(eventQueue, writer);
    }

    public void start()
    {
        processor.run();
    }

    public void publish(RowChangeEvent event)
    {
        eventQueue.put(event);
    }

    /**
     * Stops accepting events and waits for all pending events to be written.
     */
    @Override
    public void close()
    {
        eventQueue.close();
        try
        {
            processor.awaitTermination();
        } catch (InterruptedException e)
        {
            abort();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Discards pending events and interrupts processing. Already written events
     * are not rolled back.
     */
    public void abort()
    {
        processor.abort();
        eventQueue.abort();
    }
}
