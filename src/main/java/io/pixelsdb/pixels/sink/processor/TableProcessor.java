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


import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.util.BlockingBoundedQueue;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @package: io.pixelsdb.pixels.sink.processor
 * @className: TableProcessor
 * @author: AntiO2
 * @date: 2025/9/26 11:01
 */
public class TableProcessor implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(TableProcessor.class);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final PixelsSinkWriter pixelsSinkWriter;
    private final BlockingBoundedQueue<RowChangeEvent> eventQueue;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private Thread processorThread;
    private boolean tableAdded = false;

    public TableProcessor(BlockingBoundedQueue<RowChangeEvent> eventQueue)
    {
        this(eventQueue, PixelsSinkWriterFactory.getWriter());
    }

    public TableProcessor(
            BlockingBoundedQueue<RowChangeEvent> eventQueue,
            PixelsSinkWriter pixelsSinkWriter)
    {
        this.pixelsSinkWriter = pixelsSinkWriter;
        this.eventQueue = eventQueue;
    }

    @Override
    public void run()
    {
        processorThread = new Thread(this::processLoop);
        processorThread.start();
    }

    private void processLoop()
    {
        while (running.get())
        {
            RowChangeEvent event = eventQueue.take();
            if (event == null)
            {
                break;
            }
            pixelsSinkWriter.writeRow(event);
        }
        running.set(false);
        LOGGER.info("Processor thread exited");
    }

    public void awaitTermination() throws InterruptedException
    {
        if (processorThread != null)
        {
            processorThread.join();
        }
    }

    public void abort()
    {
        LOGGER.info("Aborting table processor");
        running.set(false);
        if (processorThread != null)
        {
            processorThread.interrupt();
        }
    }
}
