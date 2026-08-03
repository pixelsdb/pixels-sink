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
package io.pixelsdb.pixels.sink.source.engine;

import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.source.SinkSource;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SinkEngineSource implements SinkSource
{
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 10;

    private final PixelsSinkConfig pixelsSinkConfig;
    private final PixelsDebeziumConsumer consumer;
    private DebeziumEngine<RecordChangeEvent<SourceRecord>> engine;
    private ExecutorService executor;
    private volatile boolean running;

    public SinkEngineSource()
    {
        this.pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();
        this.consumer = new PixelsDebeziumConsumer();
    }

    public void start()
    {
        consumer.start();
        Properties debeziumProps = pixelsSinkConfig.getConfig()
                .extractPropertiesByPrefix("debezium.", true);

        this.engine = DebeziumEngine.create(ChangeEventFormat.of(Connect.class))
                .using(debeziumProps)
                .notifying(consumer)
                .build();

        this.executor = Executors.newSingleThreadExecutor();
        this.executor.execute(engine);
        running = true;
    }

    @Override
    public void close()
    {
        try
        {
            if (engine != null)
            {
                engine.close();
            }
            if (executor != null)
            {
                executor.shutdown();
                if (!executor.awaitTermination(
                        SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                {
                    executor.shutdownNow();
                    consumer.abort();
                    return;
                }
            }
            consumer.close();
        } catch (InterruptedException e)
        {
            if (executor != null)
            {
                executor.shutdownNow();
            }
            consumer.abort();
            Thread.currentThread().interrupt();
        } catch (Exception e)
        {
            if (executor != null)
            {
                executor.shutdownNow();
            }
            consumer.abort();
            throw new RuntimeException("Failed to stop PixelsSinkEngine", e);
        } finally
        {
            running = false;
        }
    }

    @Override
    public void abort()
    {
        running = false;
        if (executor != null)
        {
            executor.shutdownNow();
        }
        try
        {
            if (engine != null)
            {
                engine.close();
            }
        } catch (Exception e)
        {
            throw new RuntimeException("Failed to abort PixelsSinkEngine", e);
        } finally
        {
            consumer.abort();
        }
    }

    @Override
    public boolean isRunning()
    {
        return running;
    }
}
