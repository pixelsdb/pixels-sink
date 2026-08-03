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
package io.pixelsdb.pixels.sink.source;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.source.engine.SinkEngineSource;
import io.pixelsdb.pixels.sink.source.kafka.SinkKafkaSource;
import io.pixelsdb.pixels.sink.source.storage.MemorySinkStorageSource;
import io.pixelsdb.pixels.sink.source.storage.StreamingSinkStorageSource;

import java.util.Locale;

public class SinkSourceFactory
{
    public static SinkSource createSinkSource()
    {
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
        return switch (config.getDataSource())
        {
            case "kafka" -> new SinkKafkaSource();
            case "engine" -> new SinkEngineSource();
            case "storage" -> createStorageSource(config);
            default -> throw new IllegalStateException("Unsupported data source type: " + config.getDataSource());
        };
    }

    private static SinkSource createStorageSource(PixelsSinkConfig config)
    {
        String storageMode = config.getSinkStorageMode().trim().toLowerCase(Locale.ROOT);
        return switch (storageMode)
        {
            case "stream" -> new StreamingSinkStorageSource();
            case "memory" -> new MemorySinkStorageSource();
            default -> throw new IllegalStateException(
                    "Unsupported storage source mode: " + config.getSinkStorageMode());
        };
    }
}
