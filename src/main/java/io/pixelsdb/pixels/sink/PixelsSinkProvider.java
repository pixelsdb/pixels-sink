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
package io.pixelsdb.pixels.sink;

import io.pixelsdb.pixels.common.sink.SinkProvider;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.source.SinkSource;
import io.pixelsdb.pixels.sink.source.SinkSourceFactory;
import io.pixelsdb.pixels.sink.util.MetricsFacade;

public class PixelsSinkProvider implements SinkProvider
{
    private SinkSource sinkSource;

    public void start(ConfigFactory config)
    {
        PixelsSinkConfigFactory.initialize(config);
        MetricsFacade.getInstance();
        sinkSource = SinkSourceFactory.createSinkSource();
        sinkSource.start();
    }

    @Override
    public void shutdown()
    {
        sinkSource.close();
    }

    @Override
    public boolean isRunning()
    {
        return sinkSource.isRunning();
    }
}
