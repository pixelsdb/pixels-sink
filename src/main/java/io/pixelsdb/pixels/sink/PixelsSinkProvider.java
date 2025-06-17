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
 *
 */

package io.pixelsdb.pixels.sink;

import io.pixelsdb.pixels.common.sink.SinkProvider;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.monitor.MetricsFacade;
import io.pixelsdb.pixels.sink.monitor.SinkMonitor;

public class PixelsSinkProvider implements SinkProvider {
    private SinkMonitor sinkMonitor;

    public void start(ConfigFactory config) {
        PixelsSinkConfigFactory.initialize(config);
        MetricsFacade.initialize();
        sinkMonitor = new SinkMonitor();
        sinkMonitor.startSinkMonitor();
    }

    @Override
    public void shutdown() {
        sinkMonitor.stopMonitor();
    }

    @Override
    public boolean isRunning() {
        return sinkMonitor.isRunning();
    }
}
