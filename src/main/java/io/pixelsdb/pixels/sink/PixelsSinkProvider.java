package io.pixelsdb.pixels.sink;

import io.pixelsdb.pixels.common.sink.SinkProvider;
import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.monitor.SinkMonitor;

public class PixelsSinkProvider implements SinkProvider {
    private SinkMonitor sinkMonitor;

    public void start(ConfigFactory config) {
        PixelsSinkConfigFactory.initialize(config);
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
