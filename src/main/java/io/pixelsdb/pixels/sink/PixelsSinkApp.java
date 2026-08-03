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

import io.pixelsdb.pixels.sink.config.CommandLineConfig;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.freshness.FreshnessClient;
import io.pixelsdb.pixels.sink.source.SinkSource;
import io.pixelsdb.pixels.sink.source.SinkSourceFactory;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriterFactory;
import io.pixelsdb.pixels.sink.writer.retina.SinkContextManager;
import io.pixelsdb.pixels.sink.writer.retina.TransactionProxy;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Run PixelsSink as a server
 */
public class PixelsSinkApp
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PixelsSinkApp.class);
    private static SinkSource sinkSource;
    private static HTTPServer prometheusHttpServer;
    private static FreshnessClient freshnessClient;

    public static void main(String[] args) throws IOException
    {
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
        {
            PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
            sinkSource.close();
            TransactionProxy.staticClose();
            LOGGER.info("Pixels Sink Server shutdown complete");
            if (config.getSinkMonitorFreshnessLevel().equals("embed") && freshnessClient != null)
            {
                freshnessClient.stop();
            }
            if (prometheusHttpServer != null)
            {
                prometheusHttpServer.close();
            }
            MetricsFacade.getInstance().stop();
            PixelsSinkWriter pixelsSinkWriter = PixelsSinkWriterFactory.getWriter();
            if (pixelsSinkWriter != null)
            {
                try
                {
                    pixelsSinkWriter.close();
                } catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }

        }));

        init(args);
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
        sinkSource = SinkSourceFactory.createSinkSource();

        try
        {
            if (config.isMonitorEnabled())
            {
                DefaultExports.initialize();
                prometheusHttpServer = new HTTPServer(config.getMonitorPort());
            }
        } catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        if (config.getSinkMonitorFreshnessLevel().equals("embed"))
        {
            freshnessClient = FreshnessClient.getInstance();
            freshnessClient.start();
        }
        sinkSource.start();
    }

    private static void init(String[] args) throws IOException
    {
        CommandLineConfig cmdLineConfig = new CommandLineConfig(args);
        PixelsSinkConfigFactory.initialize(cmdLineConfig.getConfigPath());
        MetricsFacade metricsFacade = MetricsFacade.getInstance();
        metricsFacade.setSinkContextManager(SinkContextManager.getInstance());
    }
}
