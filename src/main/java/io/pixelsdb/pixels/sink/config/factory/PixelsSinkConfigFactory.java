package io.pixelsdb.pixels.sink.config.factory;

import io.pixelsdb.pixels.common.utils.ConfigFactory;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;

import java.io.IOException;
import java.util.Properties;

public class PixelsSinkConfigFactory {
    private static volatile PixelsSinkConfig instance;
    private static String configFilePath;
    private static ConfigFactory config;
    private PixelsSinkConfigFactory() {
    }


    public static synchronized void initialize(String configFilePath) throws IOException {
        if (instance != null) {
            throw new IllegalStateException("PixelsSinkConfig is already initialized!");
        }
        instance = new PixelsSinkConfig(configFilePath);
        PixelsSinkConfigFactory.configFilePath = configFilePath;
    }

    public static synchronized void initialize(ConfigFactory config) {
        PixelsSinkConfigFactory.config = config;
        instance = new PixelsSinkConfig(config);
    }

    public static synchronized void initialize(Properties props) {
        instance = new PixelsSinkConfig(props);
    }

    public static PixelsSinkConfig getInstance() {
        if (instance == null) {
            throw new IllegalStateException("PixelsSinkConfig is not initialized! Call initialize() first.");
        }
        return instance;
    }

    public static synchronized void reset() {
        instance = null;
        configFilePath = null;
    }
}
