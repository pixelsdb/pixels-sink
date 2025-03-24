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
