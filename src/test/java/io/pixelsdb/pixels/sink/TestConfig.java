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
package io.pixelsdb.pixels.sink;

import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import org.junit.jupiter.api.Assumptions;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

public final class TestConfig
{
    public static final String INTEGRATION_CONFIG_PROPERTY =
            "pixels.sink.integration.config";

    private TestConfig()
    {
    }

    public static void initializeUnitConfig() throws Exception
    {
        PixelsSinkConfigFactory.reset();
        PixelsSinkConfigFactory.initialize(resourcePath("pixels-sink-test.properties"));
    }

    public static void initializeIntegrationConfig() throws Exception
    {
        String configPath = System.getProperty(INTEGRATION_CONFIG_PROPERTY);
        Assumptions.assumeTrue(configPath != null && !configPath.isBlank(),
                "Set -D" + INTEGRATION_CONFIG_PROPERTY + " to run integration tests");
        PixelsSinkConfigFactory.reset();
        PixelsSinkConfigFactory.initialize(configPath);
    }

    public static String resourcePath(String resourceName)
    {
        try
        {
            Path path = Paths.get(Objects.requireNonNull(
                    TestConfig.class.getClassLoader().getResource(resourceName),
                    "Missing test resource: " + resourceName).toURI());
            return path.toString();
        } catch (URISyntaxException e)
        {
            throw new IllegalStateException("Invalid test resource: " + resourceName, e);
        }
    }
}
