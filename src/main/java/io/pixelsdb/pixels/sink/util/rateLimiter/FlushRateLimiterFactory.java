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
package io.pixelsdb.pixels.sink.util.rateLimiter;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;

public class FlushRateLimiterFactory
{
    private static volatile FlushRateLimiter instance;

    public static FlushRateLimiter getInstance()
    {
        if (instance == null)
        {
            synchronized (FlushRateLimiterFactory.class)
            {
                if (instance == null)
                {
                    instance = createLimiter();
                }
            }
        }
        return instance;
    }

    public static FlushRateLimiter getNewInstance()
    {
        return createLimiter();
    }

    private static FlushRateLimiter createLimiter()
    {
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();

        if (!config.isEnableSourceRateLimit() || config.getSourceRateLimit() <= 0)
        {
            return new NoOpFlushRateLimiter();
        }

        String type = config.getRateLimiterType().toLowerCase();

        switch (type)
        {
            case "guava":
                return new GuavaFlushRateLimiter(config);
            case "semaphore":
            default:
                return new SemaphoreFlushRateLimiter(config);
        }
    }
}