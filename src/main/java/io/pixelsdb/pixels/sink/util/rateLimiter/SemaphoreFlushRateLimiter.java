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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class SemaphoreFlushRateLimiter implements FlushRateLimiter
{
    private static final Logger LOGGER = LoggerFactory.getLogger(SemaphoreFlushRateLimiter.class);
    private static final long REFRESH_PERIOD_MS = 10;

    private final Semaphore semaphore;
    private final ScheduledExecutorService scheduler;
    private final int replenishmentAmount;
    private final int rateLimit;

    public SemaphoreFlushRateLimiter(PixelsSinkConfig config)
    {
        int sourceRateLimit = config.getSourceRateLimit();
        this.rateLimit = sourceRateLimit;

        double replenishmentPerMillisecond = (double) sourceRateLimit / 1000.0;
        this.replenishmentAmount = (int) Math.max(1, Math.round(replenishmentPerMillisecond * REFRESH_PERIOD_MS));
        this.semaphore = new Semaphore(this.replenishmentAmount);

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r ->
        {
            Thread t = new Thread(r, "Rate-Limiter-Replenish-Semaphore");
            t.setDaemon(true);
            return t;
        });

        this.scheduler.scheduleAtFixedRate(this::replenishTokens, REFRESH_PERIOD_MS, REFRESH_PERIOD_MS, TimeUnit.MILLISECONDS);
        LOGGER.info("SemaphoreRateLimiter initialized. Rate: {}/s", sourceRateLimit);
    }

    private void replenishTokens()
    {
        if (semaphore.availablePermits() < rateLimit)
        {
            semaphore.release(replenishmentAmount);
        }
    }

    @Override
    public void acquire(int num)
    {
        try
        {
            semaphore.acquire(num);
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void shutdown()
    {
        if (scheduler != null) scheduler.shutdownNow();
    }
}