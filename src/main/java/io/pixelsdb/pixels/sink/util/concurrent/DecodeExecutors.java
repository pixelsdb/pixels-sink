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
package io.pixelsdb.pixels.sink.util.concurrent;

import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import org.slf4j.Logger;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Shared fixed-size decode pools with caller-runs backpressure.
 * Tasks submitted after shutdown are rejected.
 */
public final class DecodeExecutors
{
    private DecodeExecutors()
    {
    }

    public static ExecutorService newFixedCallerRuns(int threads, String threadNamePrefix)
    {
        if (threads <= 0)
        {
            throw new IllegalArgumentException("decode threads must be positive");
        }
        if (threadNamePrefix == null || threadNamePrefix.isBlank())
        {
            throw new IllegalArgumentException("threadNamePrefix must be non-blank");
        }

        AtomicInteger threadId = new AtomicInteger();
        return new ThreadPoolExecutor(
                threads,
                threads,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(PixelsSinkConstants.MAX_QUEUE_SIZE),
                runnable -> new Thread(
                        runnable,
                        threadNamePrefix + "-" + threadId.incrementAndGet()),
                (task, executor) ->
                {
                    if (executor.isShutdown())
                    {
                        throw new RejectedExecutionException(
                                "Decode executor is shut down: " + threadNamePrefix);
                    }
                    task.run();
                });
    }

    public static void shutdownGracefully(
            ExecutorService executor, long timeoutSeconds, Logger logger)
    {
        if (executor == null)
        {
            return;
        }
        executor.shutdown();
        try
        {
            if (!executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS))
            {
                if (logger != null)
                {
                    logger.warn("Timed out waiting for decode executor to stop");
                }
                executor.shutdownNow();
            }
        } catch (InterruptedException e)
        {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
