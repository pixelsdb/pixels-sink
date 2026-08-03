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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * Parallel decode with per-stream publish ordering.
 * Same {@code streamKey} is published FIFO; different keys may publish in parallel.
 */
public final class StreamOrderedDecoder implements AutoCloseable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamOrderedDecoder.class);
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 30;

    @FunctionalInterface
    public interface ThrowingFunction<I, O>
    {
        O apply(I input) throws Exception;
    }

    public record DecodeResult<O>(O value, Throwable failure)
    {
    }

    private final Object lifecycleLock = new Object();
    private final Map<Object, CompletableFuture<Void>> streamTails = new HashMap<>();
    private final Set<CompletableFuture<Void>> activeCompletions = ConcurrentHashMap.newKeySet();
    private final ExecutorService decodeExecutor;
    private final boolean ownsExecutor;
    private boolean started;
    private boolean accepting;
    private boolean closed;
    private volatile boolean aborted;

    public StreamOrderedDecoder(int decodeThreads, String threadNamePrefix)
    {
        this(DecodeExecutors.newFixedCallerRuns(decodeThreads, threadNamePrefix), true);
    }

    private StreamOrderedDecoder(ExecutorService decodeExecutor, boolean ownsExecutor)
    {
        if (decodeExecutor == null)
        {
            throw new IllegalArgumentException("decodeExecutor must not be null");
        }
        this.decodeExecutor = decodeExecutor;
        this.ownsExecutor = ownsExecutor;
    }

    public void start()
    {
        synchronized (lifecycleLock)
        {
            if (started)
            {
                return;
            }
            if (closed)
            {
                throw new IllegalStateException("StreamOrderedDecoder is closed");
            }
            started = true;
            accepting = true;
        }
    }

    public <I, O> CompletableFuture<Void> submit(
            Object streamKey,
            I input,
            ThrowingFunction<I, O> decoder,
            Consumer<DecodeResult<O>> afterDecode)
    {
        CompletableFuture<Void> previous;
        CompletableFuture<Void> completion = new CompletableFuture<>();
        synchronized (lifecycleLock)
        {
            ensureAccepting();
            previous = streamTails.getOrDefault(
                    streamKey, CompletableFuture.completedFuture(null));
            streamTails.put(streamKey, completion);
            activeCompletions.add(completion);
        }

        CompletableFuture<DecodeResult<O>> decoded = CompletableFuture.supplyAsync(
                () -> decode(input, decoder), decodeExecutor);

        previous.thenCombine(decoded, (ignored, result) ->
                {
                    publish(result, afterDecode);
                    return null;
                })
                .whenComplete((ignored, failure) ->
                {
                    if (failure != null)
                    {
                        completion.completeExceptionally(failure);
                    }
                    else
                    {
                        completion.complete(null);
                    }
                    removeCompletion(streamKey, completion);
                });
        return completion;
    }

    private void ensureAccepting()
    {
        if (!started || !accepting)
        {
            throw new IllegalStateException(
                    "StreamOrderedDecoder is not accepting records");
        }
    }

    private <I, O> DecodeResult<O> decode(I input, ThrowingFunction<I, O> decoder)
    {
        try
        {
            return new DecodeResult<>(decoder.apply(input), null);
        } catch (Exception e)
        {
            return new DecodeResult<>(null, e);
        }
    }

    private <O> void publish(DecodeResult<O> result, Consumer<DecodeResult<O>> afterDecode)
    {
        if (aborted)
        {
            throw new CancellationException("StreamOrderedDecoder aborted");
        }
        afterDecode.accept(result);
    }

    private void removeCompletion(Object streamKey, CompletableFuture<Void> completion)
    {
        activeCompletions.remove(completion);
        synchronized (lifecycleLock)
        {
            // Keep a failed tail so later submits on the same stream fail fast
            // instead of starting a fresh successful chain.
            if (streamTails.get(streamKey) == completion
                    && !completion.isCompletedExceptionally())
            {
                streamTails.remove(streamKey);
            }
        }
    }

    @Override
    public void close()
    {
        List<CompletableFuture<Void>> pending;
        synchronized (lifecycleLock)
        {
            if (closed)
            {
                return;
            }
            closed = true;
            accepting = false;
            pending = new ArrayList<>(activeCompletions);
        }

        try
        {
            awaitCompletions(pending);
            if (ownsExecutor)
            {
                DecodeExecutors.shutdownGracefully(
                        decodeExecutor, SHUTDOWN_TIMEOUT_SECONDS, LOGGER);
            }
        } catch (InterruptedException e)
        {
            abort();
            Thread.currentThread().interrupt();
        }
    }

    public void abort()
    {
        List<CompletableFuture<Void>> pending;
        synchronized (lifecycleLock)
        {
            if (aborted)
            {
                return;
            }
            aborted = true;
            closed = true;
            accepting = false;
            pending = new ArrayList<>(activeCompletions);
        }
        CancellationException cancellation =
                new CancellationException("StreamOrderedDecoder aborted");
        pending.forEach(completion -> completion.completeExceptionally(cancellation));
        if (ownsExecutor)
        {
            decodeExecutor.shutdownNow();
        }
    }

    private void awaitCompletions(List<CompletableFuture<Void>> pending)
            throws InterruptedException
    {
        for (CompletableFuture<Void> completion : pending)
        {
            try
            {
                completion.get();
            } catch (ExecutionException | CancellationException e)
            {
                LOGGER.warn("StreamOrderedDecoder completed with an error", e);
            }
        }
    }
}
