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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class OrderedBatchDecoderTest
{
    private ExecutorService executor;

    @AfterEach
    void tearDown()
    {
        if (executor != null)
        {
            DecodeExecutors.shutdownGracefully(executor, 5, null);
        }
    }

    @Test
    void shouldPreserveInputOrder() throws Exception
    {
        executor = DecodeExecutors.newFixedCallerRuns(4, "ordered-batch-test");
        List<Integer> input = List.of(3, 1, 2);
        List<Integer> decoded = OrderedBatchDecoder.decodeInOrder(
                executor, input, value ->
                {
                    try
                    {
                        Thread.sleep(5L * value);
                    } catch (InterruptedException e)
                    {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                    }
                    return value * 10;
                });
        assertEquals(List.of(30, 10, 20), decoded);
    }

    @Test
    void shouldReplaceFailedRecordsWithNull() throws Exception
    {
        executor = DecodeExecutors.newFixedCallerRuns(2, "ordered-batch-fail");
        AtomicInteger calls = new AtomicInteger();
        List<Integer> decoded = OrderedBatchDecoder.decodeInOrder(
                executor,
                List.of(1, 2, 3),
                value ->
                {
                    calls.incrementAndGet();
                    if (value == 2)
                    {
                        throw new IllegalStateException("boom");
                    }
                    return value;
                });
        assertEquals(3, calls.get());
        assertEquals(1, decoded.get(0));
        assertNull(decoded.get(1));
        assertEquals(3, decoded.get(2));
    }
}
