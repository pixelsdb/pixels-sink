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
package io.pixelsdb.pixels.sink.util;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BlockingBoundedQueueTest
{
    @Test
    void shouldRejectNonPositiveCapacity()
    {
        assertThrows(IllegalArgumentException.class, () -> new BlockingBoundedQueue<>(0));
        assertThrows(IllegalArgumentException.class, () -> new BlockingBoundedQueue<>(-1));
    }

    @Test
    void shouldPutAndTakeValue()
    {
        BlockingBoundedQueue<Integer> queue = new BlockingBoundedQueue<>(1);
        queue.put(7);

        assertEquals(7, queue.take());
        queue.close();
    }

    @Test
    void shouldWakeConsumerWhenClosed()
            throws Exception
    {
        BlockingBoundedQueue<Integer> queue = new BlockingBoundedQueue<>(1);
        CountDownLatch finished = new CountDownLatch(1);
        Thread consumer = new Thread(() ->
        {
            assertNull(queue.take());
            finished.countDown();
        });
        consumer.start();

        queue.close();

        assertTrue(finished.await(1, TimeUnit.SECONDS));
        consumer.join();
    }

    @Test
    void shouldDiscardPendingValuesWhenAborted()
    {
        BlockingBoundedQueue<Integer> queue = new BlockingBoundedQueue<>(1);
        queue.put(7);
        queue.put(null);

        queue.abort();

        assertNull(queue.take());
        queue.close();
    }

    @Test
    void shouldDrainPendingValuesWhenClosed()
    {
        try (BlockingBoundedQueue<Integer> queue = new BlockingBoundedQueue<>(2))
        {
            queue.put(7);

            queue.close();

            assertEquals(7, queue.take());
            assertNull(queue.take());
        }
    }
}
