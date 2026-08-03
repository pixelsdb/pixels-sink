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

import java.io.Closeable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A bounded blocking queue with an explicit shutdown signal.
 *
 * <p>Closing the queue finishes it gracefully. Use {@link #abort()} to
 * discard pending values and wake a blocked consumer immediately.</p>
 */
public final class BlockingBoundedQueue<T> implements Closeable
{
    private static final Object POISON_PILL = new Object();

    private final BlockingQueue<Object> queue;
    private volatile boolean closed;

    public BlockingBoundedQueue(int capacity)
    {
        if (capacity <= 0)
        {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.queue = new LinkedBlockingQueue<>(capacity);
    }

    public void put(T value)
    {
        if (value == null || closed)
        {
            return;
        }
        try
        {
            queue.put(value);
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    @SuppressWarnings("unchecked")
    public T take()
    {
        try
        {
            Object value = queue.take();
            if (value == POISON_PILL)
            {
                return null;
            }
            return (T) value;
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
            return null;
        }
    }

    /**
     * Stops accepting new values and wakes the consumer after all queued values
     * have been consumed.
     *
     * <p>The caller must stop all producers before invoking this method.</p>
     */
    @Override
    public void close()
    {
        if (closed)
        {
            return;
        }
        closed = true;
        boolean interrupted = false;
        while (true)
        {
            try
            {
                queue.put(POISON_PILL);
                break;
            } catch (InterruptedException e)
            {
                interrupted = true;
            }
        }
        if (interrupted)
        {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Stops accepting new values, discards pending values, and wakes a blocked
     * consumer. Values already consumed are not rolled back.
     */
    public void abort()
    {
        closed = true;
        queue.clear();
        queue.offer(POISON_PILL);
    }
}
