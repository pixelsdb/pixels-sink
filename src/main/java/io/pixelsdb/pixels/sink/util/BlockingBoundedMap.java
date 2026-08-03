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
 */
package io.pixelsdb.pixels.sink.util;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;
import java.util.function.BiFunction;

/**
 * A thread-safe bounded map that blocks when full.
 * <p>
 * Similar to ConcurrentHashMap, but with a capacity limit.
 * When the map reaches its maximum size, any new insertion or compute
 * for a new key will block until space becomes available.
 */
public class BlockingBoundedMap<K, V>
{
    private final int maxSize;
    private final Semaphore semaphore;
    private final ConcurrentMap<K, V> map;

    public BlockingBoundedMap(int maxSize)
    {
        this.maxSize = maxSize;
        this.map = new ConcurrentHashMap<>();
        this.semaphore = new Semaphore(maxSize);
    }

    /**
     * Puts a key-value pair into the map.
     * If the map is full, this call blocks until space becomes available.
     */
    private void put(K key, V value) throws InterruptedException
    {
        semaphore.acquire(); // block if full
        V prev = map.put(key, value);
        if (prev != null)
        {
            // replaced existing value — no new space consumed
            semaphore.release();
        }
    }

    public V get(K key)
    {
        return map.get(key);
    }

    /**
     * Removes a key from the map and releases one permit if a value was present.
     */
    public V remove(K key)
    {
        V val = map.remove(key);
        if (val != null)
        {
            semaphore.release();
        }
        return val;
    }

    public int size()
    {
        return map.size();
    }

    /**
     * Atomically computes a new value for a key, blocking if capacity is full.
     * <p>
     * - If the key is new and capacity is full, this method blocks until space is freed.
     * - If the key already exists, it does not block.
     * - If the remapping function returns null, the key is removed and capacity is released.
     */
    public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction)
    {
        for (; ; )
        {
            V oldVal = map.get(key);
            if (oldVal == null)
            {
                try
                {
                    semaphore.acquire();
                } catch (InterruptedException e)
                {
                    Thread.currentThread().interrupt();
                    return null;
                }

                V newVal = remappingFunction.apply(key, null);
                if (newVal == null)
                {
                    semaphore.release();
                    return null;
                }

                V existing = map.putIfAbsent(key, newVal);
                if (existing == null)
                {
                    return newVal;
                } else
                {
                    semaphore.release();
                    continue;
                }
            } else
            {
                return map.compute(key, remappingFunction);
            }
        }
    }

    public Set<K> keySet()
    {
        return map.keySet();
    }
}
