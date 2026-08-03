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
package io.pixelsdb.pixels.sink.freshness;

import java.util.ArrayDeque;
import java.util.Deque;

public class OneSecondAverage
{
    /**
     * Time window in milliseconds
     */
    private final int windowMillis;

    /**
     * Sliding window storing timestamped values
     */
    private final Deque<TimedValue> window = new ArrayDeque<>();

    /**
     * Constructor with configurable window size (milliseconds)
     */
    public OneSecondAverage(int windowMillis)
    {
        this.windowMillis = windowMillis;
    }

    /**
     * Record a new data point
     */
    public synchronized void record(double v)
    {
        long now = System.currentTimeMillis();
        window.addLast(new TimedValue(now, v));
        evictOld(now);
    }

    /**
     * Remove all values older than windowMillis
     */
    private void evictOld(long now)
    {
        while (!window.isEmpty() && now - window.peekFirst().timestamp > windowMillis)
        {
            window.removeFirst();
        }
    }

    /**
     * Compute average of values in the time window
     */
    public synchronized double getWindowAverage()
    {
        long now = System.currentTimeMillis();
        evictOld(now);

        if (window.isEmpty())
        {
            return Double.NaN;
        }

        double sum = 0;
        for (TimedValue tv : window)
        {
            sum += tv.value;
        }
        return sum / window.size();
    }

    /**
     * Timestamped data point
     */
    private record TimedValue(long timestamp, double value)
    {
    }
}
