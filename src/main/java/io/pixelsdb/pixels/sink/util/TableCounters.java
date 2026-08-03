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

/**
 * Inner class to hold and manage per-table transaction row counts.
 */
public class TableCounters
{
    private final int totalCount;           // The expected total number of rows
    // currentCount is volatile for visibility across threads, as it's incremented during writeRow.
    private volatile int currentCount = 0;

    public TableCounters(int totalCount)
    {
        this.totalCount = totalCount;
    }

    public void increment()
    {
        currentCount++;
    }

    public boolean isComplete()
    {
        // Checks if the processed count meets or exceeds the expected total count.
        return currentCount >= totalCount;
    }

    public int getCurrentCount()
    {
        return currentCount;
    }

    public int getTotalCount()
    {
        return totalCount;
    }
}
