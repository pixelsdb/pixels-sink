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


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class FreshnessHistory
{
    private final ConcurrentLinkedQueue<Record> history = new ConcurrentLinkedQueue<>();

    public void record(double freshnessMill)
    {
        history.offer(new Record(
                System.currentTimeMillis(),
                freshnessMill,
                null
        ));
    }

    public void record(double freshnessMill, double queryTimeMill)
    {
        history.offer(new Record(
                System.currentTimeMillis(),
                freshnessMill,
                queryTimeMill
        ));
    }

    public List<Record> pollAll()
    {
        if (history.isEmpty())
        {
            return Collections.emptyList();
        }
        List<Record> records = new ArrayList<>();
        Record record;
        while ((record = history.poll()) != null)
        {
            records.add(record);
        }
        return records;
    }

    public record Record(
            long timestamp,
            double freshness,
            Double queryTimeMillis
    )
    {
        @Override
        public String toString()
        {
            if (queryTimeMillis == null)
            {
                return timestamp + "," + freshness;
            }
            return timestamp + "," + freshness + "," + queryTimeMillis;
        }
    }

}