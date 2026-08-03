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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Function;

/**
 * Decodes a batch in parallel and returns results in input order.
 * Failed records become {@code null} entries.
 */
public final class OrderedBatchDecoder
{
    private static final Logger LOGGER = LoggerFactory.getLogger(OrderedBatchDecoder.class);

    private OrderedBatchDecoder()
    {
    }

    public static <T, R> List<R> decodeInOrder(
            ExecutorService executor,
            List<T> records,
            Function<T, R> decoder) throws InterruptedException
    {
        List<Future<R>> futures = new ArrayList<>(records.size());
        for (T record : records)
        {
            futures.add(executor.submit(() -> decoder.apply(record)));
        }

        List<R> decodedRecords = new ArrayList<>(records.size());
        for (Future<R> future : futures)
        {
            try
            {
                decodedRecords.add(future.get());
            } catch (ExecutionException e)
            {
                LOGGER.warn("Failed to decode record", e.getCause());
                decodedRecords.add(null);
            }
        }
        return decodedRecords;
    }
}
