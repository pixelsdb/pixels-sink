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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamOrderedDecoderTest
{
    @Test
    void shouldPublishSameStreamInSubmitOrder() throws Exception
    {
        try (StreamOrderedDecoder decoder =
                     new StreamOrderedDecoder(4, "stream-order-test"))
        {
            decoder.start();
            List<Integer> published = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch slowStarted = new CountDownLatch(1);
            CountDownLatch releaseSlow = new CountDownLatch(1);

            CompletableFuture<Void> first = decoder.submit(
                    "s1",
                    1,
                    value ->
                    {
                        slowStarted.countDown();
                        assertTrue(releaseSlow.await(5, TimeUnit.SECONDS));
                        return value;
                    },
                    result -> published.add(result.value()));
            assertTrue(slowStarted.await(5, TimeUnit.SECONDS));

            CompletableFuture<Void> second = decoder.submit(
                    "s1",
                    2,
                    value -> value,
                    result -> published.add(result.value()));

            releaseSlow.countDown();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            assertEquals(List.of(1, 2), published);
        }
    }

    @Test
    void shouldAllowDifferentStreamsToPublishIndependently() throws Exception
    {
        try (StreamOrderedDecoder decoder =
                     new StreamOrderedDecoder(4, "stream-parallel-test"))
        {
            decoder.start();
            List<String> published = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch aStarted = new CountDownLatch(1);
            CountDownLatch releaseA = new CountDownLatch(1);

            CompletableFuture<Void> a = decoder.submit(
                    "a",
                    "a1",
                    value ->
                    {
                        aStarted.countDown();
                        assertTrue(releaseA.await(5, TimeUnit.SECONDS));
                        return value;
                    },
                    result -> published.add(result.value()));
            assertTrue(aStarted.await(5, TimeUnit.SECONDS));

            CompletableFuture<Void> b = decoder.submit(
                    "b",
                    "b1",
                    value -> value,
                    result -> published.add(result.value()));
            b.get(5, TimeUnit.SECONDS);
            assertEquals(List.of("b1"), published);

            releaseA.countDown();
            a.get(5, TimeUnit.SECONDS);
            assertEquals(List.of("b1", "a1"), published);
        }
    }

    @Test
    void shouldFailFastOnSameStreamWhenPreviousPublishFails() throws Exception
    {
        try (StreamOrderedDecoder decoder =
                     new StreamOrderedDecoder(4, "stream-fail-fast-test"))
        {
            decoder.start();
            List<Integer> published = Collections.synchronizedList(new ArrayList<>());

            CompletableFuture<Void> first = decoder.submit(
                    "s1",
                    1,
                    value -> value,
                    result ->
                    {
                        throw new IllegalStateException("publish failed");
                    });
            ExecutionException firstError = assertThrows(
                    ExecutionException.class, () -> first.get(5, TimeUnit.SECONDS));
            assertInstanceOf(IllegalStateException.class, firstError.getCause());

            // Submit after the stream has already failed; the failed tail must remain.
            CompletableFuture<Void> second = decoder.submit(
                    "s1",
                    2,
                    value -> value,
                    result -> published.add(result.value()));
            ExecutionException secondError = assertThrows(
                    ExecutionException.class, () -> second.get(5, TimeUnit.SECONDS));
            assertInstanceOf(IllegalStateException.class, secondError.getCause());
            assertTrue(published.isEmpty());
        }
    }
}
