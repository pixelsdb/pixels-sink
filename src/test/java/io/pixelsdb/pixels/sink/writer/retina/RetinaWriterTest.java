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
package io.pixelsdb.pixels.sink.writer.retina;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.TestConfig;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
class RetinaWriterTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RetinaWriterTest.class);
    private RetinaWriter coordinator;
    private List<String> dispatchedEvents;
    private ExecutorService testExecutor;
    private CountDownLatch latch;

    @BeforeEach
    void setUp() throws Exception
    {
        TestConfig.initializeIntegrationConfig();

        testExecutor = Executors.newSingleThreadExecutor(runnable ->
        {
            Thread thread = new Thread(runnable);
            thread.setDaemon(true);
            return thread;
        });
        dispatchedEvents = Collections.synchronizedList(new ArrayList<>());
        coordinator = new RetinaWriter();

        try
        {
            Field executorField = RetinaWriter.class
                    .getDeclaredField("dispatchExecutor");
            executorField.setAccessible(true);
            executorField.set(coordinator, testExecutor);
        } catch (Exception e)
        {
            throw new RuntimeException("Failed to inject executor", e);
        }
    }

    private SinkProto.TransactionMetadata buildBeginTx(String txId)
    {
        return SinkProto.TransactionMetadata.newBuilder()
                .setId(txId)
                .setStatus(SinkProto.TransactionStatus.BEGIN)
                .build();
    }

    private SinkProto.TransactionMetadata buildEndTx(String txId)
    {
        return SinkProto.TransactionMetadata.newBuilder()
                .setId(txId)
                .setStatus(SinkProto.TransactionStatus.END)
                .build();
    }

    private RowChangeEvent buildEvent(String txId, String table, long collectionOrder, long totalOrder) throws SinkException
    {
        return new RowChangeEvent(
                SinkProto.RowRecord.newBuilder().setTransaction(
                                SinkProto.TransactionInfo.newBuilder()
                                        .setId(txId)
                                        .setTotalOrder(totalOrder)
                                        .setDataCollectionOrder(collectionOrder)
                                        .build()
                        ).setSource(
                                SinkProto.SourceInfo.newBuilder()
                                        .setTable(table)
                                        .setDb("test_db")
                                        .build()
                        ).setOp(SinkProto.OperationType.INSERT)
                        .build(), null
        );
    }

    @Test
    void shouldProcessOrderedEvents() throws Exception
    {
        coordinator.writeTrans(buildBeginTx("tx1"));

        coordinator.writeRow(buildEvent("tx1", "orders", 1, 1));
        coordinator.writeRow(buildEvent("tx1", "orders", 2, 2));
        coordinator.writeTrans(buildEndTx("tx1"));

        assertEquals(2, dispatchedEvents.size());
        assertTrue(dispatchedEvents.get(0).contains("Order: 1/1"));
        assertTrue(dispatchedEvents.get(1).contains("Order: 2/2"));
    }

    @Test
    void shouldHandleOutOfOrderEvents() throws SinkException
    {
        coordinator.writeTrans(buildBeginTx("tx2"));
        coordinator.writeRow(buildEvent("tx2", "users", 3, 3));
        coordinator.writeRow(buildEvent("tx2", "users", 2, 2));
        coordinator.writeRow(buildEvent("tx2", "users", 1, 1));
        coordinator.writeTrans(buildEndTx("tx2"));
        assertTrue(dispatchedEvents.get(0).contains("Order: 1/1"));
        assertTrue(dispatchedEvents.get(1).contains("Order: 2/2"));
        assertTrue(dispatchedEvents.get(2).contains("Order: 3/3"));
    }

    @Test
    void shouldRecoverOrphanedEvents() throws SinkException
    {
        coordinator.writeRow(buildEvent("tx3", "logs", 1, 1)); // orphan event
        coordinator.writeTrans(buildBeginTx("tx3"));     // recover
        coordinator.writeTrans(buildEndTx("tx3"));
        assertTrue(dispatchedEvents.get(0).contains("Order: 1/1"));
    }

    @ParameterizedTest
    @EnumSource(value = SinkProto.OperationType.class, names = {"INSERT", "UPDATE", "DELETE", "SNAPSHOT"})
    void shouldProcessNonTransactionalEvents(SinkProto.OperationType opType) throws InterruptedException, SinkException
    {
        RowChangeEvent event = new RowChangeEvent(
                SinkProto.RowRecord.newBuilder()
                        .setOp(opType)
                        .build(), null
        );
        coordinator.writeRow(event);
        TimeUnit.MILLISECONDS.sleep(10);
        assertEquals(1, dispatchedEvents.size());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 3, 9, 16})
    void shouldHandleConcurrentEvents(int threadCount) throws SinkException, IOException, InterruptedException
    {
        latch = new CountDownLatch(threadCount);
        coordinator.writeTrans(buildBeginTx("tx5"));
        // concurrently send event
        for (int i = 0; i < threadCount; i++)
        {
            int order = i + 1;
            new Thread(() ->
            {
                try
                {
                    coordinator.writeRow(buildEvent("tx5", "concurrent", order, order));
                } catch (SinkException e)
                {
                    throw new RuntimeException(e);
                }
                latch.countDown();
            }).start();
        }

        assertTrue(latch.await(1, TimeUnit.SECONDS));

        coordinator.writeTrans(buildEndTx("tx5"));

        LOGGER.debug("Thread Count: {} DispatchedEvents size: {}", threadCount, dispatchedEvents.size());
        LOGGER.debug("Thread Count: {} DispatchedEvents size: {}", threadCount, dispatchedEvents.size());
        LOGGER.debug("Thread Count: {} DispatchedEvents size: {}", threadCount, dispatchedEvents.size());
        assertEquals(threadCount, dispatchedEvents.size());
    }
}