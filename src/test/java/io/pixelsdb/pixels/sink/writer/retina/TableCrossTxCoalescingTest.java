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
package io.pixelsdb.pixels.sink.writer.retina;

import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.TestConfig;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import org.junit.jupiter.api.Test;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantLock;
import static org.mockito.Mockito.*;

class TableCrossTxCoalescingTest
{
    @Test
    void canceledWritesMustCountOriginalEventsOnlyAfterSuccessfulAck() throws Exception
    {
        checkAck(false);
    }

    @Test
    void canceledWritesMustNotCountEventsAfterFailedAck() throws Exception
    {
        checkAck(true);
    }

    private void checkAck(boolean failure) throws Exception
    {
        TestConfig.initializeUnitConfig();
        Field singleton = SinkContextManager.class.getDeclaredField("instance");
        singleton.setAccessible(true);
        Object previous = singleton.get(null);
        SinkContextManager manager = mock(SinkContextManager.class);
        singleton.set(null, manager);
        try
        {
            SinkContext context = mock(SinkContext.class);
            when(manager.getSinkContext("tx")).thenReturn(context);
            when(context.getLock()).thenReturn(new ReentrantLock());
            when(context.getPixelsTransCtx()).thenReturn(mock(TransContext.class));
            when(context.getTimestamp()).thenReturn(123L);
            when(context.isCompleted()).thenReturn(true);
            setField(context, SinkContext.class, "tableCounterLock", new ReentrantLock());
            TableCrossTxWriter writer = mock(TableCrossTxWriter.class, CALLS_REAL_METHODS);
            RetinaServiceProxy proxy = mock(RetinaServiceProxy.class);
            InFlightControlManager inFlight = mock(InFlightControlManager.class);
            setField(writer, TableCrossTxWriter.class, "writeLock", new ReentrantLock());
            setField(writer, TableCrossTxWriter.class, "inFlightControlManager", inFlight);
            setField(writer, TableCrossTxWriter.class, "LOGGER", mock(org.slf4j.Logger.class));
            setField(writer, TableWriter.class, "delegate", proxy);
            setField(writer, TableWriter.class, "tableName", "records");
            setField(writer, TableWriter.class, "freshnessLevel", "none");
            setField(writer, TableWriter.class, "metricsFacade", mock(MetricsFacade.class));
            CompletableFuture<RetinaProto.UpdateRecordResponse> ack = new CompletableFuture<>();
            when(proxy.writeBatchAsync(eq("cdc_verify"), anyList())).thenReturn(ack);
            writer.flush(RetinaPayloadBuilderTest.transactionSequence("ID", "tx"));
            verify(proxy).writeBatchAsync("cdc_verify", List.of());
            verify(context, never()).updateCounter(anyString(), anyLong());
            if (failure) ack.completeExceptionally(new RuntimeException("write failed"));
            else ack.complete(RetinaProto.UpdateRecordResponse.newBuilder().build());
            verify(inFlight).release(1);
            if (failure)
            {
                verify(context).setFailed(true);
                verify(context, never()).updateCounter(anyString(), anyLong());
                verify(manager, never()).endTransaction(any());
            }
            else
            {
                verify(context).updateCounter("cdc_verify.records", 2);
                verify(manager).endTransaction(context);
            }
        }
        finally { singleton.set(null, previous); }
    }

    private static void setField(Object target, Class<?> owner, String name, Object value) throws Exception
    {
        Field field = owner.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
