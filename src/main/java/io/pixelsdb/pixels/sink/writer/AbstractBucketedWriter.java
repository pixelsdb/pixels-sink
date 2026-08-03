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
package io.pixelsdb.pixels.sink.writer;

import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;

public abstract class AbstractBucketedWriter<C>
{
    public void writeRowChangeEvent(RowChangeEvent event, C context) throws SinkException
    {
        if (event == null)
        {
            return;
        }

        event.initIndexKey();

        switch (event.getOp())
        {
            case UPDATE ->
            {
                if (!event.isPkChanged())
                {
                    emitBefore(event, context);
                } else
                {
                    emitPkChangedUpdate(event, context);
                }
            }

            case DELETE -> emitBefore(event, context);

            case INSERT, SNAPSHOT -> emitAfter(event, context);

            case UNRECOGNIZED ->
            {
                return;
            }
        }
    }

    /* ================= hook points ================= */

    protected void emitBefore(RowChangeEvent event, C context)
    {
        int bucketId = event.getBeforeBucketFromIndex();
        emit(event, bucketId, context);
    }

    protected void emitAfter(RowChangeEvent event, C context)
    {
        int bucketId = event.getAfterBucketFromIndex();
        emit(event, bucketId, context);
    }

    protected void emitPkChangedUpdate(RowChangeEvent event, C context) throws SinkException
    {
        // DELETE (before)
        RowChangeEvent deleteEvent = buildDeleteEvent(event);
        emitBefore(deleteEvent, context);

        // INSERT (after)
        RowChangeEvent insertEvent = buildInsertEvent(event);
        emitAfter(insertEvent, context);
    }

    protected abstract void emit(RowChangeEvent event, int bucketId, C context);

    /* ================= helpers ================= */

    private RowChangeEvent buildDeleteEvent(RowChangeEvent event) throws SinkException
    {
        SinkProto.RowRecord.Builder builder =
                event.getRowRecord().toBuilder()
                        .clearAfter()
                        .setOp(SinkProto.OperationType.DELETE);

        RowChangeEvent deleteEvent =
                new RowChangeEvent(builder.build(), event.getSchema());
        deleteEvent.initIndexKey();
        return deleteEvent;
    }

    private RowChangeEvent buildInsertEvent(RowChangeEvent event) throws SinkException
    {
        SinkProto.RowRecord.Builder builder =
                event.getRowRecord().toBuilder()
                        .clearBefore()
                        .setOp(SinkProto.OperationType.INSERT);

        RowChangeEvent insertEvent =
                new RowChangeEvent(builder.build(), event.getSchema());
        insertEvent.initIndexKey();
        return insertEvent;
    }
}
