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

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Transaction-local net effects for one detached RPC batch. No shared state or locks. */
final class TransactionBatchCoalescer
{
    private TransactionBatchCoalescer() { }

    static List<RowChangeEvent> coalesce(List<RowChangeEvent> events) throws SinkException
    {
        List<RowChangeEvent> result = new ArrayList<>();
        Map<ByteString, Change> changes = new LinkedHashMap<>();
        RowChangeEvent scope = null;
        for (RowChangeEvent event : events)
        {
            // Snapshot / unidentified transactions have no safe source transaction boundary.
            if (event.isSnapshot() || event.getTransaction().getId().isBlank())
            {
                append(changes, result);
                scope = null;
                result.add(event);
                continue;
            }
            if (scope != null && !sameScope(scope, event))
            {
                append(changes, result);
            }
            scope = event;
            event.initRoutingKey();
            if (event.isUpdate() && event.isPkChanged())
            {
                throw new SinkException("Primary-key changes must be split before Retina batching");
            }
            ByteString key = event.isDelete() ? event.getBeforeRoutingKey() : event.getAfterRoutingKey();
            if (key == null || event.getOp() == SinkProto.OperationType.UNRECOGNIZED)
            {
                throw new SinkException("Missing primary key or unsupported Retina operation");
            }
            Change change = changes.get(key);
            if (change == null)
            {
                changes.put(key, new Change(event));
            }
            else
            {
                change.accept(event);
            }
        }
        append(changes, result);
        return result;
    }

    private static boolean sameScope(RowChangeEvent a, RowChangeEvent b)
    {
        return a.getTransaction().getId().equals(b.getTransaction().getId())
                && a.getRowRecord().getSource().equals(b.getRowRecord().getSource())
                && a.getTableId() == b.getTableId()
                && a.getTableMetadata().getPrimaryIndexKeyId() == b.getTableMetadata().getPrimaryIndexKeyId();
    }

    private static void append(Map<ByteString, Change> changes, List<RowChangeEvent> result)
            throws SinkException
    {
        for (Change change : changes.values())
        {
            RowChangeEvent event = change.finish();
            if (event != null) result.add(event);
        }
        changes.clear();
    }

    private static final class Change
    {
        private final RowChangeEvent first;
        private RowChangeEvent last;
        private boolean exists;

        private Change(RowChangeEvent event)
        {
            this.first = event;
            this.last = event;
            this.exists = event.hasAfterData();
        }

        private void accept(RowChangeEvent event) throws SinkException
        {
            if (event.isInsert() == exists)
            {
                throw new SinkException("Invalid operation sequence for one primary key in a transaction");
            }
            last = event;
            exists = event.hasAfterData();
        }

        private RowChangeEvent finish() throws SinkException
        {
            if (first == last) return first;
            boolean existedBefore = first.hasBeforeData();
            if (!existedBefore && !exists) return null;
            SinkProto.RowRecord.Builder record = last.getRowRecord().toBuilder();
            if (existedBefore) record.setBefore(first.getBefore());
            else record.clearBefore();
            if (!exists) record.clearAfter();
            record.setOp(!exists ? SinkProto.OperationType.DELETE
                    : existedBefore ? SinkProto.OperationType.UPDATE : SinkProto.OperationType.INSERT);
            return new RowChangeEvent(record.build(), first.getSchema(), first.getTableMetadata());
        }
    }
}
