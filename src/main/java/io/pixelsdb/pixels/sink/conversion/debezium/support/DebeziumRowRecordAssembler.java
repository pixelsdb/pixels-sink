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
package io.pixelsdb.pixels.sink.conversion.debezium.support;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.event.RowChangeEventFactory;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;

public final class DebeziumRowRecordAssembler
{
    public RowChangeEvent assemble(
            SinkProto.OperationType operation,
            SinkProto.SourceInfo source,
            SinkProto.TransactionInfo transaction,
            SinkProto.RowValue before,
            SinkProto.RowValue after,
            TypeDescription schema) throws SinkException
    {
        return assemble(operation, source, transaction, before, after, schema, null);
    }

    public RowChangeEvent assemble(
            SinkProto.OperationType operation,
            SinkProto.SourceInfo source,
            SinkProto.TransactionInfo transaction,
            SinkProto.RowValue before,
            SinkProto.RowValue after,
            TypeDescription schema,
            TableMetadata metadata) throws SinkException
    {
        SinkProto.RowRecord.Builder builder = SinkProto.RowRecord.newBuilder()
                .setOp(operation)
                .setSource(source);
        if (transaction != null)
        {
            builder.setTransaction(transaction);
        }
        if (before != null)
        {
            builder.setBefore(before);
        }
        if (after != null)
        {
            builder.setAfter(after);
        }
        return metadata == null
                ? RowChangeEventFactory.create(builder.build(), schema)
                : RowChangeEventFactory.create(builder.build(), schema, metadata);
    }
}
