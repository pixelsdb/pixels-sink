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
package io.pixelsdb.pixels.sink.event;

import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;

public final class RowChangeEventFactory
{
    private RowChangeEventFactory()
    {
    }

    public static RowChangeEvent create(
            SinkProto.OperationType operation,
            SinkProto.SourceInfo source,
            SinkProto.TransactionInfo transaction,
            SinkProto.RowValue before,
            SinkProto.RowValue after,
            TypeDescription schema) throws SinkException
    {
        if (operation == null)
        {
            throw new SinkException("Row change operation is missing");
        }
        if (source == null)
        {
            throw new SinkException("Row change source is missing");
        }

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
        return create(builder.build(), schema);
    }

    public static RowChangeEvent create(
            SinkProto.RowRecord rowRecord,
            TypeDescription schema) throws SinkException
    {
        if (rowRecord == null)
        {
            throw new SinkException("Row change record is missing");
        }
        if (schema == null)
        {
            throw new SinkException("Row change schema is missing");
        }
        TableMetadataRegistry registry = TableMetadataRegistry.Instance();
        TableMetadata metadata = registry.getMetadata(
                rowRecord.getSource().getDb(), rowRecord.getSource().getTable());
        return create(rowRecord, schema, metadata);
    }

    public static RowChangeEvent create(
            SinkProto.RowRecord rowRecord,
            TypeDescription schema,
            TableMetadata metadata) throws SinkException
    {
        if (rowRecord == null)
        {
            throw new SinkException("Row change record is missing");
        }
        if (schema == null)
        {
            throw new SinkException("Row change schema is missing");
        }

        RowChangeEvent event = new RowChangeEvent(rowRecord, schema, metadata);
        event.initRoutingKey();
        return event;
    }
}
