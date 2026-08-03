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
package io.pixelsdb.pixels.sink.conversion.sinkproto;

import com.google.protobuf.InvalidProtocolBufferException;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.event.RowChangeEventFactory;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.sink.metadata.TableMetadata;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;

import java.nio.ByteBuffer;

public final class RowRecordConverter
{
    private final TableMetadataRegistry tableMetadataRegistry;

    public RowRecordConverter(TableMetadataRegistry tableMetadataRegistry)
    {
        this.tableMetadataRegistry = tableMetadataRegistry;
    }

    public RowChangeEvent convert(SinkProto.RowRecord rowRecord) throws SinkException
    {
        SinkProto.SourceInfo source = rowRecord.getSource();
        TableMetadata metadata = tableMetadataRegistry.getMetadata(
                source.getDb(), source.getTable());
        TypeDescription schema = metadata.getTypeDescription();
        return RowChangeEventFactory.create(rowRecord, schema, metadata);
    }

    public SinkProto.RowRecord parse(ByteBuffer buffer) throws InvalidProtocolBufferException
    {
        return SinkProto.RowRecord.parseFrom(buffer.duplicate());
    }
}
