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
import io.pixelsdb.pixels.sink.SinkProto;

import java.nio.ByteBuffer;

public final class TransactionMetadataConverter
{
    public SinkProto.TransactionMetadata convert(ByteBuffer buffer, int loopId)
            throws InvalidProtocolBufferException
    {
        SinkProto.TransactionMetadata metadata =
                SinkProto.TransactionMetadata.parseFrom(buffer.duplicate());
        return metadata.toBuilder()
                .setId(metadata.getId() + "_" + loopId)
                .build();
    }
}
