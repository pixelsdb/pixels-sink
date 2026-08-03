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
package io.pixelsdb.pixels.sink.event;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RowChangeEventTest
{
    @Test
    void shouldReturnStableNonNegativeBucket()
    {
        ByteString indexKey = getIndexKey(0);
        int expectedBucket = RowChangeEvent.getBucketIdFromByteBuffer(indexKey);

        for (int i = 0; i < 10; ++i)
        {
            int bucket = RowChangeEvent.getBucketIdFromByteBuffer(indexKey);
            assertEquals(expectedBucket, bucket);
        }
        assertTrue(expectedBucket >= 0);
    }

    private static ByteString getIndexKey(int key)
    {
        int keySize = Integer.BYTES;
        ByteBuffer byteBuffer = ByteBuffer.allocate(keySize);
        byteBuffer.putInt(key);
        return ByteString.copyFrom(byteBuffer.rewind());
    }
}
