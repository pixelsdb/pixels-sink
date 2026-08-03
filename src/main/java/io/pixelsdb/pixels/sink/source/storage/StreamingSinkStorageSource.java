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
package io.pixelsdb.pixels.sink.source.storage;

import io.pixelsdb.pixels.common.physical.PhysicalReader;
import io.pixelsdb.pixels.common.physical.PhysicalReaderUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.core.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class StreamingSinkStorageSource extends AbstractSinkStorageSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingSinkStorageSource.class);

    @Override
    public void start()
    {
        beginProcessing();
        try
        {
            for (String file : files)
            {
                if (!isRunning())
                {
                    break;
                }
                Storage.Scheme scheme = Storage.Scheme.fromPath(file);
                readers.add(PhysicalReaderUtil.newPhysicalReader(scheme, file));
            }

            do
            {
                for (PhysicalReader reader : readers)
                {
                    if (!isRunning())
                    {
                        break;
                    }

                    LOGGER.info("Start reading {}", reader.getPath());
                    reader.seek(0);
                    long offset = 0;
                    long fileLength = reader.getFileLength();
                    while (isRunning() && offset < fileLength)
                    {
                        Pair<Integer, ByteBuffer> record = readRecord(reader, offset, fileLength);
                        int valueLength = record.getRight().remaining();
                        submitRecord(record.getLeft(), record.getRight(), loopId);
                        offset += RECORD_HEADER_SIZE + (long) valueLength;
                    }
                }
                ++loopId;
            } while (storageLoopEnabled && isRunning());
        } catch (IOException e)
        {
            throw new RuntimeException("Failed to read sink proto storage", e);
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        } finally
        {
            clean();
        }
    }
}
