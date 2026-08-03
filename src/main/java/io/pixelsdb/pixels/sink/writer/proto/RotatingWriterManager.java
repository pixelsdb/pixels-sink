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
package io.pixelsdb.pixels.sink.writer.proto;


import io.pixelsdb.pixels.common.physical.PhysicalWriter;
import io.pixelsdb.pixels.common.physical.PhysicalWriterUtil;
import io.pixelsdb.pixels.common.physical.Storage;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.util.EtcdFileRegistry;

import java.io.IOException;

/**
 * @package: io.pixelsdb.pixels.sink.writer
 * @className: RotatingWriterManager
 * @author: AntiO2
 * @date: 2025/10/5 07:34
 */
public class RotatingWriterManager
{
    private final String baseDir;
    private final String topic;
    private final int maxRecordsPerFile;
    private final Storage.Scheme scheme;
    private final EtcdFileRegistry registry;
    private int currentCount = 0;
    private PhysicalWriter currentWriter;
    private String currentFileName;

    public RotatingWriterManager(String topic) throws IOException
    {
        PixelsSinkConfig sinkConfig = PixelsSinkConfigFactory.getInstance();
        this.baseDir = sinkConfig.getSinkProtoDir();
        this.topic = topic;
        this.maxRecordsPerFile = sinkConfig.getMaxRecordsPerFile();
        this.registry = new EtcdFileRegistry(topic, baseDir);
        this.scheme = Storage.Scheme.fromPath(this.baseDir);
        rotate();
    }

    private void rotate() throws IOException
    {
        if (currentWriter != null)
        {
            currentWriter.close();
            registry.markFileCompleted(registry.getCurrentFileKey());
        }

        currentFileName = registry.createNewFile();
        currentWriter = PhysicalWriterUtil.newPhysicalWriter(scheme, currentFileName);

        currentCount = 0;
    }

    public PhysicalWriter current() throws IOException
    {
        if (currentCount >= maxRecordsPerFile)
        {
            rotate();
        }
        currentCount++;
        return currentWriter;
    }

    public void close() throws IOException
    {
        if (currentWriter != null)
        {
            currentWriter.close();
            registry.markFileCompleted(registry.getCurrentFileKey());
        }
    }
}