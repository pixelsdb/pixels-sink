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

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.writer.csv.CsvWriter;
import io.pixelsdb.pixels.sink.writer.flink.FlinkPollingWriter;
import io.pixelsdb.pixels.sink.writer.proto.ProtoWriter;
import io.pixelsdb.pixels.sink.writer.retina.RetinaWriter;

import java.io.IOException;

public class PixelsSinkWriterFactory
{
    private static final PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();

    private static volatile PixelsSinkWriter writer = null;


    static public PixelsSinkWriter getWriter()
    {
        if (writer == null)
        {
            synchronized (PixelsSinkWriterFactory.class)
            {
                if (writer == null)
                {
                    try
                    {
                        switch (config.getPixelsSinkMode())
                        {
                            case CSV:
                                writer = new CsvWriter();
                                break;
                            case RETINA:
                                writer = new RetinaWriter();
                                break;
                            case PROTO:
                                writer = new ProtoWriter();
                                break;
                            case FLINK:
                                writer = new FlinkPollingWriter();
                                break;
                            case NONE:
                                writer = new NoneWriter();
                                break;
                        }
                    } catch (IOException e)
                    {
                        throw new RuntimeException("Can't create writer", e);
                    }
                }
            }
        }
        return writer;
    }
}
