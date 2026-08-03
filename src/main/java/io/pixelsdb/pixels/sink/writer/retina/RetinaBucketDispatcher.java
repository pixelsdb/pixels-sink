/*
 * Copyright 2023 PixelsDB.
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

import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.writer.AbstractBucketedWriter;

public class RetinaBucketDispatcher extends AbstractBucketedWriter<SinkContext>
{
    private final TableWriterProxy tableWriterProxy;

    public RetinaBucketDispatcher()
    {
        this.tableWriterProxy = TableWriterProxy.getInstance();
    }

    @Override
    protected void emit(RowChangeEvent event, int bucketId, SinkContext ctx)
    {
        tableWriterProxy
                .getTableWriter(event.getTable(), event.getTableId(), bucketId)
                .write(event, ctx);
    }
}
