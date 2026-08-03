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
package io.pixelsdb.pixels.sink.writer.retina;

import io.pixelsdb.pixels.common.node.BucketCache;
import io.pixelsdb.pixels.daemon.NodeProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TableWriterProxy
{
    private final static TableWriterProxy INSTANCE = new TableWriterProxy();

    private final TransactionMode transactionMode;
    private final int retinaCliNum;
    private final Map<WriterKey, TableWriter> WRITER_REGISTRY = new ConcurrentHashMap<>();

    private TableWriterProxy()
    {
        PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();
        this.transactionMode = pixelsSinkConfig.getTransactionMode();
        this.retinaCliNum = pixelsSinkConfig.getRetinaClientNum();
    }

    protected static TableWriterProxy getInstance()
    {
        return INSTANCE;
    }

    protected TableWriter getTableWriter(String tableName, long tableId, int bucket)
    {
        int cliNo = bucket % retinaCliNum;
        // warn: we assume table id is less than INT.MAX
        WriterKey key = new WriterKey(tableId, BucketCache.getInstance().getRetinaNodeInfoByBucketId(bucket), cliNo);

        return WRITER_REGISTRY.computeIfAbsent(key, t ->
        {
            switch (transactionMode)
            {
                case SINGLE ->
                {
                    return new TableSingleTxWriter(tableName, bucket);
                }
                case BATCH ->
                {
                    return new TableCrossTxWriter(tableName, bucket);
                }
                case RECORD ->
                {
                    return new TableSingleRecordWriter(tableName, bucket);
                }
                default ->
                {
                    throw new IllegalArgumentException("Unknown transaction mode: " + transactionMode);
                }
            }
        });
    }

    record WriterKey(long tableId, NodeProto.NodeInfo nodeInfo, int cliNo)
    {
    }
}
