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

import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.node.BucketCache;
import io.pixelsdb.pixels.common.retina.RetinaService;
import io.pixelsdb.pixels.common.utils.RetinaUtils;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.util.MetricsFacade;
import io.pixelsdb.pixels.sink.writer.PixelsSinkMode;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class RetinaServiceProxy
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RetinaServiceProxy.class);
    @Getter
    private static final PixelsSinkMode pixelsSinkMode = PixelsSinkMode.RETINA;
    private static final PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
    // private static final IndexService indexService = IndexService.Instance();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final RetinaService retinaService;
    private final MetricsFacade metricsFacade = MetricsFacade.getInstance();
    private final int vNodeId;
    private RetinaService.StreamHandler retinaStream = null;

    public RetinaServiceProxy(int bucketId)
    {
        if (bucketId == -1)
        {
            this.retinaService = RetinaService.Instance();
        } else
        {
            this.retinaService = RetinaUtils.getRetinaServiceFromBucketId(bucketId);
        }


        if (config.getRetinaWriteMode() == RetinaWriteMode.STREAM)
        {
            retinaStream = retinaService.startUpdateStream();
        } else
        {
            retinaStream = null;
        }

        this.vNodeId = BucketCache.getInstance().getRetinaNodeInfoByBucketId(bucketId).getVirtualNodeId();
    }

    public boolean writeTrans(String schemaName, List<RetinaProto.TableUpdateData> tableUpdateData)
    {
        if (config.getRetinaWriteMode() == RetinaWriteMode.STUB)
        {
            try
            {
                retinaService.updateRecord(schemaName, vNodeId, tableUpdateData);
            } catch (RetinaException e)
            {
                e.printStackTrace();
                return false;
            }
        } else
        {
            try
            {
                retinaStream.updateRecord(schemaName, vNodeId, tableUpdateData);
            } catch (RetinaException e)
            {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    public CompletableFuture<RetinaProto.UpdateRecordResponse> writeBatchAsync
            (String schemaName, List<RetinaProto.TableUpdateData> tableUpdateData)
    {
        if (config.getRetinaWriteMode() == RetinaWriteMode.STUB)
        {
            try
            {
                retinaService.updateRecord(schemaName, vNodeId, tableUpdateData);
            } catch (RetinaException e)
            {
                e.printStackTrace();
            }
            return null;
        } else
        {
            try
            {
                return retinaStream.updateRecord(schemaName, vNodeId, tableUpdateData);
            } catch (RetinaException e)
            {
                e.printStackTrace();
            }
            return null;
        }
    }

    public void close() throws IOException
    {
        isClosed.compareAndSet(false, true);
        if (config.getRetinaWriteMode() == RetinaWriteMode.STREAM)
        {
            retinaStream.close();
        }
    }

    public enum RetinaWriteMode
    {
        STREAM,
        STUB;

        public static RetinaWriteMode fromValue(String value)
        {
            for (RetinaWriteMode mode : values())
            {
                if (mode.name().equalsIgnoreCase(value))
                {
                    return mode;
                }
            }
            throw new RuntimeException(String.format("Can't convert %s to Retina writer type", value));
        }
    }
}
