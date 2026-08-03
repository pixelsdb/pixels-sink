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
package io.pixelsdb.pixels.sink.metadata;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.IndexException;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.index.IndexOption;
import io.pixelsdb.pixels.common.index.service.IndexService;
import io.pixelsdb.pixels.common.index.service.IndexServiceProvider;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.SinglePointIndex;
import io.pixelsdb.pixels.common.metadata.domain.Table;
import io.pixelsdb.pixels.daemon.MetadataProto;
import io.pixelsdb.pixels.index.IndexProto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

@Tag("integration")
class IndexServiceTest
{

    private final MetadataService metadataService = MetadataService.Instance();
    private final IndexService indexService =
            IndexServiceProvider.getService(IndexServiceProvider.ServiceMode.rpc);

    @Test
    public void testCreateFreshnessIndex() throws MetadataException
    {
        String testSchemaName = "pixels_bench_sf1x";
        String testTblName = "freshness";
        String keyColumn = "{\"keyColumnIds\":[15]}";
        Table table = metadataService.getTable(testSchemaName, testTblName);
        Layout layout = metadataService.getLatestLayout(testSchemaName, testTblName);

        MetadataProto.SinglePointIndex.Builder singlePointIndexbuilder = MetadataProto.SinglePointIndex.newBuilder();
        singlePointIndexbuilder.setId(0L)
                .setKeyColumns(keyColumn)
                .setPrimary(true)
                .setUnique(true)
                .setIndexScheme("rocksdb")
                .setTableId(table.getId())
                .setSchemaVersionId(layout.getSchemaVersionId());

        SinglePointIndex index = new SinglePointIndex(singlePointIndexbuilder.build());
        boolean result = metadataService.createSinglePointIndex(index);
        Assertions.assertTrue(result);
    }

    @Test
    public void testCreateIndex() throws MetadataException
    {
        String testSchemaName = "pixels_index";
        String testTblName = "ray_index";
        String keyColumn = "{\"keyColumnIds\":[11]}";
        Table table = metadataService.getTable(testSchemaName, testTblName);
        long id = table.getId();
        long schemaId = table.getSchemaId();
        Layout layout = metadataService.getLatestLayout(testSchemaName, testTblName);

        MetadataProto.SinglePointIndex.Builder singlePointIndexbuilder = MetadataProto.SinglePointIndex.newBuilder();
        singlePointIndexbuilder.setId(0L)
                .setKeyColumns(keyColumn)
                .setPrimary(true)
                .setUnique(true)
                .setIndexScheme("rocksdb")
                .setTableId(table.getId())
                .setSchemaVersionId(layout.getSchemaVersionId());

        SinglePointIndex index = new SinglePointIndex(singlePointIndexbuilder.build());
        boolean result = metadataService.createSinglePointIndex(index);
        Assertions.assertTrue(result);
    }

    @Test
    public void testGetIndex() throws MetadataException
    {
        String testSchemaName = "pixels_index";
        String testTblName = "ray_index";
        Table table = metadataService.getTable(testSchemaName, testTblName);
        long id = table.getId();
        SinglePointIndex index = metadataService.getPrimaryIndex(id);

        Assertions.assertNotNull(index);
    }

    @Test
    public void testGetRowID() throws MetadataException, IndexException
    {
        int numRowIds = 10000;
        IndexProto.RowIdBatch rowIdBatch = indexService.allocateRowIdBatch(4, numRowIds);
        Assertions.assertEquals(rowIdBatch.getLength(), numRowIds);
    }

    @Test
    public void testPutAndDelete() throws MetadataException, IndexException
    {
        String table = "customer";
        String db = "pixels_bench_sf1x";
        Table table1 = metadataService.getTable(db, table);
        long tableId = table1.getId();
        SinglePointIndex index = metadataService.getPrimaryIndex(tableId);

        String id = "2294222";

        int len = 1;
        int keySize = 0;
        keySize += id.length();
        keySize += Long.BYTES + (len + 1) * 2; // table id + index key

        ByteBuffer byteBuffer = ByteBuffer.allocate(keySize);

        byteBuffer.putLong(index.getTableId()).putChar(':');
        byteBuffer.put(id.getBytes());
        byteBuffer.putChar(':');


        IndexProto.PrimaryIndexEntry.Builder builder = IndexProto.PrimaryIndexEntry.newBuilder();
        long ts1 = 200000;
        long ts2 = 100000;
        int rgId = 100;
        int rgoffset = 10;

        builder.getIndexKeyBuilder()
                .setTimestamp(ts1)
                .setKey(ByteString.copyFrom(byteBuffer.rewind()))
                .setIndexId(index.getId())
                .setTableId(index.getTableId());
        builder.setRowId(100);
        builder.getRowLocationBuilder()
                .setRgId(rgId)
                .setFileId(0)
                .setRgRowOffset(rgoffset);
        IndexOption indexOption = IndexOption.builder().build();
        boolean b = indexService.putPrimaryIndexEntry(builder.build(), indexOption);

        builder.getIndexKeyBuilder().setTimestamp(ts2);

        IndexProto.RowLocation rowLocation = indexService.deletePrimaryIndexEntry(builder.getIndexKey(), indexOption);

    }
}
