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

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.exception.RetinaException;
import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.retina.RetinaService;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.transaction.TransService;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.TestConfig;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.exception.SinkException;
import io.pixelsdb.pixels.core.utils.DatetimeUtils;
import io.pixelsdb.pixels.sink.metadata.TableMetadataRegistry;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriter;
import io.pixelsdb.pixels.sink.writer.PixelsSinkWriterFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Tag("integration")
class RetinaWriterIntegrationTest
{

    static Logger logger = LoggerFactory.getLogger(RetinaWriterIntegrationTest.class.getName());
    static RetinaService retinaService;
    static TableMetadataRegistry metadataRegistry;
    static TransService transService;
    static int retinaPerformanceTestRowCount;
    static int retinaPerformanceTestMaxId;
    private final ExecutorService executor = Executors.newFixedThreadPool(16);

    @BeforeAll
    static void setUp() throws Exception
    {
        TestConfig.initializeIntegrationConfig();
        retinaService = RetinaService.Instance();
        metadataRegistry = TableMetadataRegistry.Instance();
        transService = TransService.Instance();
        retinaPerformanceTestRowCount = Integer.getInteger(
                "pixels.sink.test.retina.row.count", 1_000);
        retinaPerformanceTestMaxId = Integer.getInteger(
                "pixels.sink.test.retina.max.id", 2_000);
    }

    @Test
    public void insertSingleRecord() throws RetinaException, SinkException, TransException
    {

        TransContext ctx = transService.beginTrans(false);
        long timeStamp = ctx.getTimestamp();
        String schemaName = "pixels_index";
        String tableName = "ray_index";
        List<RetinaProto.TableUpdateData> tableUpdateData = new ArrayList<>();
        RetinaProto.TableUpdateData.Builder tableUpdateDataBuilder = RetinaProto.TableUpdateData.newBuilder();
        tableUpdateDataBuilder.setTableName(tableName);
        tableUpdateDataBuilder.setPrimaryIndexId(metadataRegistry.getPrimaryIndexKeyId(schemaName, tableName));
        // <int, long, string>
        for (int i = 0; i < 10; ++i)
        {
            byte[][] cols = new byte[3][];

            cols[0] = Integer.toString(i).getBytes(StandardCharsets.UTF_8);
            cols[1] = Long.toString(i * 1000L).getBytes(StandardCharsets.UTF_8);
            cols[2] = ("row_" + i).getBytes(StandardCharsets.UTF_8);
            SinkProto.RowValue.Builder afterValueBuilder = SinkProto.RowValue.newBuilder();
            afterValueBuilder
                    .addValues(
                            SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom((cols[0]))).build())
                    .addValues(
                            SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom((cols[1]))).build())
                    .addValues(
                            SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom((cols[2]))).build());


            SinkProto.RowRecord.Builder builder = SinkProto.RowRecord.newBuilder();
            builder.setOp(SinkProto.OperationType.INSERT)
                    .setAfter(afterValueBuilder)
                    .setSource(
                            SinkProto.SourceInfo.newBuilder()
                                    .setDb(schemaName)
                                    .setTable(tableName)
                                    .build()
                    );
            RowChangeEvent rowChangeEvent = new RowChangeEvent(builder.build());
            rowChangeEvent.setTimeStamp(timeStamp);
            rowChangeEvent.bindIndexKey();
            IndexProto.IndexKey indexKey = rowChangeEvent.getAfterKey();
            RetinaProto.InsertData.Builder insertDataBuilder = RetinaProto.InsertData.newBuilder();
            insertDataBuilder.
                    addColValues(ByteString.copyFrom((cols[0]))).addColValues(ByteString.copyFrom((cols[1])))
                    .addColValues(ByteString.copyFrom((cols[2])))
                    .addIndexKeys(indexKey);
            tableUpdateDataBuilder.addInsertData(insertDataBuilder.build());
        }
        tableUpdateData.add(tableUpdateDataBuilder.build());
        retinaService.updateRecord(schemaName, 0, tableUpdateData);
        tableUpdateDataBuilder.setTimestamp(timeStamp);
        transService.commitTrans(ctx.getTransId(), false);
    }

    @Test
    public void updateSingleRecord() throws RetinaException, SinkException, TransException
    {
        TransContext ctx = transService.beginTrans(false);
        long timeStamp = ctx.getTimestamp();
        String schemaName = "pixels_index";
        String tableName = "ray_index";


        List<RetinaProto.TableUpdateData> tableUpdateData = new ArrayList<>();
        RetinaProto.TableUpdateData.Builder tableUpdateDataBuilder = RetinaProto.TableUpdateData.newBuilder();
        tableUpdateDataBuilder.setTableName(tableName);
        tableUpdateDataBuilder.setPrimaryIndexId(metadataRegistry.getPrimaryIndexKeyId(schemaName, tableName));
        // <int, long, string>
        for (int i = 0; i < 10; ++i)
        {
            byte[][] cols = new byte[4][];
            cols[0] = Integer.toString(i).getBytes(StandardCharsets.UTF_8);
            cols[1] = Long.toString(i * 1000L).getBytes(StandardCharsets.UTF_8);
            cols[2] = ("row_" + i).getBytes(StandardCharsets.UTF_8);
            cols[3] = ("updated_row_" + i).getBytes(StandardCharsets.UTF_8);
            SinkProto.RowValue.Builder beforeValueBuilder = SinkProto.RowValue.newBuilder();
            beforeValueBuilder
                    .addValues(
                            SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom((cols[0]))).build())
                    .addValues(
                            SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom((cols[1]))).build())
                    .addValues(
                            SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom((cols[2]))).build());


            SinkProto.RowValue.Builder afterValueBuilder = SinkProto.RowValue.newBuilder();
            afterValueBuilder
                    .addValues(
                            SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom((cols[0]))).build())
                    .addValues(
                            SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom((cols[1]))).build())
                    .addValues(
                            SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom((cols[3]))).build());
            SinkProto.RowRecord.Builder builder = SinkProto.RowRecord.newBuilder();
            builder.setOp(SinkProto.OperationType.UPDATE)
                    .setBefore(beforeValueBuilder)
                    .setAfter(afterValueBuilder)
                    .setSource(
                            SinkProto.SourceInfo.newBuilder()
                                    .setDb(schemaName)
                                    .setTable(tableName)
                                    .build()
                    );

            RowChangeEvent rowChangeEvent = new RowChangeEvent(builder.build());
            rowChangeEvent.setTimeStamp(timeStamp);
            rowChangeEvent.bindIndexKey();
            RetinaProto.DeleteData.Builder deleteDataBuilder = RetinaProto.DeleteData.newBuilder();
            deleteDataBuilder
                    .addIndexKeys(rowChangeEvent.getBeforeKey());
            tableUpdateDataBuilder.addDeleteData(deleteDataBuilder.build());

            RetinaProto.InsertData.Builder insertDataBuilder = RetinaProto.InsertData.newBuilder();
            insertDataBuilder
                    .addColValues(ByteString.copyFrom((cols[0]))).addColValues(ByteString.copyFrom((cols[1])))
                    .addColValues(ByteString.copyFrom((cols[3])))
                    .addIndexKeys(rowChangeEvent.getAfterKey());
            tableUpdateDataBuilder.addInsertData(insertDataBuilder.build());
        }
        tableUpdateDataBuilder.setTimestamp(timeStamp);
        tableUpdateData.add(tableUpdateDataBuilder.build());
        retinaService.updateRecord(schemaName, 0, tableUpdateData);
        transService.commitTrans(ctx.getTransId(), false);
    }

    @Test
    public void testCheckingAccountInsertPerformance() throws
            RetinaException, SinkException, TransException, IOException, ExecutionException, InterruptedException
    {
        String schemaName = "pixels_bench_sf1x";
        String tableName = "savingaccount";

        RetinaServiceProxy writer = new RetinaServiceProxy(-1);

        TransactionProxy manager = TransactionProxy.Instance();
        // Step 1: Insert 10,000 records
        int totalInserts = retinaPerformanceTestMaxId;
        int batchSize = 5;
        int batchCount = totalInserts / batchSize;

        int samllBatchCount = 10;

        long start = System.currentTimeMillis();

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int b = 0; b < batchCount; )
        {
            List<RetinaProto.TableUpdateData> tableUpdateData = new ArrayList<>();
            for (int sb = 0; sb < samllBatchCount; sb++)
            {
                ++b;
                TransContext ctx = manager.getNewTransContext("batch-" + b);
                long timeStamp = ctx.getTimestamp();

                RetinaProto.TableUpdateData.Builder tableUpdateDataBuilder =
                        RetinaProto.TableUpdateData.newBuilder()
                                .setTableName(tableName)
                                .setPrimaryIndexId(metadataRegistry.getPrimaryIndexKeyId(schemaName, tableName));

                for (int i = 0; i < batchSize; i++)
                {
                    int accountID = b * batchSize + i;
                    int userID = accountID % 1000;
                    float balance = 1000.0f + accountID;
                    int isBlocked = 0;
                    long ts = System.currentTimeMillis();

                    byte[][] cols = new byte[5][];
                    cols[0] = ByteBuffer.allocate(Integer.BYTES).putInt(accountID).array();
                    cols[1] = ByteBuffer.allocate(Integer.BYTES).putInt(userID).array();
                    int intBits = Float.floatToIntBits(balance);
                    cols[2] = ByteBuffer.allocate(4).putInt(intBits).array();
                    cols[3] = ByteBuffer.allocate(Integer.BYTES).putInt(isBlocked).array();
                    cols[4] = ByteBuffer.allocate(Long.BYTES).putLong(ts).array();
                    // cols[4] = Long.toString(ts).getBytes(StandardCharsets.UTF_8);
                    // after row
                    SinkProto.RowValue.Builder afterValueBuilder = SinkProto.RowValue.newBuilder()
                            .addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom(cols[0])).build())
                            .addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom(cols[1])).build())
                            .addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom(cols[2])).build())
                            .addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom(cols[3])).build())
                            .addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom(cols[4])).build());

                    // RowRecord
                    SinkProto.RowRecord.Builder rowBuilder = SinkProto.RowRecord.newBuilder()
                            .setOp(SinkProto.OperationType.INSERT)
                            .setAfter(afterValueBuilder)
                            .setSource(
                                    SinkProto.SourceInfo.newBuilder()
                                            .setDb(schemaName)
                                            .setTable(tableName)
                                            .build()
                            );

                    RowChangeEvent rowChangeEvent = new RowChangeEvent(rowBuilder.build());
                    rowChangeEvent.setTimeStamp(timeStamp);
                    rowChangeEvent.bindIndexKey();

                    // InsertData
                    RetinaProto.InsertData.Builder insertDataBuilder = RetinaProto.InsertData.newBuilder()
                            .addColValues(ByteString.copyFrom(cols[0]))
                            .addColValues(ByteString.copyFrom(cols[1]))
                            .addColValues(ByteString.copyFrom(cols[2]))
                            .addColValues(ByteString.copyFrom(cols[3]))
                            .addColValues(ByteString.copyFrom(cols[4]))
                            .addIndexKeys(rowChangeEvent.getAfterKey());

                    tableUpdateDataBuilder.addInsertData(insertDataBuilder.build());
                }

                tableUpdateData.add(tableUpdateDataBuilder.build());

                CompletableFuture<Void> future = CompletableFuture.runAsync(() ->
                {
                    try
                    {
                        transService.commitTrans(ctx.getTransId(), false);
                    } catch (TransException e)
                    {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }

                }, executor);
                futures.add(future);
            }
            Assertions.assertNotNull(writer);
            if (!writer.writeTrans(schemaName, tableUpdateData))
            {
                throw new AssertionError("Failed to write transaction");
            }

        }

        for (CompletableFuture<Void> future : futures)
        {
            future.get();
        }

        long end = System.currentTimeMillis();
        double seconds = (end - start) / 1000.0;
        double insertsPerSec = totalInserts / seconds;
        double transPerSec = batchCount / seconds;
        logger.info("Inserted " + totalInserts + " rows in " + seconds + "s, rate=" + insertsPerSec + " inserts/s," + transPerSec + "trans/s");
        writer.close();
    }


    @Test
    public void testCheckingAccountUpdatePerformance() throws
            RetinaException, SinkException, TransException, IOException, ExecutionException, InterruptedException
    {
        String schemaName = "pixels_bench_sf1x";
        String tableName = "checkingaccount";

        int totalUpdates = retinaPerformanceTestRowCount; // 总更新条数
        int batchSize = 5;                                // 每个事务包含多少条 update
        int batchCount = totalUpdates / batchSize;

        int clientCount = 2; // 可自定义客户端数量
        ExecutorService clientExecutor = Executors.newFixedThreadPool(clientCount);

        List<RetinaServiceProxy> writers = new ArrayList<>();
        for (int c = 0; c < clientCount; c++)
        {
            writers.add(new RetinaServiceProxy(-1));
        }

        Random random = new Random();
        TransactionProxy manager = TransactionProxy.Instance();

        long start = System.currentTimeMillis();
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int b = 0; b < batchCount; b++)
        {
            final int batchIndex = b;
            CompletableFuture<Void> batchFuture = CompletableFuture.runAsync(() ->
            {
                try
                {
                    // 轮询选择客户端
                    RetinaServiceProxy writer = writers.get(batchIndex % clientCount);

                    TransContext ctx = manager.getNewTransContext("batch-" + batchIndex);
                    long timeStamp = ctx.getTimestamp();

                    List<RetinaProto.TableUpdateData> tableUpdateData = new ArrayList<>();
                    RetinaProto.TableUpdateData.Builder tableUpdateDataBuilder =
                            RetinaProto.TableUpdateData.newBuilder()
                                    .setTableName(tableName)
                                    .setPrimaryIndexId(metadataRegistry.getPrimaryIndexKeyId(schemaName, tableName));

                    for (int i = 0; i < batchSize; i++)
                    {
                        int accountID = random.nextInt(retinaPerformanceTestMaxId);
                        int userID = accountID % 1000;
                        float oldBalance = 1000.0f + accountID;
                        float newBalance = oldBalance + random.nextInt(1000);
                        int isBlocked = 0;
                        long oldTs = System.currentTimeMillis() - 1000;
                        long newTs = System.currentTimeMillis();

                        // 构建 before/after row
                        SinkProto.RowValue.Builder beforeValueBuilder = SinkProto.RowValue.newBuilder()
                                .addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFromUtf8(Integer.toString(accountID))).build())
                                .addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFromUtf8(Integer.toString(userID))).build())
                                .addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFromUtf8(Float.toString(oldBalance))).build())
                                .addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFromUtf8(Integer.toString(isBlocked))).build())
                                .addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFromUtf8(formatTimestamp(oldTs))).build());

                        SinkProto.RowValue.Builder afterValueBuilder = SinkProto.RowValue.newBuilder()
                                .addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFromUtf8(Integer.toString(accountID))).build())
                                .addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFromUtf8(Integer.toString(userID))).build())
                                .addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFromUtf8(Float.toString(newBalance))).build())
                                .addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFromUtf8(Integer.toString(isBlocked))).build())
                                .addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFromUtf8(formatTimestamp(newTs))).build());

                        SinkProto.RowRecord.Builder rowBuilder = SinkProto.RowRecord.newBuilder()
                                .setOp(SinkProto.OperationType.UPDATE)
                                .setBefore(beforeValueBuilder)
                                .setAfter(afterValueBuilder)
                                .setSource(SinkProto.SourceInfo.newBuilder().setDb(schemaName).setTable(tableName).build());

                        RowChangeEvent rowChangeEvent = new RowChangeEvent(rowBuilder.build());
                        rowChangeEvent.setTimeStamp(timeStamp);
                        rowChangeEvent.bindIndexKey();

                        // deleteData
                        RetinaProto.DeleteData.Builder deleteDataBuilder = RetinaProto.DeleteData.newBuilder()
                                .addIndexKeys(rowChangeEvent.getBeforeKey());
                        tableUpdateDataBuilder.addDeleteData(deleteDataBuilder.build());

                        // insertData
                        RetinaProto.InsertData.Builder insertDataBuilder = RetinaProto.InsertData.newBuilder()
                                .addColValues(ByteString.copyFromUtf8(Integer.toString(accountID)))
                                .addColValues(ByteString.copyFromUtf8(Integer.toString(userID)))
                                .addColValues(ByteString.copyFromUtf8(Float.toString(newBalance)))
                                .addColValues(ByteString.copyFromUtf8(Integer.toString(isBlocked)))
                        .addColValues(ByteString.copyFromUtf8(formatTimestamp(newTs)))
                                .addIndexKeys(rowChangeEvent.getAfterKey());
                        tableUpdateDataBuilder.addInsertData(insertDataBuilder.build());
                    }

                    tableUpdateData.add(tableUpdateDataBuilder.build());

                    long startTime = System.currentTimeMillis();
                    if (!writer.writeTrans(schemaName, tableUpdateData))
                    {
                        throw new AssertionError("Failed to write transaction");
                    }
                    long endTime = System.currentTimeMillis();
                    logger.debug("writeTrans batch " + batchIndex + " took " + (endTime - startTime) + " ms");

                    // commit transaction
                    transService.commitTrans(ctx.getTransId(), false);

                } catch (Exception e)
                {
                    throw new RuntimeException(e);
                }
            }, clientExecutor);

            futures.add(batchFuture);
        }

        // 等待所有 batch 完成
        for (CompletableFuture<Void> f : futures)
        {
            f.get();
        }

        long end = System.currentTimeMillis();
        double seconds = (end - start) / 1000.0;
        double updatesPerSec = totalUpdates / seconds;
        double transPerSec = batchCount / seconds;
        logger.info("Updated " + totalUpdates + " rows in " + seconds + "s, rate=" + updatesPerSec + " updates/s, " + transPerSec + " trans/s");

        clientExecutor.shutdown();
    }


    @Test
    public void testInsertTwoTablePerformance() throws
            RetinaException, SinkException, TransException, IOException, ExecutionException, InterruptedException
    {
        String schemaName = "pixels_bench_sf1x";
        String tableName = "checkingaccount";
        String tableName2 = "savingaccount";
        PixelsSinkWriter writer = PixelsSinkWriterFactory.getWriter();

        TransactionProxy manager = TransactionProxy.Instance();
        // Step 1: Insert 10,000 records
        int totalInserts = retinaPerformanceTestRowCount;
        int batchSize = 50;
        int batchCount = totalInserts / batchSize;


        long start = System.currentTimeMillis();

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int b = 0; b < batchCount; b++)
        {
            TransContext ctx = manager.getNewTransContext("batch-" + b);
            long timeStamp = ctx.getTimestamp();

            List<RetinaProto.TableUpdateData> tableUpdateData = new ArrayList<>();
            RetinaProto.TableUpdateData.Builder tableUpdateDataBuilder =
                    RetinaProto.TableUpdateData.newBuilder()
                            .setTableName(tableName)
                            .setPrimaryIndexId(metadataRegistry.getPrimaryIndexKeyId(schemaName, tableName));
            RetinaProto.TableUpdateData.Builder tableUpdateDataBuilder2 =
                    RetinaProto.TableUpdateData.newBuilder()
                            .setTableName(tableName2)
                            .setPrimaryIndexId(metadataRegistry.getPrimaryIndexKeyId(schemaName, tableName2));
            for (int i = 0; i < batchSize; i++)
            {
                int accountID = b * batchSize + i;
                int userID = accountID % 1000;
                float balance = 1000.0f + accountID;
                int isBlocked = 0;
                long ts = System.currentTimeMillis();

                byte[][] cols = new byte[5][];
                cols[0] = Integer.toString(accountID).getBytes(StandardCharsets.UTF_8);
                cols[1] = Integer.toString(userID).getBytes(StandardCharsets.UTF_8);
                cols[2] = Float.toString(balance).getBytes(StandardCharsets.UTF_8);
                cols[3] = Integer.toString(isBlocked).getBytes(StandardCharsets.UTF_8);
                cols[4] = formatTimestamp(ts).getBytes(StandardCharsets.UTF_8);
                // cols[4] = Long.toString(ts).getBytes(StandardCharsets.UTF_8);
                // after row
                SinkProto.RowValue.Builder afterValueBuilder = SinkProto.RowValue.newBuilder()
                        .addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom(cols[0])).build())
                        .addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom(cols[1])).build())
                        .addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom(cols[2])).build())
                        .addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom(cols[3])).build())
                        .addValues(SinkProto.ColumnValue.newBuilder().setValue(ByteString.copyFrom(cols[4])).build());

                // RowRecord
                SinkProto.RowRecord.Builder rowBuilder = SinkProto.RowRecord.newBuilder()
                        .setOp(SinkProto.OperationType.INSERT)
                        .setAfter(afterValueBuilder)
                        .setSource(
                                SinkProto.SourceInfo.newBuilder()
                                        .setDb(schemaName)
                                        .setTable(tableName)
                                        .build()
                        );
                SinkProto.RowRecord.Builder rowBuilder2 = SinkProto.RowRecord.newBuilder()
                        .setOp(SinkProto.OperationType.INSERT)
                        .setAfter(afterValueBuilder)
                        .setSource(
                                SinkProto.SourceInfo.newBuilder()
                                        .setDb(schemaName)
                                        .setTable(tableName2)
                                        .build()
                        );
                RowChangeEvent rowChangeEvent = new RowChangeEvent(rowBuilder.build());
                rowChangeEvent.setTimeStamp(timeStamp);
                rowChangeEvent.bindIndexKey();
                RowChangeEvent rowChangeEvent2 = new RowChangeEvent(rowBuilder.build());
                rowChangeEvent2.setTimeStamp(timeStamp);
                rowChangeEvent2.bindIndexKey();
                // InsertData
                RetinaProto.InsertData.Builder insertDataBuilder = RetinaProto.InsertData.newBuilder()
                        .addColValues(ByteString.copyFrom(cols[0]))
                        .addColValues(ByteString.copyFrom(cols[1]))
                        .addColValues(ByteString.copyFrom(cols[2]))
                        .addColValues(ByteString.copyFrom(cols[3]))
                        .addColValues(ByteString.copyFrom(cols[4]))
                        .addIndexKeys(rowChangeEvent.getAfterKey());
                RetinaProto.InsertData.Builder insertDataBuilder2 = RetinaProto.InsertData.newBuilder()
                        .addColValues(ByteString.copyFrom(cols[0]))
                        .addColValues(ByteString.copyFrom(cols[1]))
                        .addColValues(ByteString.copyFrom(cols[2]))
                        .addColValues(ByteString.copyFrom(cols[3]))
                        .addColValues(ByteString.copyFrom(cols[4]))
                        .addIndexKeys(rowChangeEvent2.getAfterKey());
                tableUpdateDataBuilder.addInsertData(insertDataBuilder.build());
                tableUpdateDataBuilder2.addInsertData(insertDataBuilder2.build());
            }

            tableUpdateData.add(tableUpdateDataBuilder.build());
            tableUpdateData.add(tableUpdateDataBuilder2.build());
            // retinaService.updateRecord(schemaName, tableUpdateData, timeStamp);
            long startTime = System.currentTimeMillis();  // 使用 nanoTime 获取更精确的时间


            CompletableFuture<Void> future = CompletableFuture.runAsync(() ->
            {
                try
                {
                    // 执行原始的 writeTrans 方法
                    // writer.writeTrans(schemaName, tableUpdateData, timeStamp);
                    // 记录结束时间
                    long endTime = System.currentTimeMillis();

                    // 计算并输出耗时（单位：毫秒）
                    long duration = endTime - startTime;
                    logger.debug("writeTrans took " + duration + " milliseconds");
                    transService.commitTrans(ctx.getTransId(), false);
                } catch (TransException e)
                {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

            }, executor);

            futures.add(future);
        }

        for (CompletableFuture<Void> future : futures)
        {
            future.get();
        }

        long end = System.currentTimeMillis();
        double seconds = (end - start) / 1000.0;
        double insertsPerSec = totalInserts * 2 / seconds;
        double transPerSec = batchCount * 2 / seconds;
        logger.info("Inserted " + totalInserts + " rows in " + seconds + "s, rate=" + insertsPerSec + " inserts/s," + transPerSec + "trans/s");
    }

    private static String formatTimestamp(long epochTs)
    {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochTs), ZoneId.systemDefault())
                .format(DatetimeUtils.SQL_LOCAL_DATE_TIME);
    }
}
