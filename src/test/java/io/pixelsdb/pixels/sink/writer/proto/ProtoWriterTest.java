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


import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.common.physical.*;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.TestConfig;
import io.pixelsdb.pixels.storage.localfs.PhysicalLocalReader;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtoWriterTest
{
    private static final String SCHEMA_NAME = "test";
    private static final String TABLE_NAME = "ray";

    private static SinkProto.RowRecord getRowRecord(int i)
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
                                .setDb(SCHEMA_NAME)
                                .setTable(TABLE_NAME)
                                .build()
                );
        return builder.build();
    }

    private static SinkProto.TransactionMetadata getTrans(int i, SinkProto.TransactionStatus status)
    {
        SinkProto.TransactionMetadata.Builder builder = SinkProto.TransactionMetadata.newBuilder();
        builder.setId(Integer.toString(i));
        builder.setStatus(status);
        builder.setTimestamp(System.currentTimeMillis());
        return builder.build();
    }

    @Tag("integration")
    @Test
    void testWriteTransInfo() throws Exception
    {
        TestConfig.initializeIntegrationConfig();
        int maxTx = Integer.getInteger("pixels.sink.test.transactions", 10);
        try (ProtoWriter transWriter = new ProtoWriter())
        {
            for (int i = 0; i < maxTx; i++)
            {
                transWriter.writeTrans(getTrans(i, SinkProto.TransactionStatus.BEGIN));
                transWriter.writeTrans(getTrans(i, SinkProto.TransactionStatus.END));
            }
        }
    }

    @Test
    void testWriteFile(@TempDir Path tempDir) throws IOException
    {
        Path path = tempDir.resolve("write.dat");
        try (PhysicalWriter writer = PhysicalWriterUtil.newPhysicalWriter(
                Storage.Scheme.file, path.toString()))
        {
            int writeNum = 3;
            ByteBuffer buf = ByteBuffer.allocate(writeNum * Integer.BYTES);
            for (int i = 0; i < writeNum; i++)
            {
                buf.putInt(i);
            }
            assertEquals(0, writer.append(buf));
        }

        assertEquals(3L * Integer.BYTES, Files.size(path));
    }

    @Test
    void testReadFile(@TempDir Path tempDir) throws IOException
    {
        Path path = tempDir.resolve("write.dat");
        try (PhysicalWriter writer = PhysicalWriterUtil.newPhysicalWriter(
                Storage.Scheme.file, path.toString()))
        {
            ByteBuffer buffer = ByteBuffer.allocate(3 * Long.BYTES);
            buffer.putLong(11L).putLong(22L).putLong(33L);
            writer.append(buffer);
        }

        try (PhysicalLocalReader reader = (PhysicalLocalReader) PhysicalReaderUtil
                .newPhysicalReader(Storage.Scheme.file, path.toString()))
        {
            assertEquals(3L * Long.BYTES, reader.getFileLength());
            assertEquals(11L, reader.readLong(ByteOrder.BIG_ENDIAN));
            assertEquals(22L, reader.readLong(ByteOrder.BIG_ENDIAN));
            assertEquals(33L, reader.readLong(ByteOrder.BIG_ENDIAN));
        }
    }

    @Test
    void testReadEmptyFile(@TempDir Path tempDir) throws IOException
    {
        Path path = tempDir.resolve("empty.dat");
        Files.createFile(path);

        try (PhysicalReader reader = PhysicalReaderUtil.newPhysicalReader(
                Storage.Scheme.file, path.toString()))
        {
            assertEquals(0, reader.getFileLength());
            assertThrows(IOException.class, () -> reader.readInt(ByteOrder.BIG_ENDIAN));
        }
    }

    @Tag("integration")
    @Test
    void testWriteRowInfo() throws Exception
    {
        TestConfig.initializeIntegrationConfig();
        int maxTx = Integer.getInteger("pixels.sink.test.transactions", 10);
        int rowCnt = 0;
        try (ProtoWriter transWriter = new ProtoWriter())
        {
            for (int i = 0; i < maxTx; i++)
            {
                transWriter.writeTrans(getTrans(i, SinkProto.TransactionStatus.BEGIN));
                for (int j = 0; j < 3; j++)
                {
                    transWriter.write(getRowRecord(rowCnt++));
                }
                transWriter.writeTrans(getTrans(i, SinkProto.TransactionStatus.END));
            }
        }
        assertEquals(maxTx * 3, rowCnt);
    }
}
