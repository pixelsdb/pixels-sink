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
package io.pixelsdb.pixels.sink.cdc;

import com.google.protobuf.ByteString;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.TestConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.event.RowChangeEvent;
import io.pixelsdb.pixels.sink.writer.AbstractBucketedWriter;
import io.pixelsdb.pixels.sink.writer.retina.RetinaPayloadBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
@Tag("cdc-validation")
class CdcOrderContractTest
{
    private CdcMetadataFixture metadataFixture;

    @BeforeEach
    void setUp() throws Exception
    {
        TestConfig.initializeUnitConfig();
        metadataFixture = CdcMetadataFixture.install(
                AbstractCdcValidationTest.DATABASE_NAME,
                AbstractCdcValidationTest.TABLE_NAME,
                "id",
                List.of(
                        new CdcMetadataFixture.ColumnSpec("id", "bigint"),
                        new CdcMetadataFixture.ColumnSpec(
                                "value", "varchar(64)")));
    }

    @AfterEach
    void tearDown()
    {
        if (metadataFixture != null)
        {
            metadataFixture.close();
        }
        PixelsSinkConfigFactory.reset();
    }

    @Test
    void shouldKeepSourceOrderInsideRepeatedOperationList() throws Exception
    {
        List<RowChangeEvent> events = List.of(
                insert(31L, "first"),
                insert(32L, "second"),
                insert(33L, "third"));

        RetinaProto.TableUpdateData update =
                RetinaPayloadBuilder.buildTableUpdateData(
                        AbstractCdcValidationTest.TABLE_NAME,
                        7001L,
                        events);

        assertEquals(List.of(31L, 32L, 33L),
                update.getInsertDataList().stream()
                        .map(data -> readLong(data.getIndexKeys(0).getKey()))
                        .toList());
        assertEquals(List.of("first", "second", "third"),
                update.getInsertDataList().stream()
                        .map(data -> data.getColValues(1).toStringUtf8())
                        .toList());
    }

    @Test
    void shouldDispatchPrimaryKeyChangeAsDeleteThenInsert() throws Exception
    {
        RowChangeEvent update = event(
                SinkProto.OperationType.UPDATE,
                row(41L, "before"),
                row(42L, "after"));
        CapturingBucketedWriter dispatcher = new CapturingBucketedWriter();

        dispatcher.writeRowChangeEvent(update, null);

        assertEquals(
                List.of(
                        SinkProto.OperationType.DELETE,
                        SinkProto.OperationType.INSERT),
                dispatcher.operations());
        assertEquals(List.of(41L, 42L), dispatcher.primaryKeys());
    }

    @Test
    void shouldParseBothDatabaseMetadataMatrices()
    {
        List<List<CdcMetadataFixture.ColumnSpec>> matrices = List.of(
                new MySqlCdcValidationTest().columns(),
                new PostgreSqlCdcValidationTest().columns());

        for (List<CdcMetadataFixture.ColumnSpec> matrix : matrices)
        {
            TypeDescription schema = TypeDescription.createSchemaFromStrings(
                    matrix.stream()
                            .map(CdcMetadataFixture.ColumnSpec::name)
                            .toList(),
                    matrix.stream()
                            .map(CdcMetadataFixture.ColumnSpec::pixelsType)
                            .toList());
            assertEquals(matrix.size(), schema.getChildren().size());
        }
    }

    private RowChangeEvent insert(long id, String value) throws Exception
    {
        return event(
                SinkProto.OperationType.INSERT, null, row(id, value));
    }

    private RowChangeEvent event(
            SinkProto.OperationType operation,
            SinkProto.RowValue before,
            SinkProto.RowValue after) throws Exception
    {
        SinkProto.RowRecord.Builder record = SinkProto.RowRecord.newBuilder()
                .setOp(operation)
                .setSource(SinkProto.SourceInfo.newBuilder()
                        .setDb(AbstractCdcValidationTest.DATABASE_NAME)
                        .setTable(AbstractCdcValidationTest.TABLE_NAME));
        if (before != null)
        {
            record.setBefore(before);
        }
        if (after != null)
        {
            record.setAfter(after);
        }
        RowChangeEvent event = new RowChangeEvent(
                record.build(),
                metadataFixture.metadata().getTypeDescription(),
                metadataFixture.metadata());
        event.setTimeStamp(7001L);
        event.initRoutingKey();
        return event;
    }

    private static SinkProto.RowValue row(long id, String value)
    {
        return SinkProto.RowValue.newBuilder()
                .addValues(SinkProto.ColumnValue.newBuilder()
                        .setValue(longValue(id)))
                .addValues(SinkProto.ColumnValue.newBuilder()
                        .setValue(ByteString.copyFromUtf8(value)))
                .build();
    }

    private static ByteString longValue(long value)
    {
        return ByteString.copyFrom(
                ByteBuffer.allocate(Long.BYTES).putLong(value).array());
    }

    private static long readLong(ByteString value)
    {
        return ByteBuffer.wrap(value.toByteArray()).getLong();
    }

    private static final class CapturingBucketedWriter
            extends AbstractBucketedWriter<Void>
    {
        private final List<RowChangeEvent> emitted = new ArrayList<>();

        @Override
        protected void emit(RowChangeEvent event, int bucketId, Void context)
        {
            emitted.add(event);
        }

        List<SinkProto.OperationType> operations()
        {
            return emitted.stream().map(RowChangeEvent::getOp).toList();
        }

        List<Long> primaryKeys()
        {
            return emitted.stream()
                    .map(event -> event.hasAfterData()
                            ? event.getAfterRoutingKey()
                            : event.getBeforeRoutingKey())
                    .map(CdcOrderContractTest::readLong)
                    .toList();
        }
    }
}
