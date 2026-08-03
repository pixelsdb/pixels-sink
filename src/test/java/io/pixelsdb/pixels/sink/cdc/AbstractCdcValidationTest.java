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
import com.google.protobuf.TextFormat;
import io.pixelsdb.pixels.common.utils.RetinaUtils;
import io.pixelsdb.pixels.index.IndexProto;
import io.pixelsdb.pixels.retina.RetinaProto;
import io.pixelsdb.pixels.sink.SinkProto;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractCdcValidationTest
{
    static final String DATABASE_NAME = "cdc_verify";
    static final String TABLE_NAME = "records";
    /**
     * The scale of decimal_value in the CDC fixtures, i.e. decimal(18,4).
     */
    static final int DECIMAL_SCALE = 4;
    static final long SNAPSHOT_ID = 1001L;
    static final long MUTATION_ID = 2002L;

    private static final int ORDER_RECORD_COUNT = 8;
    private static final Duration CDC_TIMEOUT = Duration.ofSeconds(90);

    record ExpectedRow(long id, List<ByteString> columns)
    {
    }

    private JdbcDatabaseContainer<?> container;
    private Path stateDirectory;
    private CdcMetadataFixture metadataFixture;
    private RecordingRetinaWriter writer;
    private CdcEngineHarness engineHarness;

    protected abstract String dialect();

    protected abstract String connectorClass();

    protected abstract JdbcDatabaseContainer<?> createContainer();

    protected abstract void configureConnector(
            Properties properties,
            JdbcDatabaseContainer<?> container,
            Path stateDirectory);

    protected abstract String changesResource();

    protected abstract List<CdcMetadataFixture.ColumnSpec> columns();

    protected abstract ExpectedRow snapshotRow();

    protected abstract ExpectedRow insertedRow();

    protected abstract ExpectedRow updatedRow();

    @BeforeAll
    void startCdcValidation() throws Exception
    {
        assumeDatabaseSelected();
        container = createContainer();
        container.start();

        Path dialectOutput = Path.of(
                "target", "cdc-validation", dialect());
        Files.createDirectories(dialectOutput);
        stateDirectory = Files.createTempDirectory(
                dialectOutput, "engine-state-");
        String topicPrefix = "pixels-" + dialect() + "-" +
                UUID.randomUUID().toString().substring(0, 8);
        CdcEngineHarness.initializeSinkConfig(
                stateDirectory, dialect(), connectorClass(), topicPrefix);
        metadataFixture = CdcMetadataFixture.install(
                DATABASE_NAME, TABLE_NAME, "id", columns());
        writer = new RecordingRetinaWriter(dialectOutput);

        Properties properties = CdcEngineHarness.commonConnectorProperties(
                stateDirectory, connectorClass(), topicPrefix);
        configureConnector(properties, container, stateDirectory);
        engineHarness = new CdcEngineHarness(properties, writer);
        engineHarness.start();
    }

    @AfterAll
    void stopCdcValidation() throws Exception
    {
        Exception failure = null;
        if (engineHarness != null)
        {
            try
            {
                engineHarness.close();
            } catch (Exception e)
            {
                failure = e;
            }
        } else
        {
            PixelsSinkConfigFactory.reset();
        }
        if (metadataFixture != null)
        {
            metadataFixture.close();
        }
        if (container != null)
        {
            try
            {
                container.stop();
            } catch (RuntimeException e)
            {
                if (failure == null)
                {
                    failure = e;
                } else
                {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null)
        {
            throw failure;
        }
    }

    @Test
    void shouldCaptureTypesOperationsAndOrderingWithoutExternalServices()
            throws Exception
    {
        List<RecordingRetinaWriter.RecordedRequest> snapshotRecords =
                await(records -> count(records, SinkProto.OperationType.SNAPSHOT) == 1);
        RecordingRetinaWriter.RecordedRequest snapshot = single(
                snapshotRecords, SinkProto.OperationType.SNAPSHOT, SNAPSHOT_ID);
        assertAfterRequest(snapshot, snapshotRow());

        executeSqlResource(changesResource());
        List<RecordingRetinaWriter.RecordedRequest> mutationRecords =
                await(this::containsMutationLifecycle);
        RecordingRetinaWriter.RecordedRequest insert = single(
                mutationRecords, SinkProto.OperationType.INSERT, MUTATION_ID);
        RecordingRetinaWriter.RecordedRequest update = single(
                mutationRecords, SinkProto.OperationType.UPDATE, MUTATION_ID);
        RecordingRetinaWriter.RecordedRequest delete = single(
                mutationRecords, SinkProto.OperationType.DELETE, MUTATION_ID);
        assertAfterRequest(insert, insertedRow());
        assertRowValues(insertedRow(), update.row().getBefore());
        assertAfterRequest(update, updatedRow());
        assertRowValues(updatedRow(), delete.row().getBefore());
        assertDeleteRequest(delete, updatedRow().id());

        List<Long> orderIds = sameBucketIds(ORDER_RECORD_COUNT);
        insertOrderRows(orderIds);
        List<RecordingRetinaWriter.RecordedRequest> allRecords =
                await(records -> orderRecords(records).size() == ORDER_RECORD_COUNT);
        assertOrderContract(allRecords, orderIds);
        engineHarness.assertHealthy();
    }

    private List<RecordingRetinaWriter.RecordedRequest> await(
            Predicate<List<RecordingRetinaWriter.RecordedRequest>> complete)
            throws InterruptedException
    {
        List<RecordingRetinaWriter.RecordedRequest> records =
                writer.await(complete, CDC_TIMEOUT);
        engineHarness.assertHealthy();
        return records;
    }

    private void assertAfterRequest(
            RecordingRetinaWriter.RecordedRequest recorded,
            ExpectedRow expected) throws Exception
    {
        assertRowValues(expected, recorded.row().getAfter());
        assertRequestEnvelope(recorded);
        RetinaProto.TableUpdateData update =
                recorded.request().getTableUpdateData(0);
        List<ByteString> actual;
        IndexProto.IndexKey indexKey;
        if (recorded.row().getOp() == SinkProto.OperationType.UPDATE)
        {
            assertEquals(0, update.getInsertDataCount());
            assertEquals(1, update.getUpdateDataCount());
            assertEquals(0, update.getDeleteDataCount());
            actual = update.getUpdateData(0).getColValuesList();
            indexKey = update.getUpdateData(0).getIndexKeys(0);
        } else
        {
            assertTrue(recorded.row().getOp() == SinkProto.OperationType.INSERT ||
                    recorded.row().getOp() == SinkProto.OperationType.SNAPSHOT);
            assertEquals(1, update.getInsertDataCount());
            assertEquals(0, update.getUpdateDataCount());
            assertEquals(0, update.getDeleteDataCount());
            actual = update.getInsertData(0).getColValuesList();
            indexKey = update.getInsertData(0).getIndexKeys(0);
        }
        assertColumnValues(expected.columns(), actual);
        assertIndexKey(indexKey, expected.id(), recorded.row().getTimeStamp());
    }

    private void assertDeleteRequest(
            RecordingRetinaWriter.RecordedRequest recorded,
            long expectedId) throws Exception
    {
        assertRequestEnvelope(recorded);
        RetinaProto.TableUpdateData update =
                recorded.request().getTableUpdateData(0);
        assertEquals(0, update.getInsertDataCount());
        assertEquals(0, update.getUpdateDataCount());
        assertEquals(1, update.getDeleteDataCount());
        assertIndexKey(
                update.getDeleteData(0).getIndexKeys(0),
                expectedId,
                recorded.row().getTimeStamp());
    }

    private void assertRequestEnvelope(
            RecordingRetinaWriter.RecordedRequest recorded) throws Exception
    {
        RetinaProto.UpdateRecordRequest request = recorded.request();
        assertEquals(RecordingRetinaWriter.TOKEN,
                request.getHeader().getToken());
        assertEquals(DATABASE_NAME, request.getSchemaName());
        assertEquals(RecordingRetinaWriter.VIRTUAL_NODE_ID,
                request.getVirtualNodeId());
        assertEquals(1, request.getTableUpdateDataCount());
        RetinaProto.TableUpdateData tableUpdate =
                request.getTableUpdateData(0);
        assertEquals(TABLE_NAME, tableUpdate.getTableName());
        assertEquals(CdcMetadataFixture.PRIMARY_INDEX_ID,
                tableUpdate.getPrimaryIndexId());
        assertEquals(recorded.row().getTimeStamp(), tableUpdate.getTimestamp());
        assertEquals(request,
                RetinaProto.UpdateRecordRequest.parseFrom(request.toByteArray()));

        RetinaProto.UpdateRecordRequest.Builder textProto =
                RetinaProto.UpdateRecordRequest.newBuilder();
        TextFormat.getParser().merge(
                Files.readString(
                        recorded.textProtoPath(), StandardCharsets.UTF_8),
                textProto);
        assertEquals(request, textProto.build());
        assertTrue(recorded.textProtoPath().startsWith(
                Path.of("target", "cdc-validation", dialect())));
    }

    private void assertIndexKey(
            IndexProto.IndexKey indexKey,
            long expectedId,
            long expectedTimestamp)
    {
        assertEquals(longValue(expectedId), indexKey.getKey());
        assertEquals(CdcMetadataFixture.PRIMARY_INDEX_ID,
                indexKey.getIndexId());
        assertEquals(CdcMetadataFixture.TABLE_ID, indexKey.getTableId());
        assertEquals(expectedTimestamp, indexKey.getTimestamp());
    }

    private void assertRowValues(
            ExpectedRow expected,
            SinkProto.RowValue actual)
    {
        assertColumnValues(
                expected.columns(),
                actual.getValuesList().stream()
                        .map(SinkProto.ColumnValue::getValue)
                        .toList());
    }

    private void assertColumnValues(
            List<ByteString> expected,
            List<ByteString> actual)
    {
        assertEquals(columns().size(), expected.size(),
                "Expected fixture must describe every metadata column");
        assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); ++i)
        {
            assertEquals(
                    expected.get(i),
                    actual.get(i),
                    "column " + columns().get(i).name());
        }
    }

    private boolean containsMutationLifecycle(
            List<RecordingRetinaWriter.RecordedRequest> records)
    {
        return has(records, SinkProto.OperationType.INSERT, MUTATION_ID) &&
                has(records, SinkProto.OperationType.UPDATE, MUTATION_ID) &&
                has(records, SinkProto.OperationType.DELETE, MUTATION_ID);
    }

    private void assertOrderContract(
            List<RecordingRetinaWriter.RecordedRequest> records,
            List<Long> expectedIds)
    {
        List<RecordingRetinaWriter.RecordedRequest> ordered =
                orderRecords(records);
        List<Long> expectedSequences = new ArrayList<>(ORDER_RECORD_COUNT);
        for (int i = 0; i < ORDER_RECORD_COUNT; ++i)
        {
            expectedSequences.add((long) CdcEngineHarness.ORDER_SEQUENCE_BASE + i);
        }

        assertEquals(expectedIds, ordered.stream()
                .map(record -> idOf(record.row()))
                .toList());
        assertEquals(expectedSequences, ordered.stream()
                .map(record -> int64(
                        record.row().getAfter().getValues(1).getValue()))
                .toList());
        assertEquals(1L, ordered.stream()
                .map(record -> record.row().getAfterBucketFromIndex())
                .distinct()
                .count());
        assertTrue(ordered.stream().allMatch(record ->
                record.row().getSchemaName().equals(DATABASE_NAME) &&
                        record.row().getTable().equals(TABLE_NAME)));

        assertTrue(engineHarness.interleavingDelayApplied(),
                "Expected one multi-record Engine batch to be delayed");
        assertEquals(expectedSequences, engineHarness.sourceSequenceOrder());
        assertEquals(new HashSet<>(expectedSequences),
                new HashSet<>(engineHarness.decodeAccessOrder()));
        assertNotEquals(expectedSequences, engineHarness.decodeAccessOrder(),
                "Decode access order must be intentionally interleaved");
    }

    private List<RecordingRetinaWriter.RecordedRequest> orderRecords(
            List<RecordingRetinaWriter.RecordedRequest> records)
    {
        return records.stream()
                .filter(record ->
                {
                    if (!record.row().hasAfterData() ||
                            record.row().getAfter().getValuesCount() < 2)
                    {
                        return false;
                    }
                    return int64(record.row().getAfter()
                            .getValues(1).getValue()) >=
                            CdcEngineHarness.ORDER_SEQUENCE_BASE;
                })
                .toList();
    }

    private void executeSqlResource(String resourceName) throws Exception
    {
        String script = resource(resourceName);
        try (Connection connection = connection();
             Statement statement = connection.createStatement())
        {
            connection.setAutoCommit(false);
            for (String sql : script.split(";"))
            {
                if (!sql.isBlank())
                {
                    statement.execute(sql.trim());
                }
            }
            connection.commit();
        }
    }

    private void insertOrderRows(List<Long> ids) throws Exception
    {
        try (Connection connection = connection();
             PreparedStatement statement = connection.prepareStatement(
                     "INSERT INTO records (id, sequence_no, text_value) " +
                             "VALUES (?, ?, ?)"))
        {
            connection.setAutoCommit(false);
            for (int i = 0; i < ids.size(); ++i)
            {
                statement.setLong(1, ids.get(i));
                statement.setInt(
                        2, CdcEngineHarness.ORDER_SEQUENCE_BASE + i);
                statement.setString(3, "ordered-row-" + i);
                statement.addBatch();
            }
            statement.executeBatch();
            connection.commit();
        }
    }

    private Connection connection() throws Exception
    {
        return DriverManager.getConnection(
                container.getJdbcUrl(),
                container.getUsername(),
                container.getPassword());
    }

    private String resource(String resourceName) throws IOException
    {
        try (InputStream input = getClass().getClassLoader()
                .getResourceAsStream(resourceName))
        {
            if (input == null)
            {
                throw new IOException("Missing test resource: " + resourceName);
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private void assumeDatabaseSelected()
    {
        String selected = System.getProperty("cdc.database", "").trim()
                .toLowerCase(Locale.ROOT);
        if (!selected.isEmpty() &&
                !Set.of("mysql", "postgresql").contains(selected))
        {
            Assertions.fail(
                    "cdc.database must be mysql or postgresql: " + selected);
        }
        Assumptions.assumeTrue(
                selected.isEmpty() || selected.equals(dialect()),
                () -> "Selected cdc.database=" + selected);
    }

    private List<Long> sameBucketIds(int count)
    {
        List<Long> ids = new ArrayList<>(count);
        long candidate = 10_000L;
        int targetBucket = bucket(candidate);
        while (ids.size() < count && candidate < 10_000_000L)
        {
            if (bucket(candidate) == targetBucket)
            {
                ids.add(candidate);
            }
            ++candidate;
        }
        assertEquals(count, ids.size(),
                "Unable to find enough same-bucket primary keys");
        assertFalse(ids.contains(SNAPSHOT_ID));
        assertFalse(ids.contains(MUTATION_ID));
        return ids;
    }

    private static int bucket(long id)
    {
        return RetinaUtils.getBucketIdFromByteBuffer(longValue(id));
    }

    private static long count(
            List<RecordingRetinaWriter.RecordedRequest> records,
            SinkProto.OperationType operation)
    {
        return records.stream()
                .filter(record -> record.row().getOp() == operation)
                .count();
    }

    private static boolean has(
            List<RecordingRetinaWriter.RecordedRequest> records,
            SinkProto.OperationType operation,
            long id)
    {
        return records.stream().anyMatch(record ->
                record.row().getOp() == operation &&
                        idOf(record.row()) == id);
    }

    private static RecordingRetinaWriter.RecordedRequest single(
            List<RecordingRetinaWriter.RecordedRequest> records,
            SinkProto.OperationType operation,
            long id)
    {
        List<RecordingRetinaWriter.RecordedRequest> matches = records.stream()
                .filter(record -> record.row().getOp() == operation)
                .filter(record -> idOf(record.row()) == id)
                .toList();
        assertEquals(1, matches.size(),
                operation + " records for primary key " + id);
        return matches.get(0);
    }

    private static long idOf(
            io.pixelsdb.pixels.sink.event.RowChangeEvent row)
    {
        SinkProto.RowValue values =
                row.hasAfterData() ? row.getAfter() : row.getBefore();
        return int64(values.getValues(0).getValue());
    }

    protected static ByteString nullValue()
    {
        return ByteString.EMPTY;
    }

    protected static ByteString booleanValue(boolean value)
    {
        return ByteString.copyFrom(new byte[]{(byte) (value ? 1 : 0)});
    }

    protected static ByteString byteValue(int value)
    {
        return ByteString.copyFrom(new byte[]{(byte) value});
    }

    protected static ByteString shortValue(int value)
    {
        // Pixels encodes SHORT with the same width as INT.
        return intValue((short) value);
    }

    protected static ByteString intValue(int value)
    {
        return ByteString.copyFrom(
                ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
    }

    protected static ByteString longValue(long value)
    {
        return ByteString.copyFrom(
                ByteBuffer.allocate(Long.BYTES).putLong(value).array());
    }

    protected static ByteString floatValue(float value)
    {
        return intValue(Float.floatToIntBits(value));
    }

    protected static ByteString doubleValue(double value)
    {
        return longValue(Double.doubleToLongBits(value));
    }

    protected static ByteString decimalValue(String value)
    {
        // decimal_value is a short decimal, encoded as its big-endian unscaled long.
        return longValue(new BigDecimal(value)
                .setScale(DECIMAL_SCALE, RoundingMode.UNNECESSARY)
                .unscaledValue().longValueExact());
    }

    protected static ByteString utf8(String value)
    {
        return ByteString.copyFrom(value, StandardCharsets.UTF_8);
    }

    protected static ByteString binary(int... unsignedBytes)
    {
        byte[] bytes = new byte[unsignedBytes.length];
        for (int i = 0; i < unsignedBytes.length; ++i)
        {
            bytes[i] = (byte) unsignedBytes[i];
        }
        return ByteString.copyFrom(bytes);
    }

    protected static ByteString dateValue(String value)
    {
        return intValue((int) LocalDate.parse(value).toEpochDay());
    }

    protected static ByteString timeValue(String value)
    {
        return intValue((int) (LocalTime.parse(value).toNanoOfDay() /
                1_000_000L));
    }

    protected static ByteString timestampValue(String value)
    {
        LocalDateTime timestamp = LocalDateTime.parse(value);
        long micros = timestamp.toEpochSecond(ZoneOffset.UTC) * 1_000_000L +
                timestamp.getNano() / 1_000L;
        return longValue(micros);
    }

    protected static List<ByteString> values(ByteString... values)
    {
        return Arrays.asList(values);
    }

    private static long int64(ByteString value)
    {
        if (value.size() == Integer.BYTES)
        {
            return ByteBuffer.wrap(value.toByteArray()).getInt();
        }
        return ByteBuffer.wrap(value.toByteArray()).getLong();
    }
}
