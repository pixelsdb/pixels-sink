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

import org.junit.jupiter.api.Tag;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

@Tag("integration")
@Tag("cdc-validation")
class PostgreSqlCdcValidationTest extends AbstractCdcValidationTest
{
    private static final String CONNECTOR_CLASS =
            "io.debezium.connector.postgresql.PostgresConnector";

    @Override
    protected String dialect()
    {
        return "postgresql";
    }

    @Override
    protected String connectorClass()
    {
        return CONNECTOR_CLASS;
    }

    @Override
    protected JdbcDatabaseContainer<?> createContainer()
    {
        return new PostgreSQLContainer<>(
                DockerImageName.parse("postgres:16.4-alpine"))
                .withDatabaseName(DATABASE_NAME)
                .withUsername("debezium")
                .withPassword("cdc-postgres-password")
                .withCommand(
                        "postgres",
                        "-c", "wal_level=logical",
                        "-c", "max_replication_slots=4",
                        "-c", "max_wal_senders=4")
                .withInitScript("cdc/postgresql/init.sql");
    }

    @Override
    protected void configureConnector(
            Properties properties,
            JdbcDatabaseContainer<?> container,
            Path stateDirectory)
    {
        properties.setProperty("database.hostname", container.getHost());
        properties.setProperty(
                "database.port", String.valueOf(container.getFirstMappedPort()));
        properties.setProperty("database.user", container.getUsername());
        properties.setProperty("database.password", container.getPassword());
        properties.setProperty("database.dbname", DATABASE_NAME);
        properties.setProperty("plugin.name", "pgoutput");
        properties.setProperty("schema.include.list", "public");
        properties.setProperty(
                "table.include.list", "public." + TABLE_NAME);
        String identifier = properties.getProperty("topic.prefix")
                .replace('-', '_');
        properties.setProperty("slot.name", identifier + "_slot");
        properties.setProperty("slot.drop.on.stop", "true");
        properties.setProperty("publication.name", identifier + "_publication");
        properties.setProperty(
                "publication.autocreate.mode", "filtered");
    }

    @Override
    protected String changesResource()
    {
        return "cdc/postgresql/changes.sql";
    }

    @Override
    protected List<CdcMetadataFixture.ColumnSpec> columns()
    {
        return List.of(
                column("id", "bigint"),
                column("sequence_no", "int"),
                column("boolean_value", "boolean"),
                column("byte_value", "tinyint"),
                column("small_value", "smallint"),
                column("int_value", "int"),
                column("big_value", "bigint"),
                column("float_value", "float"),
                column("double_value", "double"),
                column("decimal_value", "decimal(18,4)"),
                column("char_value", "char(4)"),
                column("varchar_value", "varchar(64)"),
                column("text_value", "varchar(1024)"),
                column("binary_value", "varbinary(65535)"),
                column("date_value", "date"),
                column("time_value", "time(3)"),
                column("timestamp_value", "timestamp(6)"),
                column("unicode_value", "varchar(64)"),
                column("nullable_value", "varchar(64)"),
                column("uuid_value", "varchar(36)"),
                column("json_value", "varchar(128)"),
                column("jsonb_value", "varchar(128)"),
                column("enum_value", "varchar(16)"));
    }

    @Override
    protected ExpectedRow snapshotRow()
    {
        return new ExpectedRow(SNAPSHOT_ID, values(
                longValue(SNAPSHOT_ID),
                intValue(0),
                booleanValue(true),
                byteValue(-7),
                shortValue(-1234),
                intValue(-123456789),
                longValue(-1234567890123456789L),
                floatValue(1.25F),
                doubleValue(-12345.5D),
                decimalValue("12345678901234.5678"),
                utf8("ABCD"),
                utf8("snapshot-varchar"),
                utf8("snapshot-text"),
                binary(0x00, 0x01, 0x02, 0xff),
                dateValue("2024-02-29"),
                timeValue("12:34:56.789"),
                timestampValue("2024-02-29T12:34:56.123456"),
                utf8("像素-快照🌟"),
                nullValue(),
                utf8("123e4567-e89b-12d3-a456-426614174000"),
                utf8("\"json-snapshot\""),
                utf8("\"jsonb-snapshot\""),
                utf8("GREEN")));
    }

    @Override
    protected ExpectedRow insertedRow()
    {
        return new ExpectedRow(MUTATION_ID, values(
                longValue(MUTATION_ID),
                intValue(1),
                booleanValue(false),
                byteValue(8),
                shortValue(2345),
                intValue(123456789),
                longValue(1234567890123456789L),
                floatValue(2.5F),
                doubleValue(67890.25D),
                decimalValue("-12345678901234.5678"),
                utf8("WXYZ"),
                utf8("insert-varchar"),
                utf8("insert-text"),
                binary(0xca, 0xfe, 0xba, 0xbe),
                dateValue("2025-01-02"),
                timeValue("01:02:03.004"),
                timestampValue("2025-01-02T03:04:05.654321"),
                utf8("像素-插入🚀"),
                nullValue(),
                utf8("123e4567-e89b-12d3-a456-426614174001"),
                utf8("\"json-insert\""),
                utf8("\"jsonb-insert\""),
                utf8("RED")));
    }

    @Override
    protected ExpectedRow updatedRow()
    {
        return new ExpectedRow(MUTATION_ID, values(
                longValue(MUTATION_ID),
                intValue(2),
                booleanValue(true),
                byteValue(-9),
                shortValue(-3456),
                intValue(-987654321),
                longValue(-2222222222222222222L),
                floatValue(-3.75F),
                doubleValue(0.125D),
                decimalValue("99999999999999.9999"),
                utf8("IJKL"),
                utf8("update-varchar"),
                utf8("update-text"),
                binary(0xff, 0xee, 0xdd, 0xcc),
                dateValue("2026-06-07"),
                timeValue("23:59:59.999"),
                timestampValue("2026-06-07T08:09:10.111222"),
                utf8("像素-更新🧪"),
                nullValue(),
                utf8("123e4567-e89b-12d3-a456-426614174002"),
                utf8("\"json-update\""),
                utf8("\"jsonb-update\""),
                utf8("BLUE")));
    }

    private static CdcMetadataFixture.ColumnSpec column(
            String name, String type)
    {
        return new CdcMetadataFixture.ColumnSpec(name, type);
    }
}
