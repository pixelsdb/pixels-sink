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
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

@Tag("integration")
@Tag("cdc-validation")
class MySqlCdcValidationTest extends AbstractCdcValidationTest
{
    private static final String CONNECTOR_CLASS =
            "io.debezium.connector.mysql.MySqlConnector";

    @Override
    protected String dialect()
    {
        return "mysql";
    }

    @Override
    protected String connectorClass()
    {
        return CONNECTOR_CLASS;
    }

    @Override
    protected JdbcDatabaseContainer<?> createContainer()
    {
        return new MySQLContainer<>(
                DockerImageName.parse("mysql:8.0.36"))
                .withDatabaseName(DATABASE_NAME)
                .withUsername("root")
                .withPassword("cdc-root-password")
                .withCommand(
                        "--server-id=223344",
                        "--log-bin=mysql-bin",
                        "--binlog-format=ROW",
                        "--binlog-row-image=FULL",
                        "--character-set-server=utf8mb4",
                        "--collation-server=utf8mb4_unicode_ci")
                .withInitScript("cdc/mysql/init.sql");
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
        properties.setProperty("database.include.list", DATABASE_NAME);
        properties.setProperty(
                "table.include.list", DATABASE_NAME + "." + TABLE_NAME);
        properties.setProperty("database.server.id", "223345");
        properties.setProperty("database.connectionTimeZone", "UTC");
        properties.setProperty("database.ssl.mode", "disabled");
        properties.setProperty("snapshot.locking.mode", "minimal");
        properties.setProperty(
                "schema.history.internal",
                "io.debezium.storage.file.history.FileSchemaHistory");
        properties.setProperty(
                "schema.history.internal.file.filename",
                stateDirectory.resolve("schema-history.dat").toString());
        properties.setProperty("converters", "boolean");
        properties.setProperty(
                "boolean.type",
                "io.debezium.connector.mysql.converters." +
                        "TinyIntOneToBooleanConverter");
        properties.setProperty(
                "boolean.selector",
                DATABASE_NAME + "." + TABLE_NAME + ".boolean_value");
    }

    @Override
    protected String changesResource()
    {
        return "cdc/mysql/changes.sql";
    }

    @Override
    protected List<CdcMetadataFixture.ColumnSpec> columns()
    {
        return List.of(
                column("id", "bigint"),
                column("sequence_no", "int"),
                column("boolean_value", "boolean"),
                column("tiny_value", "tinyint"),
                column("small_value", "smallint"),
                column("int_value", "int"),
                column("big_value", "bigint"),
                column("float_value", "float"),
                column("double_value", "double"),
                column("decimal_value", "decimal(18,4)"),
                column("char_value", "char(4)"),
                column("varchar_value", "varchar(64)"),
                column("text_value", "varchar(1024)"),
                column("binary_value", "binary(4)"),
                column("varbinary_value", "varbinary(8)"),
                column("blob_value", "varbinary(65535)"),
                column("date_value", "date"),
                column("time_value", "time(3)"),
                column("timestamp_value", "timestamp(6)"),
                column("unicode_value", "varchar(64)"),
                column("nullable_value", "varchar(64)"),
                column("json_value", "varchar(128)"),
                column("enum_value", "varchar(16)"),
                column("set_value", "varchar(32)"));
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
                binary(0xde, 0xad, 0xbe, 0xef),
                binary(0x00, 0xff, 0x10),
                dateValue("2024-02-29"),
                timeValue("12:34:56.789"),
                timestampValue("2024-02-29T12:34:56.123456"),
                utf8("像素-快照🌟"),
                nullValue(),
                utf8("\"json-snapshot\""),
                utf8("GREEN"),
                utf8("ALPHA,GAMMA")));
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
                binary(0x10, 0x20, 0x30, 0x40),
                binary(0xca, 0xfe, 0xba, 0xbe),
                binary(0x7f, 0x00, 0x80),
                dateValue("2025-01-02"),
                timeValue("01:02:03.004"),
                timestampValue("2025-01-02T03:04:05.654321"),
                utf8("像素-插入🚀"),
                nullValue(),
                utf8("\"json-insert\""),
                utf8("RED"),
                utf8("BETA")));
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
                binary(0x01, 0x23, 0x45, 0x67, 0x89),
                binary(0xaa, 0x55),
                dateValue("2026-06-07"),
                timeValue("23:59:59.999"),
                timestampValue("2026-06-07T08:09:10.111222"),
                utf8("像素-更新🧪"),
                nullValue(),
                utf8("\"json-update\""),
                utf8("BLUE"),
                utf8("ALPHA,BETA")));
    }

    private static CdcMetadataFixture.ColumnSpec column(
            String name, String type)
    {
        return new CdcMetadataFixture.ColumnSpec(name, type);
    }
}
