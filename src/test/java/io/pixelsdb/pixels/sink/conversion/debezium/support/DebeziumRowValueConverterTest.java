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
package io.pixelsdb.pixels.sink.conversion.debezium.support;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.pixelsdb.pixels.core.TypeDescription;
import io.pixelsdb.pixels.sink.SinkProto;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DebeziumRowValueConverterTest
{
    @Test
    void shouldEncodeCanonicalStructJsonAndAvroValues() throws Exception
    {
        TypeDescription typeDescription = TypeDescription.createSchemaFromStrings(
                List.of("id", "name", "amount", "note", "empty_value"),
                List.of("bigint", "varchar(64)", "decimal(10,2)", "varchar(64)", "varchar(64)"));
        Schema rowSchema = SchemaBuilder.struct()
                .field("id", Schema.INT64_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .field("amount", Decimal.builder(2)
                        .parameter("connect.decimal.precision", "10").build())
                .field("note", Schema.OPTIONAL_STRING_SCHEMA)
                .field("empty_value", Schema.STRING_SCHEMA)
                .build();
        Struct row = new Struct(rowSchema)
                .put("id", 9223372036854775806L)
                .put("name", "TDSQL value  ")
                .put("amount", new BigDecimal("24710.35"))
                .put("note", null)
                .put("empty_value", "");

        SinkProto.RowValue.Builder builder = SinkProto.RowValue.newBuilder();
        new DebeziumRowValueConverter(typeDescription).parse(row, builder);
        SinkProto.RowValue value = builder.build();

        assertEquals(9223372036854775806L,
                ByteBuffer.wrap(value.getValues(0).getValue().toByteArray()).getLong());
        assertEquals("TDSQL value  ", value.getValues(1).getValue().toStringUtf8());
        assertArrayEquals(longBytes(2471035L), bytes(value, 2));
        assertEquals(0, value.getValues(3).getValue().size());
        assertEquals("", value.getValues(4).getValue().toStringUtf8());

        ObjectNode jsonRow = new ObjectMapper().createObjectNode();
        jsonRow.put("id", 9223372036854775806L);
        jsonRow.put("name", "TDSQL value  ");
        jsonRow.put("amount", Base64.getEncoder().encodeToString(
                new BigDecimal("24710.35").unscaledValue().toByteArray()));
        jsonRow.putNull("note");
        jsonRow.put("empty_value", "");
        SinkProto.RowValue.Builder jsonBuilder = SinkProto.RowValue.newBuilder();
        new DebeziumRowValueConverter(typeDescription).parse(jsonRow, jsonBuilder);

        org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse("""
                {
                  "type": "record",
                  "name": "CanonicalRow",
                  "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "name", "type": "string"},
                    {"name": "amount", "type": "bytes"},
                    {"name": "note", "type": ["null", "string"], "default": null},
                    {"name": "empty_value", "type": "string"}
                  ]
                }
                """);
        GenericData.Record avroRow = new GenericData.Record(avroSchema);
        avroRow.put("id", 9223372036854775806L);
        avroRow.put("name", "TDSQL value  ");
        avroRow.put("amount", new BigDecimal("24710.35"));
        avroRow.put("note", null);
        avroRow.put("empty_value", "");
        SinkProto.RowValue.Builder avroBuilder = SinkProto.RowValue.newBuilder();
        new DebeziumRowValueConverter(typeDescription).parse(avroRow, avroBuilder);

        assertEquals(value, jsonBuilder.build());
        assertEquals(value, avroBuilder.build());
    }

    @Test
    void shouldEncodeIntegerWidthsExpectedByPixelsColumnVectors() throws Exception
    {
        TypeDescription typeDescription = TypeDescription.createSchemaFromStrings(
                List.of("tiny", "small", "integer"),
                List.of("tinyint", "smallint", "integer"));
        Schema rowSchema = SchemaBuilder.struct()
                .field("tiny", Schema.INT8_SCHEMA)
                .field("small", Schema.INT16_SCHEMA)
                .field("integer", Schema.INT32_SCHEMA)
                .build();
        Struct row = new Struct(rowSchema)
                .put("tiny", (byte) 7)
                .put("small", (short) 1024)
                .put("integer", 65536);

        SinkProto.RowValue.Builder builder = SinkProto.RowValue.newBuilder();
        new DebeziumRowValueConverter(typeDescription).parse(row, builder);

        assertEquals(1, builder.getValues(0).getValue().size());
        assertEquals(4, builder.getValues(1).getValue().size());
        assertEquals(4, builder.getValues(2).getValue().size());
    }

    @Test
    void shouldEncodeConnectPrimitiveValuesAsExactBytes() throws Exception
    {
        TypeDescription typeDescription = TypeDescription.createSchemaFromStrings(
                List.of(
                        "boolean_value", "numeric_boolean", "tiny", "small", "integer",
                        "big", "real_value", "double_value", "amount", "text",
                        "fixed_binary", "variable_binary", "null_text"),
                List.of(
                        "boolean", "boolean", "tinyint", "smallint", "integer",
                        "bigint", "float", "double", "decimal(12,2)", "varchar(32)",
                        "binary(5)", "varbinary(4)", "varchar(8)"));
        Schema decimalSchema = Decimal.builder(2)
                .parameter("connect.decimal.precision", "12")
                .build();
        Schema rowSchema = SchemaBuilder.struct()
                .field("boolean_value", Schema.BOOLEAN_SCHEMA)
                .field("numeric_boolean", Schema.INT16_SCHEMA)
                .field("tiny", Schema.INT16_SCHEMA)
                .field("small", Schema.INT16_SCHEMA)
                .field("integer", Schema.INT32_SCHEMA)
                .field("big", Schema.INT64_SCHEMA)
                .field("real_value", Schema.FLOAT32_SCHEMA)
                .field("double_value", Schema.FLOAT64_SCHEMA)
                .field("amount", decimalSchema)
                .field("text", Schema.STRING_SCHEMA)
                .field("fixed_binary", Schema.BYTES_SCHEMA)
                .field("variable_binary", Schema.BYTES_SCHEMA)
                .field("null_text", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        byte[] fixedBinary = new byte[]{0x00, 0x7f, (byte) 0x80, (byte) 0xff, 0x01};
        ByteBuffer variableBinary = ByteBuffer.wrap(
                new byte[]{0x55, 0x10, 0x20, (byte) 0xfe, 0x00, 0x66}, 1, 4);
        Struct row = new Struct(rowSchema)
                .put("boolean_value", true)
                .put("numeric_boolean", (short) 0)
                .put("tiny", (short) Byte.MIN_VALUE)
                .put("small", (short) 0x8123)
                .put("integer", 0x81234567)
                .put("big", 0x8123456789ABCDEFL)
                .put("real_value", -12.5f)
                .put("double_value", Math.PI)
                .put("amount", new BigDecimal("-123456789.01"))
                .put("text", "Pixels 像素 🌟")
                .put("fixed_binary", fixedBinary)
                .put("variable_binary", variableBinary)
                .put("null_text", null);

        SinkProto.RowValue value = parse(typeDescription, row);

        assertArrayEquals(new byte[]{1}, bytes(value, 0));
        assertArrayEquals(new byte[]{0}, bytes(value, 1));
        assertArrayEquals(new byte[]{(byte) 0x80}, bytes(value, 2));
        assertArrayEquals(intBytes((short) 0x8123), bytes(value, 3));
        assertArrayEquals(intBytes(0x81234567), bytes(value, 4));
        assertArrayEquals(longBytes(0x8123456789ABCDEFL), bytes(value, 5));
        assertArrayEquals(intBytes(Float.floatToIntBits(-12.5f)), bytes(value, 6));
        assertArrayEquals(longBytes(Double.doubleToLongBits(Math.PI)), bytes(value, 7));
        assertArrayEquals(longBytes(-12345678901L), bytes(value, 8));
        assertArrayEquals("Pixels 像素 🌟".getBytes(StandardCharsets.UTF_8), bytes(value, 9));
        assertArrayEquals(fixedBinary, bytes(value, 10));
        assertArrayEquals(
                new byte[]{0x10, 0x20, (byte) 0xfe, 0x00}, bytes(value, 11));
        assertArrayEquals(new byte[0], bytes(value, 12));
    }

    @Test
    void shouldEncodeDecimalsWithThePixelsCanonicalByteFormat() throws Exception
    {
        assertArrayEquals(
                pixelsType("decimal(12,2)").convertSqlStringToByte("-123456789.01"),
                bytes(parseSingle("decimal(12,2)", decimalSchema(2, 12),
                        new BigDecimal("-123456789.01")), 0));
        assertArrayEquals(
                pixelsType("decimal(20,4)").convertSqlStringToByte("-12345678901234.5678"),
                bytes(parseSingle("decimal(20,4)", decimalSchema(4, 20),
                        new BigDecimal("-12345678901234.5678")), 0));
    }

    @Test
    void shouldNormalizeLogicalTemporalUnitsUsingSchemaNames() throws Exception
    {
        Schema connectDate = org.apache.kafka.connect.data.Date.builder().build();
        Schema connectTime = org.apache.kafka.connect.data.Time.builder().build();
        Schema connectTimestamp = org.apache.kafka.connect.data.Timestamp.builder().build();
        Schema debeziumDate = logicalInt32("io.debezium.time.Date");
        Schema debeziumTime = logicalInt32("io.debezium.time.Time");
        Schema debeziumMicroTime = logicalInt64("io.debezium.time.MicroTime");
        Schema debeziumNanoTime = logicalInt64("io.debezium.time.NanoTime");
        Schema debeziumTimestamp = logicalInt64("io.debezium.time.Timestamp");
        Schema debeziumMicroTimestamp = logicalInt64("io.debezium.time.MicroTimestamp");
        Schema debeziumNanoTimestamp = logicalInt64("io.debezium.time.NanoTimestamp");

        TypeDescription typeDescription = TypeDescription.createSchemaFromStrings(
                List.of(
                        "connect_date", "debezium_date", "connect_time", "debezium_time",
                        "micro_time", "nano_time", "connect_timestamp",
                        "debezium_timestamp", "micro_timestamp", "nano_timestamp"),
                List.of(
                        "date", "date", "time(3)", "time(3)", "time(3)", "time(3)",
                        "timestamp(6)", "timestamp(6)", "timestamp(6)", "timestamp(6)"));
        Schema rowSchema = SchemaBuilder.struct()
                .field("connect_date", connectDate)
                .field("debezium_date", debeziumDate)
                .field("connect_time", connectTime)
                .field("debezium_time", debeziumTime)
                .field("micro_time", debeziumMicroTime)
                .field("nano_time", debeziumNanoTime)
                .field("connect_timestamp", connectTimestamp)
                .field("debezium_timestamp", debeziumTimestamp)
                .field("micro_timestamp", debeziumMicroTimestamp)
                .field("nano_timestamp", debeziumNanoTimestamp)
                .build();
        long connectTimestampMillis = 1_700_000_000_123L;
        long debeziumTimestampMillis = -1_234_567_890L;
        long microTimestamp = 1_700_000_000_123_456L;
        long nanoTimestamp = 1_700_000_000_123_456_000L;
        Struct row = new Struct(rowSchema)
                .put("connect_date",
                        org.apache.kafka.connect.data.Date.toLogical(connectDate, 20_000))
                .put("debezium_date", -1_234)
                .put("connect_time",
                        org.apache.kafka.connect.data.Time.toLogical(connectTime, 45_678_901))
                .put("debezium_time", 1_234)
                .put("micro_time", 12_345_000L)
                .put("nano_time", 67_890_000_000L)
                .put("connect_timestamp",
                        org.apache.kafka.connect.data.Timestamp.toLogical(
                                connectTimestamp, connectTimestampMillis))
                .put("debezium_timestamp", debeziumTimestampMillis)
                .put("micro_timestamp", microTimestamp)
                .put("nano_timestamp", nanoTimestamp);

        SinkProto.RowValue value = parse(typeDescription, row);

        assertArrayEquals(intBytes(20_000), bytes(value, 0));
        assertArrayEquals(intBytes(-1_234), bytes(value, 1));
        assertArrayEquals(intBytes(45_678_901), bytes(value, 2));
        assertArrayEquals(intBytes(1_234), bytes(value, 3));
        assertArrayEquals(intBytes(12_345), bytes(value, 4));
        assertArrayEquals(intBytes(67_890), bytes(value, 5));
        assertArrayEquals(
                longBytes(Math.multiplyExact(connectTimestampMillis, 1_000L)),
                bytes(value, 6));
        assertArrayEquals(
                longBytes(Math.multiplyExact(debeziumTimestampMillis, 1_000L)),
                bytes(value, 7));
        assertArrayEquals(longBytes(microTimestamp), bytes(value, 8));
        assertArrayEquals(longBytes(nanoTimestamp / 1_000L), bytes(value, 9));
    }

    @Test
    void shouldRejectLossyNumericAndTemporalValues() throws Exception
    {
        assertThrows(IllegalArgumentException.class,
                () -> parseSingle("tinyint", Schema.INT16_SCHEMA, (short) 128));
        assertThrows(IllegalArgumentException.class,
                () -> parseSingle("boolean", Schema.INT16_SCHEMA, (short) 2));

        Schema unsignedBigint = Decimal.builder(0)
                .parameter("connect.decimal.precision", "20")
                .build();
        assertArrayEquals(
                longBytes(Long.MAX_VALUE),
                bytes(parseSingle(
                        "bigint", unsignedBigint, new BigDecimal("9223372036854775807")), 0));
        IllegalArgumentException unsignedOverflow = assertThrows(
                IllegalArgumentException.class,
                () -> parseSingle(
                        "bigint", unsignedBigint, new BigDecimal("9223372036854775808")));
        assertTrue(unsignedOverflow.getMessage().contains("signed 64-bit range"));

        Schema scaleThree = Decimal.builder(3)
                .parameter("connect.decimal.precision", "6")
                .build();
        assertThrows(IllegalArgumentException.class,
                () -> parseSingle("decimal(5,2)", scaleThree, new BigDecimal("1.234")));
        assertThrows(IllegalArgumentException.class,
                () -> parseSingle("decimal(5,2)", scaleThree, new BigDecimal("1234.000")));

        assertThrows(IllegalArgumentException.class,
                () -> parseSingle(
                        "time(3)", logicalInt64("io.debezium.time.MicroTime"), 12_345_678L));
        assertThrows(IllegalArgumentException.class,
                () -> parseSingle(
                        "timestamp(3)",
                        logicalInt64("io.debezium.time.MicroTimestamp"),
                        1_700_000_000_123_456L));
        assertThrows(IllegalArgumentException.class,
                () -> parseSingle(
                        "timestamp(6)",
                        logicalInt64("io.debezium.time.NanoTimestamp"),
                        1_700_000_000_123_456_789L));
        assertThrows(UnsupportedOperationException.class,
                () -> parseSingle("date", Schema.INT32_SCHEMA, 20_000));
        assertThrows(IllegalArgumentException.class,
                () -> parseSingle("float", Schema.FLOAT64_SCHEMA, 0.1d));
    }

    @Test
    void shouldRejectUnsupportedLogicalAndComplexValues()
    {
        Schema zonedTimestamp = SchemaBuilder.string()
                .name("io.debezium.time.ZonedTimestamp")
                .build();
        Schema interval = SchemaBuilder.string()
                .name("io.debezium.time.Interval")
                .build();
        Schema variableScaleDecimal = SchemaBuilder.struct()
                .name("io.debezium.data.VariableScaleDecimal")
                .field("scale", Schema.INT32_SCHEMA)
                .field("value", Schema.BYTES_SCHEMA)
                .build();
        Struct variableDecimalValue = new Struct(variableScaleDecimal)
                .put("scale", 3)
                .put("value", new byte[]{0x01});
        Schema geometry = SchemaBuilder.struct()
                .name("io.debezium.data.geometry.Geometry")
                .field("wkb", Schema.BYTES_SCHEMA)
                .field("srid", Schema.OPTIONAL_INT32_SCHEMA)
                .build();
        Struct geometryValue = new Struct(geometry)
                .put("wkb", new byte[]{0x01, 0x02})
                .put("srid", 4326);
        Schema array = SchemaBuilder.array(Schema.INT32_SCHEMA).build();
        Schema struct = SchemaBuilder.struct()
                .field("nested", Schema.STRING_SCHEMA)
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> parseSingle(
                        "varchar(64)", zonedTimestamp, "2026-08-02T09:20:00Z"));
        assertThrows(UnsupportedOperationException.class,
                () -> parseSingle("varchar(64)", interval, "P1Y2M3DT4H5M6.7S"));
        assertThrows(UnsupportedOperationException.class,
                () -> parseSingle("decimal(10,3)", variableScaleDecimal, variableDecimalValue));
        assertThrows(UnsupportedOperationException.class,
                () -> parseSingle("varbinary(64)", geometry, geometryValue));
        assertThrows(UnsupportedOperationException.class,
                () -> parseSingle("varchar(64)", array, List.of(1, 2)));
        assertThrows(UnsupportedOperationException.class,
                () -> parseSingle(
                        "varchar(64)", struct,
                        new Struct(struct).put("nested", "value")));
    }

    private static SinkProto.RowValue parse(
            TypeDescription typeDescription, Struct row)
    {
        SinkProto.RowValue.Builder builder = SinkProto.RowValue.newBuilder();
        new DebeziumRowValueConverter(typeDescription).parse(row, builder);
        return builder.build();
    }

    private static SinkProto.RowValue parseSingle(
            String pixelsType, Schema connectSchema, Object value) throws Exception
    {
        TypeDescription typeDescription = TypeDescription.createSchemaFromStrings(
                List.of("value"), List.of(pixelsType));
        Schema rowSchema = SchemaBuilder.struct()
                .field("value", connectSchema)
                .build();
        return parse(typeDescription, new Struct(rowSchema).put("value", value));
    }

    private static Schema logicalInt32(String name)
    {
        return SchemaBuilder.int32().name(name).build();
    }

    private static Schema logicalInt64(String name)
    {
        return SchemaBuilder.int64().name(name).build();
    }

    private static byte[] bytes(SinkProto.RowValue rowValue, int index)
    {
        return rowValue.getValues(index).getValue().toByteArray();
    }

    private static TypeDescription pixelsType(String type) throws Exception
    {
        return TypeDescription.createSchemaFromStrings(List.of("value"), List.of(type))
                .getChildren().get(0);
    }

    private static Schema decimalSchema(int scale, int precision)
    {
        return Decimal.builder(scale)
                .parameter("connect.decimal.precision", Integer.toString(precision))
                .build();
    }

    private static byte[] intBytes(int value)
    {
        return ByteBuffer.allocate(Integer.BYTES).putInt(value).array();
    }

    private static byte[] longBytes(long value)
    {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }
}
